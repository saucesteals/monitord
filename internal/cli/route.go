package cli

import (
	"context"
	"fmt"

	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

func (c *CLI) newRouteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "route",
		Short: "Manage notification routes",
		RunE: func(_ *cobra.Command, _ []string) error {
			return requireSubcommand("route", "create, list, test")
		},
	}
	cmd.AddCommand(
		c.newRouteCreateCmd(),
		c.newRouteListCmd(),
		c.newRouteTestCmd(),
	)

	return cmd
}

func (c *CLI) routeStore() (*storage.Store, error) {
	store, _, err := c.store()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (c *CLI) newRouteCreateCmd() *cobra.Command {
	var opts routeCreateOptions

	cmd := &cobra.Command{
		Use:   "create KIND NAME",
		Short: "Create or update a route",
		Args:  exactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			store, err := c.routeStore()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			return routeCreate(store, args[0], args[1], opts)
		},
	}
	cmd.Flags().StringArrayVar(&opts.Options, "option", nil, "route setting as key=value (repeatable)")
	cmd.Flags().StringArrayVar(&opts.OptionFiles, "option-file", nil, "route setting read from key=path (repeatable)")

	return cmd
}

type routeCreateOptions struct {
	Options     []string
	OptionFiles []string
}

func routeCreate(store *storage.Store, rawKind string, rawName string, opts routeCreateOptions) error {
	kind, err := model.ParseRouteKind(rawKind)
	if err != nil {
		return err
	}
	name, err := model.NewRouteName(kind, rawName)
	if err != nil {
		return err
	}

	route, err := buildRoute(name, kind, opts)
	if err != nil {
		return err
	}
	if err := store.UpsertRoute(context.Background(), route); err != nil {
		return err
	}

	description, err := routes.DescribeRoute(route.Kind, route.Options)
	if err != nil {
		return err
	}
	fmt.Printf("route %s created\n", route.Name)
	fmt.Printf("config %s\n", description)

	return nil
}

func buildRoute(name model.RouteName, kind model.RouteKind, opts routeCreateOptions) (storage.Route, error) {
	options, err := readRouteOptions(opts.Options, opts.OptionFiles)
	if err != nil {
		return storage.Route{}, err
	}
	options, err = routes.PrepareRoute(kind, options)
	if err != nil {
		return storage.Route{}, err
	}

	return storage.Route{Name: name, Kind: kind, Options: options}, nil
}

func (c *CLI) newRouteListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List routes",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			store, err := c.routeStore()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			return routeList(store)
		},
	}
}

func routeList(store *storage.Store) error {
	items, err := store.ListRoutes(context.Background())
	if err != nil {
		return err
	}
	for _, item := range items {
		description, err := routes.DescribeRoute(item.Kind, item.Options)
		if err != nil {
			return err
		}
		fmt.Printf("%-28s %-12s %s\n", item.Name, item.Kind, description)
	}

	return nil
}

func (c *CLI) newRouteTestCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "test ROUTE",
		Short: "Send a test notification",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			store, err := c.routeStore()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			return routeTest(store, args[0])
		},
	}
}

func routeTest(store *storage.Store, rawName string) error {
	name, err := model.ParseRouteName(rawName)
	if err != nil {
		return err
	}
	route, err := store.GetRoute(context.Background(), name)
	if err != nil {
		return err
	}
	msg := routes.Message{
		Title:   "monitord route test",
		Summary: "test notification from monitord",
	}
	if err := routes.Test(context.Background(), route.Kind, route.Options, msg); err != nil {
		return err
	}

	fmt.Printf("sent test notification to %s\n", route.Name)

	return nil
}
