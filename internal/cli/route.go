package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

func (c *CLI) newRouteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "route",
		Short: "Manage agent delivery routes",
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

func (c *CLI) newRouteCreateCmd() *cobra.Command {
	var options []string

	cmd := &cobra.Command{
		Use:   "create openclaw NAME",
		Short: "Create or update an OpenClaw agent route",
		Args:  exactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.createOpenClawRoute(cmd.Context(), cmd.OutOrStdout(), args[0], args[1], options)
		},
	}
	cmd.Flags().StringArrayVar(&options, "option", nil, "route setting as key=value (repeatable)")

	return cmd
}

func (c *CLI) createOpenClawRoute(ctx context.Context, out io.Writer, rawKind string, rawName string, values []string) error {
	if rawKind != "openclaw" {
		return fmt.Errorf("only openclaw agent routes are supported")
	}

	kind, err := model.ParseRouteKind(rawKind)
	if err != nil {
		return err
	}
	name, err := model.NewRouteName(kind, rawName)
	if err != nil {
		return err
	}
	options, err := readRouteOptions(values)
	if err != nil {
		return err
	}
	options, err = routes.PrepareRoute(kind, options)
	if err != nil {
		return err
	}

	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	if err := store.UpsertRoute(ctx, storage.Route{
		Name:    name,
		Kind:    kind,
		Options: options,
	}); err != nil {
		return err
	}

	fmt.Fprintf(out, "agent route %s created\n", name)

	return nil
}

func (c *CLI) newRouteListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List agent routes",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := c.store()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			items, err := store.ListRoutes(cmd.Context())
			if err != nil {
				return err
			}
			for _, item := range items {
				description, err := routes.DescribeRoute(item.Kind, item.Options)
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%-28s %s\n", item.Name, description)
			}

			return nil
		},
	}
}

func (c *CLI) newRouteTestCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "test ROUTE",
		Short: "Send an agent route test",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, err := model.ParseRouteName(args[0])
			if err != nil {
				return err
			}
			store, _, err := c.store()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			route, err := store.GetRoute(cmd.Context(), name)
			if err != nil {
				return err
			}
			if err := routes.Test(cmd.Context(), route.Kind, route.Options, routes.Message{
				Title:   "monitord route test",
				Summary: "test notification from monitord",
			}); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "sent test notification to %s\n", route.Name)

			return nil
		},
	}
}

func readRouteOptions(values []string) (routes.Options, error) {
	options := make(routes.Options, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		key = routes.NormalizeOptionKey(key)
		if !ok || key == "" {
			return nil, errors.New("route options must use key=value")
		}
		if _, exists := options[key]; exists {
			return nil, fmt.Errorf("route option %q was provided more than once", key)
		}
		options[key] = value
	}

	return options, nil
}
