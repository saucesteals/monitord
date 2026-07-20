package cli

import (
	"context"
	"errors"
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
	var webhookURL string
	var mention string

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

			return routeCreate(store, args[0], args[1], webhookURL, mention)
		},
	}
	cmd.Flags().StringVar(&webhookURL, "webhook-url", "", "discord webhook URL")
	cmd.Flags().StringVar(&mention, "mention", "", "who to ping: user:ID, role:ID, here, everyone (comma separated)")

	return cmd
}

func routeCreate(store *storage.Store, rawKind string, rawName string, webhookURL string, mention string) error {
	if webhookURL == "" {
		return errors.New("--webhook-url is required")
	}
	mentions, err := routes.ParseMentions(mention)
	if err != nil {
		return err
	}

	kind, err := model.ParseRouteKind(rawKind)
	if err != nil {
		return err
	}
	name, err := model.NewRouteName(kind, rawName)
	if err != nil {
		return err
	}

	route := storage.Route{
		Name:       name,
		Kind:       kind,
		Target:     rawName,
		WebhookURL: webhookURL,
		Mentions:   mentions,
	}
	if err := store.UpsertRoute(context.Background(), route); err != nil {
		return err
	}

	fmt.Printf("route %s created (%s)\n", route.Name, routes.RedactURL(route.WebhookURL))
	if len(mentions) > 0 {
		fmt.Printf("pings %s\n", routes.FormatMentions(mentions))
	}

	return nil
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
		fmt.Printf("%-28s %-8s pings=%-24s %s\n",
			item.Name, item.Kind, orDash(routes.FormatMentions(item.Mentions)), routes.RedactURL(item.WebhookURL))
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
	if err := routes.SendDiscord(context.Background(), route.WebhookURL, routes.Message{
		Title:   "monitord route test",
		Summary: "test notification from monitord",
	}, route.Mentions); err != nil {
		return err
	}

	fmt.Printf("sent test notification to %s\n", route.Name)

	return nil
}
