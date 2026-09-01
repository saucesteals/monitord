package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newEventsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "events", Short: "Inspect durable event deliveries", Args: noArgs, RunE: func(_ *cobra.Command, _ []string) error {
		return requireSubcommand("events", "list, retry")
	}}
	cmd.AddCommand(c.newEventsListCmd(), c.newEventsRetryCmd())
	return cmd
}

func (c *CLI) newEventsListCmd() *cobra.Command {
	var limit int
	var dead bool
	cmd := &cobra.Command{Use: "list NAME_OR_ID", Short: "List event deliveries", Args: exactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return c.listEvents(cmd.Context(), cmd.OutOrStdout(), args[0], limit, dead)
	}}
	cmd.Flags().IntVar(&limit, "limit", 20, "maximum rows")
	cmd.Flags().BoolVar(&dead, "dead", false, "only dead-letter deliveries")
	return cmd
}

func (c *CLI) listEvents(ctx context.Context, out io.Writer, selector string, limit int, dead bool) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	deployment, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}
	rows, err := store.ListOutboxHistory(ctx, deployment.ID, limit, dead)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Fprintf(out, "%s: no event deliveries\n", deployment.Name)
		return nil
	}
	for _, row := range rows {
		fmt.Fprintf(out, "%-10s %-20s %-18s %s/%s\n", row.Status, row.CreatedAt.Local().Format("Jan 02 15:04:05"), row.EventID, row.OutboxID, row.DestinationID)
		if row.LastError != "" {
			fmt.Fprintf(out, "  error: %s\n", truncate(row.LastError, 100))
		}
	}
	return nil
}

func (c *CLI) newEventsRetryCmd() *cobra.Command {
	return &cobra.Command{Use: "retry OUTBOX_ID DESTINATION_ID", Short: "Retry a dead-letter delivery", Args: exactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		store, _, err := c.store()
		if err != nil {
			return err
		}
		defer func() { _ = store.Close() }()
		if err := store.RetryDeadDelivery(cmd.Context(), args[0], args[1], time.Now().UTC()); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "queued retry %s/%s\n", args[0], args[1])
		return nil
	}}
}
