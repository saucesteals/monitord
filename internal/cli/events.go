package cli

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"time"
)

func (c *CLI) newEventsCmd() *cobra.Command {
	var limit int
	var dead bool
	var retry string
	cmd := &cobra.Command{Use: "events NAME_OR_ID", Short: "Inspect durable event deliveries", Args: exactArgs(1), RunE: func(_ *cobra.Command, args []string) error { return c.events(args[0], limit, dead, retry) }}
	cmd.Flags().IntVar(&limit, "limit", 20, "maximum rows")
	cmd.Flags().BoolVar(&dead, "dead", false, "only dead-letter deliveries")
	cmd.Flags().StringVar(&retry, "retry", "", "retry OUTBOX_ID/DESTINATION_ID")
	return cmd
}
func (c *CLI) events(selector string, limit int, dead bool, retry string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()
	d, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}
	if retry != "" {
		outbox, dest, ok := cutRetry(retry)
		if !ok {
			return fmt.Errorf("--retry must be OUTBOX_ID/DESTINATION_ID")
		}
		if err = store.RetryDeadDelivery(ctx, outbox, dest, time.Now().UTC()); err != nil {
			return err
		}
		fmt.Printf("queued retry %s/%s\n", outbox, dest)
		return nil
	}
	rows, err := store.ListOutboxHistory(ctx, d.ID, limit, dead)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Printf("%s: no event deliveries\n", d.Name)
		return nil
	}
	for _, v := range rows {
		fmt.Printf("%-10s %-20s %-18s %s/%s\n", v.Status, v.CreatedAt.Local().Format("Jan 02 15:04:05"), v.EventID, v.OutboxID, v.DestinationID)
		if v.LastError != "" {
			fmt.Printf("  error: %s\n", truncate(v.LastError, 100))
		}
	}
	return nil
}
func cutRetry(v string) (string, string, bool) {
	for i := len(v) - 1; i >= 0; i-- {
		if v[i] == '/' && i > 0 && i < len(v)-1 {
			return v[:i], v[i+1:], true
		}
	}
	return "", "", false
}
