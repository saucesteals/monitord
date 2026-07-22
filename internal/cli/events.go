package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/saucesteals/monitord/internal/model"
	"github.com/spf13/cobra"
)

// events lists the events a monitor has emitted, newest first.
func (c *CLI) newEventsCmd() *cobra.Command {
	var limit int
	var failed bool
	var since time.Duration

	cmd := &cobra.Command{
		Use:   "events NAME",
		Short: "List a monitor's emitted events",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.events(args[0], limit, failed, since)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 20, "how many events to list")
	cmd.Flags().BoolVar(&failed, "failed", false, "only show failed deliveries")
	cmd.Flags().DurationVar(&since, "since", 0, "only events within this window, e.g. 24h")

	return cmd
}

func (c *CLI) events(rawName string, limit int, failed bool, since time.Duration) error {
	name, err := model.ParseMonitorName(rawName)
	if err != nil {
		return err
	}

	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	var window time.Time
	if since > 0 {
		window = time.Now().Add(-since)
	}
	events, err := store.ListEvents(context.Background(), name, limit, failed, window)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		fmt.Printf("%s: no events\n", name)

		return nil
	}

	for _, e := range events {
		mark := "✓"
		if !e.Delivered {
			mark = "✗"
		}
		title := e.Title
		if title == "" {
			title = "(no title)"
		}
		fmt.Printf("%s %-20s %s\n", mark, e.SentAt.Local().Format("Jan 02 15:04:05"), truncate(title, 80))
		if e.URL != "" {
			fmt.Printf("   %s\n", e.URL)
		}
		if !e.Delivered && e.Error != "" {
			fmt.Printf("   error: %s\n", truncate(e.Error, 100))
		}
	}

	return nil
}
