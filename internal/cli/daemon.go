package cli

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saucesteals/monitord/internal/daemon"
	"github.com/spf13/cobra"
)

func (c *CLI) newDaemonCmd() *cobra.Command {
	var interval time.Duration

	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Run the monitor scheduler",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.daemon(interval)
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", daemon.DefaultInterval, "maximum time to sleep while idle; scheduling itself is exact")

	return cmd
}

func (c *CLI) daemon(interval time.Duration) error {
	store, paths, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The daemon's operational log is stdout, unlike the CLI's, which uses
	// stderr to keep command output pipeable. That leaves stderr for genuine
	// process failures, so a non-empty error log is a real signal rather than
	// a copy of every INFO line.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	return daemon.New(store, paths, logger, interval).Run(ctx)
}
