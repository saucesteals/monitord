package cli

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/daemon"
	"github.com/spf13/cobra"
)

func (c *CLI) newDaemonCmd() *cobra.Command {
	var interval time.Duration

	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Run the monitor scheduler",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return c.daemon(cmd.Context(), cmd.OutOrStdout(), interval)
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", daemon.DefaultInterval, "maximum time to sleep while idle; scheduling itself is exact")

	return cmd
}

func (c *CLI) daemon(parent context.Context, out io.Writer, interval time.Duration) error {
	// The daemon's operational log is stdout, unlike the CLI's, which uses
	// stderr to keep command output pipeable. That leaves stderr for genuine
	// process failures, so a non-empty error log is a real signal rather than
	// a copy of every INFO line.
	logger := slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelInfo}))
	paths, err := config.Resolve(c.root)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	return daemon.New(paths, logger, interval).Run(ctx)
}
