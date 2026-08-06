// Package cli defines the cobra command tree for monitord.
package cli

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

// CLI holds shared state across all commands.
type CLI struct {
	root   string
	logger *slog.Logger
}

// New returns the root cobra command with all subcommands wired up.
func New() *cobra.Command {
	c := &CLI{
		logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}

	root := &cobra.Command{
		Use:           "monitord",
		Short:         "Persistent but expirable monitors",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&c.root, "root", "", "monitord root path")
	root.AddCommand(
		c.newInitCmd(),
		c.newDaemonCmd(),
		c.newRouteCmd(),
		c.newAccountCmd(),
		c.newProxyCmd(),
		c.newNewMonitorCmd(),
		c.newTestCmd(),
		c.newDeployCmd(),
		c.newRunsCmd(),
		c.newEventsCmd(),
		c.newStatsCmd(),
		c.newRemoveCmd(),
		c.newListCmd(),
		c.newInspectCmd(),
		c.newStateCmd(),
		c.newExpireCmd(),
		c.newSkillCmd(),
		c.newVersionCmd(),
	)

	return root
}

// Execute runs the root command and exits with a non-zero status on failure.
func Execute() {
	if err := New().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "monitord: %v\n", err)
		os.Exit(1)
	}
}

func (c *CLI) store() (*storage.Store, config.Paths, error) {
	paths, err := config.Resolve(c.root)
	if err != nil {
		return nil, config.Paths{}, err
	}
	store, err := storage.Open(paths.DBPath)
	if err != nil {
		return nil, config.Paths{}, err
	}

	return store, paths, nil
}

func exactArgs(n int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != n {
			return fmt.Errorf("%s expects %d positional argument(s), got %d", cmd.CommandPath(), n, len(args))
		}

		return nil
	}
}

func noArgs(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("%s expects no positional arguments, got %d", cmd.CommandPath(), len(args))
	}

	return nil
}

func formatTime(t *time.Time) string {
	if t == nil {
		return "-"
	}

	return t.Format(time.RFC3339)
}

func orDash(value string) string {
	if value == "" {
		return "-"
	}

	return value
}

func requireSubcommand(name string, choices string) error {
	return errors.New(name + " requires a subcommand: " + choices)
}
