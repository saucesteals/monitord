package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/monitor"
	"github.com/spf13/cobra"
)

// newMonitor scaffolds a monitor source directory.
func (c *CLI) newNewMonitorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "new NAME",
		Short: "Scaffold a monitor",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.newMonitor(cmd.Context(), cmd.OutOrStdout(), args[0])
		},
	}
}

func (c *CLI) newMonitor(ctx context.Context, out io.Writer, rawName string) error {
	name, err := model.ParseMonitorName(rawName)
	if err != nil {
		return err
	}

	paths, err := config.Init(c.root)
	if err != nil {
		return err
	}
	dir, err := monitor.Scaffold(paths, name)
	if err != nil {
		return err
	}
	// Resolve the SDK now so the scaffold builds without any go.mod editing.
	if err := config.Tidy(ctx, paths); err != nil {
		return err
	}

	fmt.Fprintf(out, "created %s\n", dir)
	fmt.Fprintf(out, "edit the Go package and %s in %s, then:\n", monitor.ConfigFileName, dir)
	fmt.Fprintf(out, "  monitord test %s\n", name)
	fmt.Fprintf(out, "  monitord deploy %s\n", name)

	return nil
}
