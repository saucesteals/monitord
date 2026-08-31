package cli

import (
	"context"
	"fmt"
	"path/filepath"

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
		RunE: func(_ *cobra.Command, args []string) error {
			return c.newMonitor(args[0])
		},
	}
}

func (c *CLI) newMonitor(rawName string) error {
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
	if err := config.Tidy(context.Background(), paths); err != nil {
		return err
	}

	fmt.Printf("created %s\n", dir)
	fmt.Printf("edit %s and %s, then:\n", filepath.Join(dir, "monitor.go"), filepath.Join(dir, monitor.ConfigFileName))
	fmt.Printf("  monitord test %s\n", name)
	fmt.Printf("  monitord deploy %s\n", name)

	return nil
}
