package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List deployed monitors",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return c.list(cmd.Context())
		},
	}
}

func (c *CLI) list(ctx context.Context) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	monitors, err := store.ListDeployments(ctx)
	if err != nil {
		return err
	}
	if len(monitors) == 0 {
		fmt.Println("no monitors deployed")

		return nil
	}

	fmt.Printf("%-24s %-34s %-10s %-20s\n", "NAME", "ID", "STATUS", "EXPIRES")
	for _, m := range monitors {
		fmt.Printf("%-24s %-34s %-10s %-20s\n", m.Name, m.ID, m.Status, formatTime(m.ExpiresAt))
	}

	return nil
}

func (c *CLI) newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect NAME",
		Short: "Show monitor details",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.inspect(cmd.Context(), args[0])
		},
	}
}

func (c *CLI) inspect(ctx context.Context, selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	m, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}

	fmt.Printf("name: %s\n", m.Name)
	fmt.Printf("id: %s\nimplementation: %s\n", m.ID, m.InfoName)
	fmt.Printf("status: %s\n", m.Status)
	fmt.Printf("created: %s\n", m.CreatedAt.Format(time.RFC3339))
	fmt.Printf("updated: %s\n", m.UpdatedAt.Format(time.RFC3339))
	fmt.Printf("expires: %s\n", formatTime(m.ExpiresAt))
	fmt.Printf("artifact: %s\n", m.ArtifactID)
	fmt.Printf("source: %s\n", m.SourceDir)
	fmt.Printf("generation: %d\nconfig_revision: %d\n", m.ActiveGeneration, m.ConfigRevision)
	fmt.Printf("state_version: %d\n", m.StateVersion)
	fmt.Printf("state_revision: %d\n", m.StateRevision)
	fmt.Printf("state: %d bytes (use `monitord state get %s`)\n", len(m.State), m.Name)

	return nil
}
