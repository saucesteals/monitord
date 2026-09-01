package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List deployed monitors",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return c.list(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

func (c *CLI) list(ctx context.Context, out io.Writer) error {
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
		fmt.Fprintln(out, "no monitors deployed")

		return nil
	}

	fmt.Fprintf(out, "%-24s %-34s %-10s %-20s\n", "NAME", "ID", "STATUS", "EXPIRES")
	for _, m := range monitors {
		fmt.Fprintf(out, "%-24s %-34s %-10s %-20s\n", m.Name, m.ID, m.Status, formatTime(m.ExpiresAt))
	}

	return nil
}

func (c *CLI) newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect NAME",
		Short: "Show monitor details",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.inspect(cmd.Context(), cmd.OutOrStdout(), args[0])
		},
	}
}

func (c *CLI) inspect(ctx context.Context, out io.Writer, selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	m, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "name: %s\n", m.Name)
	fmt.Fprintf(out, "id: %s\nimplementation: %s\n", m.ID, m.InfoName)
	fmt.Fprintf(out, "status: %s\n", m.Status)
	fmt.Fprintf(out, "created: %s\n", m.CreatedAt.Format(time.RFC3339))
	fmt.Fprintf(out, "updated: %s\n", m.UpdatedAt.Format(time.RFC3339))
	fmt.Fprintf(out, "expires: %s\n", formatTime(m.ExpiresAt))
	fmt.Fprintf(out, "artifact: %s\n", m.ArtifactID)
	fmt.Fprintf(out, "source: %s\n", m.SourceDir)
	fmt.Fprintf(out, "generation: %d\nconfig_revision: %d\n", m.ActiveGeneration, m.ConfigRevision)
	fmt.Fprintf(out, "state_version: %d\n", m.StateVersion)
	fmt.Fprintf(out, "state_revision: %d\n", m.StateRevision)
	fmt.Fprintf(out, "state: %d bytes (use `monitord state get %s`)\n", len(m.State), m.Name)

	return nil
}
