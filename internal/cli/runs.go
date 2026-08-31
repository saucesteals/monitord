package cli

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

func (c *CLI) newRunsCmd() *cobra.Command {
	var limit int
	var failed bool
	cmd := &cobra.Command{Use: "runs NAME_OR_ID", Short: "List deployment runs", Args: exactArgs(1), RunE: func(_ *cobra.Command, args []string) error { return c.runs(args[0], limit, failed) }}
	cmd.Flags().IntVar(&limit, "limit", 20, "maximum rows")
	cmd.Flags().BoolVar(&failed, "failed", false, "only failed runs")
	return cmd
}
func (c *CLI) runs(selector string, limit int, failed bool) error {
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
	rows, err := store.ListDeploymentRuns(ctx, d.ID, limit, failed)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Printf("%s: no runs\n", d.Name)
		return nil
	}
	for _, r := range rows {
		duration := "-"
		if r.FinishedAt != nil {
			duration = r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond).String()
		}
		detail := r.Error
		if detail == "" {
			detail = r.Summary
		}
		fmt.Printf("%-9s %-16s %-10s %s  %s\n", r.Status, r.Child, duration, r.StartedAt.Local().Format("Jan 02 15:04:05"), truncate(detail, 80))
		fmt.Printf("          %s generation=%d\n", r.ID, r.Generation)
	}
	return nil
}
func truncate(value string, limit int) string {
	value = strings.ReplaceAll(strings.TrimSpace(value), "\n", " ")
	if len(value) <= limit {
		return value
	}
	return value[:limit-1] + "…"
}
