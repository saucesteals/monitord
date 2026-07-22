package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	monitor "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/spf13/cobra"
)

// runs lists a monitor's recent runs, or shows one run in full.
func (c *CLI) newRunsCmd() *cobra.Command {
	var limit int
	var runID string
	var failed bool

	cmd := &cobra.Command{
		Use:   "runs NAME",
		Short: "List runs or show one run",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.runs(args[0], limit, runID, failed)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 20, "how many runs to list")
	cmd.Flags().StringVar(&runID, "run", "", "show one run in full")
	cmd.Flags().BoolVar(&failed, "failed", false, "only show failed runs")

	return cmd
}

func (c *CLI) runs(rawName string, limit int, runID string, failed bool) error {
	name, err := model.ParseMonitorName(rawName)
	if err != nil {
		return err
	}

	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if runID != "" {
		return c.showRun(ctx, runID)
	}

	m, err := store.GetMonitor(ctx, name)
	if err != nil {
		return err
	}
	events, err := store.ListRuns(ctx, name, limit)
	if err != nil {
		return err
	}

	health := "no runs yet"
	if m.TotalRuns > 0 {
		health = fmt.Sprintf("%d runs, %d failed (%.0f%%)",
			m.TotalRuns, m.TotalFailures, float64(m.TotalFailures)/float64(m.TotalRuns)*100)
	}
	fmt.Printf("%s: %s", m.Name, health)
	if m.ConsecutiveFailures > 0 {
		fmt.Printf(", %d consecutive failures", m.ConsecutiveFailures)
	}
	fmt.Println()

	for _, run := range events {
		if failed && run.Status != monitor.StatusFailure {
			continue
		}
		summary := run.Error
		if summary == "" {
			summary = resultSummary(run.Stdout)
		}
		fmt.Printf("%-8s %-20s %8s  %s\n",
			run.Status,
			run.StartedAt.Local().Format("Jan 02 15:04:05"),
			run.FinishedAt.Sub(run.StartedAt).Round(time.Millisecond),
			truncate(summary, 80))
		fmt.Printf("         %s\n", run.ID)
	}

	return nil
}

// showRun prints everything recorded about a single run.
func (c *CLI) showRun(ctx context.Context, runID string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	run, err := store.GetRun(ctx, runID)
	if err != nil {
		return err
	}

	fmt.Printf("run:      %s\n", run.ID)
	fmt.Printf("monitor:  %s\n", run.MonitorName)
	fmt.Printf("status:   %s\n", run.Status)
	fmt.Printf("started:  %s\n", run.StartedAt.Local().Format(time.RFC3339))
	fmt.Printf("duration: %s\n", run.FinishedAt.Sub(run.StartedAt).Round(time.Millisecond))
	fmt.Printf("notified: %t%s\n", run.NotificationSent, suffix(run.NotificationError))
	if run.Error != "" {
		fmt.Printf("error:    %s\n", run.Error)
	}

	if stream := strings.TrimSpace(run.Stdout); stream != "" {
		fmt.Println("\nstream:")
		printStream(stream)
	}
	if stderr := strings.TrimSpace(run.Stderr); stderr != "" {
		fmt.Println("\nstderr:")
		fmt.Println(indent(stderr))
	}

	return nil
}

// printStream renders captured NDJSON worker output as readable lines.
func printStream(stream string) {
	for _, line := range strings.Split(stream, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var msg monitor.Outbound
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			fmt.Printf("  %s\n", line)

			continue
		}

		switch msg.Type {
		case monitor.OutboundLog:
			fmt.Printf("  [%s] %s\n", msg.Log.Level, msg.Log.Message)
		case monitor.OutboundEvent:
			fmt.Printf("  [event/%s] %s: %s\n", msg.Event.Severity, msg.Event.Title, msg.Event.Summary)
		case monitor.OutboundResult:
			fmt.Printf("  [result/%s] %s\n", msg.Result.Status, msg.Result.Summary)
			if msg.Result.Details != "" {
				fmt.Printf("%s\n", indent(msg.Result.Details))
			}
		}
	}
}

// resultSummary pulls the result summary out of a captured stream.
func resultSummary(stream string) string {
	for _, line := range strings.Split(stream, "\n") {
		var msg monitor.Outbound
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}
		if msg.Type == monitor.OutboundResult {
			return msg.Result.Summary
		}
	}

	return ""
}

func truncate(value string, limit int) string {
	value = strings.ReplaceAll(strings.TrimSpace(value), "\n", " ")
	if len(value) <= limit {
		return value
	}

	return value[:limit-1] + "…"
}

func indent(value string) string {
	lines := strings.Split(value, "\n")
	for i, line := range lines {
		lines[i] = "    " + line
	}

	return strings.Join(lines, "\n")
}

func suffix(value string) string {
	if value == "" {
		return ""
	}

	return " (" + value + ")"
}
