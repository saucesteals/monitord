package cli

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	monitor "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

// driftWarning is how far the observed interval may stray from the configured
// one before it is called out. A scheduler that quietly runs at half rate looks
// perfectly healthy otherwise, so the number needs to be shown, not inferred.
const driftWarning = 0.10

// stats summarises a monitor's recent runs: how closely it is keeping to its
// schedule, and how long its ticks take.
func (c *CLI) newStatsCmd() *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "stats NAME",
		Short: "Summarize recent run timing",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.stats(args[0], limit)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 100, "how many recent runs to summarise")

	return cmd
}

func (c *CLI) stats(rawName string, limit int) error {
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
	m, err := store.GetMonitor(ctx, name)
	if err != nil {
		return err
	}
	runs, err := store.ListRuns(ctx, name, limit, false)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Printf("%s: no runs yet\n", name)

		return nil
	}

	// ListRuns is newest first; intervals read more naturally forwards.
	slices.Reverse(runs)
	printStats(m, runs)

	return nil
}

func printStats(m storage.Monitor, runs []storage.Run) {
	configured := time.Duration(m.IntervalSeconds) * time.Second

	fmt.Printf("%s — every %s", m.Name, configured)
	if m.ProxyPool != "" {
		fmt.Printf(", %d clients from pool %s", m.Definition.Clients, m.ProxyPool)
	}
	fmt.Printf("\n\n")

	span := runs[len(runs)-1].StartedAt.Sub(runs[0].StartedAt)
	failed := 0
	durations := make([]time.Duration, 0, len(runs))
	for _, run := range runs {
		durations = append(durations, run.FinishedAt.Sub(run.StartedAt))
		if run.Status == monitor.StatusFailure {
			failed++
		}
	}

	// Lifetime and window are both shown because they routinely disagree: the
	// window is where the percentiles come from, while `runs` and `list` report
	// lifetime. Printing only one invites comparing numbers with different
	// denominators.
	fmt.Printf("  lifetime   %d runs, %d failed%s\n",
		m.TotalRuns, m.TotalFailures, share(m.TotalFailures, m.TotalRuns))
	fmt.Printf("  window     %d runs over %s, %d failed%s\n",
		len(runs), round(span), failed, share(int64(failed), int64(len(runs))))
	if m.ConsecutiveFailures > 0 {
		fmt.Printf("             %d consecutive failures\n", m.ConsecutiveFailures)
	}
	fmt.Println()

	// Intervals need two runs to exist at all.
	intervals := make([]time.Duration, 0, len(runs))
	for i := 1; i < len(runs); i++ {
		intervals = append(intervals, runs[i].StartedAt.Sub(runs[i-1].StartedAt))
	}
	if len(intervals) > 0 {
		slices.Sort(intervals)
		median := percentile(intervals, 0.50)

		fmt.Printf("  interval   p50 %s    min %s    max %s\n",
			round(median), round(intervals[0]), round(intervals[len(intervals)-1]))

		if configured > 0 {
			drift := float64(median-configured) / float64(configured)
			note := ""
			if math.Abs(drift) > driftWarning {
				note = fmt.Sprintf("   <- not holding %s", configured)
			}
			fmt.Printf("             %+.1f%% vs configured%s\n", drift*100, note)
		}
	}

	slices.Sort(durations)
	fmt.Printf("  duration   p50 %s    p95 %s    max %s\n",
		round(percentile(durations, 0.50)),
		round(percentile(durations, 0.95)),
		round(durations[len(durations)-1]))
}

// share renders a failure rate, omitted when nothing has failed.
func share(failed, total int64) string {
	if failed == 0 || total == 0 {
		return ""
	}

	return fmt.Sprintf(" (%.0f%%)", float64(failed)/float64(total)*100)
}

// percentile returns the nearest-rank percentile of a sorted slice.
func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	i := int(math.Ceil(q*float64(len(sorted)))) - 1

	return sorted[max(0, min(i, len(sorted)-1))]
}

// round trims a duration to a readable precision for its magnitude.
func round(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return d.Round(time.Minute).String()
	case d >= time.Minute:
		return d.Round(time.Second).String()
	case d >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}
