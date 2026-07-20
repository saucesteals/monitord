package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/daemon"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/monitor"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

func (c *CLI) newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize the monitord root",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.init()
		},
	}
}

func (c *CLI) init() error {
	paths, err := config.Init(c.root)
	if err != nil {
		return err
	}
	store, err := storage.Open(paths.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	fmt.Printf("initialized %s\n", paths.Root)

	return nil
}

func (c *CLI) newDaemonCmd() *cobra.Command {
	var interval time.Duration
	var concurrency int

	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Run the monitor scheduler",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.daemon(interval, concurrency)
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", daemon.DefaultInterval, "maximum time to sleep while idle; scheduling itself is exact")
	cmd.Flags().IntVar(&concurrency, "concurrency", daemon.DefaultConcurrency, "maximum concurrent monitor ticks")

	return cmd
}

func (c *CLI) daemon(interval time.Duration, concurrency int) error {
	if concurrency <= 0 {
		return errors.New("--concurrency must be positive")
	}

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

	return daemon.New(store, paths, logger, interval, concurrency).Run(ctx)
}

func (c *CLI) newDeployCmd() *cobra.Command {
	var name string
	var every time.Duration
	var ttl time.Duration
	var route string
	var proxies string
	var timeout time.Duration
	var persistent bool
	var mention string

	cmd := &cobra.Command{
		Use:   "deploy NAME",
		Short: "Build and deploy a monitor",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.deploy(args[0], name, every, ttl, route, proxies, timeout, persistent, mention)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "monitor name")
	cmd.Flags().DurationVar(&every, "every", 0, "run interval")
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "monitor lifetime")
	cmd.Flags().StringVar(&route, "route", "", "notification route")
	cmd.Flags().StringVar(&proxies, "proxies", "", "proxy pool to draw clients from")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "per-tick timeout")
	cmd.Flags().BoolVar(&persistent, "persistent", false, "never expire this monitor")
	cmd.Flags().StringVar(&mention, "mention", "", "who this monitor pings, overriding the route: user:ID, role:ID, here, everyone, or none")

	return cmd
}

func (c *CLI) deploy(target string, name string, every time.Duration, ttl time.Duration, route string, proxies string, timeout time.Duration, persistent bool, mention string) error {
	store, paths, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	dir, monitorName, err := monitor.ResolveDir(paths, target)
	if err != nil {
		return err
	}
	if name != "" {
		if monitorName, err = model.ParseMonitorName(name); err != nil {
			return err
		}
	}

	ctx := context.Background()
	existing, existingErr := store.GetMonitor(ctx, monitorName)

	// A redeploy keeps whatever it is not told to change, so routine
	// "rebuild after an edit" needs no flags.
	req := monitor.Request{
		Dir:        dir,
		Name:       monitorName,
		Every:      every,
		TTL:        ttl,
		Timeout:    timeout,
		Persistent: persistent,
	}
	if existingErr == nil {
		req.Current = existing.State
		req.CurrentVersion = existing.StateVersion
		if req.Every == 0 {
			req.Every = time.Duration(existing.IntervalSeconds) * time.Second
		}
		if req.TTL == 0 && existing.TTLSeconds > 0 {
			req.TTL = time.Duration(existing.TTLSeconds) * time.Second
		}
		if existing.TTLSeconds == 0 {
			req.Persistent = true
		}
		if route == "" {
			req.Route = existing.Route
		}
		if proxies == "" {
			req.ProxyPool = existing.ProxyPool
		}
		if mention == "" {
			req.MentionOverride = existing.MentionOverride
		}
	}

	if route != "" {
		if req.Route, err = model.ParseRouteName(route); err != nil {
			return err
		}
	}
	if proxies != "" {
		if req.ProxyPool, err = model.ParsePoolName(proxies); err != nil {
			return err
		}
	}
	if mention != "" {
		// Validated now so a typo fails the deploy rather than the first alert.
		if _, err := routes.ResolveMentions(mention, nil); err != nil {
			return err
		}
		req.MentionOverride = mention
	}

	switch {
	case req.Every <= 0:
		return errors.New("--every is required")
	case !req.Persistent && req.TTL <= 0:
		return errors.New("--ttl is required unless --persistent is set")
	case req.Route == "":
		return errors.New("--route is required")
	case req.Timeout <= 0:
		return errors.New("--timeout must be positive")
	}

	if _, err := store.GetRoute(ctx, req.Route); err != nil {
		return err
	}
	if req.ProxyPool != "" {
		// Resolved now so a missing pool fails the deploy, not the first tick.
		if _, err := store.GetProxyPool(ctx, req.ProxyPool); err != nil {
			return err
		}
	}

	built, err := monitor.Build(ctx, paths, req)
	if err != nil {
		return err
	}
	if err := store.UpsertMonitor(ctx, built); err != nil {
		return err
	}
	if pruned, err := monitor.PruneArtifacts(paths, built.Name, filepath.Base(built.ArtifactDir)); err != nil {
		c.logger.Warn("artifact prune failed", "monitor", built.Name, "error", err)
	} else if pruned > 0 {
		fmt.Printf("pruned %d old artifact(s)\n", pruned)
	}

	fmt.Printf("deployed %s\n", built.Name)
	fmt.Printf("source   %s\n", built.SourceDir)
	fmt.Printf("artifact %s\n", built.ArtifactDir)
	fmt.Printf("schedule every %s, timeout %s, route %s\n",
		time.Duration(built.IntervalSeconds)*time.Second,
		time.Duration(built.TimeoutSeconds)*time.Second,
		built.Route)
	fmt.Printf("clients  %d", built.Definition.Clients)
	if built.ProxyPool != "" {
		fmt.Printf(" from pool %s", built.ProxyPool)
	}
	fmt.Println()
	if built.MentionOverride != "" {
		fmt.Printf("pings    %s (overriding route)\n", built.MentionOverride)
	}
	if req.CurrentVersion != 0 && req.CurrentVersion != built.StateVersion {
		fmt.Printf("state migrated v%d -> v%d\n", req.CurrentVersion, built.StateVersion)
	}

	return nil
}

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
	fmt.Printf("edit %s, then:\n", filepath.Join(dir, "monitor.go"))
	fmt.Printf("  monitord test %s\n", name)
	fmt.Printf("  monitord deploy %s --every 5m --ttl 24h --route discord:monitors\n", name)

	return nil
}

// rm deletes a monitor, its runs, and its artifacts.
func (c *CLI) newRemoveCmd() *cobra.Command {
	var purge bool

	cmd := &cobra.Command{
		Use:     "rm NAME",
		Aliases: []string{"remove"},
		Short:   "Delete a monitor",
		Args:    exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.rm(args[0], purge)
		},
	}
	cmd.Flags().BoolVar(&purge, "purge", false, "also delete the monitor source directory")

	return cmd
}

func (c *CLI) rm(rawName string, purge bool) error {
	name, err := model.ParseMonitorName(rawName)
	if err != nil {
		return err
	}

	store, paths, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// A monitor can exist on disk without ever having been deployed, so a
	// missing schedule is not fatal: the source still needs cleaning up.
	deployed := true
	if err := store.DeleteMonitor(context.Background(), name); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
		deployed = false
	}
	if err := monitor.RemoveArtifacts(paths, name); err != nil {
		return err
	}

	dir := paths.MonitorDir(name)
	_, dirErr := os.Stat(dir)
	onDisk := dirErr == nil

	if !deployed && !onDisk {
		return fmt.Errorf("monitor %s not found: no schedule and no directory at %s", name, dir)
	}
	if deployed {
		fmt.Printf("removed %s (schedule, runs, artifacts)\n", name)
	} else {
		fmt.Printf("%s was not deployed; removed any artifacts\n", name)
	}

	switch {
	case !onDisk:
		// Nothing to purge.
	case purge:
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("purge source: %w", err)
		}
		fmt.Printf("purged source %s\n", dir)
	default:
		fmt.Printf("source kept at %s\n", dir)
	}

	return nil
}

func (c *CLI) newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List deployed monitors",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.list()
		},
	}
}

func (c *CLI) list() error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	monitors, err := store.ListMonitors(context.Background())
	if err != nil {
		return err
	}
	if len(monitors) == 0 {
		fmt.Println("no monitors deployed")

		return nil
	}

	fmt.Printf("%-24s %-8s %-9s %-10s %-20s %s\n", "NAME", "STATUS", "HEALTH", "NEXT", "EXPIRES", "ROUTE")
	for _, m := range monitors {
		fmt.Printf("%-24s %-8s %-9s %-10s %-20s %s\n",
			m.Name, m.Status, health(m), until(m.NextDueAt), until(m.ExpiresAt), m.Route)
	}

	return nil
}

// health summarises a monitor's recent record for the list view.
func health(m storage.Monitor) string {
	switch {
	case m.TotalRuns == 0:
		return "-"
	case m.ConsecutiveFailures > 0:
		return fmt.Sprintf("fail x%d", m.ConsecutiveFailures)
	case m.TotalFailures > 0:
		return fmt.Sprintf("ok %.0f%%", float64(m.TotalRuns-m.TotalFailures)/float64(m.TotalRuns)*100)
	default:
		return "ok"
	}
}

// until renders a timestamp as a relative duration, which is what matters when
// scanning a schedule.
func until(t *time.Time) string {
	if t == nil {
		return "never"
	}

	d := time.Until(*t)
	if d < 0 {
		return "due"
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func (c *CLI) newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect NAME",
		Short: "Show full monitor details",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.inspect(args[0])
		},
	}
}

func (c *CLI) inspect(rawName string) error {
	name, err := model.ParseMonitorName(rawName)
	if err != nil {
		return err
	}

	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	m, err := store.GetMonitor(context.Background(), name)
	if err != nil {
		return err
	}

	fmt.Printf("name: %s\n", m.Name)
	fmt.Printf("status: %s\n", m.Status)
	fmt.Printf("route: %s\n", m.Route)
	fmt.Printf("proxies: %s\n", orDash(m.ProxyPool.String()))
	fmt.Printf("pings: %s\n", orDash(m.MentionOverride))
	fmt.Printf("clients: %d\n", m.Definition.Clients)
	fmt.Printf("every: %s\n", time.Duration(m.IntervalSeconds)*time.Second)
	fmt.Printf("timeout: %s\n", time.Duration(m.TimeoutSeconds)*time.Second)
	fmt.Printf("created: %s\n", formatTime(m.CreatedAt))
	fmt.Printf("updated: %s\n", formatTime(m.UpdatedAt))
	fmt.Printf("next_due: %s\n", formatTime(m.NextDueAt))
	fmt.Printf("expires: %s\n", formatTime(m.ExpiresAt))
	fmt.Printf("last_run: %s\n", formatTime(m.LastRunAt))
	fmt.Printf("artifact: %s\n", m.ArtifactDir)
	fmt.Printf("source: %s\n", m.SourceDir)
	fmt.Printf("last_status: %s\n", orDash(m.LastStatus.String()))
	fmt.Printf("consecutive_failures: %d\n", m.ConsecutiveFailures)
	fmt.Printf("total_runs: %d (%d failed)\n", m.TotalRuns, m.TotalFailures)
	fmt.Printf("state_version: %d\n", m.StateVersion)
	fmt.Printf("state_revision: %d\n", m.StateRevision)
	fmt.Printf("state: %s\n", indentJSON(m.State))

	return nil
}

func (c *CLI) newExpireCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "expire NAME",
		Short: "Stop scheduling a monitor",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.expire(args[0])
		},
	}
}

func (c *CLI) expire(rawName string) error {
	name, err := model.ParseMonitorName(rawName)
	if err != nil {
		return err
	}

	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	if err := store.ExpireMonitor(context.Background(), name, time.Now().UTC()); err != nil {
		return err
	}
	fmt.Printf("expired %s\n", name)

	return nil
}

func indentJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}

	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}

	return out.String()
}
