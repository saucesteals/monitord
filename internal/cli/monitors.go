package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	cmd := &cobra.Command{
		Use:   "deploy [PATH]",
		Short: "Build and deploy a monitor",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			target := "."
			if len(args) == 1 {
				target = args[0]
			}
			return c.deploy(target, name)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "deployment name (defaults to source directory)")
	return cmd
}

func (c *CLI) deploy(target, overrideName string) error {
	store, paths, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	dir, monitorName, err := monitor.ResolveDir(paths, target)
	if err != nil {
		return err
	}
	if overrideName != "" {
		monitorName, err = model.ParseMonitorName(overrideName)
		if err != nil {
			return err
		}
	}
	monitorConfig, err := monitor.LoadConfig(dir)
	if err != nil {
		return err
	}

	ctx := context.Background()
	existing, existingErr := store.GetDeployment(ctx, monitorName.String())
	req := monitor.Request{
		Dir:    dir,
		Name:   monitorName,
		Config: monitorConfig,
	}
	if existingErr == nil {
		req.Current = existing.State
		req.CurrentVersion = existing.StateVersion
	}
	for _, delivery := range monitorConfig.Deliveries {
		if delivery.Discord != nil {
			continue
		}
		route, err := store.GetRoute(ctx, delivery.Route)
		if err != nil {
			return err
		}
		if err := routes.ValidateMonitor(route.Kind, delivery.Options); err != nil {
			return fmt.Errorf("route %s: %w", delivery.Route, err)
		}
	}

	if monitorConfig.ProxyPool != "" {
		// Resolved now so a missing pool fails the deploy, not the first tick.
		if _, err := store.GetProxyPool(ctx, monitorConfig.ProxyPool); err != nil {
			return err
		}
	}

	built, err := monitor.BuildV5(ctx, paths, req)
	if err != nil {
		return err
	}
	artifact, err := store.PutArtifact(ctx, built.Artifact)
	if err != nil {
		return err
	}
	configRaw, err := json.Marshal(monitorConfig)
	if err != nil {
		return err
	}
	configHash := fmt.Sprintf("%x", sha256.Sum256(configRaw))
	var expires *time.Time
	if !monitorConfig.Persistent {
		v := time.Now().UTC().Add(monitorConfig.TTL)
		expires = &v
	}
	var deployed storage.Deployment
	if existingErr == nil {
		deployed, err = store.Redeploy(ctx, existing.ID, built.Description.Info.Name, dir, artifact.ID, configHash, expires)
	} else if errors.Is(existingErr, storage.ErrNotFound) {
		deployed, err = store.CreateDeployment(ctx, storage.CreateDeployment{Name: monitorName.String(), InfoName: built.Description.Info.Name, SourceDir: dir, ArtifactID: artifact.ID, ConfigHash: configHash, State: built.State, StateVersion: built.Description.StateVersion, ExpiresAt: expires})
	} else {
		return existingErr
	}
	if err != nil {
		return err
	}
	for i, delivery := range monitorConfig.Deliveries {
		raw, _ := json.Marshal(delivery)
		if _, err = store.PutDestinationBinding(ctx, deployed.ID, fmt.Sprintf("destination-%d", i+1), raw); err != nil {
			return err
		}
	}
	if err = store.RetireDestinationBindingsExcept(ctx, deployed.ID, len(monitorConfig.Deliveries)); err != nil {
		return err
	}
	fmt.Printf("deployed %s (%s)\n", deployed.Name, deployed.ID)
	fmt.Printf("source   %s\nartifact %s\nplan     %s\n", dir, artifact.Path, built.Description.Plan.Kind)
	for _, delivery := range monitorConfig.Deliveries {
		fmt.Printf("delivery %s\n", truncate(delivery.Describe(), 80))
	}
	if req.CurrentVersion != 0 && req.CurrentVersion != built.Description.StateVersion {
		fmt.Printf("state migrated v%d -> v%d\n", req.CurrentVersion, built.Description.StateVersion)
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
	fmt.Printf("edit %s and %s, then:\n", filepath.Join(dir, "monitor.go"), filepath.Join(dir, monitor.ConfigFileName))
	fmt.Printf("  monitord test %s\n", name)
	fmt.Printf("  monitord deploy %s\n", name)

	return nil
}

// rm deletes a monitor, its runs, and its artifacts.
func (c *CLI) newRemoveCmd() *cobra.Command {
	var purge bool
	var force bool

	cmd := &cobra.Command{
		Use:     "rm NAME",
		Aliases: []string{"remove"},
		Short:   "Delete a monitor",
		Args:    exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.rm(args[0], purge, force)
		},
	}
	cmd.Flags().BoolVar(&purge, "purge", false, "permanently delete archived deployment data")
	cmd.Flags().BoolVar(&force, "force", false, "allow purge when queued deliveries remain")

	return cmd
}

func (c *CLI) rm(selector string, purge, force bool) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	d, err := store.GetDeployment(context.Background(), selector)
	if err != nil {
		return err
	}
	if !purge {
		if d.Status != "archived" {
			err = store.ArchiveDeployment(context.Background(), d.ID)
		}
		if err == nil {
			fmt.Printf("archived %s (%s)\n", d.Name, d.ID)
		}
		return err
	}
	if d.Status != "archived" {
		return errors.New("archive deployment before purging")
	}
	if err = store.PurgeDeploymentSafe(context.Background(), d.ID, force); err != nil {
		if strings.Contains(err.Error(), "queued deliveries") {
			return errors.New("queued deliveries remain; retry later or use --force")
		}
		return err
	}
	fmt.Printf("purged %s (%s)\n", d.Name, d.ID)
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

	monitors, err := store.ListDeployments(context.Background())
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
		RunE: func(_ *cobra.Command, args []string) error {
			return c.inspect(args[0])
		},
	}
}

func (c *CLI) inspect(selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	m, err := store.GetDeployment(context.Background(), selector)
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

func (c *CLI) newResumeCmd() *cobra.Command {
	var ttl time.Duration
	cmd := &cobra.Command{Use: "resume NAME_OR_ID", Short: "Resume an expired deployment", Args: exactArgs(1), RunE: func(_ *cobra.Command, args []string) error {
		if ttl <= 0 {
			return errors.New("--ttl must be positive")
		}
		store, _, err := c.store()
		if err != nil {
			return err
		}
		defer store.Close()
		d, err := store.GetDeployment(context.Background(), args[0])
		if err != nil {
			return err
		}
		expires := time.Now().UTC().Add(ttl)
		if err = store.ResumeDeployment(context.Background(), d.ID, &expires); err != nil {
			return err
		}
		fmt.Printf("resumed %s (%s) until %s\n", d.Name, d.ID, expires.Format(time.RFC3339))
		return nil
	}}
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "fresh deployment lifetime")
	_ = cmd.MarkFlagRequired("ttl")
	return cmd
}

func (c *CLI) expire(rawName string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	d, err := store.GetDeployment(context.Background(), rawName)
	if err != nil {
		return err
	}
	if err := store.ExpireDeployment(context.Background(), d.ID); err != nil {
		return err
	}
	fmt.Printf("expired %s (%s)\n", d.Name, d.ID)

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

func deliveryNames(deliveries []routes.Delivery) string {
	names := make([]string, 0, len(deliveries))
	for _, delivery := range deliveries {
		names = append(names, delivery.Describe())
	}

	return strings.Join(names, ",")
}
