package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/monitor"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

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

	built, err := monitor.Build(ctx, paths, req)
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
