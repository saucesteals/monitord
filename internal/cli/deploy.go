package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
		RunE: func(cmd *cobra.Command, args []string) error {
			target := "."
			if len(args) == 1 {
				target = args[0]
			}
			return c.deploy(cmd.Context(), cmd.OutOrStdout(), target, name)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "deployment name (defaults to source directory)")
	return cmd
}

func (c *CLI) deploy(ctx context.Context, out io.Writer, target, overrideName string) error {
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

	existing, existingErr := store.GetDeployment(ctx, monitorName.String())
	if existingErr == nil && existing.Status == "archived" {
		return errors.New("archived deployments cannot be redeployed; purge it or choose another name")
	}
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
	var expectedStateRevision *int64
	if existingErr == nil {
		expectedStateRevision = &existing.StateRevision
	}
	if existingErr != nil && !errors.Is(existingErr, storage.ErrNotFound) {
		return existingErr
	}
	destinations := make([]json.RawMessage, 0, len(monitorConfig.Deliveries))
	for i, delivery := range monitorConfig.Deliveries {
		raw, marshalErr := json.Marshal(delivery)
		if marshalErr != nil {
			return fmt.Errorf("encode destination %d: %w", i+1, marshalErr)
		}
		destinations = append(destinations, raw)
	}
	deployed, err := store.Deploy(ctx, storage.DeployInput{
		Name: monitorName.String(), InfoName: built.Description.Info.Name,
		SourceDir: dir, Artifact: built.Artifact, ConfigHash: configHash,
		State: built.State, StateVersion: built.Description.StateVersion,
		ExpiresAt: expires, Destinations: destinations,
		ExpectedStateRevision: expectedStateRevision,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "deployed %s (%s)\n", deployed.Name, deployed.ID)
	fmt.Fprintf(out, "source   %s\nartifact %s\nplan     %s\n", dir, built.Artifact.Path, built.Description.Plan.Kind)
	for _, delivery := range monitorConfig.Deliveries {
		fmt.Fprintf(out, "delivery %s\n", truncate(delivery.Describe(), 80))
	}
	if req.CurrentVersion != 0 && req.CurrentVersion != built.Description.StateVersion {
		fmt.Fprintf(out, "state migrated v%d -> v%d\n", req.CurrentVersion, built.Description.StateVersion)
	}

	return nil
}
