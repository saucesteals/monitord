package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/secrets"
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

	fmt.Fprintf(out, "%-24s %-34s %-10s %-10s %-8s %-20s\n", "NAME", "ID", "DEPLOYMENT", "HEALTH", "FAILURES", "EXPIRES")
	for _, m := range monitors {
		view, inspectErr := store.InspectDeployment(ctx, m.ID)
		if inspectErr != nil {
			return inspectErr
		}
		fmt.Fprintf(out, "%-24s %-34s %-10s %-10s %-8d %-20s\n", m.Name, m.ID, m.Status, view.Health.Status, view.Health.ConsecutiveFailures, formatTime(m.ExpiresAt))
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
	store, paths, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	view, err := store.InspectDeployment(ctx, selector)
	if err != nil {
		return err
	}
	m := view.Deployment
	var described monitord.MonitorFrame
	if err = json.Unmarshal(view.Describe, &described); err != nil {
		return fmt.Errorf("decode monitor description: %w", err)
	}

	refs := described.Plan.SecretRefs()
	requested := make([]secrets.Ref, 0, len(refs))
	for _, ref := range refs {
		// Availability inspection must not fail merely because a required value
		// is absent; resolving as optional also gives us values used to redact
		// any stored operational error.
		requested = append(requested, secrets.Ref{Group: ref.Group, Key: ref.Key})
	}
	values, err := secrets.Resolve(requested, secrets.Sources{Root: paths.Root, MonitorDir: m.SourceDir})
	secretSourceError := err
	available := make(map[string]bool, len(values))
	for _, value := range values {
		available[secrets.RefKey(value.Ref.Group, value.Ref.Key)] = true
	}
	lastError := truncate(secrets.Redact(view.Health.LastError, values), 240)

	fmt.Fprintf(out, "name: %s\n", m.Name)
	fmt.Fprintf(out, "id: %s\nimplementation: %s\n", m.ID, m.InfoName)
	fmt.Fprintf(out, "deployment: %s\n", m.Status)
	fmt.Fprintf(out, "health: %s (%d/%d consecutive failures)\n", view.Health.Status, view.Health.ConsecutiveFailures, m.FailureThreshold)
	fmt.Fprintf(out, "last_run: %s at %s\n", orDash(view.Health.LastRunStatus), formatTime(view.Health.LastRunAt))
	fmt.Fprintf(out, "last_duration: %s\n", formatDuration(view.Health.LastDuration))
	fmt.Fprintf(out, "last_success: %s\n", formatTime(view.Health.LastSuccessAt))
	fmt.Fprintf(out, "last_failure: %s\n", formatTime(view.Health.LastFailureAt))
	fmt.Fprintf(out, "last_error: %s\n", orDash(lastError))

	fmt.Fprintf(out, "plan: %s", described.Plan.Kind)
	if described.Plan.Kind == "every" {
		fmt.Fprintf(out, " %s", described.Plan.Interval)
	}
	fmt.Fprintln(out)
	if described.Plan.Timeout > 0 {
		fmt.Fprintf(out, "timeout: %s\n", described.Plan.Timeout)
	} else {
		fmt.Fprintln(out, "timeout: none")
	}
	fmt.Fprintf(out, "events: max %d per transaction, retain %s\n", m.MaxEventsPerTransaction, m.EventRetention)

	if view.Generation == nil {
		fmt.Fprintln(out, "generation: none")
	} else {
		generation := view.Generation
		active := generation.Generation == m.ActiveGeneration && generation.Status == "active"
		fmt.Fprintf(out, "generation: %d (%s, active=%t)\n", generation.Generation, generation.Status, active)
		fmt.Fprintf(out, "generation_started: %s\n", generation.StartedAt.Format(time.RFC3339))
		fmt.Fprintf(out, "generation_ready: %s\n", formatTime(generation.ReadyAt))
		fmt.Fprintf(out, "generation_stopped: %s\n", formatTime(generation.StoppedAt))
		if generation.StopReason != "" {
			fmt.Fprintf(out, "generation_stop_reason: %s\n", generation.StopReason)
		}
		if generation.StopError != "" {
			fmt.Fprintf(out, "generation_stop_error: %s\n", truncate(secrets.Redact(generation.StopError, values), 240))
		}
	}

	fmt.Fprintf(out, "created: %s\n", m.CreatedAt.Format(time.RFC3339))
	fmt.Fprintf(out, "updated: %s\n", m.UpdatedAt.Format(time.RFC3339))
	fmt.Fprintf(out, "expires: %s\n", formatTime(m.ExpiresAt))
	fmt.Fprintf(out, "artifact: %s\n", m.ArtifactID)
	fmt.Fprintf(out, "source: %s\n", m.SourceDir)
	fmt.Fprintf(out, "config_revision: %d\n", m.ConfigRevision)
	fmt.Fprintf(out, "state: %d bytes (use `monitord state get %s`)\n", len(m.State), m.Name)

	fmt.Fprintln(out, "secrets:")
	if secretSourceError != nil {
		fmt.Fprintf(out, "  source_error: %s\n", truncate(secretSourceError.Error(), 240))
	}
	if len(refs) == 0 {
		fmt.Fprintln(out, "  none")
	}
	for _, ref := range refs {
		status := "missing"
		if secretSourceError != nil {
			status = "unknown"
		} else if available[secrets.RefKey(ref.Group, ref.Key)] {
			status = "available"
		}
		requirement := "optional"
		if ref.Required {
			requirement = "required"
		}
		fmt.Fprintf(out, "  %s/%s: %s (%s)\n", ref.Group, ref.Key, status, requirement)
	}

	fmt.Fprintln(out, "destinations:")
	if len(view.Destinations) == 0 {
		fmt.Fprintln(out, "  none")
	}
	for _, binding := range view.Destinations {
		var delivery routes.Delivery
		if err = json.Unmarshal(binding.Config, &delivery); err != nil {
			return fmt.Errorf("decode destination %s: %w", binding.ID, err)
		}
		fmt.Fprintf(out, "  %s@%d: %s\n", binding.ID, binding.Revision, delivery.Describe())
	}

	fmt.Fprintln(out, "checkpoints:")
	if len(view.Checkpoints) == 0 {
		fmt.Fprintln(out, "  none")
	}
	for _, checkpoint := range view.Checkpoints {
		fmt.Fprintf(out, "  %s: %d bytes, generation %d transaction %d, updated %s\n", checkpoint.Source, checkpoint.Size, checkpoint.UpdatedGeneration, checkpoint.UpdatedSequence, checkpoint.UpdatedAt.Format(time.RFC3339))
	}

	if view.Transaction == nil {
		fmt.Fprintln(out, "latest_transaction: none")
	} else {
		tx := view.Transaction
		fmt.Fprintf(out, "latest_transaction: generation %d sequence %d, committed %s\n", tx.Generation, tx.Sequence, tx.CommittedAt.Format(time.RFC3339))
	}
	fmt.Fprint(out, "outbox:")
	for _, status := range []string{"pending", "sending", "delivered", "dead"} {
		fmt.Fprintf(out, " %s=%d", status, view.Outbox[status])
	}
	fmt.Fprintln(out)

	return nil
}

func formatDuration(value *time.Duration) string {
	if value == nil {
		return "-"
	}
	return value.String()
}
