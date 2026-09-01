package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Inspection is one consistent, read-only view of a deployment and its
// operational state. Event payloads, secret values, and worker tokens are
// deliberately absent. Deployment state is retained only so the CLI can report
// its size and existing state revision metadata.
type Inspection struct {
	Deployment   Deployment
	Describe     json.RawMessage
	Health       DeploymentHealth
	Generation   *GenerationStatus
	Destinations []DestinationBinding
	Checkpoints  []CheckpointStatus
	Transaction  *TransactionStatus
	Outbox       map[string]int64
}

type CheckpointStatus struct {
	Source            string
	Size              int
	UpdatedGeneration int64
	UpdatedSequence   int64
	UpdatedAt         time.Time
}

type TransactionStatus struct {
	Generation  int64
	Sequence    int64
	CommittedAt time.Time
}

// InspectDeployment loads operator-visible deployment metadata in one SQLite
// read transaction so related status cannot be assembled from different
// moments in time.
func (s *Store) InspectDeployment(ctx context.Context, selector string) (Inspection, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Inspection{}, err
	}
	defer tx.Rollback()

	var view Inspection
	var state []byte
	var created, updated, retentionMS int64
	var expires, archived sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT d.id,d.name,d.info_name,d.source_dir,d.status,COALESCE(d.artifact_id,''),d.config_revision,d.config_hash,d.failure_threshold,d.max_events_per_transaction,d.event_retention_ms,d.active_generation,d.state,d.state_revision,d.created_at,d.updated_at,d.expires_at,d.archived_at,a.describe_json
		FROM deployments d JOIN artifacts a ON a.id=d.artifact_id
		WHERE d.id=? OR d.name=? ORDER BY CASE WHEN d.id=? THEN 0 ELSE 1 END LIMIT 1`, selector, selector, selector).
		Scan(&view.Deployment.ID, &view.Deployment.Name, &view.Deployment.InfoName, &view.Deployment.SourceDir,
			&view.Deployment.Status, &view.Deployment.ArtifactID, &view.Deployment.ConfigRevision,
			&view.Deployment.ConfigHash, &view.Deployment.FailureThreshold,
			&view.Deployment.MaxEventsPerTransaction, &retentionMS, &view.Deployment.ActiveGeneration,
			&state, &view.Deployment.StateRevision, &created, &updated,
			&expires, &archived, &view.Describe)
	if errors.Is(err, sql.ErrNoRows) {
		return Inspection{}, fmt.Errorf("deployment %q: %w", selector, ErrNotFound)
	}
	if err != nil {
		return Inspection{}, err
	}
	view.Deployment.State = append(json.RawMessage(nil), state...)
	view.Deployment.EventRetention = time.Duration(retentionMS) * time.Millisecond
	view.Deployment.CreatedAt = fromMs(created)
	view.Deployment.UpdatedAt = fromMs(updated)
	view.Deployment.ExpiresAt = nullTime(expires)
	view.Deployment.ArchivedAt = nullTime(archived)

	var lastRun, duration, lastSuccess, lastFailure sql.NullInt64
	var lastRunStatus sql.NullString
	var healthUpdated int64
	err = tx.QueryRowContext(ctx, `SELECT deployment_id,generation,status,consecutive_failures,last_run_status,last_run_at,last_duration_ms,last_success_at,last_failure_at,last_error,updated_at FROM deployment_health WHERE deployment_id=?`, view.Deployment.ID).
		Scan(&view.Health.DeploymentID, &view.Health.Generation, &view.Health.Status,
			&view.Health.ConsecutiveFailures, &lastRunStatus, &lastRun, &duration, &lastSuccess,
			&lastFailure, &view.Health.LastError, &healthUpdated)
	if err != nil {
		return Inspection{}, fmt.Errorf("inspect health: %w", err)
	}
	view.Health.LastRunStatus = lastRunStatus.String
	view.Health.LastRunAt = nullTime(lastRun)
	view.Health.LastSuccessAt = nullTime(lastSuccess)
	view.Health.LastFailureAt = nullTime(lastFailure)
	view.Health.UpdatedAt = fromMs(healthUpdated)
	if duration.Valid {
		value := time.Duration(duration.Int64) * time.Millisecond
		view.Health.LastDuration = &value
	}

	var generation GenerationStatus
	var started int64
	var ready, stopped sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT deployment_id,generation,status,started_at,ready_at,stopped_at,stop_reason,stop_error FROM deployment_generations WHERE deployment_id=? ORDER BY generation DESC LIMIT 1`, view.Deployment.ID).
		Scan(&generation.DeploymentID, &generation.Generation, &generation.Status, &started,
			&ready, &stopped, &generation.StopReason, &generation.StopError)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Inspection{}, fmt.Errorf("inspect generation: %w", err)
	}
	if err == nil {
		generation.StartedAt = fromMs(started)
		generation.ReadyAt = nullTime(ready)
		generation.StoppedAt = nullTime(stopped)
		view.Generation = &generation
	}

	rows, err := tx.QueryContext(ctx, `SELECT id,revision,deployment_id,config,created_at,retired_at FROM destination_bindings WHERE deployment_id=? AND retired_at IS NULL ORDER BY id`, view.Deployment.ID)
	if err != nil {
		return Inspection{}, fmt.Errorf("inspect destinations: %w", err)
	}
	for rows.Next() {
		var item DestinationBinding
		var itemCreated int64
		var retired sql.NullInt64
		if err = rows.Scan(&item.ID, &item.Revision, &item.DeploymentID, &item.Config, &itemCreated, &retired); err != nil {
			rows.Close()
			return Inspection{}, err
		}
		item.CreatedAt = fromMs(itemCreated)
		item.RetiredAt = nullTime(retired)
		view.Destinations = append(view.Destinations, item)
	}
	if err = rows.Close(); err != nil {
		return Inspection{}, err
	}

	rows, err = tx.QueryContext(ctx, `SELECT source,length(value),updated_generation,updated_seq,updated_at FROM checkpoints WHERE deployment_id=? ORDER BY source`, view.Deployment.ID)
	if err != nil {
		return Inspection{}, fmt.Errorf("inspect checkpoints: %w", err)
	}
	for rows.Next() {
		var item CheckpointStatus
		var updatedAt int64
		if err = rows.Scan(&item.Source, &item.Size, &item.UpdatedGeneration, &item.UpdatedSequence, &updatedAt); err != nil {
			rows.Close()
			return Inspection{}, err
		}
		item.UpdatedAt = fromMs(updatedAt)
		view.Checkpoints = append(view.Checkpoints, item)
	}
	if err = rows.Close(); err != nil {
		return Inspection{}, err
	}

	var transaction TransactionStatus
	var committed int64
	err = tx.QueryRowContext(ctx, `SELECT generation,seq,committed_at FROM transactions WHERE deployment_id=? ORDER BY committed_at DESC,generation DESC,seq DESC LIMIT 1`, view.Deployment.ID).
		Scan(&transaction.Generation, &transaction.Sequence, &committed)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Inspection{}, fmt.Errorf("inspect latest transaction: %w", err)
	}
	if err == nil {
		transaction.CommittedAt = fromMs(committed)
		view.Transaction = &transaction
	}

	view.Outbox = map[string]int64{"pending": 0, "sending": 0, "delivered": 0, "dead": 0}
	rows, err = tx.QueryContext(ctx, `SELECT status,count(*) FROM outbox_deliveries WHERE deployment_id=? GROUP BY status`, view.Deployment.ID)
	if err != nil {
		return Inspection{}, fmt.Errorf("inspect outbox: %w", err)
	}
	for rows.Next() {
		var status string
		var count int64
		if err = rows.Scan(&status, &count); err != nil {
			rows.Close()
			return Inspection{}, err
		}
		view.Outbox[status] = count
	}
	if err = rows.Close(); err != nil {
		return Inspection{}, err
	}
	if err = tx.Commit(); err != nil {
		return Inspection{}, err
	}
	return view, nil
}
