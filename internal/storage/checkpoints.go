package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type RuntimeDeployment struct {
	Deployment
	ArtifactPath, ArtifactHash string
	Describe                   json.RawMessage
	Checkpoints                map[string]json.RawMessage
}

// RuntimeMetadata is the scheduler's payload-free reconciliation snapshot.
type RuntimeMetadata struct {
	ID, Name, SourceDir, ArtifactID  string
	ConfigRevision, ActiveGeneration int64
	Describe                         json.RawMessage
}

// ListRuntimeMetadata lists only launch identity and secret declarations, not
// mutable state or checkpoint payloads.
func (s *Store) ListRuntimeMetadata(ctx context.Context) ([]RuntimeMetadata, error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT d.id,d.name,d.source_dir,d.artifact_id,d.config_revision,d.active_generation,a.describe_json
 FROM deployments d JOIN artifacts a ON a.id=d.artifact_id
 WHERE d.status='active' AND (d.expires_at IS NULL OR d.expires_at>?) ORDER BY d.name`, toMs(time.Now()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []RuntimeMetadata
	for rows.Next() {
		var r RuntimeMetadata
		if err := rows.Scan(&r.ID, &r.Name, &r.SourceDir, &r.ArtifactID, &r.ConfigRevision, &r.ActiveGeneration, &r.Describe); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetRuntimeDeployment reads state, checkpoints and configuration from one short
// WAL snapshot. Activation revalidates this snapshot before advancing the fence.
func (s *Store) GetRuntimeDeployment(ctx context.Context, selector string) (RuntimeDeployment, error) {
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RuntimeDeployment{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var r RuntimeDeployment
	var created, updated, retention int64
	var expires, archived sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT d.id,d.name,d.info_name,d.source_dir,d.status,COALESCE(d.artifact_id,''),d.config_revision,d.config_hash,d.failure_threshold,d.max_events_per_transaction,d.event_retention_ms,d.active_generation,d.state,d.state_revision,d.created_at,d.updated_at,d.expires_at,d.archived_at,a.path,a.content_hash,a.describe_json
 FROM deployments d JOIN artifacts a ON a.id=d.artifact_id WHERE d.id=? OR d.name=? ORDER BY CASE WHEN d.id=? THEN 0 ELSE 1 END LIMIT 1`, selector, selector, selector).
		Scan(&r.ID, &r.Name, &r.InfoName, &r.SourceDir, &r.Status, &r.ArtifactID, &r.ConfigRevision, &r.ConfigHash, &r.FailureThreshold, &r.MaxEventsPerTransaction, &retention, &r.ActiveGeneration, &r.State, &r.StateRevision, &created, &updated, &expires, &archived, &r.ArtifactPath, &r.ArtifactHash, &r.Describe)
	if errors.Is(err, sql.ErrNoRows) {
		return RuntimeDeployment{}, ErrNotFound
	}
	if err != nil {
		return RuntimeDeployment{}, err
	}
	r.EventRetention = time.Duration(retention) * time.Millisecond
	r.CreatedAt = fromMs(created)
	r.UpdatedAt = fromMs(updated)
	r.ExpiresAt = nullTime(expires)
	r.ArchivedAt = nullTime(archived)
	rows, err := tx.QueryContext(ctx, `SELECT source,value FROM checkpoints WHERE deployment_id=? ORDER BY source`, r.ID)
	if err != nil {
		return RuntimeDeployment{}, err
	}
	r.Checkpoints = make(map[string]json.RawMessage)
	for rows.Next() {
		var source string
		var value json.RawMessage
		if err := rows.Scan(&source, &value); err != nil {
			_ = rows.Close()
			return RuntimeDeployment{}, err
		}
		r.Checkpoints[source] = value
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return RuntimeDeployment{}, err
	}
	if err = tx.Commit(); err != nil {
		return RuntimeDeployment{}, err
	}
	return r, nil
}

// ClearCheckpoints removes all durable source progress for an inactive
// deployment. It also reasserts the generation fence so recovery remains safe
// if the stored lifecycle metadata was inconsistent.
func (s *Store) ClearCheckpoints(ctx context.Context, deploymentID string) (int64, error) {
	if deploymentID == "" {
		return 0, errors.New("checkpoint clear requires a deployment")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin checkpoint clear: %w", err)
	}
	defer tx.Rollback()

	now := toMs(time.Now().UTC())
	result, err := tx.ExecContext(ctx, `UPDATE deployments SET active_generation=0,updated_at=? WHERE id=? AND status='inactive'`, now, deploymentID)
	if err != nil {
		return 0, fmt.Errorf("fence deployment for checkpoint clear: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect checkpoint clear deployment: %w", err)
	}
	if rows != 1 {
		var exists int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM deployments WHERE id=?`, deploymentID).Scan(&exists); err != nil {
			return 0, fmt.Errorf("inspect checkpoint clear deployment: %w", err)
		}
		if exists == 0 {
			return 0, ErrNotFound
		}
		return 0, ErrInvalidStatus
	}

	if _, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=COALESCE(retired_at,?),stopped_at=COALESCE(stopped_at,?),stop_reason=CASE WHEN stop_reason='' THEN 'checkpoints cleared' ELSE stop_reason END WHERE deployment_id=? AND status='active'`, now, now, deploymentID); err != nil {
		return 0, fmt.Errorf("retire generation for checkpoint clear: %w", err)
	}
	if err = ensureDeploymentHealth(ctx, tx, deploymentID, 0, "stopped", now); err != nil {
		return 0, fmt.Errorf("reconcile health for checkpoint clear: %w", err)
	}

	result, err = tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE deployment_id=?`, deploymentID)
	if err != nil {
		return 0, fmt.Errorf("clear checkpoints: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect cleared checkpoints: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit checkpoint clear: %w", err)
	}

	return count, nil
}
