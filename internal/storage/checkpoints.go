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

func (s *Store) GetRuntimeDeployment(ctx context.Context, selector string) (RuntimeDeployment, error) {
	d, err := s.GetDeployment(ctx, selector)
	if err != nil {
		return RuntimeDeployment{}, err
	}
	var r RuntimeDeployment
	r.Deployment = d
	err = s.db.QueryRowContext(ctx, `SELECT path,content_hash,describe_json FROM artifacts WHERE id=?`, d.ArtifactID).Scan(&r.ArtifactPath, &r.ArtifactHash, &r.Describe)
	return r, err
}

func (s *Store) ListRuntimeDeployments(ctx context.Context) ([]RuntimeDeployment, error) {
	now := toMs(time.Now().UTC())
	rows, err := s.db.QueryContext(ctx, `SELECT d.id,d.name,d.info_name,d.source_dir,d.status,COALESCE(d.artifact_id,''),d.config_revision,d.config_hash,d.failure_threshold,d.max_events_per_transaction,d.event_retention_ms,d.active_generation,d.state,d.state_revision,d.created_at,d.updated_at,d.expires_at,d.archived_at,a.path,a.content_hash,a.describe_json FROM deployments d JOIN artifacts a ON a.id=d.artifact_id WHERE d.status='active' AND (d.expires_at IS NULL OR d.expires_at>?) ORDER BY d.name`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RuntimeDeployment
	for rows.Next() {
		var r RuntimeDeployment
		var state []byte
		var created, updated int64
		var retentionMS int64
		var expires, archived sql.NullInt64
		if err = rows.Scan(&r.ID, &r.Name, &r.InfoName, &r.SourceDir, &r.Status, &r.ArtifactID, &r.ConfigRevision, &r.ConfigHash, &r.FailureThreshold, &r.MaxEventsPerTransaction, &retentionMS, &r.ActiveGeneration, &state, &r.StateRevision, &created, &updated, &expires, &archived, &r.ArtifactPath, &r.ArtifactHash, &r.Describe); err != nil {
			return nil, err
		}
		r.State = append(json.RawMessage(nil), state...)
		r.EventRetention = time.Duration(retentionMS) * time.Millisecond
		r.CreatedAt = fromMs(created)
		r.UpdatedAt = fromMs(updated)
		r.ExpiresAt = nullTime(expires)
		r.ArchivedAt = nullTime(archived)
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	byID := make(map[string]*RuntimeDeployment, len(out))
	for i := range out {
		out[i].Checkpoints = map[string]json.RawMessage{}
		byID[out[i].ID] = &out[i]
	}
	checkpointRows, err := s.db.QueryContext(ctx, `SELECT c.deployment_id,c.source,c.value FROM checkpoints c JOIN deployments d ON d.id=c.deployment_id WHERE d.status='active' AND (d.expires_at IS NULL OR d.expires_at>?) ORDER BY c.deployment_id,c.source`, now)
	if err != nil {
		return nil, err
	}
	defer checkpointRows.Close()
	for checkpointRows.Next() {
		var deploymentID, source string
		var value []byte
		if err = checkpointRows.Scan(&deploymentID, &source, &value); err != nil {
			return nil, err
		}
		if deployment := byID[deploymentID]; deployment != nil {
			deployment.Checkpoints[source] = append(json.RawMessage(nil), value...)
		}
	}
	return out, checkpointRows.Err()
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
