package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type GenerationActivation struct {
	DeploymentID      string
	ArtifactID        string
	ConfigRevision    int64
	StateRevision     int64
	SecretFingerprint []byte
}

type ActiveGeneration struct {
	DeploymentID string
	Generation   int64
	WorkerToken  []byte
}

// RecoverGenerations fences workers left behind by a previous daemon process.
// The daemon lock guarantees no healthy peer can own these generations.
func (s *Store) RecoverGenerations(ctx context.Context, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := toMs(at)
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=COALESCE(retired_at,?),stopped_at=COALESCE(stopped_at,?),stop_reason=CASE WHEN stop_reason='' THEN 'daemon restarted' ELSE stop_reason END WHERE status='active'`, now, now); err != nil {
		return fmt.Errorf("retire orphan generations: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployments SET active_generation=0,updated_at=? WHERE status='active' AND active_generation>0`, now); err != nil {
		return fmt.Errorf("fence orphan generations: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_health SET generation=0,status='starting',updated_at=? WHERE deployment_id IN (SELECT id FROM deployments WHERE status='active')`, now); err != nil {
		return fmt.Errorf("reset recovered deployment health: %w", err)
	}
	return tx.Commit()
}

// ActivateGeneration advances the deployment fence before its worker is started.
// The returned token is the only plaintext copy; SQLite retains only its hash.
func (s *Store) ActivateGeneration(ctx context.Context, activation GenerationActivation) (ActiveGeneration, error) {
	if activation.DeploymentID == "" || activation.ArtifactID == "" || activation.ConfigRevision < 1 {
		return ActiveGeneration{}, errors.New("generation activation requires deployment, artifact, and config revision")
	}
	if len(activation.SecretFingerprint) == 0 {
		return ActiveGeneration{}, errors.New("generation activation requires a secret fingerprint")
	}

	workerToken := make([]byte, 32)
	if _, err := rand.Read(workerToken); err != nil {
		return ActiveGeneration{}, fmt.Errorf("generate worker token: %w", err)
	}
	workerTokenHash := sha256.Sum256(workerToken)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ActiveGeneration{}, fmt.Errorf("begin generation activation: %w", err)
	}
	defer tx.Rollback()

	var artifactID string
	var activeGeneration, configRevision, stateRevision int64
	err = tx.QueryRowContext(ctx, `
		SELECT artifact_id, active_generation, config_revision, state_revision
		FROM deployments
		WHERE id = ? AND status = 'active'`, activation.DeploymentID).
		Scan(&artifactID, &activeGeneration, &configRevision, &stateRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return ActiveGeneration{}, fmt.Errorf("active deployment %q not found", activation.DeploymentID)
	}
	if err != nil {
		return ActiveGeneration{}, fmt.Errorf("load deployment generation: %w", err)
	}
	if configRevision != activation.ConfigRevision {
		return ActiveGeneration{}, fmt.Errorf("config revision changed: got %d, current %d", activation.ConfigRevision, configRevision)
	}
	if artifactID != activation.ArtifactID || stateRevision != activation.StateRevision {
		return ActiveGeneration{}, errors.New("deployment snapshot changed during generation activation")
	}

	now := time.Now().UTC()
	if activeGeneration > 0 {
		result, err := tx.ExecContext(ctx, `
			UPDATE deployment_generations
			SET status = 'retired', retired_at = ?, stopped_at=COALESCE(stopped_at,?),
			    stop_reason=CASE WHEN stop_reason='' THEN 'generation replaced' ELSE stop_reason END
			WHERE deployment_id = ? AND generation = ? AND status = 'active'`,
			toMs(now), toMs(now), activation.DeploymentID, activeGeneration)
		if err != nil {
			return ActiveGeneration{}, fmt.Errorf("retire active generation: %w", err)
		}
		// A manual state replacement may already have fenced this generation.
		_, _ = result.RowsAffected()
	}

	var maxGeneration int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(generation),0) FROM deployment_generations WHERE deployment_id=?`, activation.DeploymentID).Scan(&maxGeneration); err != nil {
		return ActiveGeneration{}, fmt.Errorf("load latest generation: %w", err)
	}
	nextGeneration := maxGeneration + 1
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO deployment_generations (
			deployment_id, generation, worker_token_hash, artifact_id,
			config_revision, secret_fingerprint, status, started_at
		) VALUES (?, ?, ?, ?, ?, ?, 'active', ?)`, activation.DeploymentID,
		nextGeneration, workerTokenHash[:], activation.ArtifactID,
		activation.ConfigRevision, activation.SecretFingerprint, toMs(now)); err != nil {
		return ActiveGeneration{}, fmt.Errorf("insert active generation: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE deployments
		SET active_generation = ?, updated_at = ?
		WHERE id = ? AND artifact_id = ? AND active_generation = ? AND config_revision = ? AND state_revision = ?`,
		nextGeneration, toMs(now), activation.DeploymentID, activation.ArtifactID,
		activeGeneration, activation.ConfigRevision, activation.StateRevision)
	if err != nil {
		return ActiveGeneration{}, fmt.Errorf("advance deployment generation: %w", err)
	}
	if err := requireOneRow(result, "advance deployment generation"); err != nil {
		return ActiveGeneration{}, err
	}
	if err := ensureDeploymentHealth(ctx, tx, activation.DeploymentID, nextGeneration, "starting", toMs(now)); err != nil {
		return ActiveGeneration{}, fmt.Errorf("initialize generation health: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return ActiveGeneration{}, fmt.Errorf("commit generation activation: %w", err)
	}
	return ActiveGeneration{
		DeploymentID: activation.DeploymentID,
		Generation:   nextGeneration,
		WorkerToken:  workerToken,
	}, nil
}

func requireOneRow(result sql.Result, operation string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: inspect affected rows: %w", operation, err)
	}
	if rows != 1 {
		return fmt.Errorf("%s: concurrent mutation", operation)
	}
	return nil
}
