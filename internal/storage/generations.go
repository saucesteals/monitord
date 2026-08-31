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
	SecretFingerprint []byte
}

type ActiveGeneration struct {
	DeploymentID string
	Generation   int64
	WorkerToken  []byte
}

func hashToken(token []byte) []byte {
	sum := sha256.Sum256(token)
	return sum[:]
}

func (s *Store) generationAuthorized(ctx context.Context, id string, generation int64, token []byte) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deployments d JOIN deployment_generations g ON g.deployment_id=d.id AND g.generation=d.active_generation WHERE d.id=? AND d.status='active' AND g.generation=? AND g.status='active' AND g.worker_token_hash=?`, id, generation, hashToken(token)).Scan(&count)
	return count == 1, err
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

	var activeGeneration, configRevision int64
	err = tx.QueryRowContext(ctx, `
		SELECT active_generation, config_revision
		FROM deployments
		WHERE id = ? AND status = 'active'`, activation.DeploymentID).
		Scan(&activeGeneration, &configRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return ActiveGeneration{}, fmt.Errorf("active deployment %q not found", activation.DeploymentID)
	}
	if err != nil {
		return ActiveGeneration{}, fmt.Errorf("load deployment generation: %w", err)
	}
	if configRevision != activation.ConfigRevision {
		return ActiveGeneration{}, fmt.Errorf("config revision changed: got %d, current %d", activation.ConfigRevision, configRevision)
	}

	now := time.Now().UTC()
	if activeGeneration > 0 {
		result, err := tx.ExecContext(ctx, `
			UPDATE deployment_generations
			SET status = 'retired', retired_at = ?
			WHERE deployment_id = ? AND generation = ? AND status = 'active'`,
			toMs(now), activation.DeploymentID, activeGeneration)
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
		SET active_generation = ?, artifact_id = ?, updated_at = ?
		WHERE id = ? AND active_generation = ? AND config_revision = ?`,
		nextGeneration, activation.ArtifactID, toMs(now), activation.DeploymentID,
		activeGeneration, activation.ConfigRevision)
	if err != nil {
		return ActiveGeneration{}, fmt.Errorf("advance deployment generation: %w", err)
	}
	if err := requireOneRow(result, "advance deployment generation"); err != nil {
		return ActiveGeneration{}, err
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
