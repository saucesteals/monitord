package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/saucesteals/monitord/internal/delivery"
)

type RunStatus string

const (
	RunSucceeded RunStatus = "success"
	RunFailed    RunStatus = "failure"
)

type RunOutcome struct {
	DeploymentID string
	Generation   int64
	Status       RunStatus
	Duration     time.Duration
	Error        string
	FinishedAt   time.Time
}

type HealthTransition string

const (
	HealthBecameUnhealthy HealthTransition = "unhealthy"
	HealthRecovered       HealthTransition = "recovered"
)

type DeploymentHealth struct {
	DeploymentID        string
	Generation          int64
	Status              string
	ConsecutiveFailures int
	LastRunStatus       string
	LastRunAt           *time.Time
	LastDuration        *time.Duration
	LastSuccessAt       *time.Time
	LastFailureAt       *time.Time
	LastError           string
	UpdatedAt           time.Time
}

type GenerationStatus struct {
	DeploymentID string
	Generation   int64
	Status       string
	StartedAt    time.Time
	ReadyAt      *time.Time
	StoppedAt    *time.Time
	StopReason   string
	StopError    string
}

func (s *Store) MarkGenerationReady(ctx context.Context, deploymentID string, generation int64, at time.Time) error {
	if deploymentID == "" || generation < 1 {
		return errors.New("generation readiness requires deployment and generation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE deployment_generations SET ready_at=? WHERE deployment_id=? AND generation=? AND status='active' AND ready_at IS NULL`, toMs(at), deploymentID, generation)
	if err != nil {
		return fmt.Errorf("mark generation ready: %w", err)
	}
	if err = requireOneRow(result, "mark generation ready"); err != nil {
		return err
	}
	healthResult, err := tx.ExecContext(ctx, `UPDATE deployment_health SET generation=?,status='starting',updated_at=? WHERE deployment_id=? AND generation=?`, generation, toMs(at), deploymentID, generation)
	if err != nil {
		return fmt.Errorf("mark deployment starting: %w", err)
	}
	if err = requireOneRow(healthResult, "mark deployment starting"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RecordRun(ctx context.Context, outcome RunOutcome) (HealthTransition, error) {
	outcome.Error = boundedText(outcome.Error, maxStoredErrorBytes)
	if outcome.DeploymentID == "" || outcome.Generation < 1 || outcome.Duration < 0 {
		return "", errors.New("run outcome requires deployment, generation, and non-negative duration")
	}
	if outcome.Status != RunSucceeded && outcome.Status != RunFailed {
		return "", fmt.Errorf("invalid run status %q", outcome.Status)
	}
	if outcome.Status != RunFailed && outcome.Error != "" {
		return "", errors.New("successful run outcome cannot contain an error")
	}
	if outcome.FinishedAt.IsZero() {
		outcome.FinishedAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var activeGeneration int64
	var threshold int
	var deploymentName string
	var previousFailures int
	var unhealthyNotified bool
	err = tx.QueryRowContext(ctx, `SELECT d.active_generation,d.failure_threshold,d.name,h.consecutive_failures,h.unhealthy_notified FROM deployments d JOIN deployment_health h ON h.deployment_id=d.id WHERE d.id=? AND d.status='active'`, outcome.DeploymentID).Scan(&activeGeneration, &threshold, &deploymentName, &previousFailures, &unhealthyNotified)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrGenerationFenced
	}
	if err != nil {
		return "", fmt.Errorf("load deployment health policy: %w", err)
	}
	if activeGeneration != outcome.Generation {
		return "", ErrGenerationFenced
	}
	var transition HealthTransition
	now := toMs(outcome.FinishedAt)
	duration := outcome.Duration.Milliseconds()
	if outcome.Status == RunSucceeded {
		result, updateErr := tx.ExecContext(ctx, `UPDATE deployment_health SET generation=?,status='healthy',consecutive_failures=0,last_run_status='success',last_run_at=?,last_duration_ms=?,last_success_at=?,last_error='',updated_at=? WHERE deployment_id=? AND generation=?`, outcome.Generation, now, duration, now, now, outcome.DeploymentID, outcome.Generation)
		if updateErr != nil {
			return "", fmt.Errorf("record successful run: %w", updateErr)
		}
		if err = requireOneRow(result, "record successful run"); err != nil {
			return "", err
		}
		if unhealthyNotified {
			if err = enqueueHealthNotification(ctx, tx, outcome.DeploymentID, deploymentName, outcome.Generation, "recovered", 0, outcome.FinishedAt); err != nil {
				return "", err
			}
			transition = HealthRecovered
		}
	} else {
		failures := previousFailures + 1
		result, updateErr := tx.ExecContext(ctx, `UPDATE deployment_health SET generation=?,status=CASE WHEN consecutive_failures+1>=? THEN 'unhealthy' ELSE 'failing' END,consecutive_failures=consecutive_failures+1,last_run_status='failure',last_run_at=?,last_duration_ms=?,last_failure_at=?,last_error=?,updated_at=? WHERE deployment_id=? AND generation=?`, outcome.Generation, threshold, now, duration, now, outcome.Error, now, outcome.DeploymentID, outcome.Generation)
		if updateErr != nil {
			return "", fmt.Errorf("record failed run: %w", updateErr)
		}
		if err = requireOneRow(result, "record failed run"); err != nil {
			return "", err
		}
		if !unhealthyNotified && failures >= threshold {
			if err = enqueueHealthNotification(ctx, tx, outcome.DeploymentID, deploymentName, outcome.Generation, "unhealthy", failures, outcome.FinishedAt); err != nil {
				return "", err
			}
			transition = HealthBecameUnhealthy
		}
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return transition, nil
}

// MarkGenerationStable makes a continuously running monitor healthy without
// inventing a successful poll or timestamp for work that never occurred.
func (s *Store) MarkGenerationStable(ctx context.Context, deploymentID string, generation int64, at time.Time) (HealthTransition, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var deploymentName string
	var unhealthyNotified bool
	err = tx.QueryRowContext(ctx, `SELECT d.name,h.unhealthy_notified FROM deployments d JOIN deployment_health h ON h.deployment_id=d.id WHERE d.id=? AND d.status='active' AND d.active_generation=? AND h.generation=? AND h.status='starting'`, deploymentID, generation, generation).Scan(&deploymentName, &unhealthyNotified)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrGenerationFenced
	}
	if err != nil {
		return "", fmt.Errorf("load continuous generation health: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE deployment_health SET status='healthy',consecutive_failures=0,last_error='',updated_at=? WHERE deployment_id=? AND generation=? AND status='starting'`, toMs(at), deploymentID, generation)
	if err != nil {
		return "", fmt.Errorf("mark continuous generation stable: %w", err)
	}
	if err = requireOneRow(result, "mark continuous generation stable"); err != nil {
		return "", err
	}
	if unhealthyNotified {
		if err = enqueueHealthNotification(ctx, tx, deploymentID, deploymentName, generation, "recovered", 0, at); err != nil {
			return "", err
		}
	}
	transition := HealthTransition("")
	if unhealthyNotified {
		transition = HealthRecovered
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return transition, nil
}

// RecordGenerationFailure counts a launch or runtime failure that occurred
// outside a monitor callback. It intentionally preserves the counter across
// replacement generations until a run succeeds.
func (s *Store) RecordGenerationFailure(ctx context.Context, deploymentID string, generation int64, failure string) (HealthTransition, error) {
	return s.recordOperationalFailure(ctx, deploymentID, generation, true, failure)
}

// RecordDeploymentFailure records an operational failure that prevented a
// worker generation from being created, such as unresolved required secrets.
func (s *Store) RecordDeploymentFailure(ctx context.Context, deploymentID, failure string) (HealthTransition, error) {
	return s.recordOperationalFailure(ctx, deploymentID, 0, false, failure)
}

func (s *Store) recordOperationalFailure(ctx context.Context, deploymentID string, generation int64, fenceGeneration bool, failure string) (HealthTransition, error) {
	failure = boundedText(failure, maxStoredErrorBytes)
	if deploymentID == "" || failure == "" || (fenceGeneration && generation < 1) {
		return "", errors.New("operational failure requires deployment and error")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var activeGeneration int64
	var threshold int
	var deploymentName string
	var previousFailures int
	var unhealthyNotified bool
	if err = tx.QueryRowContext(ctx, `SELECT d.active_generation,d.failure_threshold,d.name,h.consecutive_failures,h.unhealthy_notified FROM deployments d JOIN deployment_health h ON h.deployment_id=d.id WHERE d.id=? AND d.status='active'`, deploymentID).Scan(&activeGeneration, &threshold, &deploymentName, &previousFailures, &unhealthyNotified); errors.Is(err, sql.ErrNoRows) {
		return "", ErrGenerationFenced
	} else if err != nil {
		return "", fmt.Errorf("load deployment health policy: %w", err)
	}
	if fenceGeneration && activeGeneration != generation {
		return "", ErrGenerationFenced
	}
	healthGeneration := activeGeneration
	if !fenceGeneration {
		healthGeneration = 0
	}
	now := toMs(time.Now().UTC())
	result, err := tx.ExecContext(ctx, `UPDATE deployment_health SET generation=?,status=CASE WHEN consecutive_failures+1>=? THEN 'unhealthy' ELSE 'failing' END,consecutive_failures=consecutive_failures+1,last_failure_at=?,last_error=?,updated_at=? WHERE deployment_id=?`, healthGeneration, threshold, now, failure, now, deploymentID)
	if err != nil {
		return "", fmt.Errorf("record operational failure: %w", err)
	}
	if err = requireOneRow(result, "record operational failure"); err != nil {
		return "", err
	}
	var transition HealthTransition
	if !unhealthyNotified && previousFailures+1 >= threshold {
		if err = enqueueHealthNotification(ctx, tx, deploymentID, deploymentName, healthGeneration, "unhealthy", previousFailures+1, time.UnixMilli(now).UTC()); err != nil {
			return "", err
		}
		transition = HealthBecameUnhealthy
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return transition, nil
}

func enqueueHealthNotification(ctx context.Context, tx *sql.Tx, deploymentID, deploymentName string, generation int64, status string, failures int, at time.Time) error {
	unhealthy := status == "unhealthy"
	if _, err := tx.ExecContext(ctx, `UPDATE deployment_health SET unhealthy_notified=? WHERE deployment_id=?`, unhealthy, deploymentID); err != nil {
		return fmt.Errorf("mark health notification state: %w", err)
	}
	message := delivery.Message{Footer: deploymentName, Time: at.UTC(), MuteMentions: true}
	switch status {
	case "unhealthy":
		message.Title = "Monitor unhealthy"
		message.Message = "The monitor has failed " + strconv.Itoa(failures) + " consecutive times."
		message.Level = delivery.LevelFailure
	case "recovered":
		message.Title = "Monitor recovered"
		message.Message = "The monitor is healthy again."
		message.Level = delivery.LevelSuccess
	default:
		return fmt.Errorf("unsupported health notification %q", status)
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode health notification: %w", err)
	}
	payloadHash := sha256.Sum256(payload)
	outboxID, err := randomID()
	if err != nil {
		return err
	}
	eventID := status + ":" + outboxID
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(outbox_id,deployment_id,kind,generation,transaction_seq,event_id,payload,payload_hash,created_at) VALUES(?,?,'health',?,NULL,?,?,?,?)`, outboxID, deploymentID, generation, eventID, payload, payloadHash[:], toMs(at)); err != nil {
		return fmt.Errorf("insert health notification: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_deliveries(outbox_id,deployment_id,destination_id,destination_revision,next_attempt_at) SELECT ?,deployment_id,id,revision,? FROM destination_bindings WHERE deployment_id=? AND retired_at IS NULL`, outboxID, toMs(at), deploymentID); err != nil {
		return fmt.Errorf("insert health deliveries: %w", err)
	}
	return nil
}

func (s *Store) MarkGenerationStopped(ctx context.Context, deploymentID string, generation int64, reason, stopError string, at time.Time) error {
	if deploymentID == "" || generation < 1 {
		return errors.New("generation stop requires deployment and generation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := toMs(at)
	stopError = boundedText(stopError, maxStoredErrorBytes)
	result, err := tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',stopped_at=COALESCE(stopped_at,?),stop_error=CASE WHEN stop_error='' THEN ? ELSE stop_error END,stop_reason=CASE WHEN stop_reason='' THEN ? ELSE stop_reason END,retired_at=COALESCE(retired_at,?) WHERE deployment_id=? AND generation=?`, now, stopError, reason, now, deploymentID, generation)
	if err != nil {
		return fmt.Errorf("mark generation stopped: %w", err)
	}
	if err = requireOneRow(result, "mark generation stopped"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_health SET status=CASE WHEN ?='' THEN 'stopped' ELSE status END,last_error=CASE WHEN ?='' THEN last_error ELSE ? END,updated_at=? WHERE deployment_id=? AND generation=?`, stopError, stopError, stopError, now, deploymentID, generation); err != nil {
		return fmt.Errorf("mark deployment stopped: %w", err)
	}
	return tx.Commit()
}

func ensureDeploymentHealth(ctx context.Context, tx *sql.Tx, deploymentID string, generation int64, status string, now int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO deployment_health(deployment_id,generation,status,updated_at) VALUES(?,?,?,?) ON CONFLICT(deployment_id) DO UPDATE SET generation=excluded.generation,status=excluded.status,updated_at=excluded.updated_at`, deploymentID, generation, status, now)
	return err
}
