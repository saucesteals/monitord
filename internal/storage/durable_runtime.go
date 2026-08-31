package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func hashToken(token []byte) []byte { sum := sha256.Sum256(token); return sum[:] }

type DestinationBinding struct {
	ID           string
	Revision     int64
	DeploymentID string
	Config       json.RawMessage
	CreatedAt    time.Time
	RetiredAt    *time.Time
}

// PutDestinationBinding creates an immutable revision, reusing the current revision when unchanged.
func (s *Store) PutDestinationBinding(ctx context.Context, deploymentID, id string, config json.RawMessage) (DestinationBinding, error) {
	if deploymentID == "" || id == "" || !json.Valid(config) {
		return DestinationBinding{}, errors.New("binding requires deployment, id, and valid JSON")
	}
	hash := hashJSON(config)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DestinationBinding{}, err
	}
	defer tx.Rollback()
	var revision int64
	var currentHash []byte
	err = tx.QueryRowContext(ctx, `SELECT revision,config_hash FROM destination_bindings WHERE deployment_id=? AND id=? AND retired_at IS NULL ORDER BY revision DESC LIMIT 1`, deploymentID, id).Scan(&revision, &currentHash)
	if err == nil && string(currentHash) == string(hash) {
		_ = tx.Rollback()
		return s.getBinding(ctx, deploymentID, id, revision)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return DestinationBinding{}, err
	}
	now := toMs(time.Now().UTC())
	if revision > 0 {
		if _, err = tx.ExecContext(ctx, `UPDATE destination_bindings SET retired_at=? WHERE deployment_id=? AND id=? AND revision=? AND retired_at IS NULL`, now, deploymentID, id, revision); err != nil {
			return DestinationBinding{}, err
		}
	}
	var max int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0) FROM destination_bindings WHERE deployment_id=? AND id=?`, deploymentID, id).Scan(&max); err != nil {
		return DestinationBinding{}, err
	}
	revision = max + 1
	_, err = tx.ExecContext(ctx, `INSERT INTO destination_bindings(id,revision,deployment_id,config,config_hash,created_at) VALUES(?,?,?,?,?,?)`, id, revision, deploymentID, config, hash, now)
	if err != nil {
		return DestinationBinding{}, err
	}
	if err = tx.Commit(); err != nil {
		return DestinationBinding{}, err
	}
	return s.getBinding(ctx, deploymentID, id, revision)
}
func (s *Store) RetireDestinationBindingsExcept(ctx context.Context, id string, count int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE destination_bindings SET retired_at=? WHERE deployment_id=? AND retired_at IS NULL AND CAST(SUBSTR(id,LENGTH('destination-')+1) AS INTEGER)>?`, toMs(time.Now().UTC()), id, count)
	return err
}

func (s *Store) getBinding(ctx context.Context, deploymentID, id string, revision int64) (DestinationBinding, error) {
	var b DestinationBinding
	var created int64
	var retired sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT id,revision,deployment_id,config,created_at,retired_at FROM destination_bindings WHERE deployment_id=? AND id=? AND revision=?`, deploymentID, id, revision).Scan(&b.ID, &b.Revision, &b.DeploymentID, &b.Config, &created, &retired)
	if err != nil {
		return b, err
	}
	b.CreatedAt = fromMs(created)
	b.RetiredAt = nullTime(retired)
	return b, nil
}

type RunStart struct {
	ID, DeploymentID, ChildName, Kind string
	Generation                        int64
	WorkerToken                       []byte
	StartedAt                         time.Time
}
type RunFinish struct {
	ID, DeploymentID, Status, Summary, Error string
	Generation                               int64
	WorkerToken                              []byte
	FinishedAt                               time.Time
}

type DeploymentRun struct {
	ID, DeploymentID, Child, Kind, Status, Summary, Error string
	Generation                                            int64
	StartedAt                                             time.Time
	FinishedAt                                            *time.Time
}

func (s *Store) ListDeploymentRuns(ctx context.Context, id string, limit int, failed bool) ([]DeploymentRun, error) {
	if limit <= 0 {
		limit = 20
	}
	filter := ""
	if failed {
		filter = " AND status='failure'"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,deployment_id,generation,child_name,kind,status,started_at,finished_at,summary,error FROM deployment_runs WHERE deployment_id=?`+filter+` ORDER BY started_at DESC LIMIT ?`, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentRun
	for rows.Next() {
		var r DeploymentRun
		var started int64
		var finished sql.NullInt64
		if err = rows.Scan(&r.ID, &r.DeploymentID, &r.Generation, &r.Child, &r.Kind, &r.Status, &started, &finished, &r.Summary, &r.Error); err != nil {
			return nil, err
		}
		r.StartedAt = fromMs(started)
		r.FinishedAt = nullTime(finished)
		out = append(out, r)
	}
	return out, rows.Err()
}

type OutboxHistory struct {
	OutboxID, EventID, DestinationID, Status, LastError string
	CreatedAt                                           time.Time
	AttemptCount                                        int
}

func (s *Store) ListOutboxHistory(ctx context.Context, id string, limit int, failed bool) ([]OutboxHistory, error) {
	if limit <= 0 {
		limit = 20
	}
	filter := ""
	if failed {
		filter = " AND d.status='dead'"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT e.outbox_id,e.event_id,d.destination_id,d.status,d.last_error,e.created_at,d.attempt_count FROM outbox_events e JOIN outbox_deliveries d ON d.outbox_id=e.outbox_id WHERE e.deployment_id=?`+filter+` ORDER BY e.created_at DESC LIMIT ?`, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxHistory
	for rows.Next() {
		var v OutboxHistory
		var created int64
		if err = rows.Scan(&v.OutboxID, &v.EventID, &v.DestinationID, &v.Status, &v.LastError, &created, &v.AttemptCount); err != nil {
			return nil, err
		}
		v.CreatedAt = fromMs(created)
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) StartRun(ctx context.Context, in RunStart) error {
	if in.ID == "" || in.DeploymentID == "" || in.Generation < 1 || in.ChildName == "" || (in.Kind != "poll" && in.Kind != "continuous") {
		return errors.New("invalid run start")
	}
	if in.StartedAt.IsZero() {
		in.StartedAt = time.Now().UTC()
	}
	ok, err := s.generationAuthorized(ctx, in.DeploymentID, in.Generation, in.WorkerToken)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRunFenced
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO deployment_runs(id,deployment_id,generation,child_name,kind,status,started_at) VALUES(?,?,?,?,?,'running',?)`, in.ID, in.DeploymentID, in.Generation, in.ChildName, in.Kind, toMs(in.StartedAt))
	return err
}

func (s *Store) FinishRun(ctx context.Context, in RunFinish) error {
	if in.FinishedAt.IsZero() {
		in.FinishedAt = time.Now().UTC()
	}
	ok, err := s.generationAuthorized(ctx, in.DeploymentID, in.Generation, in.WorkerToken)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRunFenced
	}
	res, err := s.db.ExecContext(ctx, `UPDATE deployment_runs SET status=?,summary=?,error=?,finished_at=? WHERE id=? AND deployment_id=? AND generation=? AND status='running'`, in.Status, in.Summary, in.Error, toMs(in.FinishedAt), in.ID, in.DeploymentID, in.Generation)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrRunFenced
	}
	return nil
}

func (s *Store) WriteHealth(ctx context.Context, deploymentID string, generation int64, token []byte, child, status, summary string) error {
	ok, err := s.generationAuthorized(ctx, deploymentID, generation, token)
	if err != nil {
		return err
	}
	if !ok {
		return ErrGenerationFenced
	}
	failExpr := `CASE WHEN excluded.status IN ('failed','degraded') THEN deployment_health.consecutive_failures+1 ELSE 0 END`
	_, err = s.db.ExecContext(ctx, `INSERT INTO deployment_health(deployment_id,child_name,generation,status,summary,consecutive_failures,updated_at) VALUES(?,?,?,?,?,CASE WHEN ? IN ('failed','degraded') THEN 1 ELSE 0 END,?) ON CONFLICT(deployment_id,child_name) DO UPDATE SET generation=excluded.generation,status=excluded.status,summary=excluded.summary,consecutive_failures=`+failExpr+`,updated_at=excluded.updated_at`, deploymentID, child, generation, status, summary, status, toMs(time.Now().UTC()))
	return err
}

func (s *Store) generationAuthorized(ctx context.Context, id string, generation int64, token []byte) (bool, error) {
	var count int
	hash := hashToken(token)
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deployments d JOIN deployment_generations g ON g.deployment_id=d.id AND g.generation=d.active_generation WHERE d.id=? AND d.status='active' AND g.generation=? AND g.status='active' AND g.worker_token_hash=?`, id, generation, hash).Scan(&count)
	return count == 1, err
}

type ClaimedDelivery struct {
	OutboxID, DeploymentID, DestinationID string
	DestinationRevision                   int64
	RenderedPayload, DestinationConfig    json.RawMessage
	AttemptCount                          int
	LeaseOwner                            string
	LeaseExpiresAt                        time.Time
}

// ClaimOutbox atomically leases due rows. Completion is intentionally generation-independent.
func (s *Store) ClaimOutbox(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]ClaimedDelivery, error) {
	if owner == "" || lease <= 0 || limit <= 0 {
		return nil, errors.New("invalid outbox claim")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	expires := now.Add(lease)
	rows, err := tx.QueryContext(ctx, `UPDATE outbox_deliveries SET status='sending',lease_owner=?,lease_expires_at=? WHERE rowid IN (SELECT d.rowid FROM outbox_deliveries d WHERE (d.status='pending' AND d.next_attempt_at<=?) OR (d.status='sending' AND d.lease_expires_at<=?) ORDER BY d.next_attempt_at LIMIT ?) RETURNING outbox_id,destination_id,destination_revision,rendered_payload,attempt_count`, owner, toMs(expires), toMs(now), toMs(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimedDelivery
	for rows.Next() {
		var d ClaimedDelivery
		if err = rows.Scan(&d.OutboxID, &d.DestinationID, &d.DestinationRevision, &d.RenderedPayload, &d.AttemptCount); err != nil {
			return nil, err
		}
		d.LeaseOwner = owner
		d.LeaseExpiresAt = expires
		err = tx.QueryRowContext(ctx, `SELECT e.deployment_id,b.config FROM outbox_events e JOIN destination_bindings b ON b.deployment_id=e.deployment_id AND b.id=? AND b.revision=? WHERE e.outbox_id=?`, d.DestinationID, d.DestinationRevision, d.OutboxID).Scan(&d.DeploymentID, &d.DestinationConfig)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) MarkDelivered(ctx context.Context, outboxID, destinationID, owner string, now time.Time) error {
	return s.finishDelivery(ctx, outboxID, destinationID, owner, `status='delivered',delivered_at=?,lease_owner=NULL,lease_expires_at=NULL,last_error=''`, toMs(now))
}
func (s *Store) MarkDeliveryFailed(ctx context.Context, outboxID, destinationID, owner, message string, now, next time.Time, maxAttempts int) error {
	if maxAttempts < 1 {
		return errors.New("max attempts must be positive")
	}
	var attempts int
	err := s.db.QueryRowContext(ctx, `SELECT attempt_count FROM outbox_deliveries WHERE outbox_id=? AND destination_id=? AND status='sending' AND lease_owner=?`, outboxID, destinationID, owner).Scan(&attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrRunFenced
	}
	if err != nil {
		return err
	}
	if attempts+1 >= maxAttempts {
		return s.finishDelivery(ctx, outboxID, destinationID, owner, `status='dead',attempt_count=attempt_count+1,dead_at=?,lease_owner=NULL,lease_expires_at=NULL,last_error=?`, toMs(now), message)
	}
	return s.finishDelivery(ctx, outboxID, destinationID, owner, `status='pending',attempt_count=attempt_count+1,next_attempt_at=?,lease_owner=NULL,lease_expires_at=NULL,last_error=?`, toMs(next), message)
}
func (s *Store) finishDelivery(ctx context.Context, outboxID, destinationID, owner, set string, args ...any) error {
	args = append(args, outboxID, destinationID, owner)
	res, err := s.db.ExecContext(ctx, `UPDATE outbox_deliveries SET `+set+` WHERE outbox_id=? AND destination_id=? AND status='sending' AND lease_owner=?`, args...)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return fmt.Errorf("delivery lease lost: %w", ErrRunFenced)
	}
	return nil
}
func (s *Store) RetryDeadDelivery(ctx context.Context, outboxID, destinationID string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE outbox_deliveries SET status='pending',next_attempt_at=?,dead_at=NULL,last_error='' WHERE outbox_id=? AND destination_id=? AND status='dead'`, toMs(now), outboxID, destinationID)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrNotFound
	}
	return nil
}

// PruneOutbox only removes events whose every destination is terminal. It never prunes transaction ledger rows.
func (s *Store) PruneOutbox(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM outbox_events WHERE outbox_id IN (SELECT e.outbox_id FROM outbox_events e WHERE e.created_at<? AND NOT EXISTS(SELECT 1 FROM outbox_deliveries d WHERE d.outbox_id=e.outbox_id AND d.status NOT IN ('delivered','dead')) ORDER BY e.created_at LIMIT ?)`, toMs(before), limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
