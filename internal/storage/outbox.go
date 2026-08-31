package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

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
