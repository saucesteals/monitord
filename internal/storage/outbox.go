package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type OutboxHistory struct {
	OutboxID, Kind, EventID, DestinationID, Status, LastError string
	CreatedAt                                                 time.Time
	AttemptCount                                              int
}

func (s *Store) ListOutboxHistory(ctx context.Context, id string, limit int, failed bool) ([]OutboxHistory, error) {
	if limit <= 0 {
		limit = 20
	}
	filter := ""
	if failed {
		filter = " AND d.status='dead'"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT e.outbox_id,e.kind,e.event_id,d.destination_id,d.status,d.last_error,e.created_at,d.attempt_count FROM outbox_events e JOIN outbox_deliveries d ON d.outbox_id=e.outbox_id WHERE e.deployment_id=?`+filter+` ORDER BY e.created_at DESC LIMIT ?`, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxHistory
	for rows.Next() {
		var v OutboxHistory
		var created int64
		if err = rows.Scan(&v.OutboxID, &v.Kind, &v.EventID, &v.DestinationID, &v.Status, &v.LastError, &created, &v.AttemptCount); err != nil {
			return nil, err
		}
		v.CreatedAt = fromMs(created)
		out = append(out, v)
	}
	return out, rows.Err()
}

type ClaimedDelivery struct {
	OutboxID, DeploymentID, DeploymentName, DestinationID string
	DestinationRevision                                   int64
	MessagePayload, DestinationConfig                     json.RawMessage
	AttemptCount                                          int
	LeaseOwner                                            string
	LeaseExpiresAt, CreatedAt                             time.Time
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
	claimID, err := randomID()
	if err != nil {
		return nil, err
	}
	claimOwner := owner + ":" + claimID
	expires := now.Add(lease)
	result, err := tx.ExecContext(ctx, `UPDATE outbox_deliveries SET status='sending',lease_owner=?,lease_expires_at=? WHERE rowid IN (SELECT d.rowid FROM outbox_deliveries d WHERE (d.status='pending' AND d.next_attempt_at<=?) OR (d.status='sending' AND d.lease_expires_at<=?) ORDER BY d.next_attempt_at LIMIT ?)`, claimOwner, toMs(expires), toMs(now), toMs(now), limit)
	if err != nil {
		return nil, err
	}
	claimed, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if claimed == 0 {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT d.outbox_id,e.deployment_id,p.name,d.destination_id,
		       d.destination_revision,e.payload,b.config,d.attempt_count,e.created_at
		FROM outbox_deliveries d
		JOIN outbox_events e ON e.outbox_id=d.outbox_id AND e.deployment_id=d.deployment_id
		JOIN deployments p ON p.id=e.deployment_id
		JOIN destination_bindings b ON b.deployment_id=d.deployment_id
		 AND b.id=d.destination_id AND b.revision=d.destination_revision
		WHERE d.status='sending' AND d.lease_owner=? AND d.lease_expires_at=?
		ORDER BY d.next_attempt_at`, claimOwner, toMs(expires))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimedDelivery
	for rows.Next() {
		var d ClaimedDelivery
		var created int64
		if err = rows.Scan(&d.OutboxID, &d.DeploymentID, &d.DeploymentName,
			&d.DestinationID, &d.DestinationRevision, &d.MessagePayload,
			&d.DestinationConfig, &d.AttemptCount, &created); err != nil {
			return nil, err
		}
		d.LeaseOwner = claimOwner
		d.LeaseExpiresAt = expires
		d.CreatedAt = fromMs(created)
		out = append(out, d)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
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

// DeferDelivery releases a lease without counting an attempt. It is used when
// a destination's local rate limit says the delivery is not due yet.
func (s *Store) DeferDelivery(ctx context.Context, outboxID, destinationID, owner string, next time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE outbox_deliveries SET status='pending',next_attempt_at=?,lease_owner=NULL,lease_expires_at=NULL WHERE outbox_id=? AND destination_id=? AND status='sending' AND lease_owner=?`, toMs(next), outboxID, destinationID, owner)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrLeaseLost
	}

	return nil
}

func (s *Store) MarkDeliveryFailed(ctx context.Context, outboxID, destinationID, owner, message string, now, next time.Time, maxAttempts int) error {
	if maxAttempts < 1 {
		return errors.New("max attempts must be positive")
	}
	message = boundedText(message, maxStoredErrorBytes)
	var attempts int
	err := s.db.QueryRowContext(ctx, `SELECT attempt_count FROM outbox_deliveries WHERE outbox_id=? AND destination_id=? AND status='sending' AND lease_owner=?`, outboxID, destinationID, owner).Scan(&attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if attempts+1 >= maxAttempts {
		return s.finishDelivery(ctx, outboxID, destinationID, owner, `status='dead',attempt_count=attempt_count+1,dead_at=?,lease_owner=NULL,lease_expires_at=NULL,last_error=?`, toMs(now), message)
	}
	return s.finishDelivery(ctx, outboxID, destinationID, owner, `status='pending',attempt_count=attempt_count+1,next_attempt_at=?,lease_owner=NULL,lease_expires_at=NULL,last_error=?`, toMs(next), message)
}

// PruneTerminalOutbox removes expired events only after every associated
// delivery has reached a terminal state. Pending and leased work is retained
// regardless of age.
func (s *Store) PruneTerminalOutbox(ctx context.Context, now time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM outbox_events AS e
		WHERE e.created_at < (
			SELECT ? - p.event_retention_ms
			FROM deployments AS p
			WHERE p.id=e.deployment_id
		)
		AND NOT EXISTS (
			SELECT 1 FROM outbox_deliveries AS d
			WHERE d.outbox_id=e.outbox_id
			AND d.status IN ('pending','sending')
		)`, toMs(now))
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}
func (s *Store) finishDelivery(ctx context.Context, outboxID, destinationID, owner, set string, args ...any) error {
	args = append(args, outboxID, destinationID, owner)
	res, err := s.db.ExecContext(ctx, `UPDATE outbox_deliveries SET `+set+` WHERE outbox_id=? AND destination_id=? AND status='sending' AND lease_owner=?`, args...)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrLeaseLost
	}
	return nil
}
func (s *Store) RetryDeadDelivery(ctx context.Context, outboxID, destinationID string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE outbox_deliveries SET status='pending',attempt_count=0,next_attempt_at=?,dead_at=NULL,last_error='' WHERE outbox_id=? AND destination_id=? AND status='dead'`, toMs(now), outboxID, destinationID)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrNotFound
	}
	return nil
}
