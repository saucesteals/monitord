package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrGenerationFenced = errors.New("worker generation is fenced")
	ErrSequenceConflict = errors.New("transaction sequence conflict")
	ErrPayloadConflict  = errors.New("transaction payload conflict")
)

type CheckpointMutation struct {
	Source string
	Value  json.RawMessage
}

type OutboxDelivery struct {
	DestinationID       string
	DestinationRevision int64
	RenderedPayload     json.RawMessage
}

type OutboxEvent struct {
	OutboxID   string
	EventID    string
	Payload    json.RawMessage
	DedupeKey  string
	DedupeFor  time.Duration
	Deliveries []OutboxDelivery
}

// TransactionFrame is the complete semantic unit retained and retried by a worker.
// PayloadHash must be computed over the canonical worker frame, not its transport envelope.
type TransactionFrame struct {
	DeploymentID      string
	Generation        int64
	WorkerToken       []byte
	Sequence          int64
	BaseStateRevision int64
	NextState         json.RawMessage
	Checkpoints       []CheckpointMutation
	Events            []OutboxEvent
	Progress          bool
	PayloadHash       [sha256.Size]byte
}

type TransactionACK struct {
	DeploymentID   string `json:"deployment_id"`
	Generation     int64  `json:"generation"`
	Sequence       int64  `json:"sequence"`
	PayloadHash    string `json:"payload_hash"`
	ResultRevision int64  `json:"result_revision"`
	Status         string `json:"status"`
}

// ApplyTransaction either applies the entire frame once or returns its durable ACK.
// Ledger lookup intentionally precedes active-generation checks so an ACK lost during
// generation replacement remains recoverable.
func (s *Store) ApplyTransaction(ctx context.Context, frame TransactionFrame) (TransactionACK, error) {
	if err := validateFrame(frame); err != nil {
		return TransactionACK{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return TransactionACK{}, fmt.Errorf("begin transaction frame: %w", err)
	}
	defer tx.Rollback()

	ack, found, err := lookupTransaction(ctx, tx, frame)
	if err != nil {
		return TransactionACK{}, err
	}
	if found {
		return ack, nil
	}

	var activeGeneration, stateRevision, lastSequence int64
	var tokenHash []byte
	err = tx.QueryRowContext(ctx, `
		SELECT d.active_generation, d.state_revision,
		       g.last_transaction_seq, g.worker_token_hash
		FROM deployments d
		JOIN deployment_generations g
		  ON g.deployment_id = d.id AND g.generation = d.active_generation
		WHERE d.id = ? AND g.status = 'active'`, frame.DeploymentID).
		Scan(&activeGeneration, &stateRevision, &lastSequence, &tokenHash)
	if errors.Is(err, sql.ErrNoRows) {
		return TransactionACK{}, ErrGenerationFenced
	}
	if err != nil {
		return TransactionACK{}, fmt.Errorf("load active generation: %w", err)
	}

	wantTokenHash := sha256.Sum256(frame.WorkerToken)
	if activeGeneration != frame.Generation || !bytes.Equal(tokenHash, wantTokenHash[:]) {
		return TransactionACK{}, ErrGenerationFenced
	}
	if frame.Sequence != lastSequence+1 {
		return TransactionACK{}, fmt.Errorf("%w: got %d, want %d", ErrSequenceConflict, frame.Sequence, lastSequence+1)
	}
	if frame.BaseStateRevision != stateRevision {
		return TransactionACK{}, fmt.Errorf("%w: base revision %d, current %d", ErrStateConflict, frame.BaseStateRevision, stateRevision)
	}

	resultRevision := stateRevision + 1
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE deployments
		SET state = ?, state_revision = ?, updated_at = ?
		WHERE id = ? AND active_generation = ? AND state_revision = ?`,
		frame.NextState, resultRevision, toMs(now), frame.DeploymentID,
		frame.Generation, frame.BaseStateRevision)
	if err != nil {
		return TransactionACK{}, fmt.Errorf("update deployment state: %w", err)
	}
	if err := requireOneRow(result, "update deployment state"); err != nil {
		return TransactionACK{}, ErrGenerationFenced
	}

	for _, checkpoint := range frame.Checkpoints {
		hash := sha256.Sum256(checkpoint.Value)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO checkpoints (
				deployment_id, source, value, value_hash, updated_generation, updated_seq
			) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(deployment_id, source) DO UPDATE SET
				value = excluded.value,
				value_hash = excluded.value_hash,
				updated_generation = excluded.updated_generation,
				updated_seq = excluded.updated_seq`, frame.DeploymentID, checkpoint.Source,
			checkpoint.Value, hash[:], frame.Generation, frame.Sequence); err != nil {
			return TransactionACK{}, fmt.Errorf("write checkpoint %q: %w", checkpoint.Source, err)
		}
	}

	for _, event := range frame.Events {
		accepted, err := claimDedupe(ctx, tx, frame.DeploymentID, event, now)
		if err != nil {
			return TransactionACK{}, err
		}
		if !accepted {
			continue
		}
		if err := insertOutboxEvent(ctx, tx, frame, event, now); err != nil {
			return TransactionACK{}, err
		}
	}

	ack = TransactionACK{
		DeploymentID: frame.DeploymentID, Generation: frame.Generation,
		Sequence: frame.Sequence, PayloadHash: hex.EncodeToString(frame.PayloadHash[:]),
		ResultRevision: resultRevision, Status: "accepted",
	}
	ackPayload, err := json.Marshal(ack)
	if err != nil {
		return TransactionACK{}, fmt.Errorf("encode transaction ack: %w", err)
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE deployment_generations SET last_transaction_seq = ?
		WHERE deployment_id = ? AND generation = ? AND last_transaction_seq = ?`,
		frame.Sequence, frame.DeploymentID, frame.Generation, lastSequence)
	if err != nil {
		return TransactionACK{}, fmt.Errorf("advance transaction sequence: %w", err)
	}
	if err := requireOneRow(result, "advance transaction sequence"); err != nil {
		return TransactionACK{}, ErrSequenceConflict
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transactions (
			deployment_id, generation, seq, payload_hash, base_revision,
			result_revision, ack_payload, committed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, frame.DeploymentID, frame.Generation,
		frame.Sequence, frame.PayloadHash[:], frame.BaseStateRevision, resultRevision,
		ackPayload, toMs(now)); err != nil {
		return TransactionACK{}, fmt.Errorf("write transaction ledger: %w", err)
	}

	if err := tx.Commit(); err != nil {
		resolved, found, lookupErr := s.resolveTransaction(ctx, frame)
		if lookupErr == nil && found {
			return resolved, nil
		}
		if lookupErr != nil {
			return TransactionACK{}, fmt.Errorf("commit transaction frame: %w (ledger resolution failed: %v)", err, lookupErr)
		}
		return TransactionACK{}, fmt.Errorf("commit transaction frame: %w", err)
	}
	return ack, nil
}

func (s *Store) resolveTransaction(ctx context.Context, frame TransactionFrame) (TransactionACK, bool, error) {
	var storedHash, ackPayload []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT payload_hash, ack_payload FROM transactions
		WHERE deployment_id = ? AND generation = ? AND seq = ?`,
		frame.DeploymentID, frame.Generation, frame.Sequence).Scan(&storedHash, &ackPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return TransactionACK{}, false, nil
	}
	if err != nil {
		return TransactionACK{}, false, fmt.Errorf("resolve transaction ledger: %w", err)
	}
	if !bytes.Equal(storedHash, frame.PayloadHash[:]) {
		return TransactionACK{}, false, ErrPayloadConflict
	}
	var ack TransactionACK
	if err := json.Unmarshal(ackPayload, &ack); err != nil {
		return TransactionACK{}, false, fmt.Errorf("decode resolved transaction ack: %w", err)
	}
	return ack, true, nil
}

func lookupTransaction(ctx context.Context, tx *sql.Tx, frame TransactionFrame) (TransactionACK, bool, error) {
	var storedHash, ackPayload []byte
	err := tx.QueryRowContext(ctx, `
		SELECT payload_hash, ack_payload FROM transactions
		WHERE deployment_id = ? AND generation = ? AND seq = ?`,
		frame.DeploymentID, frame.Generation, frame.Sequence).Scan(&storedHash, &ackPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return TransactionACK{}, false, nil
	}
	if err != nil {
		return TransactionACK{}, false, fmt.Errorf("lookup transaction ledger: %w", err)
	}
	if !bytes.Equal(storedHash, frame.PayloadHash[:]) {
		return TransactionACK{}, false, ErrPayloadConflict
	}
	var ack TransactionACK
	if err := json.Unmarshal(ackPayload, &ack); err != nil {
		return TransactionACK{}, false, fmt.Errorf("decode stored transaction ack: %w", err)
	}
	ack.Status = "replayed"
	return ack, true, nil
}

func claimDedupe(ctx context.Context, tx *sql.Tx, deploymentID string, event OutboxEvent, now time.Time) (bool, error) {
	if event.DedupeKey == "" || event.DedupeFor <= 0 {
		return true, nil
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO dedupe_claims (deployment_id, dedupe_key, claimed_at, expires_at, event_id)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(deployment_id, dedupe_key) DO UPDATE SET
			claimed_at = excluded.claimed_at,
			expires_at = excluded.expires_at,
			event_id = excluded.event_id
		WHERE dedupe_claims.expires_at <= excluded.claimed_at`, deploymentID,
		event.DedupeKey, toMs(now), toMs(now.Add(event.DedupeFor)), event.EventID)
	if err != nil {
		return false, fmt.Errorf("claim dedupe key %q: %w", event.DedupeKey, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect dedupe claim %q: %w", event.DedupeKey, err)
	}
	return rows == 1, nil
}

func insertOutboxEvent(ctx context.Context, tx *sql.Tx, frame TransactionFrame, event OutboxEvent, now time.Time) error {
	payloadHash := sha256.Sum256(event.Payload)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (
			outbox_id, deployment_id, generation, transaction_seq,
			event_id, payload, payload_hash, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, event.OutboxID, frame.DeploymentID,
		frame.Generation, frame.Sequence, event.EventID, event.Payload, payloadHash[:], toMs(now)); err != nil {
		return fmt.Errorf("insert outbox event %q: %w", event.EventID, err)
	}
	for _, delivery := range event.Deliveries {
		renderedHash := sha256.Sum256(delivery.RenderedPayload)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO outbox_deliveries (
				outbox_id, destination_id, destination_revision,
				rendered_payload, payload_hash, next_attempt_at
			) VALUES (?, ?, ?, ?, ?, ?)`, event.OutboxID, delivery.DestinationID,
			delivery.DestinationRevision, delivery.RenderedPayload, renderedHash[:], toMs(now)); err != nil {
			return fmt.Errorf("insert delivery %q: %w", delivery.DestinationID, err)
		}
	}
	return nil
}

func validateFrame(frame TransactionFrame) error {
	if frame.DeploymentID == "" || frame.Generation < 1 || frame.Sequence < 1 {
		return errors.New("transaction frame requires deployment, generation, and sequence")
	}
	if len(frame.WorkerToken) < 16 {
		return errors.New("transaction frame worker token is missing")
	}
	if len(frame.NextState) == 0 || !json.Valid(frame.NextState) {
		return errors.New("transaction frame state is not valid JSON")
	}
	for _, checkpoint := range frame.Checkpoints {
		if checkpoint.Source == "" || !json.Valid(checkpoint.Value) {
			return errors.New("transaction frame contains an invalid checkpoint")
		}
	}
	for _, event := range frame.Events {
		if event.OutboxID == "" || event.EventID == "" || !json.Valid(event.Payload) {
			return errors.New("transaction frame contains an invalid event")
		}
		for _, delivery := range event.Deliveries {
			if delivery.DestinationID == "" || delivery.DestinationRevision < 1 || !json.Valid(delivery.RenderedPayload) {
				return errors.New("transaction frame contains an invalid delivery")
			}
		}
	}
	wantHash := HashTransactionFrame(frame)
	if !bytes.Equal(frame.PayloadHash[:], wantHash[:]) {
		return errors.New("transaction frame payload hash does not match its contents")
	}
	return nil
}

// HashTransactionFrame hashes an unambiguous length-delimited representation of
// every semantic frame field except PayloadHash itself. Slice order is meaningful.
func HashTransactionFrame(frame TransactionFrame) [sha256.Size]byte {
	h := sha256.New()
	writeHashBytes(h, []byte(frame.DeploymentID))
	writeHashInt64(h, frame.Generation)
	writeHashBytes(h, frame.WorkerToken)
	writeHashInt64(h, frame.Sequence)
	writeHashInt64(h, frame.BaseStateRevision)
	writeHashBytes(h, frame.NextState)
	writeHashInt64(h, int64(len(frame.Checkpoints)))
	for _, checkpoint := range frame.Checkpoints {
		writeHashBytes(h, []byte(checkpoint.Source))
		writeHashBytes(h, checkpoint.Value)
	}
	writeHashInt64(h, int64(len(frame.Events)))
	for _, event := range frame.Events {
		writeHashBytes(h, []byte(event.OutboxID))
		writeHashBytes(h, []byte(event.EventID))
		writeHashBytes(h, event.Payload)
		writeHashBytes(h, []byte(event.DedupeKey))
		writeHashInt64(h, int64(event.DedupeFor))
		writeHashInt64(h, int64(len(event.Deliveries)))
		for _, delivery := range event.Deliveries {
			writeHashBytes(h, []byte(delivery.DestinationID))
			writeHashInt64(h, delivery.DestinationRevision)
			writeHashBytes(h, delivery.RenderedPayload)
		}
	}
	if frame.Progress {
		writeHashInt64(h, 1)
	} else {
		writeHashInt64(h, 0)
	}

	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeHashBytes(w hashWriter, value []byte) {
	writeHashInt64(w, int64(len(value)))
	_, _ = w.Write(value)
}

func writeHashInt64(w hashWriter, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = w.Write(encoded[:])
}
