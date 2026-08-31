package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrGenerationFenced = errors.New("worker generation is fenced")
	ErrSequenceConflict = errors.New("transaction sequence conflict")
	ErrMissingSequence  = errors.New("transaction sequence is behind ledger")
	ErrPayloadConflict  = errors.New("transaction payload conflict")
)

type CheckpointMutation struct {
	Source string
	Value  json.RawMessage
}

type OutboxDelivery struct {
	DestinationID       string
	DestinationRevision int64
}

type OutboxEvent struct {
	OutboxID   string
	EventID    string
	Payload    json.RawMessage
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
		if frame.Sequence <= lastSequence {
			return TransactionACK{}, fmt.Errorf("%w: got %d, ledger advanced to %d", ErrMissingSequence, frame.Sequence, lastSequence)
		}
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
	return ack, true, nil
}

func insertOutboxEvent(ctx context.Context, tx *sql.Tx, frame TransactionFrame, event OutboxEvent, now time.Time) error {
	payloadHash := sha256.Sum256(event.Payload)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (
			outbox_id, deployment_id, generation, transaction_seq,
			event_id, payload, payload_hash, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, event.OutboxID, frame.DeploymentID,
		frame.Generation, frame.Sequence, event.EventID, event.Payload, payloadHash[:], toMs(now)); err != nil {
		// Event IDs identify immutable source occurrences across transactions.
		// An inclusive replay with identical content coalesces; reusing the ID
		// for different content is a fatal application conflict.
		var storedHash []byte
		lookupErr := tx.QueryRowContext(ctx, `SELECT payload_hash FROM outbox_events WHERE deployment_id=? AND event_id=?`, frame.DeploymentID, event.EventID).Scan(&storedHash)
		if lookupErr == nil && bytes.Equal(storedHash, payloadHash[:]) {
			return nil
		}
		if lookupErr == nil {
			return fmt.Errorf("event %q: %w", event.EventID, ErrPayloadConflict)
		}
		return fmt.Errorf("insert outbox event %q: %w", event.EventID, err)
	}
	for _, delivery := range event.Deliveries {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO outbox_deliveries (
				outbox_id, destination_id, destination_revision, next_attempt_at
			) VALUES (?, ?, ?, ?)`, event.OutboxID, delivery.DestinationID,
			delivery.DestinationRevision, toMs(now)); err != nil {
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
	checkpointSources := make(map[string]struct{}, len(frame.Checkpoints))
	for _, checkpoint := range frame.Checkpoints {
		if checkpoint.Source == "" || !json.Valid(checkpoint.Value) {
			return errors.New("transaction frame contains an invalid checkpoint")
		}
		if _, exists := checkpointSources[checkpoint.Source]; exists {
			return fmt.Errorf("transaction frame repeats checkpoint %q", checkpoint.Source)
		}
		checkpointSources[checkpoint.Source] = struct{}{}
	}
	eventIDs := make(map[string]struct{}, len(frame.Events))
	outboxIDs := make(map[string]struct{}, len(frame.Events))
	for _, event := range frame.Events {
		if event.OutboxID == "" || event.EventID == "" || !json.Valid(event.Payload) {
			return errors.New("transaction frame contains an invalid event")
		}
		if _, exists := eventIDs[event.EventID]; exists {
			return fmt.Errorf("transaction frame repeats event %q", event.EventID)
		}
		if _, exists := outboxIDs[event.OutboxID]; exists {
			return fmt.Errorf("transaction frame repeats outbox id %q", event.OutboxID)
		}
		eventIDs[event.EventID], outboxIDs[event.OutboxID] = struct{}{}, struct{}{}
		destinations := make(map[string]struct{}, len(event.Deliveries))
		for _, delivery := range event.Deliveries {
			if delivery.DestinationID == "" || delivery.DestinationRevision < 1 {
				return errors.New("transaction frame contains an invalid delivery")
			}
			if _, exists := destinations[delivery.DestinationID]; exists {
				return fmt.Errorf("event %q repeats destination %q", event.EventID, delivery.DestinationID)
			}
			destinations[delivery.DestinationID] = struct{}{}
		}
	}
	if frame.PayloadHash == ([sha256.Size]byte{}) {
		return errors.New("transaction frame payload hash is missing")
	}
	return nil
}
