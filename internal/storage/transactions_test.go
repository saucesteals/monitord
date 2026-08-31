package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
)

func TestMissingOldSequenceIsFatal(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	d, g := seedStore(t, s)
	f := TransactionFrame{DeploymentID: d.ID, Generation: g.Generation, WorkerToken: g.WorkerToken, Sequence: 1, BaseStateRevision: 0, NextState: json.RawMessage(`{}`)}
	f.PayloadHash = HashTransactionFrame(f)
	if _, err := s.ApplyTransaction(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM transactions WHERE deployment_id=? AND generation=? AND seq=1`, d.ID, g.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyTransaction(ctx, f); !errors.Is(err, ErrMissingSequence) {
		t.Fatalf("missing old sequence = %v", err)
	}
}

func TestEventOccurrenceReplayAndConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	d, g := seedStore(t, s)
	event := OutboxEvent{OutboxID: "out-1", EventID: "occurrence-1", Payload: json.RawMessage(`{"title":"same"}`)}
	first := TransactionFrame{DeploymentID: d.ID, Generation: g.Generation, WorkerToken: g.WorkerToken, Sequence: 1, BaseStateRevision: 0, NextState: json.RawMessage(`{"n":1}`), Events: []OutboxEvent{event}}
	first.PayloadHash = HashTransactionFrame(first)
	if _, err := s.ApplyTransaction(ctx, first); err != nil {
		t.Fatal(err)
	}

	replay := first
	replay.Sequence = 2
	replay.BaseStateRevision = 1
	replay.NextState = json.RawMessage(`{"n":2}`)
	replay.Events[0].OutboxID = "out-2"
	replay.PayloadHash = HashTransactionFrame(replay)
	if _, err := s.ApplyTransaction(ctx, replay); err != nil {
		t.Fatalf("identical occurrence replay: %v", err)
	}

	conflict := replay
	conflict.Sequence = 3
	conflict.BaseStateRevision = 2
	conflict.NextState = json.RawMessage(`{"n":3}`)
	conflict.Events = []OutboxEvent{{OutboxID: "out-3", EventID: "occurrence-1", Payload: json.RawMessage(`{"title":"changed"}`)}}
	conflict.PayloadHash = HashTransactionFrame(conflict)
	if _, err := s.ApplyTransaction(ctx, conflict); !errors.Is(err, ErrPayloadConflict) {
		t.Fatalf("changed occurrence = %v", err)
	}
}

func TestTransactionHashCoversSemanticFields(t *testing.T) {
	f := TransactionFrame{DeploymentID: "d", Generation: 1, WorkerToken: []byte("0123456789abcdef"), Sequence: 1, NextState: json.RawMessage(`{}`)}
	a := HashTransactionFrame(f)
	f.Progress = true
	b := HashTransactionFrame(f)
	if a == b || a == ([sha256.Size]byte{}) {
		t.Fatal("hash does not cover progress")
	}
}
