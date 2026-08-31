package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTransactionReplayConflictAndOutbox(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	d, g := seedStore(t, s)
	b, err := s.PutDestinationBinding(ctx, d.ID, "discord", json.RawMessage(`{"url":"redacted"}`))
	if err != nil {
		t.Fatal(err)
	}
	f := TransactionFrame{DeploymentID: d.ID, Generation: g.Generation, WorkerToken: g.WorkerToken, Sequence: 1, BaseStateRevision: 0, NextState: json.RawMessage(`{"n":1}`), Checkpoints: []CheckpointMutation{{Source: "chain", Value: json.RawMessage(`{"block":10}`)}}, Events: []OutboxEvent{{OutboxID: "out-1", EventID: "event-1", Payload: json.RawMessage(`{"title":"one"}`), DedupeKey: "record-1", DedupeFor: time.Hour, Deliveries: []OutboxDelivery{{DestinationID: b.ID, DestinationRevision: b.Revision, RenderedPayload: json.RawMessage(`{"content":"one"}`)}}}}}
	f.PayloadHash = HashTransactionFrame(f)
	a1, err := s.ApplyTransaction(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := s.ApplyTransaction(ctx, f)
	if err != nil || a2 != a1 {
		t.Fatalf("replay: %#v %v", a2, err)
	}
	runtime, err := s.ListRuntimeDeployments(ctx)
	if err != nil || len(runtime) != 1 {
		t.Fatalf("runtime deployments: %#v %v", runtime, err)
	}
	if got := string(runtime[0].Checkpoints["chain"]); got != `{"block":10}` {
		t.Fatalf("runtime checkpoint=%s", got)
	}
	bad := f
	bad.NextState = json.RawMessage(`{"n":2}`)
	bad.PayloadHash = HashTransactionFrame(bad)
	if _, err = s.ApplyTransaction(ctx, bad); !errors.Is(err, ErrPayloadConflict) {
		t.Fatalf("hash conflict = %v", err)
	}
	claims, err := s.ClaimOutbox(ctx, "sender", time.Now().UTC(), time.Minute, 10)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim: %#v %v", claims, err)
	}
	if err = s.MarkDeliveryFailed(ctx, "out-1", "discord", "sender", "timeout", time.Now().UTC(), time.Now().Add(time.Second), 2); err != nil {
		t.Fatal(err)
	}
	claims, err = s.ClaimOutbox(ctx, "sender", time.Now().Add(2*time.Second), time.Minute, 10)
	if err != nil || len(claims) != 1 {
		t.Fatalf("reclaim: %#v %v", claims, err)
	}
	if err = s.MarkDeliveryFailed(ctx, "out-1", "discord", "sender", "again", time.Now().UTC(), time.Now(), 2); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryDeadDelivery(ctx, "out-1", "discord", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PruneOutbox(ctx, time.Now().Add(time.Hour), 10); err != nil || n != 0 {
		t.Fatalf("pending outbox was pruned: %d %v", n, err)
	}
}
