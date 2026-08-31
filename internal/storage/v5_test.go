package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func v5Store(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func seedV5(t *testing.T, s *Store) (Deployment, ActiveGeneration) {
	t.Helper()
	ctx := context.Background()
	a, err := s.PutArtifact(ctx, Artifact{ContentHash: "artifact-sha", Path: "/artifact", Describe: json.RawMessage(`{"name":"test"}`)})
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.CreateDeployment(ctx, CreateDeployment{Name: "instance", InfoName: "implementation", SourceDir: "/source", ArtifactID: a.ID, ConfigHash: "config-a", State: json.RawMessage(`{"n":0}`), StateVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.ActivateGeneration(ctx, GenerationActivation{DeploymentID: d.ID, ArtifactID: a.ID, ConfigRevision: d.ConfigRevision, SecretFingerprint: []byte("secret-fingerprint")})
	if err != nil {
		t.Fatal(err)
	}
	return d, g
}

func TestV5SchemaHasOneRuntimeIdentity(t *testing.T) {
	s := v5Store(t)
	for _, table := range []string{"monitors", "runs", "events"} {
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("legacy runtime table %q still exists", table)
		}
	}
	for _, table := range []string{"routes", "proxy_pools", "deployments", "transactions", "outbox_deliveries"} {
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("required table %q is missing", table)
		}
	}
}

func TestDeploymentIdentityStateAndGenerationLifecycle(t *testing.T) {
	s := v5Store(t)
	ctx := context.Background()
	d, g := seedV5(t, s)
	byName, err := s.GetDeployment(ctx, d.Name)
	if err != nil || byName.ID != d.ID {
		t.Fatalf("select by name: %#v %v", byName, err)
	}
	if _, err = s.ReplaceState(ctx, d.ID, 99, json.RawMessage(`{}`), 1); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("state CAS = %v", err)
	}
	rev, err := s.ReplaceState(ctx, d.ID, 0, json.RawMessage(`{"n":7}`), 2)
	if err != nil || rev != 1 {
		t.Fatalf("replace state: %d %v", rev, err)
	}
	frame := TransactionFrame{DeploymentID: d.ID, Generation: g.Generation, WorkerToken: g.WorkerToken, Sequence: 1, BaseStateRevision: 1, NextState: json.RawMessage(`{"n":8}`)}
	frame.PayloadHash = HashTransactionFrame(frame)
	if _, err = s.ApplyTransaction(ctx, frame); !errors.Is(err, ErrGenerationFenced) {
		t.Fatalf("old generation = %v", err)
	}
	g2, err := s.ActivateGeneration(ctx, GenerationActivation{DeploymentID: d.ID, ArtifactID: d.ArtifactID, ConfigRevision: d.ConfigRevision, SecretFingerprint: []byte("changed")})
	if err != nil {
		t.Fatal(err)
	}
	if g2.Generation <= g.Generation {
		t.Fatal("generation did not increase")
	}
	if err = s.ExpireDeployment(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.ResumeDeployment(ctx, d.ID, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.ArchiveDeployment(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.PurgeDeployment(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetDeployment(ctx, d.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("purged lookup = %v", err)
	}
}

func TestTransactionReplayConflictAndOutbox(t *testing.T) {
	s := v5Store(t)
	ctx := context.Background()
	d, g := seedV5(t, s)
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

func TestMissingOldSequenceIsFatal(t *testing.T) {
	s := v5Store(t)
	ctx := context.Background()
	d, g := seedV5(t, s)
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
	s := v5Store(t)
	ctx := context.Background()
	d, g := seedV5(t, s)
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

func TestRunAndHealthAreGenerationFenced(t *testing.T) {
	s := v5Store(t)
	ctx := context.Background()
	d, g := seedV5(t, s)
	start := RunStart{ID: "run-1", DeploymentID: d.ID, Generation: g.Generation, WorkerToken: g.WorkerToken, ChildName: "poll", Kind: "poll"}
	if err := s.StartRun(ctx, start); err != nil {
		t.Fatal(err)
	}
	wrong := append([]byte(nil), g.WorkerToken...)
	wrong[0] ^= 1
	if err := s.FinishRun(ctx, RunFinish{ID: "run-1", DeploymentID: d.ID, Generation: g.Generation, WorkerToken: wrong, Status: "success"}); !errors.Is(err, ErrRunFenced) {
		t.Fatalf("wrong token = %v", err)
	}
	if err := s.WriteHealth(ctx, d.ID, g.Generation, g.WorkerToken, "poll", "healthy", "ok"); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishRun(ctx, RunFinish{ID: "run-1", DeploymentID: d.ID, Generation: g.Generation, WorkerToken: g.WorkerToken, Status: "success"}); err != nil {
		t.Fatal(err)
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
