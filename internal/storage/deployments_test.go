package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestDeploymentIdentityStateAndGenerationLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	d, g := seedStore(t, s)
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
