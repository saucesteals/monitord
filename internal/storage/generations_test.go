package storage

import (
	"context"
	"testing"
)

func TestActivationAdvancesGeneration(t *testing.T) {
	s := testStore(t)
	d, first := seedStore(t, s)
	second, err := s.ActivateGeneration(context.Background(), GenerationActivation{
		DeploymentID: d.ID, ArtifactID: d.ArtifactID,
		ConfigRevision: d.ConfigRevision, SecretFingerprint: []byte("rotated"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf("generation = %d, want %d", second.Generation, first.Generation+1)
	}
}
