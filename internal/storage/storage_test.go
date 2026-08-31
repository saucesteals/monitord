package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func seedStore(t *testing.T, s *Store) (Deployment, ActiveGeneration) {
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
