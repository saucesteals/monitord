package storage

import (
	"context"
	"errors"
	"testing"
)

func TestRunAndHealthAreGenerationFenced(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	d, g := seedStore(t, s)
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
