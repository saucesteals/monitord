package daemon

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/storage"
)

func TestWorkerHandshakeTransactionAndACK(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "fake.go")
	binary := filepath.Join(dir, "fake-worker")
	if err := os.WriteFile(source, []byte(fakeWorkerSource), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "build", "-o", binary, source)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fake: %v\n%s", err, out)
	}
	s, err := storage.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	describe, _ := json.Marshal(monitord.MonitorFrame{Info: monitord.Info{Name: "fake-worker"}, Plan: monitord.PlanDescription{Kind: "every", Interval: time.Second}, StateVersion: 1})
	a, err := s.PutArtifact(ctx, storage.Artifact{ContentHash: "fake-hash", Path: binary, Describe: describe})
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.CreateDeployment(ctx, storage.CreateDeployment{Name: "fake", InfoName: "fake-worker", SourceDir: dir, ArtifactID: a.ID, ConfigHash: "config", State: json.RawMessage(`{}`), StateVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutDestinationBinding(ctx, d.ID, "capture", json.RawMessage(`{"kind":"capture"}`)); err != nil {
		t.Fatal(err)
	}
	g, err := s.ActivateGeneration(ctx, storage.GenerationActivation{DeploymentID: d.ID, ArtifactID: a.ID, ConfigRevision: 1, SecretFingerprint: []byte("none")})
	if err != nil {
		t.Fatal(err)
	}
	runtime := storage.RuntimeDeployment{Deployment: d, ArtifactPath: binary, ArtifactHash: a.ContentHash, Describe: describe}
	w, err := startWorker(ctx, slog.Default(), runtime, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.serve(ctx, s); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDeployment(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.StateRevision != 1 || string(got.State) != `{"committed":true}` {
		t.Fatalf("state = %s revision %d", got.State, got.StateRevision)
	}
	claimed, err := s.ClaimOutbox(ctx, "test", time.Now(), time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("outbox = %#v %v", claimed, err)
	}
}

const fakeWorkerSource = `package main
import("bufio";"encoding/hex";"encoding/json";"os";m "github.com/saucesteals/monitord")
type H struct{DeploymentID string ` + "`json:\"deployment_id\"`" + `;Generation uint64 ` + "`json:\"generation\"`" + `;WorkerToken string ` + "`json:\"worker_token\"`" + `;StateRevision int64 ` + "`json:\"state_revision\"`" + `}
func main(){r:=bufio.NewReader(os.Stdin);var x struct{Hello H ` + "`json:\"hello\"`" + `};b,_:=r.ReadBytes('\n');json.Unmarshal(b,&x);e:=json.NewEncoder(os.Stdout);e.Encode(map[string]any{"type":"monitor","monitor":map[string]any{"info":map[string]any{"name":"fake-worker"},"plan":map[string]any{"kind":"every","interval":1000000000},"state_version":1}});r.ReadBytes('\n');e.Encode(map[string]any{"type":"ready","ready":map[string]any{"generation":x.Hello.Generation}});f:=m.TransactionFrame{DeploymentID:x.Hello.DeploymentID,Generation:x.Hello.Generation,WorkerToken:x.Hello.WorkerToken,Sequence:1,BaseStateRevision:x.Hello.StateRevision,NextState:json.RawMessage(` + "`{\"committed\":true}`" + `),Checkpoints:map[string]json.RawMessage{"source":json.RawMessage(` + "`{\"cursor\":1}`" + `)},Events:[]m.Event{{ID:"event-1",Title:"one",DedupeKey:"record-1",DedupeFor:3600000000000}},Progress:true};sum:=m.HashTransactionFrame(f);f.PayloadHash=hex.EncodeToString(sum[:]);e.Encode(m.WorkerFrame{Type:"transaction",Transaction:&f});r.ReadBytes('\n');e.Encode(map[string]any{"type":"stopped","stopped":map[string]any{"generation":x.Hello.Generation,"clean":true}})}
`
