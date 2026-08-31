package monitord

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

type runtimeState struct {
	Count int `json:"count"`
}
type runtimeMonitor struct{}

func (runtimeMonitor) Info() Info { return Info{Name: "runtime-test"} }
func (runtimeMonitor) Plan() Plan[runtimeState] {
	return Continuous(func(ctx context.Context, s *Session[runtimeState]) error {
		if err := s.Commit(ctx, func(tx *Tx[runtimeState]) error { tx.State.Count++; tx.Progress(); return nil }); err != nil {
			return err
		}
		return Permanent(io.EOF)
	})
}

func TestWorkerHandshakeCommitAckAndStop(t *testing.T) {
	workerIn, daemonOut := io.Pipe()
	daemonIn, workerOut := io.Pipe()
	done := make(chan error, 1)
	desc, err := validateMonitor[runtimeState](runtimeMonitor{})
	if err != nil {
		t.Fatal(err)
	}
	go func() { done <- serveV5[runtimeState](runtimeMonitor{}, desc, workerIn, workerOut) }()
	enc := json.NewEncoder(daemonOut)
	read := bufio.NewReader(daemonIn)
	hello := V5Inbound{Type: "hello", Hello: &V5Hello{Version: ProtocolVersion{Major: 5}, DeploymentID: "deployment", DeploymentName: "test", Generation: 2, WorkerToken: "token", ArtifactHash: "artifact", ConfigHash: "config", StateRevision: 7, State: json.RawMessage(`{"count":0}`), Capabilities: []string{"transactions", "outbox", "health"}}}
	if err = enc.Encode(hello); err != nil {
		t.Fatal(err)
	}
	if out := readOutboundLine(t, read); out.Type != "monitor" {
		t.Fatalf("got %s", out.Type)
	}
	if err = enc.Encode(V5Inbound{Type: "start", Start: &V5Start{Plan: desc}}); err != nil {
		t.Fatal(err)
	}
	if out := readOutboundLine(t, read); out.Type != "ready" {
		t.Fatalf("got %s", out.Type)
	}
	txout := readOutboundLine(t, read)
	if txout.Type != "transaction" || txout.Transaction.Sequence != 1 || txout.Transaction.BaseStateRevision != 7 {
		t.Fatalf("transaction=%+v", txout.Transaction)
	}
	if !txout.Transaction.Progress || !strings.Contains(string(txout.Transaction.NextState), `"count":1`) {
		t.Fatalf("transaction=%+v", txout.Transaction)
	}
	ack := TransactionAck{DeploymentID: "deployment", Generation: 2, Sequence: 1, PayloadHash: txout.Transaction.PayloadHash, ResultRevision: 8, Status: "accepted"}
	if err = enc.Encode(V5Inbound{Type: "ack", Ack: &ack}); err != nil {
		t.Fatal(err)
	}
	seenStopped := false
	deadline := time.After(2 * time.Second)
	for !seenStopped {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for stopped")
		default:
			out := readOutboundLine(t, read)
			seenStopped = out.Type == "stopped"
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerResendsExactTransactionBytes(t *testing.T) {
	workerIn, daemonOut := io.Pipe()
	daemonIn, workerOut := io.Pipe()
	desc, _ := validateMonitor[runtimeState](runtimeMonitor{})
	go serveV5[runtimeState](runtimeMonitor{}, desc, workerIn, workerOut)
	enc := json.NewEncoder(daemonOut)
	read := bufio.NewReader(daemonIn)
	_ = enc.Encode(V5Inbound{Type: "hello", Hello: &V5Hello{Version: ProtocolVersion{Major: 5}, DeploymentID: "d", Generation: 1, WorkerToken: "t", State: json.RawMessage(`{}`), Capabilities: []string{"transactions", "outbox", "health"}}})
	_, _ = read.ReadBytes('\n')
	_ = enc.Encode(V5Inbound{Type: "start", Start: &V5Start{Plan: desc}})
	_, _ = read.ReadBytes('\n')
	first, err := read.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	// Stop cancels callbacks but must leave the control reader alive so this
	// already-admitted frame can be resolved.
	_ = enc.Encode(V5Inbound{Type: "stop", Stop: &V5Stop{Reason: "shutdown"}})
	second, err := read.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("retry did not retain exact frame bytes")
	}
	var out V5Outbound
	if err = json.Unmarshal(first, &out); err != nil {
		t.Fatal(err)
	}
	_ = enc.Encode(V5Inbound{Type: "ack", Ack: &TransactionAck{DeploymentID: "d", Generation: 1, Sequence: 1, PayloadHash: out.Transaction.PayloadHash, ResultRevision: 1, Status: "replayed"}})
	for {
		if readOutboundLine(t, read).Type == "stopped" {
			break
		}
	}
}

func TestCommitCancellationAdmissionBoundary(t *testing.T) {
	capture := new(captureCommitter)
	s, err := newSession[runtimeState](json.RawMessage(`{}`), nil, capture)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Commit(canceled, func(tx *Tx[runtimeState]) error { tx.State.Count++; return nil }); err != context.Canceled {
		t.Fatalf("error=%v", err)
	}
	if len(capture.commits) != 0 {
		t.Fatal("canceled pre-admission commit was published")
	}

	reader, writer := io.Pipe()
	coord := &workerCoordinator{wire: &wire{w: writer}, hello: V5Hello{DeploymentID: "d", Generation: 1, WorkerToken: "t"}, acks: make(chan TransactionAck, 1), fatal: make(chan error, 1), progress: make(chan struct{}, 1)}
	ctx, cancelAfterSend := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := coord.Commit(ctx, transactionCommit{NextState: json.RawMessage(`{"count":1}`)})
		result <- err
	}()
	br := bufio.NewReader(reader)
	first, err := br.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	cancelAfterSend()
	second, err := br.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("post-admission cancellation changed retry bytes")
	}
	var outbound V5Outbound
	if err = json.Unmarshal(first, &outbound); err != nil {
		t.Fatal(err)
	}
	coord.acks <- TransactionAck{DeploymentID: "d", Generation: 1, Sequence: 1, PayloadHash: outbound.Transaction.PayloadHash, ResultRevision: 1, Status: "replayed"}
	if err := <-result; err != nil {
		t.Fatalf("post-admission cancellation prevented ACK recovery: %v", err)
	}
}

func TestHashTransactionFrameCanonicalMapOrder(t *testing.T) {
	a := TransactionFrame{DeploymentID: "d", Generation: 1, Sequence: 1, NextState: json.RawMessage(`{}`), Checkpoints: map[string]json.RawMessage{"a": json.RawMessage(`1`), "b": json.RawMessage(`2`)}}
	b := TransactionFrame{DeploymentID: "d", Generation: 1, Sequence: 1, NextState: json.RawMessage(`{}`), Checkpoints: map[string]json.RawMessage{"b": json.RawMessage(`2`), "a": json.RawMessage(`1`)}}
	if HashTransactionFrame(a) != HashTransactionFrame(b) {
		t.Fatal("checkpoint insertion order changed canonical hash")
	}
	b.Checkpoints["a"] = json.RawMessage(`3`)
	if HashTransactionFrame(a) == HashTransactionFrame(b) {
		t.Fatal("semantic mutation did not change hash")
	}
}

func readOutboundLine(t *testing.T, r *bufio.Reader) V5Outbound {
	t.Helper()
	raw, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var out V5Outbound
	if err = json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
