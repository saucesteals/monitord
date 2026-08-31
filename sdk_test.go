package monitord

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

type isolationState struct {
	Values map[string][]int `json:"values"`
	Number *big.Int         `json:"number"`
}
type captureCommitter struct{ commits []transactionCommit }

func (c *captureCommitter) Commit(_ context.Context, v transactionCommit) (json.RawMessage, error) {
	c.commits = append(c.commits, v)
	return v.NextState, nil
}

func TestSessionSerializationIsolationAndAtomicEffects(t *testing.T) {
	capture := new(captureCommitter)
	s, err := newSession[isolationState](json.RawMessage(`{"values":{"a":[1]},"number":7}`), nil, capture)
	if err != nil {
		t.Fatal(err)
	}
	copy1 := s.State()
	copy1.Values["a"][0] = 99
	copy1.Number.SetInt64(99)
	copy2 := s.State()
	if copy2.Values["a"][0] != 1 || copy2.Number.Int64() != 7 {
		t.Fatal("State returned aliased canonical data")
	}
	payload := []int{2}
	if err = s.Commit(context.Background(), func(tx *Tx[isolationState]) error {
		tx.State.Values["b"] = payload
		return tx.Emit(Event{ID: "one", Title: "one", Fields: []Field{{Name: "x", Value: "before"}}})
	}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 42
	if got := s.State().Values["b"][0]; got != 2 {
		t.Fatalf("committed state mutated: %d", got)
	}
	before := string(capture.commits[0].NextState)
	err = s.Commit(context.Background(), func(tx *Tx[isolationState]) error { tx.State.Values["a"][0] = 8; panic("boom") })
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("panic error=%v", err)
	}
	if string(capture.commits[0].NextState) != before || len(capture.commits) != 1 {
		t.Fatal("failed closure published effects")
	}
}

func TestPlanValidationAndSecrets(t *testing.T) {
	fn := func(context.Context, *Session[struct{}]) error { return nil }
	p := Combined(Named("required", Continuous(fn, WithSecrets(OptionalSecret("api", "TOKEN"), RequiredSecret("api", "TOKEN")))), Named("optional", Every(time.Minute, fn), Optional()))
	d, err := validateMonitor(Define(Info{Name: "valid-monitor"}, p))
	if err != nil {
		t.Fatal(err)
	}
	refs := d.SecretRefs()
	if len(refs) != 1 || !refs[0].Required {
		t.Fatalf("refs=%+v", refs)
	}
	cases := []Monitor[struct{}]{Define(Info{Name: "Bad"}, Continuous(fn)), Define(Info{Name: "ok"}, Plan[struct{}]{}), Define(Info{Name: "ok"}, Combined[struct{}]()), Define(Info{Name: "ok"}, Combined(Continuous(fn))), Define(Info{Name: "ok"}, Combined(Named("x", Continuous(fn)), Named("x", Continuous(fn))))}
	for i, m := range cases {
		if _, err := validateMonitor(m); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}

func TestStrictProtocolDecode(t *testing.T) {
	good := `{"type":"hello","hello":{"version":{"major":5,"minor":0},"deployment_id":"d","deployment_name":"n","generation":1,"worker_token":"secret","artifact_hash":"a","config_hash":"c","state_revision":0,"state":{},"capabilities":["transactions","outbox","health"]}}`
	if _, err := DecodeV5Inbound(strings.NewReader(good)); err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(good, `"state":{}`, `"state":{},"unknown":true`, 1)
	if _, err := DecodeV5Inbound(strings.NewReader(bad)); err == nil {
		t.Fatal("unknown field accepted")
	}
	tooBig := bytes.NewReader(bytes.Repeat([]byte("x"), MaxFrameBytes+1))
	if _, err := DecodeV5Inbound(tooBig); err == nil {
		t.Fatal("oversize accepted")
	}
}

func TestRuntimeMonitorFrameMayOmitDescribeState(t *testing.T) {
	frame := V5Outbound{Type: "monitor", Monitor: &MonitorFrame{Info: Info{Name: "runtime"}, Plan: PlanDescription{Kind: "continuous"}, StateVersion: 1}}
	if err := frame.Validate(); err != nil {
		t.Fatalf("runtime monitor frame rejected: %v", err)
	}
}

func TestProtocolSemanticBounds(t *testing.T) {
	if err := (PlanDescription{Kind: "combined", Children: []PlanDescription{{Kind: "every", Interval: time.Second}}}).Validate(); err == nil {
		t.Fatal("accepted unnamed combined child")
	}
	tx := TransactionFrame{DeploymentID: "d", Generation: 1, WorkerToken: "t", Sequence: 1, NextState: json.RawMessage(`{}`), PayloadHash: strings.Repeat("a", 64), Events: make([]Event, MaxEventsPerTransaction+1)}
	if err := (V5Outbound{Type: "transaction", Transaction: &tx}).Validate(); err == nil {
		t.Fatal("accepted excessive events")
	}
	hello := V5Inbound{Type: "hello", Hello: &V5Hello{Version: ProtocolVersion{Major: ProtocolMajor}, DeploymentID: "d", Generation: 1, WorkerToken: "t", State: json.RawMessage(`{}`), Capabilities: []string{"transactions", "outbox", "health", "unknown"}}}
	if err := hello.Validate(); err == nil {
		t.Fatal("accepted unknown capability")
	}
}

// Compile-time generic inference and explicit instantiation fixtures.
type compileMonitor struct{}

func (compileMonitor) Info() Info { return Info{Name: "compile-monitor"} }
func (compileMonitor) Plan() Plan[struct{}] {
	return Continuous(func(context.Context, *Session[struct{}]) error { return nil })
}

var _ Monitor[struct{}] = compileMonitor{}
var _ = func() {
	if false {
		Run(compileMonitor{})
		Run[struct{}](compileMonitor{})
	}
}
