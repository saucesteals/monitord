package monitord

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestStrictProtocolDecode(t *testing.T) {
	good := `{"type":"hello","hello":{"version":{"major":5,"minor":0},"deployment_id":"d","deployment_name":"n","generation":1,"worker_token":"secret","artifact_hash":"a","config_hash":"c","state_revision":0,"state":{},"capabilities":["transactions","outbox","health"]}}`
	if _, err := DecodeDaemonFrame(strings.NewReader(good)); err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(good, `"state":{}`, `"state":{},"unknown":true`, 1)
	if _, err := DecodeDaemonFrame(strings.NewReader(bad)); err == nil {
		t.Fatal("unknown field accepted")
	}
	tooBig := bytes.NewReader(bytes.Repeat([]byte("x"), MaxFrameBytes+1))
	if _, err := DecodeDaemonFrame(tooBig); err == nil {
		t.Fatal("oversize accepted")
	}
}

func TestRuntimeMonitorFrameMayOmitDescribeState(t *testing.T) {
	frame := WorkerFrame{Type: "monitor", Monitor: &MonitorFrame{Info: Info{Name: "runtime"}, Plan: PlanDescription{Kind: "continuous"}, StateVersion: 1}}
	if err := frame.Validate(); err != nil {
		t.Fatalf("runtime monitor frame rejected: %v", err)
	}
}

func TestProtocolSemanticBounds(t *testing.T) {
	if err := (PlanDescription{Kind: "combined", Children: []PlanDescription{{Kind: "every", Interval: time.Second}}}).Validate(); err == nil {
		t.Fatal("accepted unnamed combined child")
	}
	tx := TransactionFrame{DeploymentID: "d", Generation: 1, WorkerToken: "t", Sequence: 1, NextState: json.RawMessage(`{}`), PayloadHash: strings.Repeat("a", 64), Events: make([]Event, MaxEventsPerTransaction+1)}
	if err := (WorkerFrame{Type: "transaction", Transaction: &tx}).Validate(); err == nil {
		t.Fatal("accepted excessive events")
	}
	hello := DaemonFrame{Type: "hello", Hello: &Hello{Version: ProtocolVersion{Major: ProtocolMajor}, DeploymentID: "d", Generation: 1, WorkerToken: "t", State: json.RawMessage(`{}`), Capabilities: []string{"transactions", "outbox", "health", "unknown"}}}
	if err := hello.Validate(); err == nil {
		t.Fatal("accepted unknown capability")
	}
}

// Compile-time generic inference and explicit instantiation fixtures.
