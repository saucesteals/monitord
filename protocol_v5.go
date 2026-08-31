package monitord

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const ProtocolMajor = 5
const MaxFrameBytes = 4 << 20
const MaxEventsPerTransaction = 256
const MaxCheckpointsPerTransaction = 128
const MaxStateBytes = 2 << 20
const MaxCheckpointBytes = 256 << 10

type ProtocolVersion struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
}
type V5Hello struct {
	Version        ProtocolVersion              `json:"version"`
	DeploymentID   string                       `json:"deployment_id"`
	DeploymentName string                       `json:"deployment_name"`
	Generation     uint64                       `json:"generation"`
	WorkerToken    string                       `json:"worker_token"`
	ArtifactHash   string                       `json:"artifact_hash"`
	ConfigHash     string                       `json:"config_hash"`
	StateRevision  int64                        `json:"state_revision"`
	State          json.RawMessage              `json:"state"`
	Checkpoints    map[string]json.RawMessage   `json:"checkpoints,omitempty"`
	Secrets        map[string]map[string]string `json:"secrets,omitempty"`
	Capabilities   []string                     `json:"capabilities,omitempty"`
}
type V5Start struct {
	Plan PlanDescription `json:"plan"`
}
type V5Stop struct {
	Reason   string `json:"reason,omitempty"`
	Deadline string `json:"deadline,omitempty"`
}
type TransactionAck struct {
	DeploymentID   string `json:"deployment_id"`
	Generation     uint64 `json:"generation"`
	Sequence       uint64 `json:"sequence"`
	PayloadHash    string `json:"payload_hash"`
	ResultRevision int64  `json:"result_revision"`
	Status         string `json:"status"`
}
type V5Inbound struct {
	Type  string          `json:"type"`
	Hello *V5Hello        `json:"hello,omitempty"`
	Start *V5Start        `json:"start,omitempty"`
	Ack   *TransactionAck `json:"ack,omitempty"`
	Stop  *V5Stop         `json:"stop,omitempty"`
}
type TransactionFrame struct {
	DeploymentID      string                     `json:"deployment_id"`
	Generation        uint64                     `json:"generation"`
	WorkerToken       string                     `json:"worker_token"`
	Sequence          uint64                     `json:"sequence"`
	BaseStateRevision int64                      `json:"base_state_revision"`
	NextState         json.RawMessage            `json:"next_state"`
	Checkpoints       map[string]json.RawMessage `json:"checkpoints,omitempty"`
	Events            []Event                    `json:"events,omitempty"`
	Progress          bool                       `json:"progress,omitempty"`
	PayloadHash       string                     `json:"payload_hash"`
}
type MonitorFrame struct {
	Info         Info            `json:"info"`
	Plan         PlanDescription `json:"plan"`
	StateVersion int             `json:"state_version"`
	State        json.RawMessage `json:"state"`
}
type ReadyFrame struct {
	Generation uint64 `json:"generation"`
}
type RunFrame struct {
	AttemptID string `json:"attempt_id"`
	Child     string `json:"child"`
	Phase     string `json:"phase"`
	Error     string `json:"error,omitempty"`
}
type HealthFrame struct {
	Child   string `json:"child"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}
type StoppedFrame struct {
	Generation uint64 `json:"generation"`
	Clean      bool   `json:"clean"`
}
type V5Outbound struct {
	Type        string            `json:"type"`
	Monitor     *MonitorFrame     `json:"monitor,omitempty"`
	Ready       *ReadyFrame       `json:"ready,omitempty"`
	Transaction *TransactionFrame `json:"transaction,omitempty"`
	Run         *RunFrame         `json:"run,omitempty"`
	Health      *HealthFrame      `json:"health,omitempty"`
	Stopped     *StoppedFrame     `json:"stopped,omitempty"`
}

func DecodeV5Inbound(r io.Reader) (V5Inbound, error) {
	var v V5Inbound
	if err := strictDecode(r, &v); err != nil {
		return v, err
	}
	return v, v.Validate()
}
func DecodeV5Outbound(r io.Reader) (V5Outbound, error) {
	var v V5Outbound
	if err := strictDecode(r, &v); err != nil {
		return v, err
	}
	return v, v.Validate()
}
func strictDecode(r io.Reader, v any) error {
	limited := io.LimitReader(r, MaxFrameBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(raw) > MaxFrameBytes {
		return errors.New("protocol frame exceeds maximum size")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(v); err != nil {
		return fmt.Errorf("decode protocol frame: %w", err)
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return errors.New("protocol frame contains trailing data")
	}
	return nil
}
func (i V5Inbound) Validate() error {
	n := 0
	for _, p := range []any{i.Hello, i.Start, i.Ack, i.Stop} {
		if !isNil(p) {
			n++
		}
	}
	if n != 1 {
		return errors.New("inbound frame must contain exactly one payload")
	}
	switch i.Type {
	case "hello":
		if i.Hello == nil {
			return errors.New("hello payload missing")
		}
		if i.Hello.Version.Major != ProtocolMajor {
			return fmt.Errorf("unsupported protocol major %d", i.Hello.Version.Major)
		}
		if i.Hello.Version.Minor != 0 {
			return fmt.Errorf("unsupported protocol minor %d", i.Hello.Version.Minor)
		}
		if i.Hello.DeploymentID == "" || i.Hello.Generation == 0 || i.Hello.WorkerToken == "" {
			return errors.New("hello identity, generation, and token are required")
		}
		if len(i.Hello.State) == 0 || !json.Valid(i.Hello.State) {
			return errors.New("hello state must be valid JSON")
		}
		if len(i.Hello.State) > MaxStateBytes || len(i.Hello.DeploymentID) > 128 || len(i.Hello.DeploymentName) > 128 || len(i.Hello.WorkerToken) > 512 {
			return errors.New("hello field exceeds protocol limit")
		}
		if len(i.Hello.Checkpoints) > MaxCheckpointsPerTransaction {
			return errors.New("hello has too many checkpoints")
		}
		for source, raw := range i.Hello.Checkpoints {
			if source == "" || len(source) > 128 || len(raw) > MaxCheckpointBytes || !json.Valid(raw) {
				return fmt.Errorf("invalid hello checkpoint %q", source)
			}
		}
		if len(i.Hello.Capabilities) > 64 {
			return errors.New("too many protocol capabilities")
		}
		supported := map[string]bool{"transactions": true, "outbox": true, "health": true}
		seenCaps := map[string]bool{}
		for _, capability := range i.Hello.Capabilities {
			if !supported[capability] {
				return fmt.Errorf("unsupported capability %q", capability)
			}
			if seenCaps[capability] {
				return fmt.Errorf("duplicate capability %q", capability)
			}
			seenCaps[capability] = true
		}
		for _, required := range []string{"transactions", "outbox", "health"} {
			found := false
			for _, capability := range i.Hello.Capabilities {
				if capability == required {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("required capability %q was not negotiated", required)
			}
		}
	case "start":
		if i.Start == nil {
			return errors.New("start payload missing")
		}
		if err := i.Start.Plan.Validate(); err != nil {
			return fmt.Errorf("invalid start plan: %w", err)
		}
	case "ack":
		if i.Ack == nil {
			return errors.New("ack payload missing")
		}
		if i.Ack.DeploymentID == "" || i.Ack.Generation == 0 || i.Ack.Sequence == 0 || i.Ack.ResultRevision < 0 || !validSHA256(i.Ack.PayloadHash) || (i.Ack.Status != "accepted" && i.Ack.Status != "replayed") {
			return errors.New("invalid transaction ACK")
		}
	case "stop":
		if i.Stop == nil {
			return errors.New("stop payload missing")
		}
		if i.Stop.Deadline != "" {
			if _, err := time.Parse(time.RFC3339Nano, i.Stop.Deadline); err != nil {
				return errors.New("stop deadline is invalid")
			}
		}
	default:
		return fmt.Errorf("unsupported inbound frame type %q", i.Type)
	}
	return nil
}
func (o V5Outbound) Validate() error {
	n := 0
	for _, p := range []any{o.Monitor, o.Ready, o.Transaction, o.Run, o.Health, o.Stopped} {
		if !isNil(p) {
			n++
		}
	}
	if n != 1 {
		return errors.New("outbound frame must contain exactly one payload")
	}
	switch o.Type {
	case "monitor":
		if o.Monitor == nil {
			return errors.New("monitor payload missing")
		}
		if err := o.Monitor.Info.Validate(); err != nil {
			return err
		}
		if err := o.Monitor.Plan.Validate(); err != nil {
			return err
		}
		if o.Monitor.StateVersion < 1 {
			return errors.New("monitor state version must be positive")
		}
		if len(o.Monitor.State) > 0 && (len(o.Monitor.State) > MaxStateBytes || !json.Valid(o.Monitor.State)) {
			return errors.New("monitor state is invalid or too large")
		}
	case "ready":
		if o.Ready == nil {
			return errors.New("ready payload missing")
		}
		if o.Ready.Generation == 0 {
			return errors.New("ready generation is required")
		}
	case "transaction":
		if o.Transaction == nil {
			return errors.New("transaction payload missing")
		}
		t := o.Transaction
		if t.DeploymentID == "" || t.Generation == 0 || t.Sequence == 0 || t.WorkerToken == "" || !validSHA256(t.PayloadHash) || t.BaseStateRevision < 0 || len(t.NextState) == 0 || !json.Valid(t.NextState) {
			return errors.New("transaction identity, sequence, token, and hash are required")
		}
		if len(t.NextState) > MaxStateBytes {
			return errors.New("transaction state exceeds limit")
		}
		if len(t.Checkpoints) > MaxCheckpointsPerTransaction {
			return errors.New("transaction has too many checkpoints")
		}
		for source, raw := range t.Checkpoints {
			if source == "" || len(source) > 128 || len(raw) > MaxCheckpointBytes || !json.Valid(raw) {
				return fmt.Errorf("invalid transaction checkpoint %q", source)
			}
		}
		if len(t.Events) > MaxEventsPerTransaction {
			return errors.New("transaction has too many events")
		}
		for index, event := range t.Events {
			if err := event.Validate(); err != nil {
				return fmt.Errorf("event %d: %w", index, err)
			}
		}
	case "run":
		if o.Run == nil {
			return errors.New("run payload missing")
		}
		if o.Run.AttemptID == "" || o.Run.Child == "" || (o.Run.Phase != "started" && o.Run.Phase != "finished" && o.Run.Phase != "success" && o.Run.Phase != "failure" && o.Run.Phase != "canceled") {
			return errors.New("invalid run frame")
		}
	case "health":
		if o.Health == nil {
			return errors.New("health payload missing")
		}
		if o.Health.Child == "" || (o.Health.Status != "ready" && o.Health.Status != "healthy" && o.Health.Status != "degraded" && o.Health.Status != "failed" && o.Health.Status != "terminal" && o.Health.Status != "restarting" && o.Health.Status != "stopped") {
			return errors.New("invalid health frame")
		}
	case "stopped":
		if o.Stopped == nil {
			return errors.New("stopped payload missing")
		}
		if o.Stopped.Generation == 0 {
			return errors.New("stopped generation is required")
		}
	default:
		return fmt.Errorf("unsupported outbound frame type %q", o.Type)
	}
	return nil
}
func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (p PlanDescription) Validate() error {
	if p.Timeout < 0 {
		return errors.New("plan timeout cannot be negative")
	}
	if len(p.Secrets) > 128 {
		return errors.New("plan has too many secret refs")
	}
	if _, err := normalizeSecretRefs(p.Secrets); err != nil {
		return err
	}
	switch p.Kind {
	case "continuous":
		if p.Name != "" || p.Interval != 0 || len(p.Children) != 0 {
			return errors.New("invalid continuous plan shape")
		}
	case "every":
		if p.Name != "" || p.Interval <= 0 || len(p.Children) != 0 {
			return errors.New("invalid polling plan shape")
		}
	case "named":
		if len(p.Name) > 63 || !infoNamePattern.MatchString(p.Name) || len(p.Children) != 1 || p.Interval != 0 || p.Timeout != 0 || len(p.Secrets) != 0 {
			return errors.New("invalid named plan shape")
		}
		if err := p.Children[0].Validate(); err != nil {
			return err
		}
		if p.Children[0].Kind == "named" || p.Children[0].Kind == "combined" {
			return errors.New("invalid named child")
		}
	case "combined":
		if p.Name != "" || p.Interval != 0 || p.Timeout != 0 || len(p.Secrets) != 0 || len(p.Children) == 0 {
			return errors.New("invalid combined plan shape")
		}
		seen := map[string]bool{}
		for _, child := range p.Children {
			if child.Kind != "named" {
				return errors.New("combined child is not named")
			}
			if seen[child.Name] {
				return fmt.Errorf("duplicate child name %q", child.Name)
			}
			seen[child.Name] = true
			if err := child.Validate(); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported plan kind %q", p.Kind)
	}
	return nil
}
func isNil(v any) bool {
	switch x := v.(type) {
	case *V5Hello:
		return x == nil
	case *V5Start:
		return x == nil
	case *TransactionAck:
		return x == nil
	case *V5Stop:
		return x == nil
	case *MonitorFrame:
		return x == nil
	case *ReadyFrame:
		return x == nil
	case *TransactionFrame:
		return x == nil
	case *RunFrame:
		return x == nil
	case *HealthFrame:
		return x == nil
	case *StoppedFrame:
		return x == nil
	}
	return false
}
