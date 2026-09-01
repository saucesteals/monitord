package monitord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

type SecretRef struct {
	Group    string `json:"group"`
	Key      string `json:"key"`
	Required bool   `json:"required,omitempty"`
}

func RequiredSecret(group, key string) SecretRef {
	return SecretRef{Group: group, Key: key, Required: true}
}
func OptionalSecret(group, key string) SecretRef { return SecretRef{Group: group, Key: key} }
func WithSecrets(refs ...SecretRef) CommonOption {
	return optionFunc(func(o *commonOptions) error {
		for _, r := range refs {
			if err := r.Validate(); err != nil {
				return err
			}
		}
		o.secrets = append(o.secrets, refs...)
		return nil
	})
}
func (r SecretRef) Validate() error {
	if !infoNamePattern.MatchString(r.Group) {
		return fmt.Errorf("secret group %q must be lower-case kebab case", r.Group)
	}
	if !infoNamePattern.MatchString(r.Key) {
		return fmt.Errorf("secret key %q must be lower-case kebab case", r.Key)
	}
	return nil
}
func normalizeSecretRefs(in []SecretRef) ([]SecretRef, error) {
	m := map[string]SecretRef{}
	for _, r := range in {
		if err := r.Validate(); err != nil {
			return nil, err
		}
		k := r.Group + "\x00" + r.Key
		if old, ok := m[k]; ok {
			r.Required = r.Required || old.Required
		}
		m[k] = r
	}
	out := make([]SecretRef, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	return out, nil
}

type SecretSet interface {
	Get(SecretRef) (string, bool)
	Require(SecretRef) (string, error)
}
type secretSet map[string]string

func (s secretSet) Get(ref SecretRef) (string, bool) {
	v, ok := s[ref.Group+"\x00"+ref.Key]
	return v, ok
}
func (s secretSet) Require(ref SecretRef) (string, error) {
	v, ok := s.Get(ref)
	if !ok || v == "" {
		return "", fmt.Errorf("required secret %s/%s is unavailable", ref.Group, ref.Key)
	}
	return v, nil
}

type Tx[S any] struct {
	State       *S
	events      []Event
	checkpoints map[string]json.RawMessage
	maxEvents   int
}

func (tx *Tx[S]) Emit(e Event) error {
	if len(tx.events) >= tx.maxEvents {
		return fmt.Errorf("transaction cannot emit more than %d events", tx.maxEvents)
	}
	if err := e.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("clone event: %w", err)
	}
	var clone Event
	if err = json.Unmarshal(raw, &clone); err != nil {
		return err
	}
	tx.events = append(tx.events, clone)
	return nil
}
func (tx *Tx[S]) Checkpoint(source string, value any) error {
	if source == "" {
		return errors.New("checkpoint source is required")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode checkpoint %q: %w", source, err)
	}
	if tx.checkpoints == nil {
		tx.checkpoints = map[string]json.RawMessage{}
	}
	tx.checkpoints[source] = append(json.RawMessage(nil), raw...)
	return nil
}

type transactionCommit struct {
	BaseState   json.RawMessage
	NextState   json.RawMessage
	Events      []Event
	Checkpoints map[string]json.RawMessage
}
type sessionCommitter interface {
	Commit(context.Context, transactionCommit) (json.RawMessage, error)
}
type localCommitter struct {
	mu    sync.Mutex
	state json.RawMessage
}

func (l *localCommitter) Commit(ctx context.Context, c transactionCommit) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state = append(l.state[:0], c.NextState...)
	return append(json.RawMessage(nil), l.state...), nil
}

type Session[S any] struct {
	mu          sync.RWMutex
	state       json.RawMessage
	checkpoints map[string]json.RawMessage
	secrets     secretSet
	committer   sessionCommitter
	maxEvents   int
}

func newSession[S any](state json.RawMessage, secrets map[string]string, maxEvents int, committer sessionCommitter) (*Session[S], error) {
	if maxEvents < 1 || maxEvents > MaxEventsPerTransaction {
		return nil, fmt.Errorf("max events per transaction must be between 1 and %d", MaxEventsPerTransaction)
	}
	v, err := decodeState[S](state)
	if err != nil {
		return nil, err
	}
	raw, err := encodeState(v)
	if err != nil {
		return nil, err
	}
	ss := secretSet{}
	for k, v := range secrets {
		ss[k] = v
	}
	if committer == nil {
		committer = &localCommitter{state: raw}
	}
	return &Session[S]{state: raw, checkpoints: map[string]json.RawMessage{}, secrets: ss, committer: committer, maxEvents: maxEvents}, nil
}

// Checkpoint decodes a fresh copy of a durable source checkpoint into out.
func (s *Session[S]) Checkpoint(source string, out any) (bool, error) {
	if source == "" || out == nil {
		return false, errors.New("checkpoint source and output are required")
	}
	s.mu.RLock()
	raw, ok := s.checkpoints[source]
	raw = append(json.RawMessage(nil), raw...)
	s.mu.RUnlock()
	if !ok {
		return false, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return false, fmt.Errorf("decode checkpoint %q: %w", source, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("decode checkpoint %q: trailing JSON value", source)
	}
	return true, nil
}
func (s *Session[S]) State() S {
	s.mu.RLock()
	raw := append(json.RawMessage(nil), s.state...)
	s.mu.RUnlock()
	v, err := decodeState[S](raw)
	if err != nil {
		panic(fmt.Sprintf("monitord: canonical state became invalid: %v", err))
	}
	return *v
}
func (s *Session[S]) Secrets() SecretSet { return s.secrets }
func (s *Session[S]) Commit(ctx context.Context, fn func(*Tx[S]) error) error {
	if fn == nil {
		return errors.New("commit closure is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	base := append(json.RawMessage(nil), s.state...)
	state, err := decodeState[S](base)
	if err != nil {
		return err
	}
	tx := &Tx[S]{State: state, maxEvents: s.maxEvents}
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("commit closure panicked: %v", r)
			}
		}()
		err = fn(tx)
	}()
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	next, err := encodeState(tx.State)
	if err != nil {
		return err
	}
	acked, err := s.committer.Commit(ctx, transactionCommit{BaseState: base, NextState: next, Events: tx.events, Checkpoints: tx.checkpoints})
	if err != nil {
		return err
	}
	decoded, err := decodeState[S](acked)
	if err != nil {
		return fmt.Errorf("invalid acknowledged state: %w", err)
	}
	s.state, err = encodeState(decoded)
	if err == nil {
		for source, raw := range tx.checkpoints {
			s.checkpoints[source] = append(json.RawMessage(nil), raw...)
		}
	}
	return err
}
