package monitord

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
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
