package monitord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// NoState is the state type for monitors that keep no durable data.
type NoState = struct{}

// Versioned is implemented by state types that track a schema version.
// Monitors that never change their state shape can omit it; version 1 is
// assumed.
//
//	func (State) StateVersion() int { return 2 }
type Versioned interface {
	StateVersion() int
}

// Migrator is implemented by state types that can upgrade state written by an
// older version of the monitor. Deploy fails when the stored version differs
// and the state type does not implement Migrator.
//
//	func (s *State) MigrateState(from int, raw json.RawMessage) error { ... }
type Migrator interface {
	MigrateState(from int, raw json.RawMessage) error
}

// stateVersion reports the schema version declared by S, defaulting to 1.
func stateVersion[S any]() int {
	var zero S
	if v, ok := any(&zero).(Versioned); ok {
		return v.StateVersion()
	}

	return 1
}

// decodeState builds a typed state value from stored JSON.
//
// Unknown fields are rejected so a monitor whose struct drifted from its stored
// state fails loudly at deploy instead of silently dropping data. When the
// stored version differs from the current one, S must implement Migrator.
func decodeState[S any](raw json.RawMessage, from int) (*S, error) {
	state := new(S)
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return state, nil
	}

	current := stateVersion[S]()
	if from != 0 && from != current {
		migrator, ok := any(state).(Migrator)
		if !ok {
			return nil, fmt.Errorf("stored state is version %d but monitor is version %d and %T does not implement monitord.Migrator", from, current, state)
		}
		if err := migrator.MigrateState(from, raw); err != nil {
			return nil, fmt.Errorf("migrate state from version %d to %d: %w", from, current, err)
		}

		return state, nil
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(state); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode state: trailing JSON value")
	}

	return state, nil
}

// encodeState marshals typed state for storage.
func encodeState[S any](state *S) (json.RawMessage, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode state: %w", err)
	}

	return raw, nil
}
