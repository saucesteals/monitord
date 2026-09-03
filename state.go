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

// decodeState builds a typed state value from stored JSON.
//
// Unknown fields are rejected so a monitor whose struct drifted from its stored
// state fails loudly instead of silently dropping data. Incompatible state is
// replaced or cleared explicitly through the CLI.
func decodeState[S any](raw json.RawMessage) (*S, error) {
	state := new(S)
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
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
