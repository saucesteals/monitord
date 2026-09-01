package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	monitord "github.com/saucesteals/monitord"
)

const (
	// describeTimeout bounds monitor introspection so a wedged binary cannot
	// hang a build, deploy, or state edit.
	describeTimeout = 10 * time.Second
)

// ValidateState checks candidate state against a deployed monitor binary and
// returns the canonical state that monitor would store.
//
// It runs the same introspection Build uses, so an externally edited state is
// held to the monitor's own struct instead of being accepted as bare JSON and
// failing on every later callback. Passing empty state returns the monitor's
// defaults.
func ValidateState(ctx context.Context, binaryPath string, dir string, state json.RawMessage) (json.RawMessage, error) {
	described, err := describe(ctx, binaryPath, dir, monitord.DescribeInput{
		State: state,
	})
	if err != nil {
		return nil, err
	}

	return described.State, nil
}

// Describe reports a built monitor's definition and canonical state.
func Describe(ctx context.Context, binaryPath string, dir string) (monitord.MonitorFrame, error) {
	return describe(ctx, binaryPath, dir, monitord.DescribeInput{})
}

// describe runs the monitor's introspection entrypoint, piping stored state in
// so the monitor's own types validate it.
func describe(ctx context.Context, binaryPath string, dir string, input monitord.DescribeInput) (monitord.MonitorFrame, error) {
	ctx, cancel := context.WithTimeout(ctx, describeTimeout)
	defer cancel()

	payload, err := json.Marshal(input)
	if err != nil {
		return monitord.MonitorFrame{}, fmt.Errorf("encode describe input: %w", err)
	}

	cmd := exec.CommandContext(ctx, binaryPath, monitord.FlagDescribe)
	cmd.Dir = dir
	cmd.Env = Env()
	cmd.Stdin = bytes.NewReader(payload)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return monitord.MonitorFrame{}, fmt.Errorf("describe monitor: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	var described monitord.MonitorFrame
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&described); err != nil {
		return monitord.MonitorFrame{}, fmt.Errorf("parse monitor description: %w", err)
	}
	if err := described.Info.Validate(); err != nil {
		return monitord.MonitorFrame{}, fmt.Errorf("invalid monitor description: %w", err)
	}
	if !json.Valid(described.State) {
		return monitord.MonitorFrame{}, fmt.Errorf("invalid monitor description state")
	}

	return described, nil
}

// Env is the minimal environment a monitor process needs. Monitor-specific
// configuration arrives over the worker handshake instead.
func Env() []string {
	var env []string
	for _, key := range []string{"PATH", "HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value := os.Getenv(key); value != "" {
			env = append(env, key+"="+value)
		}
	}

	return env
}
