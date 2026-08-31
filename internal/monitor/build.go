// Package monitor builds monitor sources into immutable artifacts and checks
// their executable contract.
package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/storage"
)

const (
	// buildTimeout bounds compilation.
	buildTimeout = 5 * time.Minute
)

// Request describes one monitor build.
type Request struct {
	// Dir is the monitor's source directory.
	Dir    string
	Name   model.MonitorName
	Config Config

	// Current is the monitor's existing state, if it was deployed before. The
	// new binary validates and migrates it before the deploy is accepted.
	Current json.RawMessage
	// CurrentVersion is the schema version Current was written with.
	CurrentVersion int
}

// V5Build is an immutable artifact plus deployment-local canonical state.
// State is deliberately excluded from Artifact.Describe so redeploying the
// same code never changes content-addressed artifact metadata.
type V5Build struct {
	Artifact    storage.Artifact
	Description monitord.MonitorFrame
	State       json.RawMessage
}

// BuildV5 compiles and describes an immutable V5 artifact without creating or
// mutating a deployment. Callers persist Artifact first, then atomically create
// or redeploy using State and Description.StateVersion.
func BuildV5(ctx context.Context, paths config.Paths, req Request) (V5Build, error) {
	dir, err := validateDir(req)
	if err != nil {
		return V5Build{}, err
	}
	fingerprint, err := fingerprintDir(dir)
	if err != nil {
		return V5Build{}, err
	}
	artifactDir := filepath.Join(paths.ArtifactDir(req.Name), fingerprint)
	if err = os.MkdirAll(artifactDir, 0o700); err != nil {
		return V5Build{}, fmt.Errorf("create artifact dir: %w", err)
	}
	if err = config.Tidy(ctx, paths); err != nil {
		return V5Build{}, err
	}
	binaryPath := filepath.Join(artifactDir, "monitor")
	if err = build(ctx, dir, binaryPath); err != nil {
		return V5Build{}, err
	}
	description, err := describe(ctx, binaryPath, dir, monitord.DescribeInput{State: req.Current, Version: req.CurrentVersion})
	if err != nil {
		return V5Build{}, err
	}
	state := append(json.RawMessage(nil), description.State...)
	description.State = nil
	describeJSON, err := json.Marshal(description)
	if err != nil {
		return V5Build{}, fmt.Errorf("encode V5 description: %w", err)
	}
	return V5Build{Artifact: storage.Artifact{ID: fingerprint, ContentHash: fingerprint, Path: binaryPath, Describe: describeJSON}, Description: description, State: state}, nil
}

func validateDir(req Request) (string, error) {
	if err := req.Name.Validate(); err != nil {
		return "", err
	}
	if err := req.Config.ProxyPool.Validate(); err != nil {
		return "", err
	}
	if len(req.Config.Deliveries) == 0 {
		return "", errors.New("monitor requires at least one delivery")
	}

	dir, err := filepath.Abs(req.Dir)
	if err != nil {
		return "", fmt.Errorf("resolve monitor dir: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("stat monitor dir: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("monitor source %s must be a directory", dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("read monitor dir: %w", err)
	}
	hasGo := slices.ContainsFunc(entries, func(e fs.DirEntry) bool {
		return !e.IsDir() && strings.HasSuffix(e.Name(), ".go")
	})
	if !hasGo {
		return "", fmt.Errorf("monitor dir %s contains no .go files", dir)
	}

	return dir, nil
}

func build(ctx context.Context, dir string, binaryPath string) error {
	ctx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, ".")
	cmd.Dir = dir
	// Builds are untrusted with respect to the daemon's credentials. Keep only
	// the toolchain environment; monitor secrets are resolved after activation
	// and never enter compiler subprocesses.
	cmd.Env = append(Env(), "GOWORK=off")

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build monitor: %w\n%s", err, strings.TrimSpace(stderr.String()))
	}

	return nil
}
