// Package monitor builds monitor sources into immutable artifacts and checks
// their executable contract.
package monitor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// BuildResult is an immutable artifact plus deployment-local canonical state.
// State is deliberately excluded from Artifact.Describe so redeploying the
// same code never changes content-addressed artifact metadata.
type BuildResult struct {
	Artifact    storage.Artifact
	Description monitord.MonitorFrame
	State       json.RawMessage
}

// Build compiles and describes an immutable artifact without creating or
// mutating a deployment. The published binary becomes a reusable cache entry;
// its database row is created only by a successful atomic deployment.
func Build(ctx context.Context, paths config.Paths, req Request) (BuildResult, error) {
	dir, err := validateDir(req)
	if err != nil {
		return BuildResult{}, err
	}
	if err = config.Tidy(ctx, paths); err != nil {
		return BuildResult{}, err
	}
	// Artifacts are identified globally by their binary hash, so their path is
	// global as well. The source monitor name must never affect content identity.
	artifactRoot := paths.ArtifactsDir
	if err = os.MkdirAll(artifactRoot, 0o700); err != nil {
		return BuildResult{}, fmt.Errorf("create artifact root: %w", err)
	}
	buildDir, err := os.MkdirTemp(artifactRoot, ".build-")
	if err != nil {
		return BuildResult{}, fmt.Errorf("create build directory: %w", err)
	}
	defer os.RemoveAll(buildDir)
	binaryPath := filepath.Join(buildDir, "monitor")
	if err = build(ctx, dir, binaryPath); err != nil {
		return BuildResult{}, err
	}
	description, err := describe(ctx, binaryPath, dir, monitord.DescribeInput{State: req.Current, Version: req.CurrentVersion})
	if err != nil {
		return BuildResult{}, err
	}
	state := append(json.RawMessage(nil), description.State...)
	description.State = nil
	describeJSON, err := json.Marshal(description)
	if err != nil {
		return BuildResult{}, fmt.Errorf("encode description: %w", err)
	}
	contents, err := os.ReadFile(binaryPath)
	if err != nil {
		return BuildResult{}, fmt.Errorf("read built monitor: %w", err)
	}
	sum := sha256.Sum256(contents)
	contentHash := hex.EncodeToString(sum[:])
	artifactDir := filepath.Join(artifactRoot, contentHash)
	finalPath := filepath.Join(artifactDir, "monitor")
	if err = os.Rename(buildDir, artifactDir); err != nil {
		if _, statErr := os.Stat(finalPath); statErr != nil {
			return BuildResult{}, fmt.Errorf("publish artifact: %w", err)
		}
	}
	return BuildResult{Artifact: storage.Artifact{ID: contentHash, ContentHash: contentHash, Path: finalPath, Describe: describeJSON}, Description: description, State: state}, nil
}

func validateDir(req Request) (string, error) {
	if err := req.Name.Validate(); err != nil {
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
