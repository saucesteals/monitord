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
	"github.com/saucesteals/monitord/internal/routes"
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

// Build compiles the monitor package, introspects the binary, and returns the
// monitor record to persist.
func Build(ctx context.Context, paths config.Paths, req Request) (storage.Monitor, error) {
	dir, err := validateDir(req)
	if err != nil {
		return storage.Monitor{}, err
	}

	fingerprint, err := fingerprintDir(dir)
	if err != nil {
		return storage.Monitor{}, err
	}

	artifactDir := filepath.Join(paths.ArtifactDir(req.Name), fingerprint)
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return storage.Monitor{}, fmt.Errorf("create artifact dir: %w", err)
	}

	// Resolve dependencies first so a hand-created or copied monitor directory
	// builds without the author having to touch the shared go.mod.
	if err := config.Tidy(ctx, paths); err != nil {
		return storage.Monitor{}, err
	}

	binaryPath := filepath.Join(artifactDir, "monitor")
	if err := build(ctx, dir, binaryPath); err != nil {
		return storage.Monitor{}, err
	}

	described, err := describe(ctx, binaryPath, dir, monitord.DescribeInput{
		State:   req.Current,
		Version: req.CurrentVersion,
	})
	if err != nil {
		return storage.Monitor{}, err
	}

	def := described.Definition.WithDefaults()
	def.Name = req.Name.String()
	def.Description = req.Config.Description
	def.Clients = req.Config.Clients
	def.Persistent = req.Config.Persistent
	if err := def.Validate(); err != nil {
		return storage.Monitor{}, err
	}

	now := time.Now().UTC()
	var expiresAt *time.Time
	ttlSeconds := int64(req.Config.TTL.Seconds())
	if req.Config.Persistent {
		ttlSeconds = 0
	} else {
		expires := now.Add(req.Config.TTL)
		expiresAt = &expires
	}

	return storage.Monitor{
		Name:            req.Name,
		SourceDir:       dir,
		ArtifactDir:     artifactDir,
		BinaryPath:      binaryPath,
		Definition:      def,
		State:           described.State,
		StateVersion:    def.StateVersion,
		IntervalSeconds: int64(req.Config.Every.Seconds()),
		TTLSeconds:      ttlSeconds,
		TimeoutSeconds:  int64(req.Config.Timeout.Seconds()),
		MaxEvents:       int64(req.Config.MaxEvents),
		ProxyPool:       req.Config.ProxyPool,
		Deliveries:      routes.CloneDeliveries(req.Config.Deliveries),
		Status:          model.MonitorStatusActive,
		CreatedAt:       &now,
		UpdatedAt:       &now,
		ExpiresAt:       expiresAt,
		NextDueAt:       &now,
	}, nil
}

func validateDir(req Request) (string, error) {
	if err := req.Name.Validate(); err != nil {
		return "", err
	}
	if err := req.Config.ProxyPool.Validate(); err != nil {
		return "", err
	}
	if len(req.Config.Deliveries) == 0 {
		return "", errors.New("monitor requires at least one route")
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
	cmd.Env = append(os.Environ(), "GOWORK=off")

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build monitor: %w\n%s", err, strings.TrimSpace(stderr.String()))
	}

	return nil
}
