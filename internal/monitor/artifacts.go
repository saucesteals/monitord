package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/model"
)

const (
	// KeepArtifacts is how many superseded artifacts are retained per monitor,
	// so a bad deploy can be diagnosed before it is pruned.
	KeepArtifacts = 3
)

// fingerprintDir hashes every file in the monitor directory so an artifact is
// content-addressed: rebuilding unchanged source reuses the same artifact ID,
// and any edit produces a new one.
func fingerprintDir(dir string) (string, error) {
	sum := sha256.New()

	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() != filepath.Base(dir) && strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}

			return nil
		}
		// Secrets and local VCS/tooling metadata are generation inputs, never
		// artifact content. Relevant .env changes are tracked by the daemon's
		// exact-key secret fingerprint instead.
		if entry.Name() == ".env" || strings.HasPrefix(entry.Name(), ".env.") {
			return nil
		}

		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(sum, "%s\x00%x\x00", rel, sha256.Sum256(contents))

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("fingerprint monitor dir: %w", err)
	}

	return hex.EncodeToString(sum.Sum(nil)), nil
}

// PruneArtifacts removes superseded artifacts for a monitor, keeping the live
// one plus the most recent KeepArtifacts. Returns how many were removed.
func PruneArtifacts(paths config.Paths, name model.MonitorName, keep string) (int, error) {
	root := paths.ArtifactDir(name)
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("read artifacts for %s: %w", name, err)
	}

	type aged struct {
		path string
		mod  time.Time
	}

	var candidates []aged
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == keep {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		candidates = append(candidates, aged{
			path: filepath.Join(root, entry.Name()),
			mod:  info.ModTime(),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].mod.After(candidates[j].mod)
	})

	removed := 0
	for _, candidate := range candidates[min(len(candidates), KeepArtifacts):] {
		if err := os.RemoveAll(candidate.path); err != nil {
			return removed, fmt.Errorf("prune artifact %s: %w", candidate.path, err)
		}
		removed++
	}

	return removed, nil
}

// RemoveArtifacts deletes every artifact for a monitor.
func RemoveArtifacts(paths config.Paths, name model.MonitorName) error {
	if err := os.RemoveAll(paths.ArtifactDir(name)); err != nil {
		return fmt.Errorf("remove artifacts for %s: %w", name, err)
	}

	return nil
}
