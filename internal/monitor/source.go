package monitor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/model"
)

// ResolveDir maps a deploy target to a monitor directory and name. The target
// may be a monitor name or a path to a source directory.
func ResolveDir(paths config.Paths, target string) (string, model.MonitorName, error) {
	if name, err := model.ParseMonitorName(target); err == nil {
		dir := paths.MonitorDir(name)
		if info, statErr := os.Stat(dir); statErr == nil && info.IsDir() {
			return dir, name, nil
		}
	}

	dir, err := filepath.Abs(target)
	if err != nil {
		return "", "", fmt.Errorf("resolve monitor path: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return "", "", fmt.Errorf("monitor %q not found: no directory at %s", target, dir)
	}
	if !info.IsDir() {
		return "", "", fmt.Errorf("monitor source must be a directory, got file %s", dir)
	}

	name, err := model.ParseMonitorName(filepath.Base(dir))
	if err != nil {
		return "", "", fmt.Errorf("monitor directory name is not a valid monitor name: %w", err)
	}

	return dir, name, nil
}

// Scaffold writes a starter monitor into the monitors directory.
func Scaffold(paths config.Paths, name model.MonitorName) (string, error) {
	dir := paths.MonitorDir(name)
	if _, err := os.Stat(dir); err == nil {
		return "", fmt.Errorf("monitor %s already exists at %s", name, dir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat monitor dir: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create monitor dir: %w", err)
	}

	source := scaffoldTemplate
	if err := os.WriteFile(filepath.Join(dir, "monitor.go"), []byte(source), 0o600); err != nil {
		return "", fmt.Errorf("write monitor source: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ConfigFileName), []byte(scaffoldConfig), 0o600); err != nil {
		return "", fmt.Errorf("write monitor config: %w", err)
	}

	return dir, nil
}

const scaffoldTemplate = `package main

import (
	"context"

	"github.com/saucesteals/monitord"
	http "github.com/saucesteals/fhttp"
)

// State persists across ticks and daemon restarts.
type State struct {
	LastStatus int ` + "`json:\"last_status\"`" + `
}

func main() {
	monitord.Main(run)
}

func run(ctx context.Context, r *monitord.Run[State]) monitord.Result {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/", nil)
	if err != nil {
		return monitord.Failuref("build request: %v", err)
	}

	resp, err := r.Client().Do(req)
	if err != nil {
		return monitord.Failuref("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != r.State.LastStatus {
		r.Logf(monitord.LogInfo, "status changed %d -> %d", r.State.LastStatus, resp.StatusCode)
		r.State.LastStatus = resp.StatusCode
		r.Save()
	}

	if resp.StatusCode >= 400 {
		return monitord.Failuref("HTTP %d", resp.StatusCode)
	}

	return monitord.Successf("HTTP %d", resp.StatusCode)
}
`

const scaffoldConfig = `description: HTTP status monitor
clients: 1
every: 5m
ttl: 24h
timeout: 30s
routes:
  - route: discord:alerts
`
