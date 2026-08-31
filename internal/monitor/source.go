package monitor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

	for _, file := range scaffoldFiles {
		source := strings.ReplaceAll(file.template, "MONITOR_NAME", name.String())
		if err := os.WriteFile(filepath.Join(dir, file.name), []byte(source), 0o600); err != nil {
			return "", fmt.Errorf("write %s: %w", file.name, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ConfigFileName), []byte(scaffoldConfig), 0o600); err != nil {
		return "", fmt.Errorf("write monitor config: %w", err)
	}

	return dir, nil
}

var scaffoldFiles = []struct {
	name     string
	template string
}{
	{"main.go", `package main

import (
	"time"

	"github.com/saucesteals/monitord"
)

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{Name: "MONITOR_NAME", Description: "HTTP status monitor"},
		monitord.Every(5*time.Minute, check),
	))
}
`},
	{"state.go", `package main

// State persists across checks and daemon restarts.
type State struct {
	LastStatus int ` + "`json:\"last_status\"`" + `
	Initialized bool ` + "`json:\"initialized\"`" + `
}

func (State) StateVersion() int { return 1 }
`},
	{"check.go", `package main

import (
	"context"
	"fmt"
	"time"

	http "github.com/saucesteals/fhttp"
	"github.com/saucesteals/monitord"
)

func check(ctx context.Context, session *monitord.Session[State]) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/", nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	observedAt := time.Now().UTC()

	return session.Commit(ctx, func(tx *monitord.Tx[State]) error {
		previous := tx.State.LastStatus
		tx.State.LastStatus = resp.StatusCode
		if tx.State.Initialized && previous == resp.StatusCode {
			return nil
		}
		tx.State.Initialized = true
		if resp.StatusCode >= 400 {
			return tx.Emit(monitord.Event{
				ID:    fmt.Sprintf("http:%d:%d", resp.StatusCode, observedAt.UnixNano()),
				Title: fmt.Sprintf("HTTP %d", resp.StatusCode),
			})
		}
		return nil
	})
}
`},
}

const scaffoldConfig = `ttl: 24h
deliveries:
  - discord:
      account: jarvis
      channel_id: "CHANNEL_ID"
`
