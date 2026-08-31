// Command http-watch is an example monitord monitord.
//
// It checks a list of URLs loaded from targets.json, alerts when one starts
// failing, and remembers the last status of each so a repeated failure does not
// re-alert. Copy this directory into your monitors/ directory to adapt it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	http "github.com/saucesteals/fhttp"
	"github.com/saucesteals/monitord"
)

// Target is one URL to watch, loaded from targets.json beside this source.
type Target struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// State persists across checks and daemon restarts.
type State struct {
	// LastStatus maps target name to the HTTP status last seen, so a check can
	// report transitions rather than absolute state.
	LastStatus map[string]int `json:"last_status"`
	LastOK     time.Time      `json:"last_ok"`
}

// StateVersion pins the schema. Bump it and add MigrateState when State
// changes shape, or deploy will refuse to carry the old data forward.
func (State) StateVersion() int { return 1 }

func main() {
	monitord.Run(monitord.Define(monitord.Info{Name: "http-watch", Description: "Checks configured HTTP targets"}, monitord.Every(time.Minute, run)))
}

func run(ctx context.Context, session *monitord.Session[State]) error {
	targets, err := loadTargets("targets.json")
	if err != nil {
		return fmt.Errorf("load targets: %w", err)
	}
	for _, target := range targets {
		status, err := check(ctx, target)
		if err != nil {
			if err := session.Commit(ctx, func(tx *monitord.Tx[State]) error {
				return tx.Emit(monitord.Event{
					Severity: monitord.SeverityCritical,
					Title:    fmt.Sprintf("%s unreachable", target.Name),
					Summary:  err.Error(),
					ID:       "unreachable:" + target.Name, DedupeKey: "unreachable:" + target.Name, DedupeFor: time.Minute,
				})
			}); err != nil {
				return err
			}
			continue
		}
		if err := session.Commit(ctx, func(tx *monitord.Tx[State]) error {
			if tx.State.LastStatus == nil {
				tx.State.LastStatus = map[string]int{}
			}
			tx.State.LastStatus[target.Name] = status
			if status < 400 {
				tx.State.LastOK = time.Now().UTC()
				return nil
			}
			return tx.Emit(monitord.Event{
				Severity: monitord.SeverityWarn,
				Title:    fmt.Sprintf("%s returned HTTP %d", target.Name, status),
				ID:       fmt.Sprintf("status:%s:%d", target.Name, status),
			})
		}); err != nil {
			return err
		}
	}
	return nil
}

func check(ctx context.Context, target Target) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
	if err != nil {
		return 0, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	return resp.StatusCode, nil
}

// loadTargets reads targets.json from the monitor's own directory.
func loadTargets(path string) ([]Target, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var targets []Target
	if err := json.Unmarshal(contents, &targets); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("%s lists no targets", path)
	}

	return targets, nil
}
