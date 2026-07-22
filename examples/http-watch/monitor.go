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

// State persists across ticks and daemon restarts.
type State struct {
	// LastStatus maps target name to the HTTP status last seen, so a tick can
	// report transitions rather than absolute state.
	LastStatus map[string]int `json:"last_status"`
	LastOK     time.Time      `json:"last_ok"`
}

// StateVersion pins the schema. Bump it and add MigrateState when State
// changes shape, or deploy will refuse to carry the old data forward.
func (State) StateVersion() int { return 1 }

func main() {
	monitord.Main(run)
}

func run(ctx context.Context, r *monitord.Run[State]) monitord.Result {
	targets, err := loadTargets(r.Path("targets.json"))
	if err != nil {
		return monitord.Failuref("load targets: %v", err)
	}
	if r.State.LastStatus == nil {
		r.State.LastStatus = map[string]int{}
	}

	failed := 0
	for _, target := range targets {
		status, err := check(ctx, r, target)
		if err != nil {
			failed++
			r.Emit(monitord.Event{
				Severity: monitord.SeverityCritical,
				Title:    fmt.Sprintf("%s unreachable", target.Name),
				Summary:  err.Error(),
				// Keyed per target, so one flapping target does not drown out
				// the others in the notification route.
				ID: "unreachable:" + target.Name,
			})

			continue
		}

		if previous := r.State.LastStatus[target.Name]; previous != status {
			r.Logf(monitord.LogInfo, "%s: %d -> %d", target.Name, previous, status)
		}
		r.State.LastStatus[target.Name] = status

		if status >= 400 {
			failed++
			r.Emit(monitord.Event{
				Severity: monitord.SeverityWarn,
				Title:    fmt.Sprintf("%s returned HTTP %d", target.Name, status),
				ID:       fmt.Sprintf("status:%s:%d", target.Name, status),
			})
		}
	}

	if failed == 0 {
		r.State.LastOK = time.Now().UTC()
	}
	r.Save()

	if failed > 0 {
		return monitord.Failuref("%d of %d targets unhealthy", failed, len(targets))
	}

	return monitord.Successf("all %d targets healthy", len(targets))
}

func check(ctx context.Context, r *monitord.Run[State], target Target) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
	if err != nil {
		return 0, err
	}

	resp, err := r.Client().Do(req)
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
