package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	http "github.com/saucesteals/fhttp"
	"github.com/saucesteals/monitord"
)

//go:embed targets.json
var targetsJSON []byte

type Target struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type observation struct {
	target Target
	status int
	err    error
	time   time.Time
}

func checkTargets(ctx context.Context, session *monitord.Session[State]) error {
	targets, err := loadTargets(targetsJSON)
	if err != nil {
		return fmt.Errorf("load targets: %w", err)
	}

	for _, target := range targets {
		result := observe(ctx, target)
		if err := session.Commit(ctx, func(tx *monitord.Tx[State]) error {
			return commitObservation(tx, result)
		}); err != nil {
			return err
		}
	}
	return nil
}

func loadTargets(contents []byte) ([]Target, error) {
	var targets []Target
	if err := json.Unmarshal(contents, &targets); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("targets.json lists no targets")
	}
	return targets, nil
}

func observe(ctx context.Context, target Target) observation {
	result := observation{target: target, time: time.Now().UTC()}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
	if err != nil {
		result.err = err
		return result
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		result.err = err
		return result
	}
	defer func() { _ = resp.Body.Close() }()

	result.status = resp.StatusCode
	return result
}

func commitObservation(tx *monitord.Tx[State], result observation) error {
	if tx.State.Targets == nil {
		tx.State.Targets = make(map[string]TargetState)
	}

	previous, seen := tx.State.Targets[result.target.Name]
	next := TargetState{Status: result.status, Reachable: result.err == nil, Transitions: previous.Transitions}
	if next.Reachable && next.Status < http.StatusBadRequest {
		tx.State.LastOK = result.time
	}

	unchanged := seen && previous.Status == next.Status && previous.Reachable == next.Reachable
	initiallyHealthy := !seen && next.Reachable && next.Status < http.StatusBadRequest
	if !unchanged && !initiallyHealthy {
		next.Transitions++
	}
	tx.State.Targets[result.target.Name] = next
	if unchanged || initiallyHealthy {
		return nil
	}

	event := monitord.Event{
		ID:       fmt.Sprintf("http:%s:%d", result.target.Name, next.Transitions),
		Severity: monitord.SeverityWarn,
		Title:    fmt.Sprintf("%s returned HTTP %d", result.target.Name, result.status),
	}
	switch {
	case result.err != nil:
		event.Severity = monitord.SeverityCritical
		event.Title = result.target.Name + " is unreachable"
		event.Body = result.err.Error()
	case result.status < http.StatusBadRequest:
		event.Severity = monitord.SeverityInfo
		event.Title = result.target.Name + " recovered"
	}
	return tx.Emit(event)
}
