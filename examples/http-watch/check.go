package main

import (
	"context"
	"fmt"
	"time"

	http "github.com/saucesteals/fhttp"
	"github.com/saucesteals/monitord"
)

type observation struct {
	target Target
	status int
	err    error
	time   time.Time
}

func checkTargets(ctx context.Context, session *monitord.Session[State]) error {
	targets, err := loadTargets("targets.json")
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
	next := TargetState{Status: result.status, Reachable: result.err == nil}
	tx.State.Targets[result.target.Name] = next
	if next.Reachable && next.Status < http.StatusBadRequest {
		tx.State.LastOK = result.time
	}
	unchanged := seen && previous == next
	initiallyHealthy := !seen && next.Reachable && next.Status < http.StatusBadRequest
	if unchanged || initiallyHealthy {
		return nil
	}

	event := monitord.Event{
		ID:       fmt.Sprintf("http:%s:%d", result.target.Name, result.time.UnixNano()),
		Severity: monitord.SeverityWarn,
		Title:    fmt.Sprintf("%s returned HTTP %d", result.target.Name, result.status),
	}
	switch {
	case result.err != nil:
		event.Severity = monitord.SeverityCritical
		event.Title = result.target.Name + " is unreachable"
		event.Summary = result.err.Error()
	case result.status < http.StatusBadRequest:
		event.Severity = monitord.SeverityInfo
		event.Title = result.target.Name + " recovered"
	}

	return tx.Emit(event)
}
