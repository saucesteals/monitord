package monitord

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

type runtimeChild[S any] struct {
	name              string
	optional          bool
	continuous        ContinuousFunc[S]
	check             CheckFunc[S]
	interval, timeout time.Duration
}

func runtimeChildren[S any](p Plan[S]) []runtimeChild[S] {
	n := p.node
	if n.kind == planCombined {
		var out []runtimeChild[S]
		for _, named := range n.children {
			c := named.child
			out = append(out, runtimeChild[S]{named.name, named.optional, c.continuous, c.check, c.interval, c.options.timeout})
		}
		return out
	}
	return []runtimeChild[S]{{name: "default", continuous: n.continuous, check: n.check, interval: n.interval, timeout: n.options.timeout}}
}

type childResult[S any] struct {
	child runtimeChild[S]
	err   error
}

func runChild[S any](ctx context.Context, w *wire, s *Session[S], generation uint64, child runtimeChild[S], done chan<- childResult[S]) {
	var err error
	if child.continuous != nil {
		err = runContinuous(ctx, w, s, child)
	} else {
		err = runPolling(ctx, w, s, generation, child)
	}
	done <- childResult[S]{child, err}
}
func runContinuous[S any](ctx context.Context, w *wire, s *Session[S], child runtimeChild[S]) error {
	backoff := time.Second
	failures := 0
	for {
		started := time.Now()
		attempt := ctx
		cancel := func() {}
		if child.timeout > 0 {
			attempt, cancel = context.WithTimeout(ctx, child.timeout)
		}
		err := safeCallback(func() error { return child.continuous(attempt, s) })
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return errors.New("continuous callback returned nil while active")
		}
		if time.Since(started) >= time.Minute {
			failures = 0
			backoff = time.Second
		}
		failures++
		if isPermanent(err) || failures >= 6 {
			return err
		}
		if sendErr := sendHealth(w, child.name, "restarting", err.Error()); sendErr != nil {
			return sendErr
		}
		delay := backoff + time.Duration(rand.Int64N(int64(backoff/4+1)))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
func runPolling[S any](ctx context.Context, w *wire, s *Session[S], generation uint64, child runtimeChild[S]) error {
	for attempt := uint64(1); ; attempt++ {
		id := fmt.Sprintf("%d-%s-%d", generation, child.name, attempt)
		if err := w.send(WorkerFrame{Type: "run", Run: &RunFrame{AttemptID: id, Child: child.name, Phase: "started"}}); err != nil {
			return err
		}
		runctx := ctx
		cancel := func() {}
		if child.timeout > 0 {
			runctx, cancel = context.WithTimeout(ctx, child.timeout)
		}
		err := safeCallback(func() error { return child.check(runctx, s) })
		cancel()
		finish := &RunFrame{AttemptID: id, Child: child.name, Phase: "finished"}
		if err != nil {
			finish.Error = err.Error()
		}
		if sendErr := w.send(WorkerFrame{Type: "run", Run: finish}); sendErr != nil {
			return sendErr
		}
		status := "healthy"
		if err != nil {
			status = "degraded"
		}
		if sendErr := sendHealth(w, child.name, status, finish.Error); sendErr != nil {
			return sendErr
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(child.interval):
		}
	}
}
func safeCallback(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("callback panicked: %v", r)
		}
	}()
	return fn()
}
func sendHealth(w *wire, child, status, message string) error {
	return w.send(WorkerFrame{Type: "health", Health: &HealthFrame{Child: child, Status: status, Message: message}})
}
