package monitord

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type callbackStillRunningError struct{ cause error }

func (e *callbackStillRunningError) Error() string { return e.cause.Error() }
func (e *callbackStillRunningError) Unwrap() error { return e.cause }

func callbackStillRunning(err error) bool {
	var target *callbackStillRunningError
	return errors.As(err, &target)
}

func runPlan[S any](ctx context.Context, session *Session[S], plan Plan[S], once bool) error {
	node := plan.node
	if node.kind == planContinuous {
		err := runCallback(ctx, node.options.timeout, func(callbackCtx context.Context) error {
			return node.continuous(callbackCtx, session)
		})
		if err == nil && ctx.Err() == nil {
			return fmt.Errorf("continuous callback returned while active")
		}
		return err
	}
	for {
		if err := runCallback(ctx, node.options.timeout, func(callbackCtx context.Context) error {
			return node.check(callbackCtx, session)
		}); err != nil {
			return err
		}
		if once {
			return nil
		}
		timer := time.NewTimer(node.interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func runCallback(ctx context.Context, timeout time.Duration, callback func(context.Context) error) error {
	callbackCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		callbackCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	result := make(chan error, 1)
	go func() { result <- safeCallback(func() error { return callback(callbackCtx) }) }()
	select {
	case err := <-result:
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		return err
	case <-callbackCtx.Done():
		err := callbackCtx.Err()
		cancel()
		if ctx.Err() != nil {
			// Lifecycle cleanup must not overlap a callback that still owns its
			// resources. The daemon's shutdown deadline bounds an uncooperative
			// callback by terminating the worker process.
			<-result
			return nil
		}
		return &callbackStillRunningError{cause: fmt.Errorf("callback deadline exceeded: %w", err)}
	}
}

func safeCallback(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("callback panicked: %v", recovered)
		}
	}()
	return fn()
}
