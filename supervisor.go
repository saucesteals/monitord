package monitord

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

type callbackStillRunningError struct{ cause error }
type callbackPanicError struct{ value any }
type reportedRunError struct{ cause error }

func (e *callbackStillRunningError) Error() string { return e.cause.Error() }
func (e *callbackStillRunningError) Unwrap() error { return e.cause }
func (e *callbackPanicError) Error() string        { return fmt.Sprintf("callback panicked: %v", e.value) }
func (e *reportedRunError) Error() string          { return e.cause.Error() }
func (e *reportedRunError) Unwrap() error          { return e.cause }

func runFailureWasReported(err error) bool {
	var target *reportedRunError
	return errors.As(err, &target)
}

func callbackStillRunning(err error) bool {
	var target *callbackStillRunningError
	return errors.As(err, &target)
}

func callbackFatal(err error) bool {
	var panicErr *callbackPanicError
	return callbackStillRunning(err) || errors.As(err, &panicErr)
}

func runPlan[S any](ctx context.Context, session *Session[S], plan Plan[S], once bool, report func(RunFrame) error) error {
	node := plan.node
	if node.kind == planContinuous {
		started := time.Now()
		err := runCallback(ctx, node.options.timeout, func(callbackCtx context.Context) error {
			return node.continuous(callbackCtx, session)
		})
		if err == nil && ctx.Err() == nil {
			err = fmt.Errorf("continuous callback returned while active")
		}
		if err != nil && ctx.Err() == nil {
			message := boundedOperationalError(err, "continuous monitor failed")
			if reportErr := report(RunFrame{Status: "failure", Duration: time.Since(started), Error: message}); reportErr != nil {
				return reportErr
			}
			return &reportedRunError{cause: err}
		}
		return err
	}
	for {
		started := time.Now()
		err := runCallback(ctx, node.options.timeout, func(callbackCtx context.Context) error {
			return node.check(callbackCtx, session)
		})
		if ctx.Err() != nil {
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		}
		outcome := RunFrame{Status: "success", Duration: time.Since(started)}
		if err != nil {
			outcome.Status = "failure"
			outcome.Error = boundedOperationalError(err, "monitor check failed")
		}
		if reportErr := report(outcome); reportErr != nil {
			return reportErr
		}
		if err != nil && (once || callbackFatal(err)) {
			return &reportedRunError{cause: err}
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

func boundedOperationalError(err error, fallback string) string {
	message := err.Error()
	if message == "" {
		message = fallback
	}
	if len(message) > MaxOperationalErrorBytes {
		message = message[:MaxOperationalErrorBytes]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return message
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
			resultErr := <-result
			if resultErr != nil && !errors.Is(resultErr, context.Canceled) {
				return resultErr
			}
			return nil
		}
		return &callbackStillRunningError{cause: fmt.Errorf("callback deadline exceeded: %w", err)}
	}
}

func safeCallback(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &callbackPanicError{value: recovered}
		}
	}()
	return fn()
}
