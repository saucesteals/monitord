package monitord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
)

// commitSettlementTimeout bounds each admitted transaction, including writes and retries.
const commitSettlementTimeout = 30 * time.Second

type callbackCommitKey struct{}

type callbackCommitScope struct {
	mu       sync.Mutex
	deadline time.Time
	changed  chan struct{}
}

func (s *callbackCommitScope) begin(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	s.deadline = time.Now().Add(commitSettlementTimeout)
	s.notify()
	return true
}

func (s *callbackCommitScope) end() {
	s.mu.Lock()
	s.deadline = time.Time{}
	s.notify()
	s.mu.Unlock()
}

func (s *callbackCommitScope) notify() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *callbackCommitScope) remaining() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Until(s.deadline), !s.deadline.IsZero()
}

type transactionUncertainError struct{ cause error }

func (e *transactionUncertainError) Error() string {
	return fmt.Sprintf("transaction settlement failed; durable outcome unknown: %v", e.cause)
}
func (e *transactionUncertainError) Unwrap() error { return e.cause }

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
	var uncertain *transactionUncertainError
	return callbackStillRunning(err) || errors.As(err, &panicErr) || errors.As(err, &uncertain)
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
	defer cancel()
	scope := &callbackCommitScope{changed: make(chan struct{}, 1)}
	callbackCtx = context.WithValue(callbackCtx, callbackCommitKey{}, scope)
	result := make(chan error, 1)
	go func() { result <- safeCallback(func() error { return callback(callbackCtx) }) }()
	var settlement *time.Timer
	var settlementDeadline <-chan time.Time
	defer func() {
		if settlement != nil {
			settlement.Stop()
		}
	}()
	for {
		select {
		case err := <-result:
			if ctx.Err() != nil {
				return nil
			}
			return err
		case <-scope.changed:
			if settlement != nil {
				settlement.Stop()
			}
			settlementDeadline = nil
			if remaining, active := scope.remaining(); active {
				settlement = time.NewTimer(max(remaining, 0))
				settlementDeadline = settlement.C
			}
		case <-settlementDeadline:
			if remaining, active := scope.remaining(); active && remaining <= 0 {
				return &callbackStillRunningError{cause: errors.New("transaction settlement deadline exceeded; durable outcome unknown")}
			}
			settlementDeadline = nil
		case <-callbackCtx.Done():
			cause := callbackCtx.Err()
			if ctx.Err() != nil {
				// The daemon's earlier stop deadline bounds shutdown. Never clean up
				// resources while the callback still owns them.
				err := <-result
				if err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
				return nil
			}
			// Cancellation rejects further admission. Preserve the original submission
			// deadline, then allow a short unwind after the ACK updates canonical state.
			for {
				remaining, active := scope.remaining()
				if !active {
					remaining = 100 * time.Millisecond
				}
				timer := time.NewTimer(max(remaining, 0))
				select {
				case err := <-result:
					timer.Stop()
					return fmt.Errorf("callback deadline exceeded: %w", errors.Join(cause, err))
				case <-scope.changed:
					timer.Stop()
					continue
				case <-timer.C:
					message := "callback deadline exceeded"
					if active {
						message = "transaction settlement deadline exceeded; durable outcome unknown"
					}
					return &callbackStillRunningError{cause: fmt.Errorf("%s: %w", message, cause)}
				}
			}
		}
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
