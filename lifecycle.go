package monitord

import (
	"context"
	"fmt"
	"time"
)

const (
	monitorStartTimeout = 10 * time.Second
	monitorStopTimeout  = 5 * time.Second
)

// Environment contains the worker-scoped dependencies available to a monitor.
type Environment struct {
	secrets SecretSet
}

// Secrets returns the immutable secrets supplied for this worker generation.
func (e Environment) Secrets() SecretSet { return e.secrets }

// Starter is implemented by monitors that initialize worker-scoped resources.
// Start completes before the worker reports ready or begins executing its plan.
type Starter interface {
	Start(context.Context, Environment) error
}

// Stopper is implemented by monitors that release worker-scoped resources.
// Stop is called only after Start has returned nil and active callbacks have
// stopped. A failing Start must release anything it initialized before return.
type Stopper interface {
	Stop(context.Context) error
}

func startMonitor(ctx context.Context, monitor any, env Environment) error {
	starter, ok := monitor.(Starter)
	if !ok {
		return nil
	}
	return runLifecycleHook(ctx, "start", func(ctx context.Context) error {
		return starter.Start(ctx, env)
	})
}

func stopMonitor(ctx context.Context, monitor any) error {
	stopper, ok := monitor.(Stopper)
	if !ok {
		return nil
	}
	return runLifecycleHook(ctx, "stop", stopper.Stop)
}

func runLifecycleHook(ctx context.Context, name string, hook func(context.Context) error) error {
	result := make(chan error, 1)
	go func() {
		result <- safeCallback(func() error { return hook(ctx) })
	}()
	select {
	case err := <-result:
		if err != nil {
			return fmt.Errorf("monitor %s: %w", name, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("monitor %s: %w", name, ctx.Err())
	}
}

func lifecycleContext(timeout time.Duration, deadline time.Time) (context.Context, context.CancelFunc) {
	if !deadline.IsZero() && time.Until(deadline) < timeout {
		return context.WithDeadline(context.Background(), deadline)
	}
	return context.WithTimeout(context.Background(), timeout)
}
