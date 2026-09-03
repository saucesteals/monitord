package monitord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

func dispatchMonitor[S any](m Monitor[S]) error {
	d, err := validateMonitor(m)
	if err != nil {
		return fmt.Errorf("invalid monitor: %w", err)
	}
	if len(os.Args) != 2 {
		return errors.New("usage: monitor --monitord-describe | --monitord-worker")
	}
	switch os.Args[1] {
	case FlagDescribe:
		var input DescribeInput
		if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("decode describe input: %w", err)
		}
		state, err := decodeState[S](input.State)
		if err != nil {
			return err
		}
		raw, err := encodeState(state)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(MonitorFrame{Info: m.Info(), Plan: d, State: raw})
	case FlagWorker:
		return serveWorker(m, d, os.Stdin, os.Stdout)
	default:
		return fmt.Errorf("unknown monitor flag %q", os.Args[1])
	}
}

func serveWorker[S any](m Monitor[S], desc PlanDescription, in io.Reader, out io.Writer) (returnErr error) {
	w := newWire(in, out)
	helloMsg, _, err := w.readInbound()
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if helloMsg.Type != "hello" {
		return fmt.Errorf("expected hello, got %q", helloMsg.Type)
	}
	hello := *helloMsg.Hello
	if err = w.send(WorkerFrame{Type: "monitor", Monitor: &MonitorFrame{Info: m.Info(), Plan: desc, State: json.RawMessage("null")}}); err != nil {
		return err
	}
	start, _, err := w.readInbound()
	if err != nil {
		return fmt.Errorf("read start: %w", err)
	}
	if start.Type != "start" {
		return fmt.Errorf("expected start, got %q", start.Type)
	}
	want, _ := json.Marshal(desc)
	got, _ := json.Marshal(start.Start.Plan)
	if !bytes.Equal(want, got) {
		return errors.New("start plan differs from described plan")
	}
	coord := &workerCoordinator{wire: w, hello: hello, revision: hello.StateRevision, ackReady: make(chan struct{}, 1), stop: make(chan Stop, 1), commitStop: make(chan Stop, 1), fatalDone: make(chan struct{})}
	if err := validateHandshakeSecrets(desc, hello.Secrets); err != nil {
		return err
	}
	secrets := map[string]string{}
	for g, keys := range hello.Secrets {
		for k, v := range keys {
			secrets[g+"\x00"+k] = v
		}
	}
	session, err := newSession[S](hello.State, secrets, hello.Policy.Events.MaxPerTransaction, coord)
	if err != nil {
		return err
	}
	for source, raw := range hello.Checkpoints {
		session.checkpoints[source] = append(json.RawMessage(nil), raw...)
	}
	startCtx, cancelStart := lifecycleContext(monitorStartTimeout, time.Time{})
	err = startMonitor(startCtx, m, Environment{secrets: session.Secrets()})
	cancelStart()
	if err != nil {
		return w.send(WorkerFrame{Type: "stopped", Stopped: &StoppedFrame{
			Generation: hello.Generation,
			Clean:      false,
			Error:      boundedOperationalError(err, "monitor start failed"),
		}})
	}
	lifecycleStarted := true
	var stopDeadline time.Time
	stopLifecycle := func() error {
		if !lifecycleStarted {
			return nil
		}
		lifecycleStarted = false
		deadline := stopDeadline
		if !deadline.IsZero() {
			deadline = deadline.Add(-stopReportGrace)
		}
		stopCtx, cancelStop := lifecycleContext(monitorStopTimeout, deadline)
		defer cancelStop()
		return stopMonitor(stopCtx, m)
	}
	defer func() {
		if lifecycleStarted {
			returnErr = errors.Join(returnErr, stopLifecycle())
		}
	}()
	if err = w.send(WorkerFrame{Type: "ready", Ready: &ReadyFrame{Generation: hello.Generation}}); err != nil {
		return err
	}
	go readControl(w, coord)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runPlan(ctx, session, m.Plan(), start.Start.Once, func(run RunFrame) error {
			run.Generation = hello.Generation
			return w.send(WorkerFrame{Type: "run", Run: &run})
		})
	}()
	select {
	case runErr := <-done:
		cancel()
		var stopErr error
		if callbackStillRunning(runErr) {
			// The process is about to exit, which releases its resources safely.
			// Calling Stop here could race the callback that exceeded its deadline.
			lifecycleStarted = false
		} else {
			stopErr = stopLifecycle()
		}
		return sendStopped(w, hello.Generation, runErr, stopErr)
	case request := <-coord.stop:
		if request.Deadline != "" {
			stopDeadline, _ = time.Parse(time.RFC3339Nano, request.Deadline)
		}
		cancel()
		runErr := <-done
		stopErr := stopLifecycle()
		return sendStopped(w, hello.Generation, runErr, stopErr)
	case <-coord.fatalDone:
		cancel()
		// The daemon connection is gone or invalid. Exit the process instead of
		// racing lifecycle cleanup against a callback that may not cooperate.
		lifecycleStarted = false
		return coord.fatalError()
	}
}

func sendStopped(w *wire, generation uint64, runErr, stopErr error) error {
	resultErr := errors.Join(runErr, stopErr)
	stopped := &StoppedFrame{Generation: generation, Clean: resultErr == nil, RunFailureReported: runFailureWasReported(runErr)}
	if resultErr != nil {
		stopped.Error = boundedOperationalError(resultErr, "monitor stopped unsuccessfully")
	}
	return w.send(WorkerFrame{Type: "stopped", Stopped: stopped})
}

func validateHandshakeSecrets(desc PlanDescription, supplied map[string]map[string]string) error {
	allowed := map[string]SecretRef{}
	for _, ref := range desc.SecretRefs() {
		allowed[ref.Group+"\x00"+ref.Key] = ref
	}
	seen := map[string]bool{}
	for group, keys := range supplied {
		for key := range keys {
			id := group + "\x00" + key
			if _, ok := allowed[id]; !ok {
				return fmt.Errorf("unauthorized secret %s/%s in handshake", group, key)
			}
			seen[id] = true
		}
	}
	for id, ref := range allowed {
		if ref.Required && !seen[id] {
			return fmt.Errorf("required secret %s/%s is unavailable", ref.Group, ref.Key)
		}
	}
	return nil
}
