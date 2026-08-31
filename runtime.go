package monitord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// Permanent marks a continuous-source error as terminal.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

type permanentError struct{ error }

func (permanentError) Permanent() {}
func isPermanent(err error) bool  { var p interface{ Permanent() }; return errors.As(err, &p) }

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
		state, err := decodeState[S](input.State, input.Version)
		if err != nil {
			return err
		}
		raw, err := encodeState(state)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(MonitorFrame{Info: m.Info(), Plan: d, StateVersion: stateVersion[S](), State: raw})
	case FlagWorker:
		return serveWorker(m, d, os.Stdin, os.Stdout)
	default:
		return fmt.Errorf("unknown monitor flag %q", os.Args[1])
	}
}

func serveWorker[S any](m Monitor[S], desc PlanDescription, in io.Reader, out io.Writer) error {
	w := newWire(in, out)
	helloMsg, _, err := w.readInbound()
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if helloMsg.Type != "hello" {
		return fmt.Errorf("expected hello, got %q", helloMsg.Type)
	}
	hello := *helloMsg.Hello
	if err = w.send(WorkerFrame{Type: "monitor", Monitor: &MonitorFrame{Info: m.Info(), Plan: desc, StateVersion: stateVersion[S](), State: json.RawMessage("null")}}); err != nil {
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
	coord := &workerCoordinator{wire: w, hello: hello, revision: hello.StateRevision, acks: make(chan TransactionAck, 1), stop: make(chan Stop, 1), fatal: make(chan error, 1), progress: make(chan struct{}, 1)}
	if err := validateHandshakeSecrets(desc, hello.Secrets); err != nil {
		return err
	}
	secrets := map[string]string{}
	for g, keys := range hello.Secrets {
		for k, v := range keys {
			secrets[g+"\x00"+k] = v
		}
	}
	session, err := newSession[S](hello.State, secrets, coord)
	if err != nil {
		return err
	}
	for source, raw := range hello.Checkpoints {
		session.checkpoints[source] = append(json.RawMessage(nil), raw...)
	}
	if err = w.send(WorkerFrame{Type: "ready", Ready: &ReadyFrame{Generation: hello.Generation}}); err != nil {
		return err
	}
	go readControl(w, coord)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan childResult[S], 8)
	children := runtimeChildren(m.Plan())
	for _, child := range children {
		if child.check != nil {
			_ = sendHealth(w, child.name, "ready", "schedule activated")
		}
		go runChild(ctx, w, session, hello.Generation, child, done)
	}
	remaining := len(children)
	clean := true
	for remaining > 0 {
		select {
		case result := <-done:
			remaining--
			if result.err != nil {
				clean = false
				_ = sendHealth(w, result.child.name, "terminal", result.err.Error())
				if !result.child.optional {
					cancel()
				}
			}
		case <-coord.stop:
			cancel()
		case err := <-coord.fatal:
			cancel()
			return err
		case <-coord.progress:
			for _, child := range children {
				if child.continuous != nil {
					_ = sendHealth(w, child.name, "ready", "progress committed")
				}
			}
		}
	}
	return w.send(WorkerFrame{Type: "stopped", Stopped: &StoppedFrame{Generation: hello.Generation, Clean: clean}})
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
