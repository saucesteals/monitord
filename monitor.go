// Package monitord is the authoring SDK for monitord monitors.
//
// A monitor is a Go package with a main function that hands its tick function
// to Main. Scheduling and runtime configuration live in monitor.yaml:
//
//	type State struct {
//		LastSeenID string `json:"last_seen_id"`
//	}
//
//	func main() {
//		monitord.Main(run)
//	}
//
//	func run(ctx context.Context, r *monitord.Run[State]) monitord.Result {
//		resp, err := r.Client().Do(req)
//		...
//		r.State.LastSeenID = id
//		r.Save()
//
//		return monitord.Success("ok")
//	}
//
// The compiled binary is a long-lived worker: monitord starts it once, hands it
// its network assignment, and sends one tick per scheduled run over stdin. HTTP
// clients and their connection pools live for the lifetime of the process, not
// the tick.
package monitord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Runner performs one monitor tick.
type Runner[S any] func(context.Context, *Run[S]) Result

// Run is the authoring context for one scheduled tick.
type Run[S any] struct {
	// Monitor is the deployed monitor name.
	Monitor MonitorName
	// Routes are the monitor's configured notification routes.
	Routes []RouteName
	// RunID identifies this tick.
	RunID string
	// Deadline is when the daemon will abandon this tick, if set.
	Deadline time.Time
	// State is the monitor's durable cross-tick state, loaded before the tick
	// and persisted by the daemon when Save is called.
	State *S

	clients *Clients
	stream  *stream
	dir     string

	mu      sync.Mutex
	saved   json.RawMessage
	saveErr error
}

// Main implements the monitord executable contract and never returns.
func Main[S any](runner Runner[S]) {
	if err := dispatch(runner, os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func dispatch[S any](runner Runner[S], args []string, stdin io.Reader, stdout io.Writer) error {
	def := Definition{}
	def = def.WithDefaults()
	def.StateVersion = stateVersion[S]()
	if err := def.Validate(); err != nil {
		return fmt.Errorf("invalid definition: %w", err)
	}

	if len(args) != 1 {
		return errors.New("usage: monitor " + FlagDescribe + " | " + FlagWorker)
	}

	switch args[0] {
	case FlagDescribe:
		return describe[S](def, stdin, stdout)
	case FlagWorker:
		return serve(runner, stdin, stdout)
	default:
		return fmt.Errorf("unknown flag %q", args[0])
	}
}

// describe reports the definition alongside canonical state. Stored state, if
// piped in on stdin, is validated and migrated against the monitor's own types
// so a bad or drifted state fails at deploy rather than at the first tick.
func describe[S any](def Definition, stdin io.Reader, stdout io.Writer) error {
	var input DescribeInput
	if err := json.NewDecoder(stdin).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
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

	if err := json.NewEncoder(stdout).Encode(Describe{
		Definition: def,
		State:      raw,
	}); err != nil {
		return fmt.Errorf("encode describe: %w", err)
	}

	return nil
}

// serve runs the worker loop: one handshake, then one tick per inbound message.
func serve[S any](runner Runner[S], stdin io.Reader, stdout io.Writer) error {
	out := newStream(stdout)
	dec := json.NewDecoder(stdin)

	worker, err := handshake(dec, out)
	if err != nil {
		return err
	}

	for {
		var msg Inbound
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("decode inbound message: %w", err)
		}
		if err := msg.Validate(); err != nil {
			return err
		}
		if msg.Type != InboundTick {
			return fmt.Errorf("unexpected %q message after handshake", msg.Type)
		}
		if err := tick(runner, worker, *msg.Tick, out); err != nil {
			return err
		}
	}
}

// worker holds the runtime configuration established at handshake time and
// reused by every tick.
type worker struct {
	hello   Hello
	clients *Clients
}

func handshake(dec *json.Decoder, out *stream) (*worker, error) {
	var msg Inbound
	if err := dec.Decode(&msg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("worker stdin closed before handshake")
		}

		return nil, fmt.Errorf("decode handshake: %w", err)
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	if msg.Type != InboundHello {
		return nil, fmt.Errorf("expected %q handshake, got %q", InboundHello, msg.Type)
	}

	clients, err := newClients(msg.Hello.Network)
	if err != nil {
		return nil, err
	}

	if err := out.write(Outbound{
		Type:  OutboundReady,
		Ready: &Ready{Clients: clients.Len()},
	}); err != nil {
		return nil, err
	}

	return &worker{
		hello:   *msg.Hello,
		clients: clients,
	}, nil
}

func tick[S any](runner Runner[S], w *worker, t Tick, out *stream) error {
	state, err := decodeState[S](t.State, 0)
	if err != nil {
		return out.result(t.RunID, Failuref("load state: %v", err))
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if t.Deadline.IsZero() {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithDeadline(ctx, t.Deadline)
	}
	defer cancel()

	run := &Run[S]{
		Monitor:  w.hello.Monitor,
		Routes:   append([]RouteName(nil), w.hello.Routes...),
		RunID:    t.RunID,
		Deadline: t.Deadline,
		State:    state,
		clients:  w.clients,
		dir:      w.hello.Dir,
		stream:   out.forRun(t.RunID),
	}

	result := safeRun(ctx, runner, run)
	if result.Status == "" {
		result.Status = StatusSuccess
	}

	run.mu.Lock()
	saveErr, saved := run.saveErr, run.saved
	run.mu.Unlock()

	if saveErr != nil {
		result = Failuref("save state: %v", saveErr)
	} else {
		result.State = saved
		result.Revision = t.Revision
	}

	return out.result(t.RunID, result)
}

// safeRun converts a panicking tick into a failed result so one bad tick does
// not take down a worker that other ticks still depend on.
func safeRun[S any](ctx context.Context, runner Runner[S], run *Run[S]) (result Result) {
	defer func() {
		if r := recover(); r != nil {
			result = Failuref("monitor panicked: %v", r)
		}
	}()

	return runner(ctx, run)
}

// Client returns the next HTTP client in this worker's cycle. Each client is
// pinned to one proxy and keeps its own connection pool warm across ticks.
func (r *Run[S]) Client() *Client {
	return r.clients.Next()
}

// Clients returns the worker's full client cycle, for monitors that need to
// pick a specific client rather than round-robin.
func (r *Run[S]) Clients() *Clients {
	return r.clients
}

// Dir returns the monitor's source directory, which is also its working
// directory. Use it to read files deployed alongside the monitor.
func (r *Run[S]) Dir() string {
	return r.dir
}

// Path joins one or more elements onto the monitor's directory.
func (r *Run[S]) Path(elem ...string) string {
	return filepath.Join(append([]string{r.dir}, elem...)...)
}

// Save records the current state to be persisted by the daemon when this tick
// finishes. Later mutations are ignored unless Save is called again. A failure
// to encode state fails the tick.
func (r *Run[S]) Save() {
	raw, err := encodeState(r.State)

	r.mu.Lock()
	defer r.mu.Unlock()

	if err != nil {
		r.saveErr = err

		return
	}
	r.saved = raw
}

// Log emits a structured log line.
func (r *Run[S]) Log(level LogLevel, message string) {
	_ = r.stream.log(level, message)
}

// Logf emits a formatted structured log line.
func (r *Run[S]) Logf(level LogLevel, format string, args ...any) {
	r.Log(level, fmt.Sprintf(format, args...))
}

// Emit sends one notification event to the monitor's routes. Events are delivered
// immediately, in the order emitted, independent of the tick's final result.
func (r *Run[S]) Emit(event Event) {
	_ = r.stream.event(event)
}

// stream writes framed monitor protocol messages to stdout.
type stream struct {
	enc   *json.Encoder
	runID string
	mu    *sync.Mutex
}

func newStream(w io.Writer) *stream {
	return &stream{
		enc: json.NewEncoder(w),
		mu:  &sync.Mutex{},
	}
}

// forRun returns a stream that stamps messages with a run ID, sharing the
// underlying writer lock.
func (s *stream) forRun(runID string) *stream {
	return &stream{
		enc:   s.enc,
		runID: runID,
		mu:    s.mu,
	}
}

func (s *stream) log(level LogLevel, message string) error {
	return s.write(Outbound{
		Type: OutboundLog,
		Log: &Log{
			Level:   level,
			Message: message,
			Time:    time.Now().UTC(),
		},
	})
}

func (s *stream) event(event Event) error {
	if event.Severity == "" {
		event.Severity = SeverityInfo
	}
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}

	return s.write(Outbound{
		Type:  OutboundEvent,
		Event: &event,
	})
}

func (s *stream) result(runID string, result Result) error {
	return (&stream{enc: s.enc, runID: runID, mu: s.mu}).write(Outbound{
		Type:   OutboundResult,
		Result: &result,
	})
}

func (s *stream) write(msg Outbound) error {
	msg.RunID = s.runID
	if err := msg.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.enc.Encode(msg)
}
