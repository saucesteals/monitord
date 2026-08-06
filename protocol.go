package monitord

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Protocol is the monitord worker wire version. The daemon refuses artifacts
// built against a different version.
//
// v4 removes route names from the worker handshake. Deliveries are daemon-owned
// monitor configuration, so existing artifacts must be redeployed.
const Protocol = 4

// Executable flags implementing the monitord worker contract.
const (
	// FlagDescribe asks a monitor to report its definition and canonical state.
	FlagDescribe = "--monitord-describe"
	// FlagWorker starts the long-lived worker loop.
	FlagWorker = "--monitord-worker"
)

// ResultStatus is the outcome of a single monitor tick.
type ResultStatus string

const (
	// StatusSuccess marks a healthy tick.
	StatusSuccess ResultStatus = "success"
	// StatusFailure marks a failed tick.
	StatusFailure ResultStatus = "failure"
)

// LogLevel is the severity of a structured log line.
type LogLevel string

const (
	// LogDebug is verbose diagnostic output.
	LogDebug LogLevel = "debug"
	// LogInfo is normal operational output.
	LogInfo LogLevel = "info"
	// LogWarn is a warning that does not fail the tick by itself.
	LogWarn LogLevel = "warn"
	// LogError is an error-level log line.
	LogError LogLevel = "error"
)

// Severity is the importance of a monitor event.
type Severity string

const (
	// SeverityInfo is an informational event.
	SeverityInfo Severity = "info"
	// SeverityWarn is a warning event.
	SeverityWarn Severity = "warn"
	// SeverityCritical is a high-importance event.
	SeverityCritical Severity = "critical"
)

// MonitorName identifies a deployed monitor.
type MonitorName string

// Definition describes a monitor to monitord at deploy time.
type Definition struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	// Clients is the number of HTTP clients the monitor wants. The daemon
	// assigns one proxy per client from the monitor's network profile.
	Clients int `json:"clients,omitempty"`
	// Persistent disables TTL expiry for this monitor.
	Persistent bool `json:"persistent,omitempty"`
	// Protocol is set by the SDK and verified by the daemon.
	Protocol int `json:"protocol"`
	// StateVersion is set by the SDK from the monitor's state type.
	StateVersion int `json:"state_version"`
}

// Network is the daemon-assigned network runtime for one worker.
type Network struct {
	// Proxies are the proxy URLs assigned to this worker, one per client.
	// Empty means direct connections.
	Proxies []string `json:"proxies,omitempty"`
}

// Hello is the first message the daemon sends to a worker.
type Hello struct {
	Monitor MonitorName `json:"monitor"`
	// Dir is the monitor's source directory and working directory, so a
	// monitor can read config or data files that live beside its source.
	Dir     string  `json:"dir,omitempty"`
	Network Network `json:"network"`
}

// Tick asks a worker to perform one scheduled run.
type Tick struct {
	RunID     string          `json:"run_id"`
	StartedAt time.Time       `json:"started_at"`
	Deadline  time.Time       `json:"deadline,omitempty"`
	State     json.RawMessage `json:"state,omitempty"`
	// Revision is the daemon's state revision the tick was built from.
	Revision int64 `json:"revision"`
}

// InboundType identifies a daemon-to-worker message.
type InboundType string

const (
	// InboundHello carries worker runtime configuration.
	InboundHello InboundType = "hello"
	// InboundTick carries one scheduled run.
	InboundTick InboundType = "tick"
)

// Inbound is one framed daemon-to-worker message.
type Inbound struct {
	Type  InboundType `json:"type"`
	Hello *Hello      `json:"hello,omitempty"`
	Tick  *Tick       `json:"tick,omitempty"`
}

// OutboundType identifies a worker-to-daemon message.
type OutboundType string

const (
	// OutboundReady acknowledges a successful handshake.
	OutboundReady OutboundType = "ready"
	// OutboundLog carries an incremental structured log line.
	OutboundLog OutboundType = "log"
	// OutboundEvent carries a notification event to deliver immediately.
	OutboundEvent OutboundType = "event"
	// OutboundResult carries the final result for one tick.
	OutboundResult OutboundType = "result"
)

// Ready acknowledges that a worker accepted its runtime configuration.
type Ready struct {
	Clients int `json:"clients"`
}

// Log is an incremental structured log line.
type Log struct {
	Level   LogLevel  `json:"level"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

// Field is one labelled value shown alongside a notification.
type Field struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	// Inline packs the field beside its neighbours rather than on its own row.
	Inline bool `json:"inline,omitempty"`
}

// Author is the small attribution line rendered above an event's title.
type Author struct {
	Name    string `json:"name,omitempty"`
	URL     string `json:"url,omitempty"`
	IconURL string `json:"icon_url,omitempty"`
}

// Event is one notification a monitor emits during a tick. It maps directly onto
// a Discord embed and is delivered on its own the moment it is emitted,
// independent of the tick's final result. Build it as a plain struct literal.
type Event struct {
	// ID is the event's stable identity. Repeats of the same ID are suppressed
	// for the dedupe window and recorded as one alert history entry.
	ID string `json:"id"`
	// Severity is a shortcut for the accent colour. Color overrides it.
	Severity Severity `json:"severity,omitempty"`
	// Color is an explicit accent as 0xRRGGBB. Zero derives it from severity.
	Color     int    `json:"color,omitempty"`
	Title     string `json:"title"`
	Summary   string `json:"summary,omitempty"`
	Details   string `json:"details,omitempty"`
	URL       string `json:"url,omitempty"`
	Image     string `json:"image,omitempty"`
	Thumbnail string `json:"thumbnail,omitempty"`
	Author    Author `json:"author,omitempty"`
	Footer    string `json:"footer,omitempty"`
	// FooterIcon is a small icon shown beside the footer text.
	FooterIcon string `json:"footer_icon,omitempty"`
	// Fields are labelled values rendered beside the event.
	Fields []Field   `json:"fields,omitempty"`
	Time   time.Time `json:"time"`
}

// Result is the health verdict for one monitor tick. Notifications are emitted
// as events during the tick; the result only reports whether the check ran and
// whether the watched thing is healthy. The daemon pages on failure and
// recovery from the status alone.
type Result struct {
	Status  ResultStatus `json:"status"`
	Summary string       `json:"summary,omitempty"`
	Details string       `json:"details,omitempty"`
	// State is the monitor state after this tick, set by the SDK when the
	// monitor saved state. Nil means unchanged.
	State json.RawMessage `json:"state,omitempty"`
	// Revision is the state revision this tick was based on.
	Revision int64 `json:"revision,omitempty"`
}

// Outbound is one framed worker-to-daemon message.
type Outbound struct {
	RunID  string       `json:"run_id,omitempty"`
	Type   OutboundType `json:"type"`
	Ready  *Ready       `json:"ready,omitempty"`
	Log    *Log         `json:"log,omitempty"`
	Event  *Event       `json:"event,omitempty"`
	Result *Result      `json:"result,omitempty"`
}

// Describe is what a monitor reports for FlagDescribe.
type Describe struct {
	Definition Definition      `json:"definition"`
	State      json.RawMessage `json:"state"`
}

// DescribeInput is the optional stored state piped to FlagDescribe on stdin.
type DescribeInput struct {
	State   json.RawMessage `json:"state,omitempty"`
	Version int             `json:"version,omitempty"`
}

// Success returns a successful result.
func Success(summary string) Result {
	return Result{
		Status:  StatusSuccess,
		Summary: summary,
	}
}

// Failure returns a failed result. The daemon pages on the failure edge.
func Failure(summary string) Result {
	return Result{
		Status:  StatusFailure,
		Summary: summary,
	}
}

// Failuref returns a failed result using a formatted summary.
func Failuref(format string, args ...any) Result {
	return Failure(fmt.Sprintf(format, args...))
}

// Successf returns a successful result using a formatted summary.
func Successf(format string, args ...any) Result {
	return Success(fmt.Sprintf(format, args...))
}

// WithDefaults returns d with zero-value protocol fields filled in.
func (d Definition) WithDefaults() Definition {
	if d.Clients <= 0 {
		d.Clients = 1
	}
	if d.Protocol == 0 {
		d.Protocol = Protocol
	}

	return d
}

// Validate reports whether the definition is usable by the daemon.
func (d Definition) Validate() error {
	if d.Clients <= 0 {
		return fmt.Errorf("monitor clients must be positive, got %d", d.Clients)
	}
	if d.Protocol != Protocol {
		return fmt.Errorf("monitor speaks protocol %d, daemon speaks %d", d.Protocol, Protocol)
	}

	return nil
}

// Validate reports whether the hello message is usable by a worker.
func (h Hello) Validate() error {
	if h.Monitor == "" {
		return errors.New("hello monitor name is required")
	}
	return nil
}

// Validate reports whether the tick is usable by a worker.
func (t Tick) Validate() error {
	if t.RunID == "" {
		return errors.New("tick run id is required")
	}
	if t.StartedAt.IsZero() {
		return errors.New("tick started_at is required")
	}

	return nil
}

// Validate reports whether the inbound message is internally consistent.
func (i Inbound) Validate() error {
	switch i.Type {
	case InboundHello:
		if i.Hello == nil {
			return errors.New("hello message missing hello payload")
		}

		return i.Hello.Validate()
	case InboundTick:
		if i.Tick == nil {
			return errors.New("tick message missing tick payload")
		}

		return i.Tick.Validate()
	default:
		return fmt.Errorf("unsupported inbound message type %q", i.Type)
	}
}

// Validate reports whether the outbound message is internally consistent.
func (o Outbound) Validate() error {
	switch o.Type {
	case OutboundReady:
		if o.Ready == nil {
			return errors.New("ready message missing ready payload")
		}

		return nil
	case OutboundLog:
		if o.Log == nil {
			return errors.New("log message missing log payload")
		}

		return o.Log.Validate()
	case OutboundEvent:
		if o.Event == nil {
			return errors.New("event message missing event payload")
		}

		return o.Event.Validate()
	case OutboundResult:
		if o.Result == nil {
			return errors.New("result message missing result payload")
		}

		return o.Result.Validate()
	default:
		return fmt.Errorf("unsupported outbound message type %q", o.Type)
	}
}

// Validate reports whether the log line is usable.
func (l Log) Validate() error {
	if err := l.Level.Validate(); err != nil {
		return err
	}
	if l.Message == "" {
		return errors.New("log message is required")
	}

	return nil
}

// Validate checks only the structural wire fields. Content problems — a blank
// title, an empty field, a malformed URL — are not rejected here: the renderer
// substitutes a visible per-field fallback so a broken event still delivers and
// flags itself inline rather than vanishing.
func (i Event) Validate() error {
	if strings.TrimSpace(i.ID) == "" {
		return errors.New("event id is required")
	}
	if i.Severity != "" {
		return i.Severity.Validate()
	}

	return nil
}

// Validate reports whether the result is usable.
func (r Result) Validate() error {
	return r.Status.Validate()
}

// Validate reports whether the result status is supported.
func (s ResultStatus) Validate() error {
	switch s {
	case StatusSuccess, StatusFailure:
		return nil
	case "":
		return errors.New("result status is required")
	default:
		return fmt.Errorf("unsupported result status %q", s)
	}
}

// Validate reports whether the log level is supported.
func (l LogLevel) Validate() error {
	switch l {
	case LogDebug, LogInfo, LogWarn, LogError:
		return nil
	default:
		return fmt.Errorf("unsupported log level %q", l)
	}
}

// Validate reports whether the event severity is supported.
func (s Severity) Validate() error {
	switch s {
	case SeverityInfo, SeverityWarn, SeverityCritical:
		return nil
	default:
		return fmt.Errorf("unsupported event severity %q", s)
	}
}

// String returns the raw result status.
func (s ResultStatus) String() string { return string(s) }

// String returns the raw log level.
func (l LogLevel) String() string { return string(l) }

// String returns the raw severity.
func (s Severity) String() string { return string(s) }

// String returns the raw monitor name.
func (n MonitorName) String() string { return string(n) }
