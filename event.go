package monitord

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	FlagDescribe = "--monitord-describe"
	FlagWorker   = "--monitord-worker"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarn     Severity = "warn"
	SeverityCritical Severity = "critical"
)

// Event is a transport-neutral occurrence emitted by a monitor. Delivery
// adapters decide how to present its fields on their respective platforms.
type Event struct {
	ID       string            `json:"id"`
	Severity Severity          `json:"severity,omitempty"`
	Title    string            `json:"title"`
	Body     string            `json:"body,omitempty"`
	URL      string            `json:"url,omitempty"`
	Time     time.Time         `json:"time"`
	Data     map[string]string `json:"data,omitempty"`
}

func (e Event) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return errors.New("event id is required")
	}
	if len(e.ID) > 512 {
		return errors.New("event identity exceeds limit")
	}
	if strings.TrimSpace(e.Title) == "" {
		return errors.New("event title is required")
	}
	if e.Severity != "" {
		return e.Severity.Validate()
	}
	return nil
}
func (s Severity) Validate() error {
	switch s {
	case SeverityInfo, SeverityWarn, SeverityCritical:
		return nil
	default:
		return fmt.Errorf("unsupported event severity %q", s)
	}
}
func (s Severity) String() string { return string(s) }
