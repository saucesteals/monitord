package monitord

import (
	"errors"
	"fmt"
	"strings"
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
	ID           string            `json:"id"`
	CorrectionOf string            `json:"correction_of,omitempty"`
	Severity     Severity          `json:"severity,omitempty"`
	Title        string            `json:"title"`
	Body         string            `json:"body,omitempty"`
	URL          string            `json:"url,omitempty"`
	Data         map[string]string `json:"data,omitempty"`

	// Description supplies extended notification text, separate from Body.
	Description string `json:"description,omitempty"`
	// Image is the notification thumbnail URL for adapters that support it.
	Image string `json:"image,omitempty"`
	// Color overrides the severity color; zero keeps the adapter default.
	Color int `json:"color,omitempty"`
	// InlineData requests inline layout for Data fields where supported.
	InlineData bool `json:"inline_data,omitempty"`
	// HideFooter omits the deployment-name footer from the notification.
	HideFooter bool `json:"hide_footer,omitempty"`
	// MuteMentions suppresses configured mentions for this occurrence.
	MuteMentions bool `json:"mute_mentions,omitempty"`
}

func (e Event) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return errors.New("event id is required")
	}
	if len(e.ID) > 512 {
		return errors.New("event identity exceeds limit")
	}
	if e.CorrectionOf != "" {
		if strings.TrimSpace(e.CorrectionOf) == "" {
			return errors.New("corrected event identity is blank")
		}
		if len(e.CorrectionOf) > 512 {
			return errors.New("corrected event identity exceeds limit")
		}
		if e.CorrectionOf == e.ID {
			return errors.New("correction must have its own event identity")
		}
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
