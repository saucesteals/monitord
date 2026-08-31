package monitord

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const Protocol = 5
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

type Field struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}
type Author struct {
	Name    string `json:"name,omitempty"`
	URL     string `json:"url,omitempty"`
	IconURL string `json:"icon_url,omitempty"`
}
type Mentions struct {
	Users    []string `json:"users,omitempty"`
	Roles    []string `json:"roles,omitempty"`
	Everyone bool     `json:"everyone,omitempty"`
	Here     bool     `json:"here,omitempty"`
}

func (m Mentions) Validate() error {
	for _, id := range m.Users {
		if !isSnowflake(id) {
			return fmt.Errorf("invalid mention user %q", id)
		}
	}
	for _, id := range m.Roles {
		if !isSnowflake(id) {
			return fmt.Errorf("invalid mention role %q", id)
		}
	}
	return nil
}

type Event struct {
	ID         string        `json:"id"`
	DedupeKey  string        `json:"dedupe_key,omitempty"`
	DedupeFor  time.Duration `json:"dedupe_for,omitempty"`
	Mentions   *Mentions     `json:"mentions,omitempty"`
	Severity   Severity      `json:"severity,omitempty"`
	Color      int           `json:"color,omitempty"`
	Title      string        `json:"title"`
	Message    string        `json:"message,omitempty"`
	Summary    string        `json:"summary,omitempty"`
	Details    string        `json:"details,omitempty"`
	URL        string        `json:"url,omitempty"`
	Image      string        `json:"image,omitempty"`
	Thumbnail  string        `json:"thumbnail,omitempty"`
	Author     Author        `json:"author,omitempty"`
	Footer     string        `json:"footer,omitempty"`
	FooterIcon string        `json:"footer_icon,omitempty"`
	Fields     []Field       `json:"fields,omitempty"`
	Time       time.Time     `json:"time"`
}

func (e Event) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return errors.New("event id is required")
	}
	if len(e.ID) > 512 || len(e.DedupeKey) > 512 {
		return errors.New("event identity exceeds limit")
	}
	if (e.DedupeKey == "") != (e.DedupeFor == 0) || e.DedupeFor < 0 {
		return errors.New("event dedupe key and positive duration must be set together")
	}
	if e.Mentions != nil {
		if err := e.Mentions.Validate(); err != nil {
			return err
		}
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
func isSnowflake(v string) bool {
	if len(v) < 5 || len(v) > 25 {
		return false
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

type DescribeInput struct {
	State   json.RawMessage `json:"state,omitempty"`
	Version int             `json:"version,omitempty"`
}
