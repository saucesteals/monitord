// Package monitord is the authoring SDK for durable monitors.
package monitord

import (
	"errors"
	"fmt"
	"regexp"
)

type Info struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

var infoNamePattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

func (i Info) Validate() error {
	if i.Name == "" {
		return errors.New("monitor name is required")
	}
	if len(i.Name) > 63 {
		return fmt.Errorf("monitor name must be at most 63 bytes, got %d", len(i.Name))
	}
	if !infoNamePattern.MatchString(i.Name) {
		return fmt.Errorf("monitor name %q must be lower-case kebab case", i.Name)
	}
	return nil
}

type Monitor[S any] interface {
	Info() Info
	Plan() Plan[S]
}
type definedMonitor[S any] struct {
	info Info
	plan Plan[S]
}

func (m definedMonitor[S]) Info() Info                 { return m.info }
func (m definedMonitor[S]) Plan() Plan[S]              { return m.plan }
func Define[S any](info Info, plan Plan[S]) Monitor[S] { return definedMonitor[S]{info, plan} }

// Run implements the monitor executable contract and does not return.
func Run[S any](monitor Monitor[S]) {
	if err := dispatchMonitor(monitor); err != nil {
		panic(err)
	}
}
