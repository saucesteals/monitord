package model

import (
	"fmt"
	"strings"
)

// MonitorName identifies a deployed monitor.
type MonitorName string

// RouteKind identifies a notification route backend.
type RouteKind string

const (
	// RouteKindDiscord sends notifications through Discord webhooks.
	RouteKindDiscord RouteKind = "discord"
)

// RouteName identifies a daemon-owned notification route.
type RouteName string

// MonitorStatus is the scheduler lifecycle state for a monitor.
type MonitorStatus string

const (
	// MonitorStatusActive means a monitor is eligible to run.
	MonitorStatusActive MonitorStatus = "active"
	// MonitorStatusExpired means a monitor has been manually or automatically expired.
	MonitorStatusExpired MonitorStatus = "expired"
)

// PoolName identifies a stored proxy pool.
type PoolName string

// ParseMonitorName validates a monitor name.
func ParseMonitorName(value string) (MonitorName, error) {
	if !validName(value) {
		return "", fmt.Errorf("invalid monitor name %q", value)
	}

	return MonitorName(value), nil
}

// ParseRouteKind validates a route kind.
func ParseRouteKind(value string) (RouteKind, error) {
	kind := RouteKind(value)
	if err := kind.Validate(); err != nil {
		return "", err
	}

	return kind, nil
}

// NewRouteName constructs a route name from kind and local name.
func NewRouteName(kind RouteKind, name string) (RouteName, error) {
	if err := kind.Validate(); err != nil {
		return "", err
	}
	if !validName(name) {
		return "", fmt.Errorf("invalid route target %q", name)
	}

	return RouteName(string(kind) + ":" + name), nil
}

// ParseRouteName validates a full route name.
func ParseRouteName(value string) (RouteName, error) {
	route := RouteName(value)
	if err := route.Validate(); err != nil {
		return "", err
	}

	return route, nil
}

// ParseMonitorStatus validates a monitor lifecycle status.
func ParseMonitorStatus(value string) (MonitorStatus, error) {
	status := MonitorStatus(value)
	if err := status.Validate(); err != nil {
		return "", err
	}

	return status, nil
}

// ParsePoolName validates an optional proxy pool name.
func ParsePoolName(value string) (PoolName, error) {
	if value == "" {
		return "", nil
	}
	if !validName(value) {
		return "", fmt.Errorf("invalid proxy pool name %q", value)
	}

	return PoolName(value), nil
}

// String returns the raw monitor name.
func (n MonitorName) String() string {
	return string(n)
}

// Validate checks whether a monitor name is safe for storage and paths.
func (n MonitorName) Validate() error {
	if !validName(string(n)) {
		return fmt.Errorf("invalid monitor name %q", n)
	}

	return nil
}

// String returns the raw route kind.
func (k RouteKind) String() string {
	return string(k)
}

// String returns the raw route name.
func (n RouteName) String() string {
	return string(n)
}

// String returns the raw monitor status.
func (s MonitorStatus) String() string {
	return string(s)
}

// String returns the raw pool name.
func (p PoolName) String() string {
	return string(p)
}

// Validate checks whether a pool name is safe for lookup.
func (p PoolName) Validate() error {
	if p == "" {
		return nil
	}
	if !validName(string(p)) {
		return fmt.Errorf("invalid proxy pool name %q", p)
	}

	return nil
}

// Validate checks whether a route kind is supported.
func (k RouteKind) Validate() error {
	switch k {
	case RouteKindDiscord:
		return nil
	default:
		return fmt.Errorf("unsupported route kind %q", k)
	}
}

// Validate checks whether a route name has a supported kind and safe target.
func (n RouteName) Validate() error {
	kind, target, ok := strings.Cut(string(n), ":")
	if !ok || kind == "" || target == "" {
		return fmt.Errorf("invalid route name %q", n)
	}
	if _, err := ParseRouteKind(kind); err != nil {
		return err
	}
	if !validName(target) {
		return fmt.Errorf("invalid route target %q", target)
	}

	return nil
}

// Validate checks whether a monitor status is supported.
func (s MonitorStatus) Validate() error {
	switch s {
	case MonitorStatusActive, MonitorStatusExpired:
		return nil
	default:
		return fmt.Errorf("unsupported monitor status %q", s)
	}
}

func validName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		if r == '-' || r == '_' || r == '.' {
			continue
		}

		return false
	}

	return true
}
