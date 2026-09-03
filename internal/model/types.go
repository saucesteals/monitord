package model

import (
	"fmt"
)

// MonitorName identifies a deployed monitor.
type MonitorName string

// ParseMonitorName validates a monitor name.
func ParseMonitorName(value string) (MonitorName, error) {
	if !validName(value) {
		return "", fmt.Errorf("invalid monitor name %q", value)
	}

	return MonitorName(value), nil
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
