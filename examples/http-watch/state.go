package main

import "time"

// State persists across checks and daemon restarts.
type State struct {
	Targets map[string]TargetState `json:"targets"`
	LastOK  time.Time              `json:"last_ok"`
}

// TargetState is the last observation committed for one configured target.
type TargetState struct {
	Status    int  `json:"status,omitempty"`
	Reachable bool `json:"reachable"`
}

// StateVersion pins the stored schema. Bump it and implement MigrateState when
// State changes shape.
func (State) StateVersion() int { return 1 }
