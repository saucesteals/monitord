// Command http-watch demonstrates a small, package-structured monitor.
package main

import (
	"time"

	"github.com/saucesteals/monitord"
)

// State is the monitor's durable schema.
type State struct {
	Targets map[string]TargetState `json:"targets"`
	LastOK  time.Time              `json:"last_ok"`
}

type TargetState struct {
	Status    int  `json:"status,omitempty"`
	Reachable bool `json:"reachable"`
}

func (State) StateVersion() int { return 1 }

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{
			Name:        "http-watch",
			Description: "Checks configured HTTP targets",
		},
		monitord.Every(time.Minute, checkTargets),
	))
}
