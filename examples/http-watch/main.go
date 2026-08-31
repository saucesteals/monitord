// Command http-watch demonstrates a small, package-structured monitor.
package main

import (
	"time"

	"github.com/saucesteals/monitord"
)

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{
			Name:        "http-watch",
			Description: "Checks configured HTTP targets",
		},
		monitord.Every(time.Minute, checkTargets),
	))
}
