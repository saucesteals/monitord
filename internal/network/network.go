// Package network owns proxy pools and how they are handed to monitor workers.
//
// A pool is a resource monitord stores and owns, not something the daemon reads
// from its environment: proxies are imported once, kept in the database beside
// the routes that already hold their own credentials, and assigned to workers
// on demand. Adding proxies therefore never requires restarting the daemon.
package network

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"

	"github.com/saucesteals/monitord/internal/model"
)

// Strategy decides which slice of a pool a worker receives.
type Strategy string

const (
	// StrategyRoundRobin hands out consecutive slices, advancing a durable
	// offset so concurrent monitors spread across the pool.
	StrategyRoundRobin Strategy = "round_robin"
	// StrategyRandom shuffles the pool per worker start.
	StrategyRandom Strategy = "random"
	// StrategySticky derives a stable offset from the monitor name, so a
	// monitor keeps the same proxies across restarts.
	StrategySticky Strategy = "sticky"
)

// ParseStrategy validates a strategy, defaulting to round-robin.
func ParseStrategy(value string) (Strategy, error) {
	if value == "" {
		return StrategyRoundRobin, nil
	}

	strategy := Strategy(value)
	if err := strategy.Validate(); err != nil {
		return "", err
	}

	return strategy, nil
}

// Validate reports whether the strategy is supported.
func (s Strategy) Validate() error {
	switch s {
	case StrategyRoundRobin, StrategyRandom, StrategySticky:
		return nil
	default:
		return fmt.Errorf("unsupported proxy strategy %q: want round_robin, random, or sticky", s)
	}
}

// String returns the raw strategy.
func (s Strategy) String() string { return string(s) }

// Assignment is the proxy slice handed to one worker.
type Assignment struct {
	Proxies []string
	// NextOffset is the round-robin offset to persist.
	NextOffset int64
	// Persist reports whether NextOffset must be written back.
	Persist bool
}

// Assign selects size proxies from the pool for the given monitor.
//
// When the pool is smaller than size, proxies repeat so the monitor still gets
// the client count it asked for; each client keeps its own connection pool
// regardless.
func Assign(pool []string, size int, strategy Strategy, name model.MonitorName, offset int64) Assignment {
	if size <= 0 {
		size = 1
	}
	if len(pool) == 0 {
		return Assignment{}
	}

	switch strategy {
	case StrategyRandom:
		shuffled := append([]string(nil), pool...)
		rand.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})

		return Assignment{Proxies: take(shuffled, 0, size)}
	case StrategySticky:
		return Assignment{Proxies: take(pool, int64(hashName(name)), size)}
	default:
		return Assignment{
			Proxies:    take(pool, offset, size),
			NextOffset: (offset + int64(size)) % int64(len(pool)),
			Persist:    true,
		}
	}
}

// take returns size entries from pool starting at offset, wrapping around.
func take(pool []string, offset int64, size int) []string {
	n := int64(len(pool))
	start := ((offset % n) + n) % n

	out := make([]string, 0, size)
	for i := range size {
		out = append(out, pool[(start+int64(i))%n])
	}

	return out
}

func hashName(name model.MonitorName) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))

	return h.Sum32()
}
