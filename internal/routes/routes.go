// Package routes delivers monitor notifications to their destinations.
package routes

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/saucesteals/monitord/internal/model"
)

// Options is persisted configuration owned by a route driver.
type Options map[string]string

// Delivery binds one stored route to its per-monitor options.
type Delivery struct {
	Route   model.RouteName `json:"route"`
	Options Options         `json:"options,omitempty"`
}

// Level selects a notification's accent colour.
type Level string

const (
	// LevelInfo is neutral information.
	LevelInfo Level = "info"
	// LevelSuccess is a healthy or recovered state.
	LevelSuccess Level = "success"
	// LevelWarn is a warning.
	LevelWarn Level = "warn"
	// LevelFailure is a failed check.
	LevelFailure Level = "failure"
	// LevelCritical is a high-importance failure.
	LevelCritical Level = "critical"
)

// Field is one labelled value shown in a notification.
type Field struct {
	Name   string
	Value  string
	Inline bool
}

// Author is the attribution line above a message title.
type Author struct {
	Name    string
	URL     string
	IconURL string
}

// Message is a route-neutral monitor notification.
type Message struct {
	Title      string
	Summary    string
	Details    string
	URL        string
	Image      string
	Thumbnail  string
	Author     Author
	Level      Level
	Color      int // explicit accent as 0xRRGGBB; zero derives from Level
	Fields     []Field
	Footer     string
	FooterIcon string
	// MuteMentions prevents drivers that support mentions from notifying their
	// configured targets. Health failures and recoveries use this so only
	// monitor-declared events page people.
	MuteMentions bool
	// Time is the notification timestamp. Zero means "now" at render.
	Time time.Time
}

// Driver owns validation, display, and delivery for one route kind.
type Driver interface {
	Kind() model.RouteKind
	PrepareRoute(Options) (Options, error)
	ValidateMonitor(Options) error
	DescribeRoute(Options) string
	DescribeMonitor(Options) string
	TestOptions() Options
	Deliver(context.Context, Options, Options, Message) error
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[model.RouteKind]Driver)
)

// Register makes a route driver available to storage, the CLI, and the daemon.
func Register(driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()

	kind := driver.Kind()
	if err := kind.Validate(); err != nil {
		panic(err)
	}
	if _, exists := drivers[kind]; exists {
		panic(fmt.Sprintf("route driver %q already registered", kind))
	}
	drivers[kind] = driver
}

// PrepareRoute applies driver defaults and validates persisted route options.
func PrepareRoute(kind model.RouteKind, options Options) (Options, error) {
	driver, err := getDriver(kind)
	if err != nil {
		return nil, err
	}

	return driver.PrepareRoute(CloneOptions(options))
}

// ValidateMonitor checks per-monitor options against the selected driver.
func ValidateMonitor(kind model.RouteKind, options Options) error {
	driver, err := getDriver(kind)
	if err != nil {
		return err
	}

	return driver.ValidateMonitor(options)
}

// DescribeRoute renders non-secret route configuration for CLI output.
func DescribeRoute(kind model.RouteKind, options Options) (string, error) {
	driver, err := getDriver(kind)
	if err != nil {
		return "", err
	}

	return driver.DescribeRoute(options), nil
}

// DescribeMonitor renders per-monitor route options for CLI output.
func DescribeMonitor(kind model.RouteKind, options Options) (string, error) {
	driver, err := getDriver(kind)
	if err != nil {
		return "", err
	}

	return driver.DescribeMonitor(options), nil
}

// Deliver sends one notification through the selected route driver.
func Deliver(ctx context.Context, kind model.RouteKind, routeOptions Options, monitorOptions Options, msg Message) error {
	driver, err := getDriver(kind)
	if err != nil {
		return err
	}
	if err := driver.ValidateMonitor(monitorOptions); err != nil {
		return err
	}

	return driver.Deliver(ctx, routeOptions, monitorOptions, msg)
}

// Test sends a driver-owned test notification.
func Test(ctx context.Context, kind model.RouteKind, routeOptions Options, msg Message) error {
	driver, err := getDriver(kind)
	if err != nil {
		return err
	}

	return driver.Deliver(ctx, routeOptions, driver.TestOptions(), msg)
}

// CloneOptions returns an independently mutable copy.
func CloneOptions(options Options) Options {
	cloned := make(Options, len(options))
	for key, value := range options {
		cloned[NormalizeOptionKey(key)] = value
	}

	return cloned
}

// CloneDeliveries returns an independently mutable delivery list.
func CloneDeliveries(deliveries []Delivery) []Delivery {
	cloned := make([]Delivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		cloned = append(cloned, Delivery{
			Route:   delivery.Route,
			Options: CloneOptions(delivery.Options),
		})
	}

	return cloned
}

// NormalizeOptionKey gives CLI and JSON spellings one canonical form.
func NormalizeOptionKey(key string) string {
	return strings.ReplaceAll(strings.TrimSpace(key), "-", "_")
}

func getDriver(kind model.RouteKind) (Driver, error) {
	driversMu.RLock()
	driver := drivers[kind]
	driversMu.RUnlock()
	if driver == nil {
		return nil, fmt.Errorf("unsupported route kind %q", kind)
	}

	return driver, nil
}

func validateOptionKeys(options Options, allowed ...string) error {
	allow := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allow[key] = struct{}{}
	}

	var unknown []string
	for key := range options {
		if _, ok := allow[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)

	return fmt.Errorf("unsupported route option(s): %s", strings.Join(unknown, ", "))
}
