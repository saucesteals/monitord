// Package routes delivers monitor notifications to their destinations.
package routes

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/saucesteals/monitord/internal/model"
)

// Delivery is one monitor-owned notification destination.
type Delivery struct {
	Discord   *Discord        `json:"discord,omitempty"`
	Route     model.RouteName `json:"route,omitempty"`
	Options   Options         `json:"options,omitempty"`
	RateLimit RateLimit       `json:"rate_limit"`
}

// RateLimit bounds delivery attempts to one destination. Both fields must be
// set together; a zero value disables throttling.
type RateLimit struct {
	PerSecond float64 `json:"per_second"`
	Burst     int     `json:"burst"`
}

// Options is persisted configuration owned by an agent route driver.
type Options map[string]string

// Discord is a Discord bot or webhook destination. Account plus ChannelID are
// mutually exclusive with WebhookURL. Bot credentials are loaded from Keychain.
type Discord struct {
	Account    string `json:"account,omitempty"`
	ChannelID  string `json:"channel_id,omitempty"`
	ThreadID   string `json:"thread_id,omitempty"`
	WebhookURL string `json:"webhook_url,omitempty"`
	Mentions   string `json:"mentions,omitempty"`
}

// Validate reports whether d has exactly one usable Discord destination.
func (d Delivery) Validate() error {
	if err := d.RateLimit.Validate(); err != nil {
		return fmt.Errorf("rate limit: %w", err)
	}
	if d.Discord != nil {
		if d.Route != "" {
			return errors.New("discord delivery cannot also name an agent route")
		}
		if len(d.Options) != 0 {
			return errors.New("discord delivery cannot include route options")
		}

		return d.Discord.Validate()
	}

	return d.Route.Validate()
}

// Validate reports whether the rate limit is disabled or fully configured.
func (l RateLimit) Validate() error {
	if math.IsNaN(l.PerSecond) || math.IsInf(l.PerSecond, 0) {
		return errors.New("per_second must be finite")
	}
	if l.PerSecond == 0 && l.Burst == 0 {
		return nil
	}
	if l.PerSecond <= 0 {
		return errors.New("per_second must be positive")
	}
	if l.Burst <= 0 {
		return errors.New("burst must be positive")
	}

	return nil
}

// Enabled reports whether delivery throttling is configured.
func (l RateLimit) Enabled() bool { return l.PerSecond > 0 }

// Validate reports whether d has exactly one usable Discord destination.
func (d Discord) Validate() error {
	d.Account = strings.TrimSpace(d.Account)
	d.ChannelID = strings.TrimSpace(d.ChannelID)
	d.ThreadID = strings.TrimSpace(d.ThreadID)
	d.WebhookURL = strings.TrimSpace(d.WebhookURL)

	if _, err := ParseMentions(d.Mentions); err != nil {
		return err
	}
	if d.ThreadID != "" && !isSnowflake(d.ThreadID) {
		return fmt.Errorf("invalid discord thread_id %q", d.ThreadID)
	}

	switch {
	case d.Account != "" && d.WebhookURL != "":
		return errors.New("discord account and webhook_url are mutually exclusive")
	case d.Account == "" && d.WebhookURL == "":
		return errors.New("discord requires account or webhook_url")
	case d.WebhookURL != "":
		if d.ChannelID != "" {
			return errors.New("discord webhook_url is mutually exclusive with channel_id")
		}
		if err := validateDiscordWebhookURL(d.WebhookURL); err != nil {
			return err
		}
	case d.Account == "":
		return errors.New("discord account is required with channel_id")
	case !isAccountName(d.Account):
		return fmt.Errorf("invalid discord account %q", d.Account)
	case !isSnowflake(d.ChannelID):
		return errors.New("discord channel_id is required and must be a Discord ID")
	}

	return nil
}

// Describe returns non-secret destination details for CLI output.
func (d Delivery) Describe() string {
	description := ""
	if d.Discord == nil {
		description = d.Route.String()
	} else if d.Discord.WebhookURL != "" {
		description = "discord webhook " + RedactURL(d.Discord.WebhookURL)
	} else {
		target := d.Discord.ChannelID
		if d.Discord.ThreadID != "" {
			target = d.Discord.ThreadID
		}
		description = fmt.Sprintf("discord account=%s channel=%s", d.Discord.Account, target)
	}
	if d.RateLimit.Enabled() {
		description += fmt.Sprintf(" rate=%.4g/s burst=%d", d.RateLimit.PerSecond, d.RateLimit.Burst)
	}

	return description
}

// DeliverDiscord sends msg to a direct Discord destination.
func DeliverDiscord(ctx context.Context, delivery Delivery, msg Message) error {
	if delivery.Discord == nil {
		return errors.New("delivery is not direct Discord")
	}
	if err := delivery.Discord.Validate(); err != nil {
		return err
	}

	mentions, err := ParseMentions(delivery.Discord.Mentions)
	if err != nil {
		return err
	}
	if msg.MuteMentions {
		mentions = nil
	}

	if delivery.Discord.WebhookURL != "" {
		webhookURL, err := withDiscordThreadID(delivery.Discord.WebhookURL, delivery.Discord.ThreadID)
		if err != nil {
			return err
		}

		return SendDiscord(ctx, webhookURL, msg, mentions)
	}

	token, err := AccountToken(ctx, "discord", delivery.Discord.Account)
	if err != nil {
		return err
	}
	target := delivery.Discord.ChannelID
	if delivery.Discord.ThreadID != "" {
		target = delivery.Discord.ThreadID
	}

	return SendDiscordBot(ctx, token, target, msg, mentions)
}

// CloneDeliveries returns an independently mutable delivery list.
func CloneDeliveries(deliveries []Delivery) []Delivery {
	cloned := make([]Delivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		copy := delivery
		if delivery.Discord != nil {
			discord := *delivery.Discord
			copy.Discord = &discord
		}
		copy.Options = CloneOptions(delivery.Options)
		cloned = append(cloned, copy)
	}

	return cloned
}

// Level selects a notification's accent colour.
type Level string

const (
	// LevelInfo is neutral information.
	LevelInfo Level = "info"
	// LevelSuccess is a healthy or recovered state.
	LevelSuccess Level = "success"
	// LevelWarn is a warning event.
	LevelWarn Level = "warn"
	// LevelFailure is a failed check.
	LevelFailure Level = "failure"
	// LevelCritical is a high-importance failure.
	LevelCritical Level = "critical"
)

// Field is one labelled value shown in a notification.
type Field struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

// Author is the attribution line above a message title.
type Author struct {
	Name    string `json:"name"`
	URL     string `json:"url,omitempty"`
	IconURL string `json:"icon_url,omitempty"`
}

// Message is a destination-neutral monitor notification.
type Message struct {
	Title string `json:"title"`
	// Message is a compact notification preview. Direct Discord deliveries put
	// it in top-level content ahead of any configured mention.
	Message    string  `json:"message,omitempty"`
	Summary    string  `json:"summary,omitempty"`
	Details    string  `json:"details,omitempty"`
	URL        string  `json:"url,omitempty"`
	Image      string  `json:"image,omitempty"`
	Thumbnail  string  `json:"thumbnail,omitempty"`
	Author     Author  `json:"author,omitempty"`
	Level      Level   `json:"level,omitempty"`
	Color      int     `json:"color,omitempty"`
	Fields     []Field `json:"fields,omitempty"`
	Footer     string  `json:"footer,omitempty"`
	FooterIcon string  `json:"footer_icon,omitempty"`
	// MuteMentions prevents health failures and recoveries from paging people.
	MuteMentions bool `json:"mute_mentions,omitempty"`
	// Time is the notification timestamp. Zero means now at render time.
	Time time.Time `json:"time"`
}

// Driver owns validation and delivery for an agent route backend.
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

// Register makes an agent route driver available to storage and the CLI.
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

// PrepareRoute validates one persisted agent route.
func PrepareRoute(kind model.RouteKind, options Options) (Options, error) {
	driver, err := getDriver(kind)
	if err != nil {
		return nil, err
	}

	return driver.PrepareRoute(CloneOptions(options))
}

// ValidateMonitor validates a monitor's agent route options.
func ValidateMonitor(kind model.RouteKind, options Options) error {
	driver, err := getDriver(kind)
	if err != nil {
		return err
	}

	return driver.ValidateMonitor(options)
}

// DescribeRoute renders non-secret agent route configuration.
func DescribeRoute(kind model.RouteKind, options Options) (string, error) {
	driver, err := getDriver(kind)
	if err != nil {
		return "", err
	}

	return driver.DescribeRoute(options), nil
}

// DescribeMonitor renders monitor-owned agent route options.
func DescribeMonitor(kind model.RouteKind, options Options) (string, error) {
	driver, err := getDriver(kind)
	if err != nil {
		return "", err
	}

	return driver.DescribeMonitor(options), nil
}

// Deliver sends one notification through an agent route driver.
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

// Test sends a driver-owned agent route test notification.
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
