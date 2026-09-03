// Package delivery sends monitor notifications to their destinations.
package delivery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Delivery is one monitor-owned notification destination.
type Delivery struct {
	Discord   *Discord  `json:"discord,omitempty"`
	OpenClaw  *OpenClaw `json:"openclaw,omitempty"`
	RateLimit RateLimit `json:"rate_limit"`
}

// RateLimit bounds delivery attempts to one destination. Both fields must be
// set together; a zero value disables throttling.
type RateLimit struct {
	PerSecond float64 `json:"per_second"`
	Burst     int     `json:"burst"`
}

// Discord is a Discord bot or webhook destination. Account plus ChannelID are
// mutually exclusive with WebhookURL. Bot credentials are loaded from Keychain.
type Discord struct {
	Account    string `json:"account,omitempty"`
	ChannelID  string `json:"channel_id,omitempty"`
	ThreadID   string `json:"thread_id,omitempty"`
	WebhookURL string `json:"webhook_url,omitempty"`
	Mentions   string `json:"mentions,omitempty"`
}

// Validate reports whether d has exactly one usable destination.
func (d Delivery) Validate() error {
	if err := d.RateLimit.Validate(); err != nil {
		return fmt.Errorf("rate limit: %w", err)
	}
	switch {
	case d.Discord != nil && d.OpenClaw != nil:
		return errors.New("delivery must contain exactly one of discord or openclaw")
	case d.Discord != nil:
		return d.Discord.Validate()
	case d.OpenClaw != nil:
		return d.OpenClaw.Validate()
	default:
		return errors.New("delivery requires discord or openclaw")
	}
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
	if d.OpenClaw != nil {
		description = d.OpenClaw.Describe()
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
		if delivery.OpenClaw != nil {
			openClaw := *delivery.OpenClaw
			copy.OpenClaw = &openClaw
		}
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
