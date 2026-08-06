package routes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// color returns the embed accent for a level.
func (l Level) color() int {
	switch l {
	case LevelSuccess:
		return 0x2ECC71
	case LevelWarn:
		return 0xF1C40F
	case LevelFailure:
		return 0xE74C3C
	case LevelCritical:
		return 0x992D22
	default:
		return 0x3498DB
	}
}

// MentionKind identifies who a delivery pings.
type MentionKind string

const (
	// MentionUser pings one user.
	MentionUser MentionKind = "user"
	// MentionRole pings a role.
	MentionRole MentionKind = "role"
	// MentionHere pings online members of the channel.
	MentionHere MentionKind = "here"
	// MentionEveryone pings the whole channel.
	MentionEveryone MentionKind = "everyone"
)

// Mention is one target a delivery pings when it fires.
type Mention struct {
	Kind MentionKind
	ID   string
}

// ParseMention reads a mention spec: "user:123", "role:456", "here", or
// "everyone".
func ParseMention(value string) (Mention, error) {
	value = strings.TrimSpace(value)
	switch value {
	case "here":
		return Mention{Kind: MentionHere}, nil
	case "everyone":
		return Mention{Kind: MentionEveryone}, nil
	}

	kind, id, ok := strings.Cut(value, ":")
	if !ok {
		return Mention{}, fmt.Errorf("invalid mention %q: want user:ID, role:ID, here, or everyone", value)
	}
	if !isSnowflake(id) {
		return Mention{}, fmt.Errorf("invalid mention %q: %q is not a Discord ID", value, id)
	}

	switch MentionKind(kind) {
	case MentionUser:
		return Mention{Kind: MentionUser, ID: id}, nil
	case MentionRole:
		return Mention{Kind: MentionRole, ID: id}, nil
	default:
		return Mention{}, fmt.Errorf("unsupported mention kind %q", kind)
	}
}

// ParseMentions reads a comma-separated list of mention specs.
func ParseMentions(value string) ([]Mention, error) {
	if strings.TrimSpace(value) == MentionNone {
		return nil, nil
	}

	var mentions []Mention
	for _, part := range strings.Split(value, ",") {
		if strings.TrimSpace(part) == "" {
			continue
		}
		mention, err := ParseMention(part)
		if err != nil {
			return nil, err
		}
		mentions = append(mentions, mention)
	}

	return mentions, nil
}

// String returns the storable spec for a mention.
func (m Mention) String() string {
	switch m.Kind {
	case MentionHere, MentionEveryone:
		return string(m.Kind)
	default:
		return string(m.Kind) + ":" + m.ID
	}
}

// Render returns the Discord markup that produces the ping.
func (m Mention) Render() string {
	switch m.Kind {
	case MentionUser:
		return "<@" + m.ID + ">"
	case MentionRole:
		return "<@&" + m.ID + ">"
	case MentionHere:
		return "@here"
	case MentionEveryone:
		return "@everyone"
	default:
		return ""
	}
}

// FormatMentions joins mention specs for display and storage.
func FormatMentions(mentions []Mention) string {
	if len(mentions) == 0 {
		return ""
	}

	specs := make([]string, 0, len(mentions))
	for _, mention := range mentions {
		specs = append(specs, mention.String())
	}

	return strings.Join(specs, ",")
}

// MentionNone silences a delivery's mention list.
const MentionNone = "none"

// allowedMentions constrains which pings Discord will honour in a payload.
//
// This is a hard allowlist, not a formatting detail: monitor output can contain
// scraped text, and without it a page containing "@everyone" would ping the
// whole server.
type allowedMentions struct {
	Parse []string `json:"parse"`
	Users []string `json:"users,omitempty"`
	Roles []string `json:"roles,omitempty"`
}

type embedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

type embedFooter struct {
	Text    string `json:"text"`
	IconURL string `json:"icon_url,omitempty"`
}

type embedImage struct {
	URL string `json:"url"`
}

type embedAuthor struct {
	Name    string `json:"name"`
	URL     string `json:"url,omitempty"`
	IconURL string `json:"icon_url,omitempty"`
}

type embed struct {
	Title       string       `json:"title,omitempty"`
	Description string       `json:"description,omitempty"`
	URL         string       `json:"url,omitempty"`
	Color       int          `json:"color"`
	Author      *embedAuthor `json:"author,omitempty"`
	Fields      []embedField `json:"fields,omitempty"`
	Image       *embedImage  `json:"image,omitempty"`
	Thumbnail   *embedImage  `json:"thumbnail,omitempty"`
	Footer      *embedFooter `json:"footer,omitempty"`
	Timestamp   string       `json:"timestamp,omitempty"`
}

type discordPayload struct {
	// Content carries the compact notification preview followed by mentions.
	// Mentions inside an embed do not notify anyone, so the ping has to live
	// out here.
	Content         string          `json:"content,omitempty"`
	Embeds          []embed         `json:"embeds,omitempty"`
	AllowedMentions allowedMentions `json:"allowed_mentions"`
}

// Discord's documented limits.
const (
	maxTitle       = 256
	maxContent     = 2000
	maxDescription = 4096
	maxFieldName   = 256
	maxFieldValue  = 1024
	maxFields      = 25
	maxAuthor      = 256
	maxFooter      = 2048
)

// SendDiscord sends a monitor notification to a Discord webhook.
func SendDiscord(ctx context.Context, webhookURL string, msg Message, mentions []Mention) error {
	return sendDiscord(ctx, webhookURL, "", msg, mentions)
}

// SendDiscordBot sends a monitor notification to a Discord channel as a bot.
func SendDiscordBot(ctx context.Context, token string, channelID string, msg Message, mentions []Mention) error {
	endpoint := "https://discord.com/api/v10/channels/" + channelID + "/messages"

	return sendDiscord(ctx, endpoint, "Bot "+token, msg, mentions)
}

func sendDiscord(ctx context.Context, endpoint string, authorization string, msg Message, mentions []Mention) error {
	body, err := json.Marshal(discordPayload{
		Content:         renderContent(msg.Message, mentions),
		Embeds:          []embed{buildEmbed(msg)},
		AllowedMentions: allowFor(mentions),
	})
	if err != nil {
		return fmt.Errorf("encode discord payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build discord request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "monitord/0")
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send discord notification: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("discord returned HTTP %d", resp.StatusCode)
	}

	return nil
}

func validateDiscordWebhookURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse discord webhook_url: %w", err)
	}
	if u.Scheme != "https" || u.Host != "discord.com" {
		return errors.New("discord webhook_url must use https://discord.com")
	}
	if u.Query().Has("thread_id") {
		return errors.New("discord webhook_url must not include thread_id; set thread_id beside it")
	}
	parts := strings.Split(strings.Trim(path.Clean(u.Path), "/"), "/")
	if len(parts) != 4 || parts[0] != "api" || parts[1] != "webhooks" || !isSnowflake(parts[2]) || parts[3] == "" {
		return errors.New("invalid Discord webhook_url")
	}

	return nil
}

func withDiscordThreadID(raw string, threadID string) (string, error) {
	if threadID == "" {
		return raw, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse discord webhook_url: %w", err)
	}
	query := u.Query()
	query.Set("thread_id", threadID)
	u.RawQuery = query.Encode()

	return u.String(), nil
}

func isAccountName(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}

	return true
}

// buildEmbed renders a message as a single Discord embed.
//
// Every field is sanitised rather than validated away: a blank title, an empty
// field name or value, or a malformed URL would each make Discord reject the
// whole embed with a 400, silently losing the notification. Instead each field
// falls back to a visible placeholder (or, for a URL, is dropped) so the alert
// always lands and shows its own defect inline.
func buildEmbed(msg Message) embed {
	description := msg.Summary
	if msg.Details != "" {
		if description != "" {
			description += "\n\n"
		}
		description += msg.Details
	}

	title := strings.TrimSpace(msg.Title)
	if title == "" {
		title = "(no title)"
	}

	color := msg.Color
	if color == 0 {
		color = msg.Level.color()
	}
	stamp := msg.Time
	if stamp.IsZero() {
		stamp = time.Now()
	}
	out := embed{
		Title:       truncate(title, maxTitle),
		Description: truncate(description, maxDescription),
		URL:         safeURL(msg.URL),
		Color:       color,
		Timestamp:   stamp.UTC().Format(time.RFC3339),
	}
	if msg.Footer != "" {
		out.Footer = &embedFooter{Text: truncate(msg.Footer, maxFooter), IconURL: safeURL(msg.FooterIcon)}
	}
	if msg.Author.Name != "" || msg.Author.URL != "" || msg.Author.IconURL != "" {
		name := strings.TrimSpace(msg.Author.Name)
		if name == "" {
			name = "(unnamed)"
		}
		out.Author = &embedAuthor{Name: truncate(name, maxAuthor), URL: safeURL(msg.Author.URL), IconURL: safeURL(msg.Author.IconURL)}
	}
	if u := safeURL(msg.Image); u != "" {
		out.Image = &embedImage{URL: u}
	}
	if u := safeURL(msg.Thumbnail); u != "" {
		out.Thumbnail = &embedImage{URL: u}
	}

	for _, field := range msg.Fields {
		if len(out.Fields) == maxFields {
			break
		}
		name := strings.TrimSpace(field.Name)
		if name == "" {
			name = "(unnamed)"
		}
		value := strings.TrimSpace(field.Value)
		if value == "" {
			value = "(empty)"
		}
		out.Fields = append(out.Fields, embedField{
			Name:   truncate(name, maxFieldName),
			Value:  truncate(value, maxFieldValue),
			Inline: field.Inline,
		})
	}

	return out
}

// safeURL returns raw when it is an absolute http(s) URL, and "" otherwise.
// Discord rejects the entire embed if any URL field is malformed, so an invalid
// link is dropped rather than allowed to sink the whole notification.
func safeURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return ""
	}

	return raw
}

func renderMentions(mentions []Mention) string {
	rendered := make([]string, 0, len(mentions))
	for _, mention := range mentions {
		if markup := mention.Render(); markup != "" {
			rendered = append(rendered, markup)
		}
	}

	return strings.Join(rendered, " ")
}

// renderContent puts the human-readable notification preview before the ping,
// which keeps Discord's notification preview useful while still allowing the
// configured mention to notify the intended audience.
func renderContent(message string, mentions []Mention) string {
	message = strings.TrimSpace(message)
	ping := renderMentions(mentions)
	if message == "" {
		return truncate(ping, maxContent)
	}
	if ping == "" {
		return truncate(message, maxContent)
	}

	messageLimit := maxContent - len(ping) - 1
	if messageLimit <= 0 {
		return truncate(ping, maxContent)
	}

	return truncate(message, messageLimit) + "\n" + ping
}

// allowFor builds an allowlist matching exactly the delivery's mentions. Parse is
// a non-nil empty slice by default, which tells Discord to honour no mention it
// finds in the text.
func allowFor(mentions []Mention) allowedMentions {
	allowed := allowedMentions{Parse: []string{}}
	for _, mention := range mentions {
		switch mention.Kind {
		case MentionUser:
			allowed.Users = append(allowed.Users, mention.ID)
		case MentionRole:
			allowed.Roles = append(allowed.Roles, mention.ID)
		case MentionHere, MentionEveryone:
			allowed.Parse = append(allowed.Parse, "everyone")
		}
	}

	return allowed
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}

	return value[:limit-1] + "…"
}

func isSnowflake(value string) bool {
	if len(value) < 5 || len(value) > 25 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// RedactURL redacts write-only webhook capability URLs for logs and CLI output.
func RedactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<redacted>"
	}
	if parsed.Host == "" {
		return "<redacted>"
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) > 0 {
		parts[len(parts)-1] = "redacted"
	}
	parsed.Path = "/" + strings.Join(parts, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.User = nil

	return parsed.String()
}
