package daemon

import (
	"context"
	"fmt"
	"strings"

	monitor "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
)

// eventMessage renders a monitor event as a route message. The footer defaults to
// the monitor name so every alert carries its provenance unless the event
// overrides it.
func eventMessage(m storage.Monitor, it monitor.Event) routes.Message {
	footer := it.Footer
	if footer == "" {
		footer = m.Name.String()
	}

	return routes.Message{
		Title:      it.Title,
		Summary:    it.Summary,
		Details:    strings.TrimSpace(it.Details),
		URL:        it.URL,
		Image:      it.Image,
		Thumbnail:  it.Thumbnail,
		Author:     routes.Author{Name: it.Author.Name, URL: it.Author.URL, IconURL: it.Author.IconURL},
		Level:      eventLevel(it.Severity),
		Color:      it.Color,
		Fields:     toFields(it.Fields),
		Footer:     footer,
		FooterIcon: it.FooterIcon,
		Time:       it.Time,
	}
}

func resultMessage(m storage.Monitor, result monitor.Result, status monitor.ResultStatus, exitCode int, errorText string, failures int64) routes.Message {
	summary := result.Summary
	if summary == "" {
		summary = errorText
	}
	if summary == "" {
		summary = fmt.Sprintf("exit code %d", exitCode)
	}

	// A result only ever renders as a failure or a recovery; steady states
	// never reach here. Each gets its own headline and colour.
	var (
		title = m.Name.String()
		level = routes.LevelInfo
	)
	switch {
	case status == monitor.StatusFailure:
		title = fmt.Sprintf("%s failed", m.Name)
		level = routes.LevelFailure
	case m.NotifiedStatus == monitor.StatusFailure:
		title = fmt.Sprintf("%s recovered", m.Name)
		level = routes.LevelSuccess
	}

	var fields []routes.Field
	if failures > 1 {
		fields = append(fields, routes.Field{
			Name:   "Consecutive failures",
			Value:  fmt.Sprintf("%d", failures),
			Inline: true,
		})
	}

	return routes.Message{
		Title:        title,
		Summary:      summary,
		Details:      strings.TrimSpace(result.Details),
		Level:        level,
		Fields:       fields,
		Footer:       m.Name.String(),
		MuteMentions: true,
	}
}

// toFields converts monitor-declared fields to their route representation.
func toFields(fields []monitor.Field) []routes.Field {
	if len(fields) == 0 {
		return nil
	}

	out := make([]routes.Field, 0, len(fields))
	for _, field := range fields {
		out = append(out, routes.Field{
			Name:   field.Name,
			Value:  field.Value,
			Inline: field.Inline,
		})
	}

	return out
}

// eventLevel maps a monitor event severity to a notification accent.
func eventLevel(severity monitor.Severity) routes.Level {
	switch severity {
	case monitor.SeverityCritical:
		return routes.LevelCritical
	case monitor.SeverityWarn:
		return routes.LevelWarn
	default:
		return routes.LevelInfo
	}
}

// deliverRoute sends one rendered message to a single route, merging the route's
// stored config with the monitor's per-route options.
func (d *Daemon) deliverRoute(ctx context.Context, delivery routes.Delivery, msg routes.Message) error {
	route, err := d.store.GetRoute(ctx, delivery.Route)
	if err != nil {
		return err
	}

	return routes.Deliver(ctx, route.Kind, route.Options, delivery.Options, msg)
}

func deliveryNames(deliveries []routes.Delivery) string {
	names := make([]string, 0, len(deliveries))
	for _, delivery := range deliveries {
		names = append(names, delivery.Route.String())
	}

	return strings.Join(names, ",")
}
