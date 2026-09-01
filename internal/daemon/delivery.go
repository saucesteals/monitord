package daemon

import (
	"context"
	"errors"
	"sort"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/delivery"
)

func dataFields(data map[string]string) []delivery.Field {
	if len(data) == 0 {
		return nil
	}
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]delivery.Field, 0, len(data))
	for _, key := range keys {
		out = append(out, delivery.Field{Name: key, Value: data[key]})
	}
	return out
}
func eventLevel(severity monitord.Severity) delivery.Level {
	switch severity {
	case monitord.SeverityCritical:
		return delivery.LevelCritical
	case monitord.SeverityWarn:
		return delivery.LevelWarn
	default:
		return delivery.LevelInfo
	}
}
func (d *Daemon) deliverDestination(ctx context.Context, destination delivery.Delivery, msg delivery.Message) error {
	if err := destination.Validate(); err != nil {
		return err
	}
	switch {
	case destination.Discord != nil:
		return delivery.DeliverDiscord(ctx, destination, msg)
	case destination.OpenClaw != nil:
		return delivery.DeliverOpenClaw(ctx, *destination.OpenClaw, msg)
	default:
		return errors.New("delivery has no destination")
	}
}
