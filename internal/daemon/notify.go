package daemon

import (
	"context"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/routes"
)

func toFields(fields []monitord.Field) []routes.Field {
	if len(fields) == 0 {
		return nil
	}
	out := make([]routes.Field, 0, len(fields))
	for _, field := range fields {
		out = append(out, routes.Field{Name: field.Name, Value: field.Value, Inline: field.Inline})
	}
	return out
}
func eventLevel(severity monitord.Severity) routes.Level {
	switch severity {
	case monitord.SeverityCritical:
		return routes.LevelCritical
	case monitord.SeverityWarn:
		return routes.LevelWarn
	default:
		return routes.LevelInfo
	}
}
func (d *Daemon) deliverRoute(ctx context.Context, delivery routes.Delivery, msg routes.Message) error {
	if delivery.Discord != nil {
		return routes.DeliverDiscord(ctx, delivery, msg)
	}
	route, err := d.store.GetRoute(ctx, delivery.Route)
	if err != nil {
		return err
	}
	return routes.Deliver(ctx, route.Kind, route.Options, delivery.Options, msg)
}
