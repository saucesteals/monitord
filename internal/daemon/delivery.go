package daemon

import (
	"context"
	"sort"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/routes"
)

func dataFields(data map[string]string) []routes.Field {
	if len(data) == 0 {
		return nil
	}
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]routes.Field, 0, len(data))
	for _, key := range keys {
		out = append(out, routes.Field{Name: key, Value: data[key]})
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
