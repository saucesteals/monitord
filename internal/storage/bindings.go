package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

func (s *Store) ListActiveBindings(ctx context.Context, deploymentID string) ([]DestinationBinding, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,revision,deployment_id,config,created_at,retired_at FROM destination_bindings WHERE deployment_id=? AND retired_at IS NULL ORDER BY id`, deploymentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DestinationBinding
	for rows.Next() {
		var b DestinationBinding
		var created int64
		var retired sql.NullInt64
		if err = rows.Scan(&b.ID, &b.Revision, &b.DeploymentID, &b.Config, &created, &retired); err != nil {
			return nil, err
		}
		b.CreatedAt = fromMs(created)
		b.RetiredAt = nullTime(retired)
		out = append(out, b)
	}
	return out, rows.Err()
}

type DestinationBinding struct {
	ID           string
	Revision     int64
	DeploymentID string
	Config       json.RawMessage
	CreatedAt    time.Time
	RetiredAt    *time.Time
}
