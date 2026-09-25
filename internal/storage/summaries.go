package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DeploymentSummary contains only the fields displayed by the deployment list.
type DeploymentSummary struct {
	ID, Name, Status, HealthStatus string
	ConsecutiveFailures            int
	ExpiresAt                      *time.Time
}

// ListDeploymentSummaries reads deployment status without loading state or history.
func (s *Store) ListDeploymentSummaries(ctx context.Context) ([]DeploymentSummary, error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT d.id,d.name,d.status,h.status,h.consecutive_failures,d.expires_at
 FROM deployments d LEFT JOIN deployment_health h ON h.deployment_id=d.id ORDER BY d.name`)
	if err != nil {
		return nil, fmt.Errorf("list deployment summaries: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []DeploymentSummary
	for rows.Next() {
		var item DeploymentSummary
		var expires sql.NullInt64
		var health sql.NullString
		var failures sql.NullInt64
		if err := rows.Scan(&item.ID, &item.Name, &item.Status, &health, &failures, &expires); err != nil {
			return nil, fmt.Errorf("scan deployment summary: %w", err)
		}
		if !health.Valid || !failures.Valid {
			return nil, fmt.Errorf("deployment %q is missing health", item.ID)
		}
		item.HealthStatus = health.String
		item.ConsecutiveFailures = int(failures.Int64)
		item.ExpiresAt = nullTime(expires)
		out = append(out, item)
	}

	return out, rows.Err()
}
