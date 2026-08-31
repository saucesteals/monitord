package storage

import (
	"context"
	"time"
)

func (s *Store) WriteHealth(ctx context.Context, deploymentID string, generation int64, token []byte, child, status, summary string) error {
	ok, err := s.generationAuthorized(ctx, deploymentID, generation, token)
	if err != nil {
		return err
	}
	if !ok {
		return ErrGenerationFenced
	}
	failExpr := `CASE WHEN excluded.status IN ('failed','degraded') THEN deployment_health.consecutive_failures+1 ELSE 0 END`
	_, err = s.db.ExecContext(ctx, `INSERT INTO deployment_health(deployment_id,child_name,generation,status,summary,consecutive_failures,updated_at) VALUES(?,?,?,?,?,CASE WHEN ? IN ('failed','degraded') THEN 1 ELSE 0 END,?) ON CONFLICT(deployment_id,child_name) DO UPDATE SET generation=excluded.generation,status=excluded.status,summary=excluded.summary,consecutive_failures=`+failExpr+`,updated_at=excluded.updated_at`, deploymentID, child, generation, status, summary, status, toMs(time.Now().UTC()))
	return err
}
