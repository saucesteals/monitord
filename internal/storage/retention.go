package storage

import (
	"context"
	"fmt"
	"time"
)

// PruneRetiredTransactions removes a bounded batch of unreferenced ACK records
// from generations retired for at least seven days. Active generations retain
// their entire replay ledger; pruning never advances their replay floor.
func (s *Store) PruneRetiredTransactions(ctx context.Context, now time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
  DELETE FROM transactions WHERE rowid IN (
   SELECT t.rowid FROM deployment_generations g
   JOIN transactions t ON t.deployment_id=g.deployment_id AND t.generation=g.generation
   WHERE g.status='retired' AND g.retired_at < ?
   AND NOT EXISTS (
    SELECT 1 FROM outbox_events e
    WHERE e.deployment_id=t.deployment_id AND e.generation=t.generation AND e.transaction_seq=t.seq
   )
   LIMIT 1000
  )`, toMs(now.Add(-7*24*time.Hour)))
	if err != nil {
		return 0, fmt.Errorf("prune retired transaction ledger: %w", err)
	}

	return result.RowsAffected()
}
