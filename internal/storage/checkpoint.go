package storage

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

const (
	checkpointInterval     = time.Second
	checkpointMaxAge       = 5 * time.Second
	checkpointBackoffMax   = 30 * time.Second
	checkpointFrames       = 1000
	checkpointWarningBytes = 64 << 20
)

type walStatus struct {
	Busy         int
	Frames       int64
	Checkpointed int64
}

func (s walStatus) backlog() int64 {
	return max(0, s.Frames-s.Checkpointed)
}

func readWAL(ctx context.Context, db *sql.DB, checkpoint bool) (walStatus, error) {
	query := "PRAGMA main.wal_checkpoint(NOOP)"
	if checkpoint {
		query = "PRAGMA main.wal_checkpoint(PASSIVE)"
	}
	var status walStatus
	err := db.QueryRowContext(ctx, query).Scan(&status.Busy, &status.Frames, &status.Checkpointed)
	return status, err
}

// runCheckpointer never borrows the operational pool or escalates to a blocking
// checkpoint. A PASSIVE checkpoint can still take time doing physical I/O.
func (s *Store) runCheckpointer(ctx context.Context, db *sql.DB, logger *slog.Logger, pageSize int64) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	delay := checkpointInterval
	lastAttempt := time.Time{}
	lastWarning := time.Time{}
	stalledSince := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		before, err := readWAL(ctx, db, false)
		s.checkpointPressure.Store(err != nil || before.Busy != 0 || before.backlog()*pageSize >= checkpointWarningBytes)
		if err == nil && before.Busy == 0 && before.backlog() == 0 {
			delay = checkpointInterval
			stalledSince = time.Time{}
			timer.Reset(delay)
			continue
		}
		if err == nil && before.Busy == 0 && before.backlog() < checkpointFrames && time.Since(lastAttempt) < checkpointMaxAge {
			timer.Reset(checkpointInterval)
			continue
		}
		start := time.Now()
		after := before
		if err == nil {
			after, err = readWAL(ctx, db, true)
			lastAttempt = start
		}
		if ctx.Err() != nil {
			return
		}
		s.checkpointPressure.Store(err != nil || after.Busy != 0 || after.backlog()*pageSize >= checkpointWarningBytes)
		duration := time.Since(start)
		// Compare within one attempt, not across WAL cycles: resets reuse frame numbers.
		progress := err == nil && after.Busy == 0 && after.Frames >= 0 &&
			(after.backlog() == 0 || after.Checkpointed > before.Checkpointed || after.Frames < before.Frames)
		if progress {
			delay = checkpointInterval
			stalledSince = time.Time{}
		} else {
			if stalledSince.IsZero() {
				stalledSince = start
			}
			delay = min(checkpointBackoffMax, delay*2)
		}
		stalledFor := time.Duration(0)
		if !stalledSince.IsZero() {
			stalledFor = time.Since(stalledSince)
		}
		attrs := []any{"duration", duration, "busy", after.Busy, "frames", after.Frames,
			"checkpointed_frames", after.Checkpointed, "backlog_frames", after.backlog(),
			"backlog_bytes", after.backlog() * pageSize, "stalled_for", stalledFor, "retry_in", delay}
		if err != nil {
			attrs = append(attrs, "error", err)
		}
		warn := err != nil || after.Busy != 0 || after.backlog()*pageSize >= checkpointWarningBytes || stalledFor >= checkpointBackoffMax
		if warn && (lastWarning.IsZero() || time.Since(lastWarning) >= checkpointBackoffMax) {
			logger.Warn("wal checkpoint needs attention", attrs...)
			lastWarning = time.Now()
		} else if duration >= time.Second {
			logger.Warn("slow wal checkpoint", attrs...)
		} else {
			logger.Debug("wal checkpoint", attrs...)
		}
		timer.Reset(delay)
	}
}

// CheckpointPressure reports whether optional cleanup should pause because checkpointing failed or
// more than 64 MiB await backfill. It does not stop durable monitor commits.
func (s *Store) CheckpointPressure() bool {
	return s.checkpointPressure.Load()
}
