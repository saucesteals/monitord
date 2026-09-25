package storage

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
)

const schemaVersion = 2

//go:embed schema.sql
var initialSchema string

const performanceIndexes = `
CREATE INDEX outbox_events_transaction ON outbox_events(deployment_id,generation,transaction_seq);
CREATE INDEX outbox_deliveries_deployment ON outbox_deliveries(deployment_id,status);
`

func initializeSchema(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version == schemaVersion {
		return nil
	}
	if version < 0 || version > schemaVersion {
		return fmt.Errorf("unsupported database schema %d; expected %d", version, schemaVersion)
	}

	// Serialize schema changes across CLI and daemon openers before reading the
	// version again. A deferred transaction cannot safely upgrade a stale snapshot.
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire schema connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("begin schema migration: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(ctx, `ROLLBACK`) }()
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read migration version: %w", err)
	}
	switch version {
	case 0:
		var tables int
		if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
			return fmt.Errorf("inspect unversioned database: %w", err)
		}
		if tables != 0 {
			return errors.New("database has an incompatible unversioned schema; create a clean monitord root")
		}
		if _, err := conn.ExecContext(ctx, initialSchema); err != nil {
			return fmt.Errorf("initialize database schema: %w", err)
		}
	case 1:
		if _, err := conn.ExecContext(ctx, performanceIndexes); err != nil {
			return fmt.Errorf("create storage indexes: %w", err)
		}
	case schemaVersion:
		// Another opener completed the migration while we waited.
	default:
		return fmt.Errorf("unsupported database schema %d; expected %d", version, schemaVersion)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("record database schema: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("commit database schema: %w", err)
	}

	return nil
}
