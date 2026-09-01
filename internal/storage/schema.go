package storage

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
)

const schemaVersion = 1

//go:embed schema.sql
var initialSchema string

func initializeSchema(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version == schemaVersion {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("unsupported database schema %d; expected %d", version, schemaVersion)
	}
	var tables int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
		return fmt.Errorf("inspect unversioned database: %w", err)
	}
	if tables != 0 {
		return errors.New("database has an incompatible unversioned schema; create a clean V5 root")
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(initialSchema); err != nil {
		return fmt.Errorf("initialize database schema: %w", err)
	}
	if _, err = tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("record database schema: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit database schema: %w", err)
	}
	return nil
}
