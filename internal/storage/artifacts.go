package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Artifact struct {
	ID, ContentHash, Path string
	Describe              json.RawMessage
	CreatedAt             time.Time
}

func putArtifact(ctx context.Context, tx *sql.Tx, artifact Artifact) (Artifact, error) {
	if artifact.ContentHash == "" || artifact.Path == "" || !json.Valid(artifact.Describe) {
		return Artifact{}, errors.New("artifact requires hash, path, and valid describe JSON")
	}
	if artifact.ID == "" {
		artifact.ID = artifact.ContentHash
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = time.Now().UTC()
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO artifacts(id,content_hash,path,describe_json,created_at)
		VALUES(?,?,?,?,?) ON CONFLICT(content_hash) DO NOTHING`, artifact.ID, artifact.ContentHash,
		artifact.Path, artifact.Describe, toMs(artifact.CreatedAt))
	if err != nil {
		return Artifact{}, fmt.Errorf("put artifact: %w", err)
	}
	row := tx.QueryRowContext(ctx, `SELECT id,content_hash,path,describe_json,created_at FROM artifacts WHERE content_hash=?`, artifact.ContentHash)
	var created int64
	if err := row.Scan(&artifact.ID, &artifact.ContentHash, &artifact.Path, &artifact.Describe, &created); err != nil {
		return Artifact{}, err
	}
	artifact.CreatedAt = fromMs(created)
	return artifact, nil
}
