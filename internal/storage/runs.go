package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type RunStart struct {
	ID, DeploymentID, ChildName, Kind string
	Generation                        int64
	WorkerToken                       []byte
	StartedAt                         time.Time
}
type RunFinish struct {
	ID, DeploymentID, Status, Summary, Error string
	Generation                               int64
	WorkerToken                              []byte
	FinishedAt                               time.Time
}

type DeploymentRun struct {
	ID, DeploymentID, Child, Kind, Status, Summary, Error string
	Generation                                            int64
	StartedAt                                             time.Time
	FinishedAt                                            *time.Time
}

func (s *Store) ListDeploymentRuns(ctx context.Context, id string, limit int, failed bool) ([]DeploymentRun, error) {
	if limit <= 0 {
		limit = 20
	}
	filter := ""
	if failed {
		filter = " AND status='failure'"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,deployment_id,generation,child_name,kind,status,started_at,finished_at,summary,error FROM deployment_runs WHERE deployment_id=?`+filter+` ORDER BY started_at DESC LIMIT ?`, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentRun
	for rows.Next() {
		var r DeploymentRun
		var started int64
		var finished sql.NullInt64
		if err = rows.Scan(&r.ID, &r.DeploymentID, &r.Generation, &r.Child, &r.Kind, &r.Status, &started, &finished, &r.Summary, &r.Error); err != nil {
			return nil, err
		}
		r.StartedAt = fromMs(started)
		r.FinishedAt = nullTime(finished)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) StartRun(ctx context.Context, in RunStart) error {
	if in.ID == "" || in.DeploymentID == "" || in.Generation < 1 || in.ChildName == "" || (in.Kind != "poll" && in.Kind != "continuous") {
		return errors.New("invalid run start")
	}
	if in.StartedAt.IsZero() {
		in.StartedAt = time.Now().UTC()
	}
	ok, err := s.generationAuthorized(ctx, in.DeploymentID, in.Generation, in.WorkerToken)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRunFenced
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO deployment_runs(id,deployment_id,generation,child_name,kind,status,started_at) VALUES(?,?,?,?,?,'running',?)`, in.ID, in.DeploymentID, in.Generation, in.ChildName, in.Kind, toMs(in.StartedAt))
	return err
}

func (s *Store) FinishRun(ctx context.Context, in RunFinish) error {
	if in.FinishedAt.IsZero() {
		in.FinishedAt = time.Now().UTC()
	}
	ok, err := s.generationAuthorized(ctx, in.DeploymentID, in.Generation, in.WorkerToken)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRunFenced
	}
	res, err := s.db.ExecContext(ctx, `UPDATE deployment_runs SET status=?,summary=?,error=?,finished_at=? WHERE id=? AND deployment_id=? AND generation=? AND status='running'`, in.Status, in.Summary, in.Error, toMs(in.FinishedAt), in.ID, in.DeploymentID, in.Generation)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrRunFenced
	}
	return nil
}
