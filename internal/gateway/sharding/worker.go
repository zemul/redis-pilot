package sharding

import (
	"context"
	"errors"
	"time"
)

type WorkerPhase string

const (
	WorkerRecover       WorkerPhase = "recover"
	WorkerCopy          WorkerPhase = "copy"
	WorkerVerify        WorkerPhase = "verify"
	WorkerAwaitFinalize WorkerPhase = "awaiting_finalize"
)

// Progress is observational, never a routing authority or permission to retire
// a node. Cursor is only a hint: each new runner/cycle replays recovery and scans
// from zero. Counters include retries/duplicates, not unique migrated keys.
type WorkerProgress struct {
	Generation                                           int64
	Phase                                                WorkerPhase
	SourceIndex                                          int
	Cursor                                               string
	Pages, Visited, Attempts, Failures, VerifiedSequence int64
	LastError                                            string
	UpdatedMillis                                        int64
}

func (j *Journal) upgradeWorkerSchema(ctx context.Context) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, query := range []string{
		`CREATE TABLE worker_progress (id INTEGER PRIMARY KEY CHECK(id=1), generation INTEGER NOT NULL, phase TEXT NOT NULL, source_index INTEGER NOT NULL, cursor TEXT NOT NULL, pages INTEGER NOT NULL, visited INTEGER NOT NULL, attempts INTEGER NOT NULL, failures INTEGER NOT NULL, verified_sequence INTEGER NOT NULL, last_error TEXT NOT NULL, updated_ms INTEGER NOT NULL)`,
		`INSERT INTO worker_progress VALUES(1,0,'recover',0,'0',0,0,0,0,0,'',0)`,
		`CREATE INDEX handoff_state_sequence ON handoff(state,sequence)`,
		`PRAGMA user_version=2`,
	} {
		if _, err = tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (p WorkerProgress) valid() bool {
	if p.Generation < 0 || p.SourceIndex < 0 || len(p.Cursor) > 1024 || p.Pages < 0 || p.Visited < 0 || p.Attempts < 0 || p.Failures < 0 || p.VerifiedSequence < 0 || p.UpdatedMillis < 0 || len(p.LastError) > 256 {
		return false
	}
	switch p.Phase {
	case WorkerRecover, WorkerCopy, WorkerVerify, WorkerAwaitFinalize:
		return true
	}
	return false
}
func (j *Journal) WorkerProgress(ctx context.Context) (WorkerProgress, error) {
	var p WorkerProgress
	err := j.db.QueryRowContext(ctx, `SELECT generation,phase,source_index,cursor,pages,visited,attempts,failures,verified_sequence,last_error,updated_ms FROM worker_progress WHERE id=1`).Scan(&p.Generation, &p.Phase, &p.SourceIndex, &p.Cursor, &p.Pages, &p.Visited, &p.Attempts, &p.Failures, &p.VerifiedSequence, &p.LastError, &p.UpdatedMillis)
	if err == nil && (!p.valid() || p.SourceIndex > len(j.change.ScanSources())) {
		err = errors.New("invalid persisted worker progress")
	}
	return p, err
}

func (j *Journal) BeginWorker(ctx context.Context) (WorkerProgress, error) {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerProgress{}, err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `UPDATE worker_progress SET generation=generation+1,phase='recover',source_index=0,cursor='0',last_error='',updated_ms=? WHERE id=1 AND generation<9223372036854775807 AND EXISTS(SELECT 1 FROM finalization WHERE id=1 AND phase='active')`, time.Now().UnixMilli())
	if err != nil {
		return WorkerProgress{}, err
	}
	n, err := r.RowsAffected()
	if err != nil || n != 1 {
		return WorkerProgress{}, errors.New("worker generation unavailable")
	}
	var p WorkerProgress
	err = tx.QueryRowContext(ctx, `SELECT generation,phase,source_index,cursor,pages,visited,attempts,failures,verified_sequence,last_error,updated_ms FROM worker_progress WHERE id=1`).Scan(&p.Generation, &p.Phase, &p.SourceIndex, &p.Cursor, &p.Pages, &p.Visited, &p.Attempts, &p.Failures, &p.VerifiedSequence, &p.LastError, &p.UpdatedMillis)
	if err != nil {
		return p, err
	}
	if !p.valid() {
		return p, errors.New("invalid worker progress")
	}
	return p, tx.Commit()
}

func (j *Journal) SaveWorker(ctx context.Context, p WorkerProgress) error {
	if !p.valid() || p.Generation == 0 || p.SourceIndex > len(j.change.ScanSources()) {
		return errors.New("invalid worker progress")
	}
	r, err := j.db.ExecContext(ctx, `UPDATE worker_progress SET phase=?,source_index=?,cursor=?,pages=?,visited=?,attempts=?,failures=?,verified_sequence=?,last_error=?,updated_ms=? WHERE id=1 AND generation=? AND pages<=? AND visited<=? AND attempts<=? AND failures<=? AND EXISTS(SELECT 1 FROM finalization WHERE id=1 AND phase='active')`, p.Phase, p.SourceIndex, p.Cursor, p.Pages, p.Visited, p.Attempts, p.Failures, p.VerifiedSequence, p.LastError, time.Now().UnixMilli(), p.Generation, p.Pages, p.Visited, p.Attempts, p.Failures)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("stale worker progress")
	}
	return nil
}

func (j *Journal) Highwater(ctx context.Context) (int64, error) {
	var n int64
	err := j.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0) FROM handoff`).Scan(&n)
	return n, err
}
func (j *Journal) HasPending(ctx context.Context) (bool, error) {
	var pending bool
	err := j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM handoff WHERE state IN ('prepared','committed') LIMIT 1)`).Scan(&pending)
	return pending, err
}

// Limit recovery to a high-water mark so concurrent requests cannot keep
// appending work forever ahead of the source scans. A final barrier rechecks.
func (j *Journal) PendingPage(ctx context.Context, after, through int64, limit int) ([]Handoff, error) {
	if after < 0 || through < after || limit < 1 || limit > 1000 {
		return nil, errors.New("invalid recovery page bounds")
	}
	rows, err := j.db.QueryContext(ctx, `SELECT sequence,key,state FROM handoff WHERE sequence>? AND sequence<=? AND state IN ('prepared','committed') ORDER BY sequence LIMIT ?`, after, through, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Handoff
	for rows.Next() {
		h, err := j.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
