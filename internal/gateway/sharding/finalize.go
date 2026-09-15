package sharding

import (
	"context"
	"errors"
)

type FinalPhase string

const (
	FinalActive    FinalPhase = "active"
	FinalPublished FinalPhase = "published"
	FinalComplete  FinalPhase = "complete"
)

// Published is an irreversible routing decision, independent of worker cursors.
// The task identity and target snapshot survive record reclamation for restart.
func (j *Journal) FinalPhase(ctx context.Context) (FinalPhase, error) {
	var p FinalPhase
	err := j.db.QueryRowContext(ctx, `SELECT phase FROM finalization WHERE id=1`).Scan(&p)
	if err == nil && p != FinalActive && p != FinalPublished && p != FinalComplete {
		err = errors.New("invalid finalization phase")
	}
	return p, err
}

func (j *Journal) upgradeFinalSchema(ctx context.Context) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`CREATE TABLE finalization (id INTEGER PRIMARY KEY CHECK(id=1),phase TEXT NOT NULL CHECK(phase IN ('active','published','complete')))`,
		`INSERT INTO finalization VALUES(1,'active')`,
		`PRAGMA user_version=3`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (j *Journal) Target() *Topology { return j.change.to }

// Caller must hold the runtime publication barrier and fence old backend
// writers before calling this. SQLite alone cannot prove backend isolation.
func (j *Journal) Publish(ctx context.Context) error {
	r, err := j.db.ExecContext(ctx, `UPDATE finalization SET phase='published' WHERE id=1 AND phase='active'
AND EXISTS(SELECT 1 FROM worker_progress WHERE id=1 AND phase='awaiting_finalize')
AND NOT EXISTS(SELECT 1 FROM handoff WHERE state!='cleaned')`)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("task not ready for publication")
	}
	return nil
}

// Forget is allowed only AFTER both backend markers have been removed under
// the new owner epoch. It never deletes backend data, or active-task records.
func (j *Journal) Forget(ctx context.Context, h Handoff) error {
	if h.TaskID != j.taskID || h.Sequence <= 0 || h.Placement != j.change.DesiredPlacement(h.Key) {
		return ErrHandoffConflict
	}
	r, err := j.db.ExecContext(ctx, `DELETE FROM handoff WHERE sequence=? AND key=? AND state='cleaned'
AND EXISTS(SELECT 1 FROM finalization WHERE id=1 AND phase='published')`, h.Sequence, binaryKey(h.Key))
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrHandoffConflict
	}
	return nil
}

func (j *Journal) Complete(ctx context.Context) error {
	r, err := j.db.ExecContext(ctx, `UPDATE finalization SET phase='complete' WHERE id=1 AND phase='published' AND NOT EXISTS(SELECT 1 FROM handoff)`)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("task metadata cleanup incomplete")
	}
	return nil
}
