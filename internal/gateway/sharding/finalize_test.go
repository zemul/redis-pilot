package sharding

import (
	"context"
	"testing"
)

func TestFinalPublicationAndReclamation(t *testing.T) {
	j, dir := newTestJournal(t)
	ctx := context.Background()
	key := movingKeys(t, j.change, 1)[0]
	h, err := j.Prepare(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if j.Publish(ctx) == nil || j.Forget(ctx, h) == nil || j.Complete(ctx) == nil {
		t.Fatal("premature finalization accepted")
	}
	p, err := j.BeginWorker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Phase = WorkerAwaitFinalize
	if err := j.SaveWorker(ctx, p); err != nil {
		t.Fatal(err)
	}
	if j.Publish(ctx) == nil {
		t.Fatal("pending handoff published")
	}
	if err := j.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkCleaned(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := j.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Prepare(ctx, key); err == nil {
		t.Fatal("published task prepared a key")
	}
	if _, err := j.BeginWorker(ctx); err == nil {
		t.Fatal("published task restarted copying")
	}
	if j.Complete(ctx) == nil {
		t.Fatal("unreclaimed records became complete")
	}
	j.Close()
	j, err = OpenJournal(ctx, dir, "task-1", j.change.from, j.change.to)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if phase, err := j.FinalPhase(ctx); err != nil || phase != FinalPublished {
		t.Fatal("publication lost", phase, err)
	}
	if err := j.Forget(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	if phase, err := j.FinalPhase(ctx); err != nil || phase != FinalComplete {
		t.Fatal("completion lost", phase, err)
	}
	if page, err := j.Page(ctx, 0, 10); err != nil || len(page) != 0 {
		t.Fatal("records not reclaimed")
	}
	if !j.Target().Equal(j.change.to) {
		t.Fatal("target snapshot lost")
	}
}

func TestFinalSchemaUpgradeAndDamage(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(map[bool]string{true: "v2", false: "damaged_v3"}[legacy], func(t *testing.T) {
			j, dir := newTestJournal(t)
			if _, err := j.db.Exec(`DROP TABLE finalization`); err != nil {
				t.Fatal(err)
			}
			if legacy {
				if _, err := j.db.Exec(`PRAGMA user_version=2`); err != nil {
					t.Fatal(err)
				}
			}
			j.Close()
			next, err := OpenJournal(context.Background(), dir, "task-1", j.change.from, j.change.to)
			if !legacy {
				if err == nil {
					next.Close()
					t.Fatal("missing publication silently rebuilt")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			if phase, err := next.FinalPhase(context.Background()); err != nil || phase != FinalActive {
				t.Fatal(phase, err)
			}
		})
	}
}
