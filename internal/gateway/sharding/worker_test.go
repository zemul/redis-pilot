package sharding

import (
	"context"
	"testing"
)

func TestWorkerProgressRecoveryAndFencing(t *testing.T) {
	j, dir := newTestJournal(t)
	ctx := context.Background()
	p, err := j.BeginWorker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Phase = WorkerCopy
	p.Cursor = "opaque"
	p.Pages = 3
	p.Visited = 42
	p.Attempts = 2
	if err := j.SaveWorker(ctx, p); err != nil {
		t.Fatal(err)
	}
	old := p
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenJournal(ctx, dir, "task-1", j.change.from, j.change.to)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	p, err = j.WorkerProgress(ctx)
	if err != nil || p.Cursor != "opaque" || p.Pages != 3 {
		t.Fatalf("progress missing: %+v %v", p, err)
	}
	p, err = j.BeginWorker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p.Generation <= old.Generation || p.Cursor != "0" || p.Phase != WorkerRecover || p.Pages != 3 {
		t.Fatalf("unsafe restart: %+v", p)
	}
	if err := j.SaveWorker(ctx, old); err == nil {
		t.Fatal("old runner progress accepted")
	}
	p.Pages = 0
	if err := j.SaveWorker(ctx, p); err == nil {
		t.Fatal("counter rollback accepted")
	}
}

func TestWorkerPendingRecoveryBounds(t *testing.T) {
	j, _ := newTestJournal(t)
	ctx := context.Background()
	keys := movingKeys(t, j.change, 3)
	h, err := j.Prepare(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	high, err := j.Highwater(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Prepare(ctx, keys[1]); err != nil {
		t.Fatal(err)
	}
	page, err := j.PendingPage(ctx, 0, high, 100)
	if err != nil || len(page) != 1 || page[0].Sequence != h.Sequence {
		t.Fatalf("unbounded recovery %+v %v", page, err)
	}
	if pending, err := j.HasPending(ctx); err != nil || !pending {
		t.Fatal("pending not found")
	}
	if err := j.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkCleaned(ctx, h); err != nil {
		t.Fatal(err)
	}
	page, err = j.PendingPage(ctx, 0, high, 100)
	if err != nil || len(page) != 0 {
		t.Fatal("cleaned record scheduled for recopy")
	}
}

func TestWorkerJournalUpgradeAndMissingProgress(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(map[bool]string{true: "upgrade_v1", false: "damaged_v2"}[legacy], func(t *testing.T) {
			j, dir := newTestJournal(t)
			ctx := context.Background()
			key := movingKeys(t, j.change, 1)[0]
			if _, err := j.Prepare(ctx, key); err != nil {
				t.Fatal(err)
			}
			if _, err := j.db.Exec(`DROP TABLE worker_progress`); err != nil {
				t.Fatal(err)
			}
			if legacy {
				if _, err := j.db.Exec(`DROP TABLE finalization`); err != nil {
					t.Fatal(err)
				}
				if _, err := j.db.Exec(`DROP INDEX handoff_state_sequence`); err != nil {
					t.Fatal(err)
				}
				if _, err := j.db.Exec(`PRAGMA user_version=1`); err != nil {
					t.Fatal(err)
				}
			}
			j.Close()
			reopened, err := OpenJournal(ctx, dir, "task-1", j.change.from, j.change.to)
			if !legacy {
				if err == nil {
					reopened.Close()
					t.Fatal("damaged progress silently rebuilt")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, found, err := reopened.Lookup(ctx, key); err != nil || !found {
				t.Fatal("upgrade lost handoff")
			}
			if _, err := reopened.BeginWorker(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
