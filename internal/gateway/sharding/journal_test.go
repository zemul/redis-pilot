package sharding

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func journalTopologies(t testing.TB) (*Topology, *Topology, *Transition) {
	t.Helper()
	from, to := testTopology(t, 3, 1), testTopology(t, 4, 2)
	c, err := NewTransition(from, to)
	if err != nil {
		t.Fatal(err)
	}
	return from, to, c
}

func movingKeys(t testing.TB, c *Transition, n int) [][]byte {
	t.Helper()
	var out [][]byte
	for i := 0; len(out) < n; i++ {
		key := []byte(fmt.Sprintf("binary\x00\xff-key-%d", i))
		if c.DesiredPlacement(key).Moves() {
			out = append(out, key)
		}
	}
	return out
}

func newTestJournal(t testing.TB) (*Journal, string) {
	t.Helper()
	from, to, _ := journalTopologies(t)
	dir := filepath.Join(t.TempDir(), "migration ?#1") // SQLite URI escaping
	j, err := CreateJournal(context.Background(), dir, "task-1", from, to)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	return j, dir
}

func TestJournalOwnershipAndRecovery(t *testing.T) {
	ctx := context.Background()
	j, dir := newTestJournal(t)
	keys := movingKeys(t, j.change, 3)
	for i, key := range keys {
		loc, err := j.Locate(ctx, key)
		if err != nil || !loc.NeedsHandoff || loc.Node != j.change.DesiredPlacement(key).Source || loc.State != "" {
			t.Fatalf("unprepared: %+v %v", loc, err)
		}
		h, err := j.Prepare(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if err = j.MarkCleaned(ctx, h); !errors.Is(err, ErrHandoffConflict) {
			t.Fatalf("skipped commit: %v", err)
		}
		if i > 0 {
			if err = j.Commit(ctx, h); err != nil {
				t.Fatal(err)
			}
			if err = j.Commit(ctx, h); err != nil {
				t.Fatal("duplicate commit", err)
			}
		}
		if i > 1 {
			if err = j.MarkCleaned(ctx, h); err != nil {
				t.Fatal(err)
			}
			if err = j.MarkCleaned(ctx, h); err != nil {
				t.Fatal("duplicate cleanup", err)
			}
			if err = j.Commit(ctx, h); !errors.Is(err, ErrHandoffConflict) {
				t.Fatalf("backtracking: %v", err)
			}
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err := OpenJournal(ctx, dir, "task-1", j.change.from, j.change.to)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i, key := range keys {
		want := []HandoffState{Prepared, Committed, Cleaned}[i]
		h, err := j.Prepare(ctx, key) // repeat scan must not reset ownership
		if err != nil || h.State != want {
			t.Fatalf("recovered: %+v %v", h, err)
		}
		loc, err := j.Locate(ctx, key)
		if err != nil || loc.State != want || loc.NeedsHandoff != (i == 0) {
			t.Fatalf("locate: %+v %v", loc, err)
		}
		if i > 0 && loc.Node != h.Placement.Target {
			t.Fatal("committed key routed back to source")
		}
	}
	var all []Handoff
	var cursor int64
	for {
		page, err := j.Page(ctx, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		if page[0].Sequence <= cursor {
			t.Fatal("cursor did not advance")
		}
		cursor = page[0].Sequence
		all = append(all, page...)
	}
	if len(all) != 3 || all[2].State != Cleaned {
		t.Fatalf("lost retained records: %+v", all)
	}
}

func TestJournalIdentityAndExclusiveOpen(t *testing.T) {
	ctx := context.Background()
	j, dir := newTestJournal(t)
	if second, err := OpenJournal(ctx, dir, "task-1", j.change.from, j.change.to); err == nil {
		second.Close()
		t.Fatal("concurrent writer accepted")
	}
	if second, err := CreateJournal(ctx, dir, "task-1", j.change.from, j.change.to); err == nil {
		second.Close()
		t.Fatal("overwrote journal")
	}
	keys := movingKeys(t, j.change, 2)
	h, err := j.Prepare(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Handoff){
		func(h *Handoff) { h.TaskID = "task-2" },
		func(h *Handoff) { h.Sequence++ },
		func(h *Handoff) { h.Key = keys[1]; h.Placement = j.change.DesiredPlacement(h.Key) },
		func(h *Handoff) { h.Placement.Target.Address = "127.0.0.1:1" },
	} {
		bad := h
		mutate(&bad)
		if err := j.Commit(ctx, bad); !errors.Is(err, ErrHandoffConflict) {
			t.Fatalf("bad identity accepted: %v", err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mismatch := range []struct {
		id       string
		from, to *Topology
	}{
		{"other-task", j.change.from, j.change.to},
		{"task-1", testTopology(t, 3, 10), testTopology(t, 4, 11)},
		{"task-1", j.change.from, testTopology(t, 5, 2)},
	} {
		if other, err := OpenJournal(ctx, dir, mismatch.id, mismatch.from, mismatch.to); err == nil {
			other.Close()
			t.Fatal("mismatched journal accepted")
		}
	}
	// A failed identity check must not keep the file lock held.
	other, err := OpenJournal(ctx, dir, "task-1", j.change.from, j.change.to)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
}

func TestJournalStorageErrorsAreNotMisses(t *testing.T) {
	ctx := context.Background()
	j, dir := newTestJournal(t)
	key := movingKeys(t, j.change, 1)[0]
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := j.Prepare(canceled, key); err == nil {
		t.Fatal("canceled prepare succeeded")
	}
	if _, found, err := j.Lookup(ctx, key); found || err != nil {
		t.Fatalf("canceled prepare persisted: %v %v", found, err)
	}
	// Inject a genuine SQLite write failure without changing permissions (which
	// may be ignored by root), and verify a failed ACK leaves ownership unchanged.
	h, err := j.Prepare(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.db.Exec(`CREATE TRIGGER fail_commit BEFORE UPDATE ON handoff BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err = j.Commit(ctx, h); err == nil {
		t.Fatal("failed commit acknowledged")
	}
	loc, err := j.Locate(ctx, key)
	if err != nil || loc.State != Prepared || !loc.NeedsHandoff {
		t.Fatalf("failed commit published: %+v %v", loc, err)
	}
	j.Close()
	if _, _, err := j.Lookup(ctx, key); err == nil {
		t.Fatal("closed journal treated as a miss")
	}
	if _, err := j.Locate(ctx, key); err == nil {
		t.Fatal("closed journal routed a request")
	}
	if _, err := j.Prepare(ctx, key); err == nil {
		t.Fatal("closed journal accepted write")
	}
	if _, err := j.Page(ctx, 0, 10); err == nil {
		t.Fatal("closed journal treated as empty")
	}
	if other, err := OpenJournal(ctx, dir+"-missing", "task-1", j.change.from, j.change.to); err == nil {
		other.Close()
		t.Fatal("missing journal accepted")
	}
	if _, err := os.Stat(dir + "-missing"); !os.IsNotExist(err) {
		t.Fatal("open created missing journal")
	}
	for _, content := range []string{"", "not a sqlite database"} {
		dir := filepath.Join(t.TempDir(), "broken")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "handoffs.db"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if other, err := OpenJournal(ctx, dir, "task-1", j.change.from, j.change.to); err == nil {
			other.Close()
			t.Fatal("empty/corrupt journal accepted")
		}
	}
}

func TestJournalConcurrentPrepareAndBounds(t *testing.T) {
	j, _ := newTestJournal(t)
	ctx := context.Background()
	keys := movingKeys(t, j.change, 12)
	var wg sync.WaitGroup
	for i := 0; i < 48; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := j.Prepare(ctx, keys[i%len(keys)]); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	page, err := j.Page(ctx, 0, 1000)
	if err != nil || len(page) != len(keys) {
		t.Fatalf("duplicate records: %d %v", len(page), err)
	}
	for _, bounds := range [][2]int{{-1, 1}, {0, 0}, {0, 1001}} {
		if _, err := j.Page(ctx, int64(bounds[0]), bounds[1]); err == nil {
			t.Fatal("invalid bounds accepted")
		}
	}
	if _, err := j.Prepare(ctx, bytes.Repeat([]byte("x"), 4097)); err == nil {
		t.Fatal("oversize key accepted")
	}
	for i := 0; ; i++ {
		key := []byte(fmt.Sprint(i))
		if j.change.DesiredPlacement(key).Moves() {
			continue
		}
		if _, err := j.Prepare(ctx, key); err == nil {
			t.Fatal("unchanged key recorded")
		}
		loc, err := j.Locate(ctx, key)
		if err != nil || loc.NeedsHandoff || loc.Node != j.change.from.Owner(key) {
			t.Fatalf("unchanged key: %+v %v", loc, err)
		}
		break
	}
}

func TestJournalRejectsDamagedMetadata(t *testing.T) {
	for _, statement := range []string{
		`PRAGMA user_version=99`,
		`DELETE FROM task`,
		`UPDATE task SET source=x'00'`,
		`DROP TABLE handoff`,
	} {
		t.Run(statement, func(t *testing.T) {
			j, dir := newTestJournal(t)
			if _, err := j.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if other, err := OpenJournal(context.Background(), dir, "task-1", j.change.from, j.change.to); err == nil {
				other.Close()
				t.Fatal("incompatible/incomplete journal accepted")
			}
		})
	}
	// A bad record must be a read failure, not a source fallback.
	j, _ := newTestJournal(t)
	key := movingKeys(t, j.change, 1)[0]
	if _, err := j.Prepare(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec(`UPDATE handoff SET state='unknown'`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Locate(context.Background(), key); err == nil {
		t.Fatal("invalid state routed to source")
	}
	if _, err := j.Page(context.Background(), 0, 10); err == nil {
		t.Fatal("invalid state omitted from recovery")
	}
}

func TestJournalEmptyKey(t *testing.T) {
	ctx := context.Background()
	from := testTopology(t, 1, 1)
	var to *Topology
	for n := 2; ; n++ {
		to = testTopology(t, n, 2)
		if from.Owner(nil).ID != to.Owner(nil).ID {
			break
		}
	}
	j, err := CreateJournal(ctx, filepath.Join(t.TempDir(), "empty-key"), "task", from, to)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	h, err := j.Prepare(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := j.Prepare(ctx, []byte{})
	if err != nil || h.Sequence != other.Sequence {
		t.Fatalf("nil/empty differ: %+v %v", other, err)
	}
	if err := j.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	loc, err := j.Locate(ctx, nil)
	if err != nil || loc.Node != to.Owner(nil) || loc.NeedsHandoff {
		t.Fatalf("empty-key owner: %+v %v", loc, err)
	}
}

// The helper waits after a durable ACK WITHOUT closing SQLite. Parent kills
// the actual process, then reopens the DB/WAL. This is not a power-loss test.
func TestJournalCrashHelper(t *testing.T) {
	dir := os.Getenv("PILOT_JOURNAL_CRASH_DIR")
	if dir == "" {
		return
	}
	ctx := context.Background()
	from, to, c := journalTopologies(t)
	j, err := CreateJournal(ctx, dir, "crash-task", from, to)
	if err != nil {
		t.Fatal(err)
	}
	h, err := j.Prepare(ctx, movingKeys(t, c, 1)[0])
	if err != nil {
		t.Fatal(err)
	}
	state := HandoffState(os.Getenv("PILOT_JOURNAL_CRASH_STATE"))
	if state == "uncommitted" {
		tx, err := j.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE handoff SET state='committed' WHERE sequence=?`, h.Sequence); err != nil {
			t.Fatal(err)
		}
		// Leave this transaction open: only the previously ACKed prepare may
		// survive, never this uncommitted ownership change.
	} else if state != Prepared {
		if err := j.Commit(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	if state == Cleaned {
		if err := j.MarkCleaned(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("journal-durable")
	for {
		time.Sleep(time.Second)
	}
}

func TestJournalSIGKILLRecovery(t *testing.T) {
	for _, state := range []HandoffState{Prepared, Committed, Cleaned, "uncommitted"} {
		t.Run(string(state), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			dir := filepath.Join(t.TempDir(), "crash")
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJournalCrashHelper$")
			cmd.Env = append(os.Environ(), "PILOT_JOURNAL_CRASH_DIR="+dir, "PILOT_JOURNAL_CRASH_STATE="+string(state))
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { cmd.Process.Kill(); cmd.Wait() }()
			s := bufio.NewScanner(stdout)
			ready := false
			for s.Scan() {
				if strings.TrimSpace(s.Text()) == "journal-durable" {
					ready = true
					break
				}
			}
			if !ready {
				cmd.Wait()
				t.Fatalf("helper not ready: %v %s", s.Err(), stderr.String())
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err == nil {
				t.Fatal("helper exited without being killed")
			}
			from, to, c := journalTopologies(t)
			j, err := OpenJournal(context.Background(), dir, "crash-task", from, to)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			key := movingKeys(t, c, 1)[0]
			want := state
			if want == "uncommitted" {
				want = Prepared
			}
			h, found, err := j.Lookup(context.Background(), key)
			if err != nil || !found || h.State != want {
				t.Fatalf("lost acknowledged state: %+v %v %v", h, found, err)
			}
			loc, err := j.Locate(context.Background(), key)
			if err != nil || loc.NeedsHandoff != (want == Prepared) {
				t.Fatalf("bad recovery owner: %+v %v", loc, err)
			}
		})
	}
}
