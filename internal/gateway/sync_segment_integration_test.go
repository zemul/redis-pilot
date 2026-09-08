package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func appendSegmentCommand(t *testing.T, s *syncSegments, args ...string) {
	t.Helper()
	raw := encodeCommand(args...)
	p := s.durablePosition()
	p.Offset += int64(len(raw))
	if err := s.append(syncRecord{"command", p, raw}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncSegmentGCChild(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_SEGMENT_CHILD") != "isolated" {
		t.Skip("isolated segment GC child only")
	}
	s, err := openSyncSegments(os.Getenv("GATEWAY_TEST_SEGMENT_DIR"), 1024, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.fault = func(stage string) error {
		if stage == os.Getenv("GATEWAY_TEST_SEGMENT_STAGE") {
			fmt.Println("segment-gc-kill-boundary")
			time.Sleep(20 * time.Second) // parent kills this real process here
			return errors.New("parent did not kill child")
		}
		return nil
	}
	target := syncColdTarget{address: os.Getenv("GATEWAY_TEST_SEGMENT_ADDRESS"), timeout: 5 * time.Second}
	if err := target.pruneSegments(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	t.Fatal("GC fault stage was not reached")
}

func kvCommitSegmentExperiments(t *testing.T, addr string, c *kvCommitClient) {
	dir := os.Getenv("GATEWAY_TEST_SYNC_FIXTURE_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("explicit fixture directory required")
	}
	read := func(name string) []syncRecord {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || len(b) > 4<<20 || !bytes.HasPrefix(b, []byte(syncJournalMagic)) {
			t.Fatal("invalid fixture", err)
		}
		r := bytes.NewReader(b[len(syncJournalMagic):])
		var records []syncRecord
		for {
			record, err := readSyncRecord(r)
			if err == io.EOF {
				return records
			}
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
	}
	one, two := read("phase1.log"), read("phase2.log")
	client := &syncColdClient{conn: c.c, reader: c.r, timeout: 5 * time.Second}
	keys, err := client.keys()
	if err != nil || len(keys) != 0 {
		t.Fatal("segment experiment requires new empty DB", err)
	}
	owned := []string{syncPendingCPKey, syncPendingKey, syncAnchorKey, "seed-counter", "seed-delete", "binary\x00key", "after-snapshot", "during-restart"}
	reset := func() {
		raw, err := c.request(append([]string{"DEL"}, owned...)...)
		if err != nil || len(raw) == 0 || raw[0] != ':' {
			t.Fatal("exact fixture cleanup failed", err)
		}
	}
	// Run before the outer experiment closes its shared connection; t.Cleanup
	// runs later than that defer and would try to clean via a closed socket.
	defer reset()
	target := syncColdTarget{address: addr, timeout: 5 * time.Second}
	open := func(t *testing.T, path string) *syncSegments {
		t.Helper()
		s, err := openSyncSegments(path, 1024, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.close() })
		return s
	}
	fixture := func(t *testing.T) (*syncSegments, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "segments")
		s := open(t, path)
		for _, r := range one {
			if err := s.append(r); err != nil {
				t.Fatal(err)
			}
		}
		return s, path
	}
	apply := func(t *testing.T, s *syncSegments) {
		t.Helper()
		if p, err := target.applySegments(context.Background(), s); err != nil || p != s.durablePosition() {
			t.Fatal("segment apply", p, err)
		}
	}
	prune := func(t *testing.T, s *syncSegments) {
		t.Helper()
		if err := target.pruneSegments(context.Background(), s); err != nil {
			t.Fatal("segment prune", err)
		}
	}
	grow := func(t *testing.T, s *syncSegments) {
		t.Helper()
		for i := 0; i < 12; i++ {
			appendSegmentCommand(t, s, "INCR", "seed-counter")
		}
		apply(t, s)
		kvCommitGet(t, c, "seed-counter", "54")
	}
	t.Run("real_fixture_gc_restart_and_suffix", func(t *testing.T) {
		reset()
		s, path := fixture(t)
		apply(t, s)
		kvCommitGet(t, c, "seed-counter", "42")
		prune(t, s)
		s.close()
		s = open(t, path)
		apply(t, s)
		for _, r := range two {
			if r.Kind == "command" && r.Position.Offset > s.durablePosition().Offset {
				if err := s.append(r); err != nil {
					t.Fatal(err)
				}
			}
		}
		apply(t, s)
		prune(t, s)
		s.close()
		s = open(t, path)
		apply(t, s)
		kvCommitGet(t, c, "seed-counter", "43")
		kvCommitGet(t, c, "during-restart", "retained")
		t.Log("real Redis RDB/PSYNC fixture kept counter=42 then 43 across segmented GC/reopen")
	})
	for _, mode := range []string{"before", "reply", "late"} {
		t.Run("pending_after_gc_"+mode, func(t *testing.T) {
			reset()
			s, path := fixture(t)
			grow(t, s)
			prune(t, s)
			s.close()
			s = open(t, path)
			appendSegmentCommand(t, s, "INCR", "seed-counter")
			relay, reached, release, done := pendingFaultRelay(t, addr, "apply", mode)
			faulty := target
			faulty.address, faulty.timeout = relay, 250*time.Millisecond
			if _, err := faulty.applySegments(context.Background(), s); !errors.Is(err, errSyncPendingUncertain) {
				t.Fatal("expected uncertain commit", err)
			}
			select {
			case <-reached:
			case <-time.After(time.Second):
				t.Fatal("fault stage not reached")
			}
			if mode != "reply" {
				if err := target.pruneSegments(context.Background(), s); !errors.Is(err, errSyncPendingUncertain) {
					t.Fatal("unconfirmed item permitted GC", err)
				}
			}
			s.close()
			s = open(t, path)
			apply(t, s)
			if mode == "late" {
				release <- struct{}{}
				if result := <-done; result != "*-1\r\n" && result != "$-1\r\n" {
					t.Fatalf("stale EXEC not aborted: %q", result)
				}
			} else {
				<-done
			}
			prune(t, s)
			apply(t, s)
			kvCommitGet(t, c, "seed-counter", "55")
			t.Log("post-GC unresolved/reply-loss/late-EXEC recovery did not duplicate INCR")
		})
	}
	t.Run("gc_rejects_after_drift", func(t *testing.T) {
		reset()
		s, _ := fixture(t)
		grow(t, s)
		c.expect(t, "+OK\r\n", "SET", "seed-counter", "999")
		if err := target.pruneSegments(context.Background(), s); !errors.Is(err, errSyncTargetDrift) {
			t.Fatal("drift allowed GC", err)
		}
		v, err := s.pendingView()
		if err != nil {
			t.Fatal(err)
		}
		defer v.close()
		if v.confirmed.Version != 0 || v.floor != v.base {
			t.Fatal("failed GC changed manifest")
		}
	})
	for _, kind := range []string{"inside_retained_segment", "below_floor", "metadata_lost_empty_db"} {
		t.Run("gc_rejects_cold_rollback_"+kind, func(t *testing.T) {
			reset()
			s, path := fixture(t)
			grow(t, s)
			meta, err := client.pendingMeta()
			if err != nil || meta.pending == nil {
				t.Fatal(err)
			}
			prune(t, s)
			s.close()
			s = open(t, path)
			v, err := s.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			defer v.close()
			if v.floor.Position.Offset <= v.base.Position.Offset || v.confirmed != meta.checkpoint {
				t.Fatal("test did not actually GC or persist confirmed watermark")
			}
			switch kind {
			case "inside_retained_segment":
				if meta.pending.Previous.Position.Offset < v.floor.Position.Offset {
					t.Fatal("pending evidence segment was reclaimed")
				}
				c.expect(t, "+OK\r\n", "MSET", syncPendingCPKey, meta.pending.Previous.encode(), "seed-counter", string(meta.pending.Before.Data))
			case "below_floor":
				c.expect(t, "+OK\r\n", "SET", syncPendingCPKey, v.base.encode())
				c.expect(t, ":1\r\n", "DEL", syncPendingKey)
			case "metadata_lost_empty_db":
				reset()
			}
			if _, err := target.applySegments(context.Background(), s); !errors.Is(err, errSyncHistoryGap) {
				t.Fatal("cold rollback accepted", err)
			}
			if kind == "inside_retained_segment" {
				kvCommitGet(t, c, "seed-counter", "53")
			} else if kind == "metadata_lost_empty_db" {
				keys, err := client.keys()
				if err != nil || len(keys) != 0 {
					t.Fatal("GC history incorrectly rebootstrapped empty cold DB", err)
				}
			}
			t.Log("cold rollback rejected without data replay or snapshot replacement")
		})
	}
	t.Run("gc_process_sigkill_after_manifest", func(t *testing.T) {
		reset()
		s, path := fixture(t)
		grow(t, s)
		s.close()
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, "-test.run", "^TestSyncSegmentGCChild$", "-test.timeout=25s")
		cmd.Env = append(os.Environ(), "GATEWAY_TEST_SEGMENT_CHILD=isolated", "GATEWAY_TEST_SEGMENT_DIR="+path, "GATEWAY_TEST_SEGMENT_ADDRESS="+addr, "GATEWAY_TEST_SEGMENT_STAGE=gc_before_unlink")
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer cmd.Process.Kill()
		reached := make(chan bool, 1)
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				if scanner.Text() == "segment-gc-kill-boundary" {
					reached <- true
					return
				}
			}
			reached <- false
		}()
		select {
		case ok := <-reached:
			if !ok {
				cmd.Wait()
				t.Fatal("GC child did not reach kill boundary", stderr.String())
			}
		case <-time.After(8 * time.Second):
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatal("GC child timeout", stderr.String())
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Wait(); err == nil {
			t.Fatal("child was not killed")
		}
		s = open(t, path)
		v, err := s.pendingView()
		if err != nil {
			t.Fatal(err)
		}
		if v.confirmed.Version == 0 || v.floor == v.base {
			t.Fatal("GC manifest not preserved after process kill")
		}
		v.close()
		// G6a-7: a real killed GC left retired files behind. Explicit cleanup
		// removes only those unreferenced names; replay evidence stays intact.
		cleaned, err := s.cleanupOrphans(context.Background())
		if err != nil || cleaned.RemovedFiles == 0 {
			t.Fatal("GC crash leftovers were not reclaimed", cleaned, err)
		}
		again, err := s.cleanupOrphans(context.Background())
		if err != nil || again.RemovedFiles != 0 {
			t.Fatal("orphan cleanup was not idempotent", again, err)
		}
		apply(t, s)
		kvCommitGet(t, c, "seed-counter", "54")
		appendSegmentCommand(t, s, "INCR", "seed-counter")
		apply(t, s)
		prune(t, s)
		kvCommitGet(t, c, "seed-counter", "55")
		t.Log("real GC process SIGKILL after durable manifest/before deletion; explicit orphan cleanup removed leftovers, replay preserved counter")
	})
	t.Run("resource_disk_pressure_allows_cold_drain", func(t *testing.T) {
		reset()
		s, _ := fixture(t)
		// Inject availability only; never fill the development host filesystem.
		s.availableBytes = func() (uint64, error) { return 0, nil }
		before := s.durablePosition()
		raw := encodeCommand("INCR", "seed-counter")
		position := before
		position.Offset += int64(len(raw))
		record := syncRecord{"command", position, raw}
		if err := s.append(record); !errors.Is(err, errSyncCapturePressure) || s.durablePosition() != before || s.poisoned {
			t.Fatal("pressure append changed durable capture or poisoned handle", err)
		}
		apply(t, s) // low capture watermark must not disable cold work
		prune(t, s)
		status, err := s.resourceStatus()
		if !errors.Is(err, errSyncCapturePressure) || !status.DiskPaused || status.UnconfirmedBytes != 0 {
			t.Fatal("bad pressure state after cold drain", status, err)
		}
		kvCommitGet(t, c, "seed-counter", "42")
		s.availableBytes = func() (uint64, error) { return 1 << 40, nil }
		if err := s.append(record); err != nil {
			t.Fatal("capture did not resume", err)
		}
		apply(t, s)
		prune(t, s)
		kvCommitGet(t, c, "seed-counter", "43")
		t.Log("injected low availability stopped capture without ACK progress; real cold apply/GC drained; resumed one INCR")
	})
	t.Run("resource_unconfirmed_watermark_is_conservative", func(t *testing.T) {
		reset()
		s, _ := fixture(t)
		for i := 0; i < 12; i++ {
			appendSegmentCommand(t, s, "INCR", "seed-counter")
		}
		// Test-only policy change before any concurrent operation: historical
		// captured backlog is above this deliberately small configured limit.
		s.policy.MaxUnconfirmedBytes, s.policy.ResumeUnconfirmedBytes = 100, 50
		status, err := s.resourceStatus()
		if !errors.Is(err, errSyncCapturePressure) || !status.BacklogPaused {
			t.Fatal("backlog did not pause capture", status, err)
		}
		apply(t, s)
		status, err = s.resourceStatus()
		if !errors.Is(err, errSyncCapturePressure) || !status.BacklogPaused {
			t.Fatal("cold apply without durable local confirmation prematurely released pressure", status, err)
		}
		prune(t, s)
		status, err = s.resourceStatus()
		if err != nil || status.BacklogPaused || status.UnconfirmedBytes != 0 {
			t.Fatal("confirmed GC did not release backlog pressure", status, err)
		}
		appendSegmentCommand(t, s, "INCR", "seed-counter")
		apply(t, s)
		kvCommitGet(t, c, "seed-counter", "55")
		t.Log("cold apply alone did not release conservative backlog; durable GC confirmation did")
	})
	reset()
	keys, err = client.keys()
	if err != nil || len(keys) != 0 {
		t.Fatal("test DB not empty after exact-key cleanup", err)
	}
}
