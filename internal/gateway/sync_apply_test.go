package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func syncTestRDB(body ...byte) []byte {
	b := append([]byte("REDIS0011"), body...)
	b = append(b, 255)
	return binary.LittleEndian.AppendUint64(b, syncRDBCRC(b))
}

func TestSyncRDB(t *testing.T) {
	if got := syncRDBCRC([]byte("123456789")); got != 0xe9c6d914c4b8d9ca {
		t.Fatalf("CRC64 vector: %x", got)
	}
	valid := syncTestRDB(254, 0, 251, 1, 0, 0, 1, 'k', 192, 41)
	state, err := decodeSyncRDB(valid)
	if err != nil || len(state) != 1 || state["k"] != "41" {
		t.Fatal(state, err)
	}
	for name, b := range map[string][]byte{
		"truncated":   valid[:len(valid)-1],
		"ttl":         syncTestRDB(254, 0, 252),
		"list":        syncTestRDB(254, 0, 1),
		"db1":         syncTestRDB(254, 1),
		"lzf":         syncTestRDB(254, 0, 0, 1, 'k', 195),
		"huge_length": syncTestRDB(254, 0, 0, 0x81, 255, 255, 255, 255, 255, 255, 255, 255),
		"duplicate":   syncTestRDB(254, 0, 0, 1, 'k', 1, 'a', 0, 1, 'k', 1, 'b'),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSyncRDB(b); err == nil {
				t.Fatal("invalid RDB accepted")
			}
		})
	}
	corrupt := bytes.Clone(valid)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := decodeSyncRDB(corrupt); !errors.Is(err, errSyncJournalCorrupt) {
		t.Fatal(err)
	}
	if _, err := decodeSyncRDB(syncTestRDB()); err != nil {
		t.Fatal(err)
	}
}

func FuzzSyncRDB(f *testing.F) {
	f.Add(syncTestRDB())
	f.Add(syncTestRDB(254, 0, 0, 1, 'k', 192, 41))
	f.Fuzz(func(t *testing.T, b []byte) {
		state, err := decodeSyncRDB(b)
		if err == nil {
			if err := checkSyncState(state); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestSyncColdScan(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages []string
		fail  bool
	}{
		{"binary_and_empty_keys", []string{"*2\r\n$4\r\nnext\r\n*2\r\n$0\r\n\r\n$1\r\n\x00\r\n", "*2\r\n$1\r\n0\r\n*1\r\n$1\r\nx\r\n"}, false},
		{"cursor_loop", []string{"*2\r\n$4\r\nnext\r\n*0\r\n", "*2\r\n$4\r\nnext\r\n*0\r\n"}, true},
		{"null_key", []string{"*2\r\n$1\r\n0\r\n*1\r\n$-1\r\n"}, true},
		{"wrong_shape", []string{"*1\r\n+OK\r\n"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			front, back := net.Pipe()
			defer front.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer back.Close()
				back.SetDeadline(time.Now().Add(time.Second))
				r, _ := NewReader(back, DefaultConfig().Limits)
				for _, page := range tc.pages {
					if _, _, err := r.ReadCommand(); err != nil {
						return
					}
					if _, err := back.Write([]byte(page)); err != nil {
						return
					}
				}
			}()
			r, _ := NewReader(front, DefaultConfig().Limits)
			c := &syncColdClient{conn: front, reader: r, timeout: time.Second}
			keys, err := c.keys()
			front.Close()
			<-done
			if tc.fail {
				if err == nil {
					t.Fatal("invalid SCAN accepted")
				}
				return
			}
			if err != nil || len(keys) != 3 || !keys[""] || !keys["\x00"] || !keys["x"] {
				t.Fatal("bad SCAN result", keys, err)
			}
		})
	}
}

func appendSyncTestCommand(t *testing.T, j *syncJournal, args ...string) {
	t.Helper()
	raw := encodeCommand(args...)
	p := j.durablePosition()
	p.Offset += int64(len(raw))
	if err := j.append(syncRecord{"command", p, raw}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncApplyPlan(t *testing.T) {
	for _, cmd := range [][]string{{"SET", "x", "1", "PX", "10"}, {"PEXPIRE", "x", "10"}, {"SELECT", "1"}, {"MULTI"}, {"EXEC"}, {"UNLINK", "x"}, {"DEL", "x", "y"}, {"SET", syncCheckpointKey, "x"}, {"INCR", syncCheckpointKey}, {"DEL", syncCheckpointKey}} {
		j, _ := captureJournal(t, 1<<20)
		r := captureSnapshot("127.0.0.1:12345")
		r.Data = syncTestRDB()
		if err := j.append(r); err != nil {
			t.Fatal(err)
		}
		appendSyncTestCommand(t, j, cmd...)
		records, err := j.applicationPrefix()
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := planSyncPrefix(records); err == nil {
			t.Fatalf("accepted %q", cmd)
		}
	}
	j, _ := captureJournal(t, 1<<20)
	r := captureSnapshot("127.0.0.1:12345")
	r.Data = syncTestRDB()
	if err := j.append(r); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range [][]string{{"SELECT", "0"}, {"SET", "counter", "41"}, {"INCR", "counter"}, {"DEL", "counter"}, {"PING"}, {"REPLCONF", "GETACK", "*"}} {
		appendSyncTestCommand(t, j, cmd...)
	}
	records, err := j.applicationPrefix()
	if err != nil {
		t.Fatal(err)
	}
	state, steps, err := planSyncPrefix(records)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if err := advanceSyncState(state, step.command); err != nil {
			t.Fatal(err)
		}
	}
	if len(state) != 0 || steps[len(steps)-1].checkpoint.Position != j.durablePosition() {
		t.Fatal("bad final state/position")
	}
	original := steps[len(steps)-1].checkpoint.Chain
	// Same byte positions and final state, but changed earlier payload.
	records[2].Data = encodeCommand("SET", "counter", "40")
	_, changed, err := planSyncPrefix(records)
	if err != nil || original == changed[len(changed)-1].checkpoint.Chain {
		t.Fatal("history digest did not bind payload", err)
	}
}

// Captures a real initial RDB and incremental PSYNC history. Optional export
// creates a fresh /tmp bundle for the isolated remote Kvrocks integration;
// files contain only these synthetic test keys, never business data/secrets.
func TestSyncApplyFixtureRedis(t *testing.T) {
	addr := syncTestRedis(t)
	conn, reader, err := authenticated(addr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &kvCommitClient{conn, reader}
	c.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-diskless-sync", "no")
	c.expect(t, "+OK\r\n", "CONFIG", "SET", "rdbcompression", "no")
	c.expect(t, "+OK\r\n", "MSET", "seed-counter", "41", "seed-delete", "gone", "binary\x00key", "value\x00\r\n")
	j, path := captureJournal(t, 4<<20)
	source := syncSource{address: addr, passwordFile: secretFile(t, "g4-backend-test"), timeout: 5 * time.Second, maxSnapshot: 1 << 20}
	start := func(j *syncJournal) (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- source.capture(ctx, j) }()
		t.Cleanup(cancel)
		return cancel, done
	}
	stop := func(cancel context.CancelFunc, done <-chan error) {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("capture did not stop")
		}
	}
	offset := func() int64 {
		raw, err := c.request("INFO", "replication")
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\r\n") {
			if strings.HasPrefix(line, "master_repl_offset:") {
				n, err := strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
		}
		t.Fatal("missing source offset")
		return 0
	}
	cancel, done := start(j)
	waitCaptured(t, j, 0)
	c.expect(t, "+OK\r\n", "SET", "after-snapshot", "hello")
	c.expect(t, ":42\r\n", "INCR", "seed-counter")
	c.expect(t, ":1\r\n", "DEL", "seed-delete")
	waitCaptured(t, j, offset())
	stop(cancel, done)
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	j.close()
	c.expect(t, ":43\r\n", "INCR", "seed-counter")
	c.expect(t, "+OK\r\n", "SET", "during-restart", "retained")
	j2, err := openSyncJournal(path, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.close()
	cancel, done = start(j2)
	waitCaptured(t, j2, offset())
	stop(cancel, done)
	records, err := j2.applicationPrefix()
	if err != nil {
		t.Fatal(err)
	}
	state, steps, err := planSyncPrefix(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 3 || state["seed-counter"] != "41" || state["binary\x00key"] != "value\x00\r\n" {
		t.Fatal("bad decoded real RDB", state)
	}
	for _, step := range steps {
		if err := advanceSyncState(state, step.command); err != nil {
			t.Fatal(err)
		}
	}
	if len(state) != 4 || state["seed-counter"] != "43" || state["during-restart"] != "retained" {
		t.Fatal("bad final model", state)
	}
	for key, value := range state {
		kvCommitGet(t, c, key, value)
	}
	c.expect(t, "$-1\r\n", "GET", "seed-delete")
	if os.Getenv("GATEWAY_TEST_SYNC_EXPORT") == "1" {
		dir, err := os.MkdirTemp("/tmp", "redis-pilot-sync-e2e-")
		if err != nil {
			t.Fatal(err)
		}
		second, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string][]byte{"phase1.log": first, "phase2.log": second} {
			if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("synthetic capture bundle retained: %s", dir)
	}
	t.Logf("real RDB decoded (3 keys); PSYNC resumed; model/source match at offset=%d with counter=43 and binary value intact", j2.durablePosition().Offset)
}

func kvCommitApplyExperiments(t *testing.T, addr string, c *kvCommitClient) {
	dir := os.Getenv("GATEWAY_TEST_SYNC_FIXTURE_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("requires explicit absolute synthetic capture fixture directory")
	}
	var fixtures [][]byte
	for _, name := range []string{"phase1.log", "phase2.log"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || len(b) > 4<<20 {
			t.Fatal("fixture unavailable/too large", err)
		}
		fixtures = append(fixtures, b)
	}
	openFixture := func(data []byte) *syncJournal {
		path := filepath.Join(t.TempDir(), "capture.log")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		j, err := openSyncJournal(path, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { j.close() })
		return j
	}
	first, second := openFixture(fixtures[0]), openFixture(fixtures[1])
	records, err := second.applicationPrefix()
	if err != nil {
		t.Fatal(err)
	}
	initial, steps, err := planSyncPrefix(records)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{syncCheckpointKey: true, "unexpected": true}
	for k := range initial {
		keys[k] = true
	}
	for _, step := range steps {
		if len(step.command) > 0 {
			keys[step.command[1]] = true
		}
	}
	// Refuse to run destructive test setup on any pre-populated DB. Only
	// delete the exact synthetic keys created by this test between cases.
	assertEmpty := func() {
		client := &syncColdClient{conn: c.c, reader: c.r, timeout: 5 * time.Second}
		keys, err := client.keys()
		if err != nil || len(keys) != 0 {
			t.Fatal("dedicated target must be empty", err)
		}
	}
	assertEmpty()
	reset := func() {
		args := []string{"DEL"}
		for k := range keys {
			args = append(args, k)
		}
		raw, err := c.request(args...)
		if err != nil || len(raw) == 0 || raw[0] != ':' {
			t.Fatal("fixture cleanup failed", err)
		}
		assertEmpty()
	}
	target := syncColdTarget{address: addr, timeout: 5 * time.Second}
	for _, before := range []bool{true, false} {
		name := "lost_reply"
		if before {
			name = "before_exec"
		}
		t.Run(name, func(t *testing.T) {
			reset()
			relay, done := kvCommitFaultExecMatching(t, addr, before, "INCR")
			faulty := target
			faulty.address = relay
			if _, err := faulty.apply(context.Background(), first); !errors.Is(err, errSyncApplyUncertain) {
				t.Fatal("expected ambiguous INCR commit", err)
			}
			<-done
			if p, err := target.apply(context.Background(), first); err != nil || p != first.durablePosition() {
				t.Fatal("resume first prefix", p, err)
			}
			kvCommitGet(t, c, "seed-counter", "42")
			if p, err := target.apply(context.Background(), first); err != nil || p != first.durablePosition() {
				t.Fatal("duplicate prefix", err)
			}
			kvCommitGet(t, c, "seed-counter", "42")
			if p, err := target.apply(context.Background(), second); err != nil || p != second.durablePosition() {
				t.Fatal("resume captured source restart", p, err)
			}
			kvCommitGet(t, c, "seed-counter", "43")
			kvCommitGet(t, c, "during-restart", "retained")
			kvCommitGet(t, c, "binary\x00key", "value\x00\r\n")
			c.expect(t, "$-1\r\n", "GET", "seed-delete")
			t.Logf("real snapshot+PSYNC applied; %s retry/duplicate did not repeat INCR; resumed to offset=%d counter=43", name, second.durablePosition().Offset)
		})
	}
	t.Run("state_drift_refuses_resume", func(t *testing.T) {
		reset()
		if _, err := target.apply(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		c.expect(t, "+OK\r\n", "SET", "seed-counter", "999")
		if _, err := target.apply(context.Background(), second); !errors.Is(err, errSyncTargetDrift) {
			t.Fatal("state drift accepted", err)
		}
		kvCommitGet(t, c, "seed-counter", "999")
		c.expect(t, "$-1\r\n", "GET", "during-restart")
	})
	for _, ahead := range []string{"data", "checkpoint"} {
		t.Run(ahead+"_ahead_refuses_resume", func(t *testing.T) {
			reset()
			if _, err := target.apply(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			if ahead == "data" {
				c.expect(t, ":43\r\n", "INCR", "seed-counter")
			} else {
				c.expect(t, "+OK\r\n", "SET", syncCheckpointKey, steps[len(steps)-1].checkpoint.encode())
			}
			if _, err := target.apply(context.Background(), second); !errors.Is(err, errSyncTargetDrift) {
				t.Fatal("partial state accepted", err)
			}
			want := "42"
			if ahead == "data" {
				want = "43"
			}
			kvCommitGet(t, c, "seed-counter", want)
			c.expect(t, "$-1\r\n", "GET", "during-restart")
		})
	}
	t.Run("changed_history_refuses_resume", func(t *testing.T) {
		reset()
		if _, err := target.apply(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		changed, _ := captureJournal(t, 4<<20)
		for _, record := range records {
			record.Position.History = strings.Repeat("f", 40)
			if err := changed.append(record); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := target.apply(context.Background(), changed); !errors.Is(err, errSyncHistoryGap) {
			t.Fatal("different source history accepted", err)
		}
		kvCommitGet(t, c, "seed-counter", "42")
	})
	t.Run("unsupported_prefix_refuses_all_writes", func(t *testing.T) {
		reset()
		j := openFixture(fixtures[0])
		appendSyncTestCommand(t, j, "PEXPIRE", "seed-counter", "1000")
		if _, err := target.apply(context.Background(), j); !errors.Is(err, errSyncUnsupported) {
			t.Fatal("TTL accepted", err)
		}
		assertEmpty()
	})
	t.Run("uninitialized_nonempty_target_refused", func(t *testing.T) {
		reset()
		c.expect(t, "+OK\r\n", "SET", "unexpected", "preserve")
		if _, err := target.apply(context.Background(), first); !errors.Is(err, errSyncTargetDrift) {
			t.Fatal("nonempty target accepted", err)
		}
		kvCommitGet(t, c, "unexpected", "preserve")
		c.expect(t, "$-1\r\n", "GET", syncCheckpointKey)
	})
	reset()
}
