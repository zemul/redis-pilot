package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func captureSnapshot(source string) syncRecord {
	return syncRecord{Kind: "snapshot", Position: syncPosition{Source: source, History: strings.Repeat("a", 40), Offset: 100}, Data: []byte("REDIS0011opaque-test-snapshot")}
}

func captureJournal(t *testing.T, limit int64) (*syncJournal, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "capture.log")
	j, err := openSyncJournal(path, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.close() })
	return j, path
}

func capturedRecords(t *testing.T, path string) []syncRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Seek(int64(len(syncJournalMagic)), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var result []syncRecord
	for {
		r, err := readSyncRecord(f)
		if err == io.EOF {
			return result
		}
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, r)
	}
}

func TestSyncJournalRestartAndBound(t *testing.T) {
	j, path := captureJournal(t, 1024)
	snap := captureSnapshot("127.0.0.1:1234")
	if err := j.append(snap); err != nil {
		t.Fatal(err)
	}
	raw := encodeCommand("SET", "counter", "41")
	pos := snap.Position
	pos.Offset += int64(len(raw))
	if err := j.append(syncRecord{Kind: "command", Position: pos, Data: raw}); err != nil {
		t.Fatal(err)
	}
	if err := j.append(syncRecord{Kind: "command", Position: pos, Data: raw}); !errors.Is(err, errSyncJournalCorrupt) {
		t.Fatal("duplicate offset accepted", err)
	}
	if second, err := openSyncJournal(path, 1024); err == nil {
		second.close()
		t.Fatal("concurrent journal owner accepted")
	}
	j.close()
	resumed, err := openSyncJournal(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.close()
	if resumed.durablePosition() != pos {
		t.Fatalf("position %+v", resumed.durablePosition())
	}
	tooBig := encodeCommand("SET", "large", strings.Repeat("x", 1024))
	next := pos
	next.Offset += int64(len(tooBig))
	if err := resumed.append(syncRecord{Kind: "command", Position: next, Data: tooBig}); !errors.Is(err, errSyncJournalFull) {
		t.Fatal("capacity not enforced", err)
	}
	if resumed.durablePosition() != pos {
		t.Fatal("position advanced after failure")
	}
	records := capturedRecords(t, path)
	if len(records) != 2 || !bytes.Equal(records[1].Data, raw) {
		t.Fatalf("records %+v", records)
	}
}

func TestSyncJournalRejectsCorruption(t *testing.T) {
	for _, kind := range []string{"torn_header", "torn_body", "checksum", "permissions", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			j, path := captureJournal(t, 1<<20)
			if err := j.append(captureSnapshot("127.0.0.1:1234")); err != nil {
				t.Fatal(err)
			}
			j.close()
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "torn_header":
				data = append(data, 0, 0, 0)
			case "torn_body":
				data = data[:len(data)-1]
			case "checksum":
				data[len(data)-1] ^= 1
			case "permissions":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := filepath.Join(filepath.Dir(path), "link")
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			}
			if kind != "permissions" && kind != "symlink" {
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if opened, err := openSyncJournal(path, 1<<20); err == nil {
				opened.close()
				t.Fatal("unsafe journal accepted")
			}
		})
	}
}

func TestSyncJournalCrashRecovery(t *testing.T) {
	if os.Getenv("GATEWAY_SYNC_JOURNAL_CRASH_HELPER") == "1" {
		j, err := openSyncJournal(os.Getenv("GATEWAY_SYNC_JOURNAL_TEST_PATH"), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		r := captureSnapshot("127.0.0.1:1234")
		if err := j.append(r); err != nil {
			t.Fatal(err)
		}
		raw := encodeCommand("SET", "durable-before-kill", "yes")
		r.Position.Offset += int64(len(raw))
		if err := j.append(syncRecord{Kind: "command", Position: r.Position, Data: raw}); err != nil {
			t.Fatal(err)
		}
		fmt.Println("durable")
		// Parent kills this exact test process; no Close or graceful shutdown.
		select {}
	}
	path := filepath.Join(t.TempDir(), "crash.log")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSyncJournalCrashRecovery$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "GATEWAY_SYNC_JOURNAL_CRASH_HELPER=1", "GATEWAY_SYNC_JOURNAL_TEST_PATH="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, readErr := bufio.NewReader(stdout).ReadString('\n')
	killErr := cmd.Process.Kill()
	cmd.Wait()
	if readErr != nil || line != "durable\n" || killErr != nil {
		t.Fatalf("crash helper: %q %v %v", line, readErr, killErr)
	}
	j, err := openSyncJournal(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	want := int64(100 + len(encodeCommand("SET", "durable-before-kill", "yes")))
	if j.durablePosition().Offset != want || len(capturedRecords(t, path)) != 2 {
		t.Fatal("durable records lost after process kill")
	}
}

// Fake replication endpoint; no Redis or user data involved.
func syncFixtureServer(t *testing.T, fn func(net.Conn, []string)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(3 * time.Second))
		r, _ := NewReader(c, DefaultConfig().Limits)
		for {
			_, args, err := r.ReadCommand()
			if err != nil {
				return
			}
			if string(args[0]) == "PSYNC" {
				str := make([]string, len(args))
				for i := range args {
					str[i] = string(args[i])
				}
				fn(c, str)
				return
			}
			writeAll(c, []byte("+OK\r\n"))
		}
	}()
	t.Cleanup(func() { l.Close(); <-done })
	return l.Addr().String()
}

func TestSyncCaptureResumeAndGap(t *testing.T) {
	for _, reply := range []string{"+CONTINUE", "+FULLRESYNC " + strings.Repeat("b", 40) + " 1000", "+CONTINUE " + strings.Repeat("b", 40)} {
		t.Run(reply[:9], func(t *testing.T) {
			j, _ := captureJournal(t, 1<<20)
			addr := syncFixtureServer(t, func(c net.Conn, args []string) {
				if len(args) != 3 || args[1] != strings.Repeat("a", 40) || args[2] != "101" {
					t.Error("incorrect resume request", args)
				}
				writeAll(c, []byte(reply+"\r\n"))
				if reply == "+CONTINUE" {
					writeAll(c, encodeCommand("SET", "after-restart", "yes"))
					r, _ := NewReader(c, DefaultConfig().Limits)
					r.ReadCommand()
					time.Sleep(20 * time.Millisecond)
				}
			})
			snap := captureSnapshot(addr)
			if err := j.append(snap); err != nil {
				t.Fatal(err)
			}
			err := (syncSource{address: addr, timeout: time.Second, maxSnapshot: 1 << 20}).capture(context.Background(), j)
			if reply == "+CONTINUE" {
				if j.durablePosition().Offset != 100+int64(len(encodeCommand("SET", "after-restart", "yes"))) {
					t.Fatal("incorrect captured offset", err)
				}
			} else {
				if !errors.Is(err, errSyncHistoryGap) || j.durablePosition() != snap.Position {
					t.Fatal("history gap was not fenced", err)
				}
			}
		})
	}
}

func TestSyncCaptureSnapshotLimits(t *testing.T) {
	for _, body := range []string{"$999999999\r\n", "$EOF:unsupported\r\n", "$20\r\nREDIS0009"} {
		t.Run(body[:4], func(t *testing.T) {
			j, _ := captureJournal(t, 1<<20)
			addr := syncFixtureServer(t, func(c net.Conn, args []string) {
				writeAll(c, []byte("+FULLRESYNC "+strings.Repeat("a", 40)+" 0\r\n"+body))
			})
			if err := (syncSource{address: addr, timeout: time.Second, maxSnapshot: 1024}).capture(context.Background(), j); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
			if j.durablePosition().History != "" {
				t.Fatal("incomplete snapshot advanced checkpoint")
			}
		})
	}
}

func waitCaptured(t *testing.T, j *syncJournal, offset int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p := j.durablePosition()
		if p.History != "" && p.Offset >= offset {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture timeout at %+v, want %d", p, offset)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func syncTestRedis(t *testing.T) string {
	t.Helper()
	addr := g4Redis(t)
	// PSYNC can fork an RDB child. Graceful shutdown stops/reaps that child
	// before g4Redis's final kill/wait and TempDir cleanup; killing only the
	// parent can race with a child still writing its temporary RDB file.
	t.Cleanup(func() {
		conn, reader, err := authenticated(addr, "g4-backend-test")
		if err != nil {
			t.Error("test Redis shutdown connection:", err)
			return
		}
		defer conn.Close()
		if err := writeAll(conn, encodeCommand("SHUTDOWN", "NOSAVE")); err != nil {
			t.Error(err)
			return
		}
		if raw, err := reader.ReadFrame(); !errors.Is(err, io.EOF) {
			t.Errorf("expected shutdown EOF, got %q err=%v", raw, err)
		}
	})
	return addr
}

// Opt-in, disposable real Redis. Verifies restart+PSYNC byte offsets and actual
// backlog loss; no existing Gateway/Redis/Kvrocks processes are contacted.
func TestSyncCaptureRedis(t *testing.T) {
	addr := syncTestRedis(t)
	conn, reader, err := authenticated(addr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	command := func(args ...string) string {
		t.Helper()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := writeAll(conn, encodeCommand(args...)); err != nil {
			t.Fatal(err)
		}
		raw, err := reader.ReadFrame()
		if err != nil || len(raw) == 0 || raw[0] == '-' {
			t.Fatalf("command %s: %q %v", args[0], raw, err)
		}
		return string(raw)
	}
	command("CONFIG", "SET", "repl-diskless-sync", "no")
	command("CONFIG", "SET", "repl-backlog-size", "16384")
	command("SET", "snapshot-seed", "present")
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
		raw := command("INFO", "replication")
		for _, line := range strings.Split(raw, "\r\n") {
			if strings.HasPrefix(line, "master_repl_offset:") {
				n, err := strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
		}
		t.Fatal("missing master offset")
		return 0
	}
	cancel, done := start(j)
	waitCaptured(t, j, 0)
	command("SET", "counter", "41")
	command("INCR", "counter")
	command("PEXPIRE", "counter", "60000")
	command("SET", "delete-me", "x")
	command("DEL", "delete-me")
	waitCaptured(t, j, offset())
	stop(cancel, done)
	previous := j.durablePosition()
	j.close()
	command("SET", "during-restart", "retained")
	j2, err := openSyncJournal(path, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.close()
	if j2.durablePosition() != previous {
		t.Fatal("checkpoint not recovered")
	}
	cancel, done = start(j2)
	waitCaptured(t, j2, offset())
	stop(cancel, done)
	records := capturedRecords(t, path)
	snapshots, restartWrites := 0, 0
	for _, r := range records {
		if r.Kind == "snapshot" {
			snapshots++
			if !bytes.Contains(r.Data, []byte("snapshot-seed")) {
				t.Fatal("snapshot seed missing")
			}
		} else if bytes.Equal(r.Data, encodeCommand("SET", "during-restart", "retained")) {
			restartWrites++
		}
	}
	if snapshots != 1 || restartWrites != 1 {
		t.Fatalf("snapshots=%d restart writes=%d", snapshots, restartWrites)
	}
	// Overflow the small source backlog while the capture is disconnected.
	for i := 0; i < 100; i++ {
		command("SET", fmt.Sprintf("gap-%d", i), strings.Repeat("x", 4096))
	}
	before := j2.durablePosition()
	if err := source.capture(context.Background(), j2); !errors.Is(err, errSyncHistoryGap) {
		t.Fatalf("expected full-resync refusal, got %v", err)
	}
	if j2.durablePosition() != before {
		t.Fatal("full resync replaced existing history")
	}
	t.Logf("captured %d records; restart resumed once; source backlog loss rejected", len(records))
}

// Verify the actual propagated commands, rather than assuming INCR becomes
// SET of the resulting value. The companion Kvrocks experiment consumes the
// same command shape after the deadline; these are two component tests, not
// an end-to-end importer/applier.
func TestSyncCaptureRedisExpiryCommands(t *testing.T) {
	addr := syncTestRedis(t)
	conn, reader, err := authenticated(addr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &kvCommitClient{conn, reader}
	c.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-diskless-sync", "no")
	j, path := captureJournal(t, 4<<20)
	ctx, cancel := context.WithCancel(context.Background())
	source := syncSource{address: addr, passwordFile: secretFile(t, "g4-backend-test"), timeout: 5 * time.Second, maxSnapshot: 1 << 20}
	done := make(chan error, 1)
	go func() { done <- source.capture(ctx, j) }()
	stopped := false
	t.Cleanup(func() {
		cancel()
		if stopped {
			return
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("capture did not exit")
		}
	})
	waitCaptured(t, j, 0)
	key := "expiry-counter"
	deadline := strconv.FormatInt(time.Now().Add(30*time.Second).UnixMilli(), 10)
	c.expect(t, "+OK\r\n", "SET", key, "41", "PXAT", deadline)
	c.expect(t, ":42\r\n", "INCR", key)
	c.expect(t, ":1\r\n", "PERSIST", key)
	kvCommitGet(t, c, key, "42")
	c.expect(t, ":-1\r\n", "PTTL", key)
	// Also capture the first-stage allowlisted PEXPIRE extension, not only
	// PERSIST: extensions must not lose the pre-expiry value during replay.
	c.expect(t, "+OK\r\n", "SET", "expiry-extension", "41", "PXAT", deadline)
	c.expect(t, ":42\r\n", "INCR", "expiry-extension")
	c.expect(t, ":1\r\n", "PEXPIRE", "expiry-extension", "120000")
	raw, err := c.request("INFO", "replication")
	if err != nil {
		t.Fatal(err)
	}
	var target int64 = -1
	for _, line := range strings.Split(string(raw), "\r\n") {
		if strings.HasPrefix(line, "master_repl_offset:") {
			target, err = strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if target < 0 {
		t.Fatal("missing source offset")
	}
	waitCaptured(t, j, target)
	// No more commands are needed; waitCaptured guarantees all six writes
	// are durably recorded. Stop before reading the file to avoid a torn read.
	cancel()
	select {
	case <-done:
		stopped = true
	case <-time.After(2 * time.Second):
		t.Fatal("capture did not exit")
	}
	var commands [][]string
	for _, record := range capturedRecords(t, path) {
		if record.Kind != "command" {
			continue
		}
		r, err := NewReader(bytes.NewReader(record.Data), DefaultConfig().Limits)
		if err != nil {
			t.Fatal(err)
		}
		_, args, err := r.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if len(args) < 2 || (string(args[1]) != key && string(args[1]) != "expiry-extension") {
			continue
		}
		var command []string
		for _, arg := range args {
			command = append(command, string(arg))
		}
		command[0] = strings.ToUpper(command[0])
		commands = append(commands, command)
	}
	if len(commands) != 6 {
		t.Fatalf("unexpected stream: %q", commands)
	}
	want := [][]string{{"SET", key, "41", "PXAT", deadline}, {"INCR", key}, {"PERSIST", key}, {"SET", "expiry-extension", "41", "PXAT", deadline}, {"INCR", "expiry-extension"}}
	for i, command := range want {
		if !bytes.Equal(encodeCommand(commands[i]...), encodeCommand(command...)) {
			t.Fatalf("command %d got %q want %q", i, commands[i], command)
		}
	}
	last := commands[5]
	if len(last) != 3 || last[0] != "PEXPIREAT" || last[1] != "expiry-extension" {
		t.Fatalf("unexpected expiry propagation: %q", last)
	}
	if until, err := strconv.ParseInt(last[2], 10, 64); err != nil || until <= time.Now().UnixMilli() {
		t.Fatal("expected future absolute extension", last)
	}
	t.Logf("source final counter=42 PTTL=-1; actual PSYNC commands=%q", commands)
}
