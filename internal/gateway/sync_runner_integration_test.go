package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Multi-connection cold relay: startup probes/bootstrap must pass normally
// before the first INCR's EXEC reply is discarded. It never retries commands.
func runnerDropINCRReply(t *testing.T, addr string) (string, <-chan struct{}) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var fired atomic.Bool
	dropped := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			front, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer front.Close()
				back, reader, err := authenticated(addr, "")
				if err != nil {
					return
				}
				defer back.Close()
				stop := context.AfterFunc(ctx, func() { front.Close(); back.Close() })
				defer stop()
				front.SetDeadline(time.Now().Add(10 * time.Second))
				back.SetDeadline(time.Now().Add(10 * time.Second))
				commands, _ := NewReader(front, DefaultConfig().Limits)
				incr := false
				for {
					raw, args, err := commands.ReadCommand()
					if err != nil {
						return
					}
					incr = incr || string(args[0]) == "INCR"
					if err := writeAll(back, raw); err != nil {
						return
					}
					reply, err := reader.ReadFrame()
					if err != nil {
						return
					}
					if incr && string(args[0]) == "EXEC" && fired.CompareAndSwap(false, true) {
						close(dropped)
						return
					}
					if err := writeAll(front, reply); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { cancel(); l.Close(); wg.Wait() })
	return l.Addr().String(), dropped
}

func kvCommitRunnerExperiments(t *testing.T, coldAddr string, cold *kvCommitClient) {
	sourceAddr := os.Getenv("GATEWAY_TEST_RUNNER_SOURCE")
	source := syncSource{address: sourceAddr, timeout: 5 * time.Second, maxSnapshot: 1 << 20}
	if source.validate() != nil || sourceAddr == coldAddr {
		t.Fatal("explicit separate loopback Redis source required")
	}
	hot := kvCommitConnect(t, sourceAddr)
	defer hot.c.Close()
	info, err := hot.request("INFO", "server")
	if err != nil || !strings.Contains(string(info), "redis_version:7.2.5\r\n") {
		t.Fatal("expected isolated Redis 7.2.5 source", err)
	}
	hotCheck := &syncColdClient{hot.c, hot.r, 5 * time.Second}
	coldCheck := &syncColdClient{cold.c, cold.r, 5 * time.Second}
	for _, c := range []*syncColdClient{hotCheck, coldCheck} {
		keys, err := c.keys()
		if err != nil || len(keys) != 0 {
			t.Fatal("runner integration requires empty isolated source and cold DB", err)
		}
	}
	hot.expect(t, "+OK\r\n", "CONFIG", "SET", "rdbcompression", "no")
	hot.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-diskless-sync", "no")
	owned := []string{"runner-counter", "runner-delete", "runner-added", "runner-offline", "runner-ttl", "runner-large"}
	reset := func() {
		for _, c := range []*kvCommitClient{hot, cold} {
			keys := append([]string{}, owned...)
			if c == cold {
				keys = append(keys, syncPendingCPKey, syncPendingKey, syncAnchorKey)
			}
			if raw, err := c.request(append([]string{"DEL"}, keys...)...); err != nil || len(raw) == 0 || raw[0] != ':' {
				t.Fatal("explicit runner key cleanup failed", err)
			}
		}
	}
	defer reset()
	newLog := func(t *testing.T) (*syncSegments, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "segments")
		j, err := openSyncSegments(path, 1024, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { j.close() })
		return j, path
	}
	config := syncRunnerConfig{TickInterval: 10 * time.Millisecond, RetryMin: 20 * time.Millisecond, RetryMax: 100 * time.Millisecond, CleanupInterval: 100 * time.Millisecond, MaxPressure: 3 * time.Second, MaxRetries: 5}
	launch := func(t *testing.T, j *syncSegments, targetAddr string) (*syncRunner, func() error) {
		t.Helper()
		r := &syncRunner{source: source, target: syncColdTarget{address: targetAddr, timeout: time.Second}, config: config}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.run(ctx, j) }()
		var once sync.Once
		var runErr error
		stop := func() error {
			once.Do(func() {
				cancel()
				select {
				case runErr = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("runner did not stop and join")
				}
			})
			return runErr
		}
		t.Cleanup(func() { stop() })
		return r, stop
	}
	wait := func(t *testing.T, r *syncRunner, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(6 * time.Second)
		for !condition() {
			if status := r.snapshot(); status.State == "failed" {
				t.Fatalf("runner failed before condition: %+v", status)
			}
			if time.Now().After(deadline) {
				t.Fatalf("runner condition timeout: %+v", r.snapshot())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	value := func(key, want string) bool { v, ok, err := coldCheck.get(key); return err == nil && ok && v == want }
	confirmed := func(r *syncRunner) bool {
		s := r.snapshot()
		return s.Confirmed.History != "" && s.Confirmed == s.Captured && s.Applied == s.Captured
	}
	// Cancel is intentionally immediate, not a graceful transaction drain. A
	// concurrently interrupted EXEC must remain uncertain, even when our last
	// status snapshot was caught up. Explicitly reconcile that outcome instead
	// of requiring cancellation to hide it or retrying it inside the runner.
	stopChecked := func(t *testing.T, r *syncRunner, j *syncSegments, stop func() error, want string) {
		t.Helper()
		err := stop()
		if errors.Is(err, errSyncPendingUncertain) {
			if r.snapshot().State != "failed" || r.snapshot().LastError != "cold_outcome_uncertain" {
				t.Fatal("canceled EXEC did not retain terminal status", r.snapshot())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := (syncColdTarget{address: coldAddr, timeout: time.Second}).drainSegments(ctx, j); err != nil {
				t.Fatal("explicit reconciliation after cancel failed", err)
			}
			t.Log("cancel interrupted EXEC; terminal outcome retained and explicitly reconciled")
		} else if !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected stop result", err)
		}
		if !value("runner-counter", want) {
			t.Fatal("stop/reconciliation changed expected cold counter", want)
		}
		if status, err := j.resourceStatus(); err != nil || status.Views != 0 {
			t.Fatal("views leaked on stop", status, err)
		}
	}
	t.Run("continuous_capture_reconnect_and_restart", func(t *testing.T) {
		reset()
		hot.expect(t, "+OK\r\n", "MSET", "runner-counter", "41", "runner-delete", "gone")
		j, path := newLog(t)
		r, stop := launch(t, j, coldAddr)
		wait(t, r, func() bool { return value("runner-counter", "41") })
		hot.expect(t, ":42\r\n", "INCR", "runner-counter")
		hot.expect(t, ":1\r\n", "DEL", "runner-delete")
		hot.expect(t, "+OK\r\n", "SET", "runner-added", "v")
		wait(t, r, func() bool { return value("runner-counter", "42") && value("runner-added", "v") && confirmed(r) })
		if _, exists, err := coldCheck.get("runner-delete"); err != nil || exists {
			t.Fatal("deleted source key remains in cold storage", err)
		}
		attempts := r.snapshot().SourceAttempts
		killed, err := hot.request("CLIENT", "KILL", "TYPE", "replica")
		if err != nil || string(killed) != ":1\r\n" {
			t.Fatalf("expected exactly one owned replication client: %q %v", killed, err)
		}
		hot.expect(t, ":43\r\n", "INCR", "runner-counter")
		wait(t, r, func() bool {
			return r.snapshot().SourceAttempts > attempts && value("runner-counter", "43") && confirmed(r)
		})
		stopChecked(t, r, j, stop, "43")
		position := j.durablePosition()
		j.close()
		hot.expect(t, ":44\r\n", "INCR", "runner-counter")
		hot.expect(t, "+OK\r\n", "SET", "runner-offline", "retained")
		j, err = openSyncSegments(path, 1024, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer j.close()
		if j.durablePosition() != position {
			t.Fatal("reopen changed durable capture")
		}
		r, stop = launch(t, j, coldAddr)
		wait(t, r, func() bool {
			return value("runner-counter", "44") && value("runner-offline", "retained") && confirmed(r)
		})
		stopChecked(t, r, j, stop, "44")
		t.Log("real Redis 41->42; killed replica connection then automatic PSYNC reached43; stopped/reopened runner consumed offline write44 exactly once")
	})
	t.Run("pressure_pauses_and_resumes_real_source", func(t *testing.T) {
		reset()
		hot.expect(t, "+OK\r\n", "SET", "runner-counter", "41")
		j, _ := newLog(t)
		var free atomic.Uint64
		free.Store(1 << 40)
		j.availableBytes = func() (uint64, error) { return free.Load(), nil }
		r, stop := launch(t, j, coldAddr)
		wait(t, r, func() bool { return value("runner-counter", "41") })
		free.Store(0)
		wait(t, r, func() bool { return r.snapshot().State == "pressure" })
		before, attempts := j.durablePosition(), r.snapshot().SourceAttempts
		hot.expect(t, ":42\r\n", "INCR", "runner-counter")
		time.Sleep(80 * time.Millisecond)
		if j.durablePosition() != before || r.snapshot().SourceAttempts != attempts {
			t.Fatal("capture/ACK or reconnect attempts advanced during low disk")
		}
		free.Store(1 << 40)
		wait(t, r, func() bool { return value("runner-counter", "42") && confirmed(r) })
		stopChecked(t, r, j, stop, "42")
		t.Log("injected low availability paused capture without busy reconnect; actual Redis offline write synchronized after recovery")
	})
	t.Run("uncertain_cold_commit_stops_then_explicit_restart", func(t *testing.T) {
		reset()
		hot.expect(t, "+OK\r\n", "SET", "runner-counter", "41")
		j, _ := newLog(t)
		relay, dropped := runnerDropINCRReply(t, coldAddr)
		r, stop := launch(t, j, relay)
		wait(t, r, func() bool { return value("runner-counter", "41") })
		hot.expect(t, ":42\r\n", "INCR", "runner-counter")
		select {
		case <-dropped:
		case <-time.After(5 * time.Second):
			t.Fatal("INCR reply fault not reached")
		}
		deadline := time.Now().Add(3 * time.Second)
		for r.snapshot().State != "failed" && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if err := stop(); !errors.Is(err, errSyncPendingUncertain) {
			t.Fatal("uncertain write was not terminal", err)
		}
		kvCommitGet(t, cold, "runner-counter", "42")
		position := j.durablePosition()
		time.Sleep(50 * time.Millisecond)
		if j.durablePosition() != position {
			t.Fatal("capture remained active after terminal error")
		}
		r, stop = launch(t, j, coldAddr)
		wait(t, r, func() bool { return confirmed(r) })
		kvCommitGet(t, cold, "runner-counter", "42")
		hot.expect(t, ":43\r\n", "INCR", "runner-counter")
		wait(t, r, func() bool { return value("runner-counter", "43") && confirmed(r) })
		stopChecked(t, r, j, stop, "43")
		t.Log("discarded real cold EXEC reply: loop stopped and joined capture; explicit restart reconciled42 and next INCR reached43")
	})
	t.Run("graceful_shutdown_confirms_frozen_tail", func(t *testing.T) {
		reset()
		hot.expect(t, "+OK\r\n", "SET", "runner-counter", "41")
		j, _ := newLog(t)
		r, stop := launch(t, j, coldAddr)
		wait(t, r, func() bool { return value("runner-counter", "41") })
		hot.expect(t, ":42\r\n", "INCR", "runner-counter")
		info, err := hot.request("INFO", "replication")
		if err != nil {
			t.Fatal(err)
		}
		var offset int64 = -1
		for _, line := range strings.Split(string(info), "\r\n") {
			if strings.HasPrefix(line, "master_repl_offset:") {
				offset, err = strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
			}
		}
		if err != nil || offset < 0 {
			t.Fatal("source offset missing", err)
		}
		wait(t, r, func() bool { return j.durablePosition().Offset >= offset })
		grace := func(r *syncRunner, stop func() error, want string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.shutdown(ctx); err != nil {
				t.Fatal("graceful shutdown failed", err)
			}
			if err := stop(); err != nil {
				t.Fatal("run did not return graceful completion", err)
			}
			s := r.snapshot()
			if s.State != "drained" || s.DrainTarget != j.durablePosition() || s.Applied != s.DrainTarget || s.Confirmed != s.DrainTarget || !s.StopRequested || s.FinishedAt.IsZero() || !value("runner-counter", want) {
				t.Fatal("graceful tail was not confirmed", s)
			}
		}
		grace(r, stop, "42")
		frozen := j.durablePosition()
		hot.expect(t, ":43\r\n", "INCR", "runner-counter")
		if j.durablePosition() != frozen || !value("runner-counter", "42") {
			t.Fatal("stopped runner continued to capture/apply")
		}
		r, stop = launch(t, j, coldAddr)
		wait(t, r, func() bool { return value("runner-counter", "43") && confirmed(r) })
		grace(r, stop, "43")
		t.Log("graceful stop confirmed frozen local tail; later source writes waited for explicit restart")
	})
	t.Run("unsupported_ttl_stops_without_cold_expiry", func(t *testing.T) {
		reset()
		hot.expect(t, "+OK\r\n", "SET", "runner-counter", "41")
		j, _ := newLog(t)
		r, stop := launch(t, j, coldAddr)
		wait(t, r, func() bool { return value("runner-counter", "41") })
		hot.expect(t, "+OK\r\n", "SET", "runner-ttl", "value", "PX", "60000")
		deadline := time.Now().Add(3 * time.Second)
		for r.snapshot().State != "failed" && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if err := stop(); !errors.Is(err, errSyncUnsupported) {
			t.Fatal("unsupported record not terminal", err)
		}
		cold.expect(t, "$-1\r\n", "GET", "runner-ttl")
		kvCommitGet(t, cold, "runner-counter", "41")
		t.Log("real Redis propagated TTL command was rejected; no automatic skip or cold expiry replay")
	})
	t.Run("real_backlog_gap_never_replaces_cold_history", func(t *testing.T) {
		reset()
		hot.expect(t, "+OK\r\n", "SET", "runner-counter", "41")
		j, _ := newLog(t)
		r, stop := launch(t, j, coldAddr)
		wait(t, r, func() bool { return value("runner-counter", "41") })
		hot.expect(t, ":42\r\n", "INCR", "runner-counter")
		wait(t, r, func() bool { return value("runner-counter", "42") && confirmed(r) })
		stopChecked(t, r, j, stop, "42")
		position := j.durablePosition()
		hot.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-backlog-size", "16384")
		defer hot.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-backlog-size", "1048576")
		hot.expect(t, "+OK\r\n", "SET", "runner-large", strings.Repeat("x", 128<<10))
		r, stop = launch(t, j, coldAddr)
		deadline := time.Now().Add(3 * time.Second)
		for r.snapshot().State != "failed" && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if err := stop(); !errors.Is(err, errSyncHistoryGap) {
			t.Fatal("real replication backlog gap not terminal", err)
		}
		if j.durablePosition() != position {
			t.Fatal("FULLRESYNC replaced existing journal")
		}
		kvCommitGet(t, cold, "runner-counter", "42")
		cold.expect(t, "$-1\r\n", "GET", "runner-large")
		t.Log("actual Redis backlog overflow forced FULLRESYNC; loop stopped and preserved prior cold/history without snapshot replacement")
	})
	reset()
	for _, c := range []*syncColdClient{hotCheck, coldCheck} {
		keys, err := c.keys()
		if err != nil || len(keys) != 0 {
			t.Fatal("runner fixture DB not empty after exact-key cleanup", err)
		}
	}
	t.Log(fmt.Sprintf("isolated continuous runner completed against %s and %s", sourceAddr, coldAddr))
}
