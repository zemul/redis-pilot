package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type evictionColdFixture struct {
	j         *syncSegments
	op        *evictionOperation
	target    syncColdTarget
	hot, cold *kvCommitClient
	root      string
	intent    evictionIntent
}

func evictionColdEnvironment(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("GATEWAY_TEST_EVICTION_COLD") != "isolated" {
		t.Skip("requires dedicated empty Redis and Kvrocks")
	}
	hot, cold := os.Getenv("GATEWAY_TEST_EVICTION_SOURCE"), os.Getenv("GATEWAY_TEST_EVICTION_TARGET")
	if syncCanonicalEndpoint(hot) != hot || syncCanonicalEndpoint(cold) != cold || hot == "" || cold == "" || hot == cold {
		t.Fatal("explicit distinct loopback endpoints required")
	}
	return hot, cold
}

func newEvictionColdFixture(t *testing.T, root string, cleanup bool) *evictionColdFixture {
	return newEvictionColdFixturePrepared(t, root, cleanup, true)
}

func newEvictionColdFixturePrepared(t *testing.T, root string, cleanup, prepare bool) *evictionColdFixture {
	t.Helper()
	hotAddr, coldAddr := evictionColdEnvironment(t)
	f := &evictionColdFixture{hot: kvCommitConnect(t, hotAddr), cold: kvCommitConnect(t, coldAddr), root: root, target: syncColdTarget{address: coldAddr, taskID: "cold-confirm-probe", timeout: 2 * time.Second}}
	t.Cleanup(func() { f.hot.c.Close(); f.cold.c.Close() })
	f.hot.expect(t, ":0\r\n", "DBSIZE")
	f.cold.expect(t, ":0\r\n", "DBSIZE")
	f.hot.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-diskless-sync", "no")
	f.hot.expect(t, "+OK\r\n", "CONFIG", "SET", "maxmemory-policy", "noeviction")
	f.hot.expect(t, "+OK\r\n", "SET", "counter", "41")
	id := syncTaskIdentity{TaskID: f.target.taskID, Source: hotAddr, Cold: coldAddr}
	var err error
	f.j, err = openSyncSegmentsForTask(filepath.Join(root, "segments"), 1024, 4<<20, defaultSyncResourcePolicy(), &id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.j.close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- (syncSource{address: hotAddr, timeout: 3 * time.Second, maxSnapshot: 1 << 20}).capture(ctx, f.j)
	}()
	stopped := false
	t.Cleanup(func() {
		cancel()
		if !stopped {
			select {
			case <-done:
			case <-time.After(4 * time.Second):
				t.Error("capture leaked")
			}
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for f.j.durablePosition().History == "" {
		if time.Now().After(deadline) {
			t.Fatal("snapshot timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.hot.expect(t, ":42\r\n", "INCR", "counter")
	f.intent, err = newEvictionIntent(id.TaskID, 1, f.j.durablePosition().History, []byte("counter"), []byte("42"))
	if err != nil {
		t.Fatal(err)
	}
	opDir := filepath.Join(root, "operation")
	if err := os.MkdirAll(opDir, 0700); err != nil {
		t.Fatal(err)
	}
	f.op, err = openEvictionOperation(filepath.Join(opDir, "intent.log"), id, &f.intent, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.op.close() })
	if prepare {
		f.hot.expect(t, ":1\r\n", "EVAL", evictionPrepareProbeScript, "2", "counter", f.intent.prepareKey(), "42", "prepare:"+f.intent.encode())
		f.hot.expect(t, "+OK\r\n", "SET", "after-barrier", "not-applied")
	}
	raw, err := f.hot.request("INFO", "replication")
	if err != nil {
		t.Fatal(err)
	}
	pos, reason := parseSyncSourceInfo(raw)
	if reason != "none" {
		t.Fatal(reason)
	}
	for f.j.durablePosition().Offset < pos.Offset {
		if time.Now().After(deadline) {
			t.Fatal("capture timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
		stopped = true
	case <-time.After(4 * time.Second):
		t.Fatal("capture stop timeout")
	}
	v, err := f.j.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	err = v.walkFrom(v.base, func(r syncRecord, prev, _ syncApplyCheckpoint) error {
		if bytes.Equal(r.Data, f.intent.prepareFrame()) {
			count++
			return f.op.recordPreparation(evictionPreparation{prev.Position, r})
		}
		return nil
	})
	v.close()
	want := 0
	if prepare {
		want = 1
	}
	if err != nil || count != want {
		t.Fatal("prepare lookup", count, err)
	}
	if cleanup {
		t.Cleanup(func() {
			f.hot.request("DEL", "counter", "after-barrier", f.intent.prepareKey())
			c := &syncColdClient{taskID: id.TaskID}
			f.cold.request("DEL", "counter", "after-barrier", c.pendingCheckpointKey(), c.pendingKey(), c.anchorKey(), c.evictionReceiptKey())
		})
	}
	return f
}

func (f *evictionColdFixture) reopen(t *testing.T) {
	t.Helper()
	s, err := f.op.snapshot()
	if err != nil { // poisoned local write; identity comes from fixed test binding
		s.Identity = syncTaskIdentity{f.target.taskID, os.Getenv("GATEWAY_TEST_EVICTION_SOURCE"), f.target.address}
	}
	f.op.close()
	f.op, err = openEvictionOperation(filepath.Join(f.root, "operation", "intent.log"), s.Identity, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *evictionColdFixture) applyPrefix(t *testing.T) {
	t.Helper()
	s, _ := f.op.snapshot()
	f.j.applyMu.Lock()
	defer f.j.applyMu.Unlock()
	v, err := f.j.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	a, err := v.proveEvictionPreparation(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.target.applySegmentView(context.Background(), v, a.Previous); err != nil {
		t.Fatal(err)
	}
}

// Per-connection RESP relay: preserve the bound target identity while dropping
// only the confirmation EXEC, not bootstrap or normal pending commits.
func evictionReceiptFault(t *testing.T, address, key, mode string, mutate func()) (func(context.Context, string) (net.Conn, error), chan struct{}, <-chan string) {
	t.Helper()
	release := make(chan struct{})
	stop := make(chan struct{})
	result := make(chan string, 1)
	var fired atomic.Bool
	var wg sync.WaitGroup
	t.Cleanup(func() { close(stop); wg.Wait() })
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		back, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		front, relay := net.Pipe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer relay.Close()
			defer back.Close()
			back.SetDeadline(time.Now().Add(8 * time.Second))
			relay.SetDeadline(time.Now().Add(8 * time.Second))
			in, _ := NewReader(relay, DefaultConfig().Limits)
			out, _ := NewReader(back, DefaultConfig().Limits)
			matched := false
			for {
				raw, args, err := in.ReadCommand()
				if err != nil {
					return
				}
				if len(args) == 3 && string(args[0]) == "SET" && string(args[1]) == key {
					matched = true
				}
				fault := matched && string(args[0]) == "EXEC" && fired.CompareAndSwap(false, true)
				if fault && mode == "before" {
					result <- "not-sent"
					return
				}
				if fault && mode == "late" {
					relay.Close()
					select {
					case <-release:
					case <-stop:
						return
					}
				}
				if fault && mode == "conflict" {
					mutate()
				}
				if writeAll(back, raw) != nil {
					return
				}
				reply, err := out.ReadFrame()
				if err != nil {
					return
				}
				if fault {
					result <- string(reply)
					return
				}
				if writeAll(relay, reply) != nil {
					return
				}
			}
		}()
		return front, nil
	}
	return dial, release, result
}

func TestEvictionColdConfirmation(t *testing.T) {
	evictionColdEnvironment(t)
	for _, scenario := range []string{"normal", "before", "reply", "late", "conflict", "value_drift", "ttl", "wrong_type", "checkpoint_drift", "foreign_receipt", "receipt_missing", "receipt_ttl", "unavailable", "canceled", "local_fsync"} {
		t.Run(scenario, func(t *testing.T) {
			f := newEvictionColdFixture(t, t.TempDir(), true)
			c := &syncColdClient{taskID: f.target.taskID}
			key := c.evictionReceiptKey()
			if scenario == "normal" {
				if _, err := f.target.applySegments(context.Background(), f.j); err == nil {
					t.Fatal("ordinary apply accepted marker")
				}
				f.cold.expect(t, ":0\r\n", "DBSIZE")
			}
			bad := scenario == "value_drift" || scenario == "ttl" || scenario == "wrong_type" || scenario == "foreign_receipt" || scenario == "checkpoint_drift"
			if bad {
				f.applyPrefix(t)
				switch scenario {
				case "value_drift":
					f.cold.expect(t, "+OK\r\n", "SET", "counter", "43")
				case "ttl":
					f.cold.expect(t, ":1\r\n", "PEXPIRE", "counter", "60000")
				case "wrong_type":
					f.cold.expect(t, ":1\r\n", "DEL", "counter")
					f.cold.expect(t, ":1\r\n", "RPUSH", "counter", "x")
				case "foreign_receipt":
					f.cold.expect(t, "+OK\r\n", "SET", key, "foreign")
				case "checkpoint_drift":
					f.cold.expect(t, "+OK\r\n", "SET", c.pendingCheckpointKey(), "invalid-checkpoint")
				}
			}
			var release chan struct{}
			var result <-chan string
			if scenario == "before" || scenario == "reply" || scenario == "late" || scenario == "conflict" {
				f.target.testDial, release, result = evictionReceiptFault(t, f.target.address, key, scenario, func() { f.cold.expect(t, "+OK\r\n", "SET", "counter", "43") })
			}
			if scenario == "local_fsync" {
				f.op.fault = func(stage string) error {
					if stage == "receipt_after_sync" {
						return errors.New("injected local fsync uncertainty")
					}
					return nil
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			if scenario == "unavailable" {
				f.target.testDial = func(context.Context, string) (net.Conn, error) { return nil, errors.New("injected unavailable") }
			}
			a, err := f.target.confirmEviction(ctx, f.j, f.op)
			expectError := bad || result != nil || scenario == "local_fsync" || scenario == "unavailable" || scenario == "canceled"
			if (err != nil) != expectError {
				t.Fatalf("confirmation error=%v wantError=%v", err, expectError)
			}
			if err != nil && a != (evictionColdReceipt{}) {
				t.Fatal("error returned usable receipt")
			}
			if bad || scenario == "conflict" {
				if scenario != "foreign_receipt" {
					f.cold.expect(t, "$-1\r\n", "GET", key)
				}
				s, _ := f.op.snapshot()
				if s.Receipt != nil {
					t.Fatal("bad state saved ACK")
				}
				return
			}
			f.target.testDial = nil
			f.reopen(t)
			a, err = f.target.confirmEviction(context.Background(), f.j, f.op)
			if err != nil {
				t.Fatal("reconnect confirmation", err)
			}
			if scenario == "late" {
				close(release)
				select {
				case reply := <-result:
					if reply != "*-1\r\n" && reply != "$-1\r\n" {
						t.Fatal("old transaction executed", reply)
					}
				case <-time.After(4 * time.Second):
					t.Fatal("late transaction did not finish")
				}
			}
			kvCommitGet(t, f.cold, "counter", "42")
			kvCommitGet(t, f.hot, "counter", "42")
			f.cold.expect(t, "$-1\r\n", "GET", "after-barrier")
			kvCommitGet(t, f.cold, key, a.encode())
			kvCommitGet(t, f.cold, c.pendingCheckpointKey(), a.Previous.encode())
			s, _ := f.op.snapshot()
			if s.Receipt == nil || *s.Receipt != a {
				t.Fatal("ACK not durable locally")
			}
			if scenario == "receipt_missing" {
				f.cold.expect(t, ":1\r\n", "DEL", key)
				f.reopen(t)
				if _, err := f.target.confirmEviction(context.Background(), f.j, f.op); err == nil {
					t.Fatal("missing cold receipt accepted")
				}
			}
			if scenario == "receipt_ttl" {
				f.cold.expect(t, ":1\r\n", "PEXPIRE", key, "60000")
				f.reopen(t)
				if _, err := f.target.confirmEviction(context.Background(), f.j, f.op); err == nil {
					t.Fatal("expiring receipt accepted")
				}
			}
			t.Log("source prefix applied once; exact cold receipt + local receipt verified; no hot deletion")
		})
	}
}

func TestEvictionColdRestart(t *testing.T) {
	hotAddr, coldAddr := evictionColdEnvironment(t)
	phase, root := os.Getenv("GATEWAY_TEST_EVICTION_RESTART_PHASE"), os.Getenv("GATEWAY_TEST_EVICTION_RESTART_DIR")
	if phase == "" {
		t.Skip("explicit prepare/verify restart phase")
	}
	if !filepath.IsAbs(root) || root == "/" {
		t.Fatal("explicit dedicated restart directory required")
	}
	if phase == "prepare" {
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		f := newEvictionColdFixture(t, root, false)
		if _, err := f.target.confirmEviction(context.Background(), f.j, f.op); err != nil {
			t.Fatal(err)
		}
		t.Log("receipt committed with sync WAL; ready for external kill of dedicated Kvrocks")
		return
	}
	if phase != "verify" {
		t.Fatal("unknown phase")
	}
	id := syncTaskIdentity{"cold-confirm-probe", hotAddr, coldAddr}
	j, err := openSyncSegmentsForTask(filepath.Join(root, "segments"), 1024, 4<<20, defaultSyncResourcePolicy(), &id)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	op, err := openEvictionOperation(filepath.Join(root, "operation", "intent.log"), id, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	s, _ := op.snapshot()
	if s.Receipt == nil {
		t.Fatal("local receipt missing")
	}
	a, err := (syncColdTarget{address: coldAddr, taskID: id.TaskID, timeout: 2 * time.Second}).confirmEviction(context.Background(), j, op)
	if err != nil || a != *s.Receipt {
		t.Fatal("cold restart confirmation", err)
	}
	// Also exercise an old verification transaction whose receipt already
	// existed: a fresh identical SET must still invalidate that old WATCH.
	target := syncColdTarget{address: coldAddr, taskID: id.TaskID, timeout: 2 * time.Second}
	key := (&syncColdClient{taskID: id.TaskID}).evictionReceiptKey()
	relay, release, result := evictionReceiptFault(t, coldAddr, key, "late", nil)
	stale := target
	stale.testDial = relay
	if r, err := stale.confirmEviction(context.Background(), j, op); err == nil || r != (evictionColdReceipt{}) {
		t.Fatal("late verification reported success", err)
	}
	if _, err := target.confirmEviction(context.Background(), j, op); err != nil {
		t.Fatal("fresh verification", err)
	}
	close(release)
	select {
	case reply := <-result:
		if reply != "*-1\r\n" && reply != "$-1\r\n" {
			t.Fatal("old same-receipt WATCH survived", reply)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("old verification leaked")
	}
	t.Log("same durable cold receipt revalidated after Kvrocks process restart")
}
