package gateway

// G6a-2 capability/fault experiments only. This is NOT a cold-layer writer:
// Includes normal-path recovery and deliberately unsafe wrappers as controls.

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const kvCommitPrefix = "__pilot_g6_commit_test__:{single-slot}:"

type kvCommitClient struct {
	c net.Conn
	r *Reader
}

func kvCommitConnect(t *testing.T, addr string) *kvCommitClient {
	t.Helper()
	c, r, err := authenticated(addr, "")
	if err != nil {
		t.Fatal(err)
	}
	return &kvCommitClient{c: c, r: r}
}

func (c *kvCommitClient) request(args ...string) ([]byte, error) {
	c.c.SetDeadline(time.Now().Add(5 * time.Second))
	if err := writeAll(c.c, encodeCommand(args...)); err != nil {
		return nil, err
	}
	return c.r.ReadFrame()
}

func (c *kvCommitClient) expect(t *testing.T, want string, args ...string) {
	t.Helper()
	raw, err := c.request(args...)
	if err != nil || string(raw) != want {
		t.Fatalf("%s: got %q want %q err=%v", args[0], raw, want, err)
	}
}

func kvCommitGet(t *testing.T, c *kvCommitClient, key, want string) {
	t.Helper()
	c.expect(t, fmt.Sprintf("$%d\r\n%s\r\n", len(want), want), "GET", key)
}

// Drop exactly the EXEC reply after reading the full backend response. This
// injects an ambiguous client result while proving that the backend executed.
func kvCommitDropExecReply(t *testing.T, addr string) (string, <-chan struct{}) {
	return kvCommitFaultExec(t, addr, false)
}

// before=true drops EXEC before forwarding; otherwise only its reply is lost.
func kvCommitFaultExec(t *testing.T, addr string, before bool) (string, <-chan struct{}) {
	return kvCommitFaultExecMatching(t, addr, before, "")
}

func kvCommitFaultExecMatching(t *testing.T, addr string, before bool, command string) (string, <-chan struct{}) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer l.Close()
		front, err := l.Accept()
		if err != nil {
			return
		}
		defer front.Close()
		front.SetDeadline(time.Now().Add(5 * time.Second))
		back, reader, err := authenticated(addr, "")
		if err != nil {
			t.Error(err)
			return
		}
		defer back.Close()
		back.SetDeadline(time.Now().Add(5 * time.Second))
		commands, _ := NewReader(front, DefaultConfig().Limits)
		matched := command == ""
		for {
			raw, args, err := commands.ReadCommand()
			if err != nil {
				return
			}
			if string(args[0]) == command {
				matched = true
			}
			if matched && before && string(args[0]) == "EXEC" {
				return
			}
			if writeAll(back, raw) != nil {
				return
			}
			reply, err := reader.ReadFrame()
			if err != nil {
				t.Error(err)
				return
			}
			if matched && string(args[0]) == "EXEC" {
				return
			}
			if writeAll(front, reply) != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		l.Close()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("fault relay did not exit")
		}
	})
	return l.Addr().String(), done
}

func kvCommitTxn(t *testing.T, c *kvCommitClient, key, checkpoint string, next int) {
	t.Helper()
	c.expect(t, "+OK\r\n", "MULTI")
	c.expect(t, "+QUEUED\r\n", "INCR", key)
	c.expect(t, "+QUEUED\r\n", "SET", checkpoint, strconv.Itoa(next))
}

// Run only inside a disposable Kvrocks 2.15.0 container with --network none.
// The fixed prefix is not sufficient isolation: the runner MUST create a fresh
// data directory. No production engine or gateway is contacted by default.
func TestKvrocksCommitExperiment(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_KVROCKS_COMMIT") != "isolated" {
		t.Skip("explicit isolated Kvrocks commit experiment only")
	}
	addr := os.Getenv("GATEWAY_TEST_KVROCKS_ADDRESS")
	host, _, err := net.SplitHostPort(addr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		t.Fatal("Kvrocks commit experiment requires literal loopback")
	}
	c := kvCommitConnect(t, addr)
	defer c.c.Close()
	info, err := c.request("INFO", "server")
	if err != nil || !strings.Contains(string(info), "kvrocks_version:2.15.0") {
		t.Fatalf("expected Kvrocks 2.15.0: %q %v", info, err)
	}
	for _, setting := range []struct{ key, value string }{{"rocksdb.write_options.sync", "yes"}, {"rocksdb.write_options.disable_wal", "no"}} {
		raw, err := c.request("CONFIG", "GET", setting.key)
		want := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(setting.key), setting.key, len(setting.value), setting.value)
		if err != nil || string(raw) != want {
			t.Fatalf("durability setting %s: %q %v", setting.key, raw, err)
		}
	}
	phase := os.Getenv("GATEWAY_TEST_KVROCKS_PHASE")
	key, checkpoint := kvCommitPrefix+"crash-counter", kvCommitPrefix+"crash-checkpoint"
	switch phase {
	case "crash_prepare":
		c.expect(t, "+OK\r\n", "MSET", key, "0", checkpoint, "0")
		for i := 1; i <= 64; i++ {
			kvCommitTxn(t, c, key, checkpoint, i)
			c.expect(t, fmt.Sprintf("*2\r\n:%d\r\n+OK\r\n", i), "EXEC")
		}
		t.Log("64 acknowledged transactions prepared for external SIGKILL; no graceful engine shutdown")
		return
	case "crash_verify":
		kvCommitGet(t, c, key, "64")
		kvCommitGet(t, c, checkpoint, "64")
		// Replay token 64 is already present: do not issue INCR again.
		kvCommitGet(t, c, key, "64")
		kvCommitTxn(t, c, key, checkpoint, 65)
		c.expect(t, "*2\r\n:65\r\n+OK\r\n", "EXEC")
		t.Log("after SIGKILL: value=64 checkpoint=64; duplicate skipped; next transaction reached 65")
		return
	case "semantics":
	case "replay":
		kvCommitReplayExperiments(t, addr, c)
		return
	case "apply":
		kvCommitApplyExperiments(t, addr, c)
		return
	case "pending":
		kvCommitPendingExperiments(t, addr, c)
		return
	case "segments":
		kvCommitSegmentExperiments(t, addr, c)
		return
	case "runner":
		kvCommitRunnerExperiments(t, addr, c)
		return
	default:
		t.Fatal("expected explicit semantics/replay/apply/pending/segments/runner/crash_prepare/crash_verify phase")
	}

	t.Run("reply_loss_and_checkpoint_readback", func(t *testing.T) {
		key, cp := kvCommitPrefix+"reply-counter", kvCommitPrefix+"reply-checkpoint"
		c.expect(t, "+OK\r\n", "MSET", key, "0", cp, "0")
		relay, done := kvCommitDropExecReply(t, addr)
		faulty := kvCommitConnect(t, relay)
		defer faulty.c.Close()
		kvCommitTxn(t, faulty, key, cp, 1)
		if _, err := faulty.request("EXEC"); err == nil {
			t.Fatal("EXEC reply was not lost")
		}
		<-done
		// No automatic write retry. Read the authoritative committed checkpoint.
		kvCommitGet(t, c, cp, "1")
		kvCommitGet(t, c, key, "1")
		t.Log("EXEC response discarded; readback found value=1 checkpoint=1; no duplicate INCR issued")
	})
	t.Run("disconnect_before_exec", func(t *testing.T) {
		key, cp := kvCommitPrefix+"queued-counter", kvCommitPrefix+"queued-checkpoint"
		c.expect(t, "+OK\r\n", "MSET", key, "0", cp, "0")
		queued := kvCommitConnect(t, addr)
		kvCommitTxn(t, queued, key, cp, 1)
		queued.c.Close()
		kvCommitGet(t, c, key, "0")
		kvCommitGet(t, c, cp, "0")
	})
	t.Run("unsafe_generic_transaction_runtime_error", func(t *testing.T) {
		key, cp := kvCommitPrefix+"bad-counter", kvCommitPrefix+"bad-checkpoint"
		c.expect(t, "+OK\r\n", "MSET", key, "not-an-integer", cp, "0")
		kvCommitTxn(t, c, key, cp, 1)
		raw, err := c.request("EXEC")
		if err != nil || !strings.Contains(string(raw), "-ERR") || !strings.HasSuffix(string(raw), "+OK\r\n") {
			t.Fatalf("expected per-command failure: %q %v", raw, err)
		}
		kvCommitGet(t, c, key, "not-an-integer")
		kvCommitGet(t, c, cp, "1")
		t.Log("COUNTEREXAMPLE: INCR failed but checkpoint advanced; generic MULTI is unsafe")
	})
	t.Run("unsafe_script_error_inside_exec", func(t *testing.T) {
		key, cp := kvCommitPrefix+"script-counter", kvCommitPrefix+"script-checkpoint"
		c.expect(t, "+OK\r\n", "MSET", key, "0", cp, "0")
		c.expect(t, "+OK\r\n", "MULTI")
		script := `redis.call('INCR',KEYS[1]); error('injected_before_checkpoint'); redis.call('SET',KEYS[2],'1')`
		c.expect(t, "+QUEUED\r\n", "EVAL", script, "2", key, cp)
		raw, err := c.request("EXEC")
		if err != nil || !strings.Contains(string(raw), "injected_before_checkpoint") {
			t.Fatalf("expected injected script error: %q %v", raw, err)
		}
		kvCommitGet(t, c, key, "1")
		kvCommitGet(t, c, cp, "0")
		t.Log("COUNTEREXAMPLE: script failed after INCR, value committed without checkpoint; Lua does not imply rollback")
	})
	t.Run("watch_conflict_prevents_commit", func(t *testing.T) {
		key, cp := kvCommitPrefix+"watch-counter", kvCommitPrefix+"watch-checkpoint"
		c.expect(t, "+OK\r\n", "MSET", key, "0", cp, "0")
		watcher := kvCommitConnect(t, addr)
		defer watcher.c.Close()
		watcher.expect(t, "+OK\r\n", "WATCH", cp)
		kvCommitTxn(t, watcher, key, cp, 1)
		c.expect(t, "+OK\r\n", "SET", cp, "external-change")
		raw, err := watcher.request("EXEC")
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != "$-1\r\n" && string(raw) != "*-1\r\n" {
			t.Fatalf("expected aborted EXEC, got %q", raw)
		}
		kvCommitGet(t, c, key, "0")
		kvCommitGet(t, c, cp, "external-change")
	})
}
