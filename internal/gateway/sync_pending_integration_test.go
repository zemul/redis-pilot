package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// One connection relay. Delay EXEC itself, not a simulated status flag: a
// stale queued transaction remains alive at the backend while recovery runs.
func pendingFaultRelay(t *testing.T, addr, phase, mode string) (string, <-chan struct{}, chan struct{}, <-chan string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reached := make(chan struct{})
	release := make(chan struct{}, 1)
	result := make(chan string, 1)
	go func() {
		defer l.Close()
		defer close(result)
		front, err := l.Accept()
		if err != nil {
			return
		}
		defer front.Close()
		back, reader, err := authenticated(addr, "")
		if err != nil {
			t.Error(err)
			return
		}
		defer back.Close()
		front.SetDeadline(time.Now().Add(10 * time.Second))
		back.SetDeadline(time.Now().Add(10 * time.Second))
		commands, _ := NewReader(front, DefaultConfig().Limits)
		matched := false
		for {
			raw, args, err := commands.ReadCommand()
			if err != nil {
				return
			}
			switch phase {
			case "bootstrap":
				matched = matched || string(args[0]) == "MSET"
			case "apply":
				matched = matched || string(args[0]) == "INCR"
			case "delete":
				matched = matched || string(args[0]) == "DEL"
			case "prepare", "control":
				if len(args) == 3 && string(args[0]) == "SET" && string(args[1]) == syncPendingKey {
					var p syncPending
					if json.Unmarshal(args[2], &p) == nil {
						cmd, _ := syncPendingCommand(p.Record.Data)
						matched = matched || (phase == "prepare" && len(cmd) > 0 && cmd[0] == "INCR") || (phase == "control" && len(cmd) == 0)
					}
				}
			}
			fault := matched && string(args[0]) == "EXEC"
			if fault && mode != "reply" {
				close(reached)
				if mode == "before" {
					return
				}
				select {
				case <-release:
				case <-time.After(6 * time.Second):
					t.Error("late EXEC not released")
					return
				}
			}
			if err := writeAll(back, raw); err != nil {
				return
			}
			reply, err := reader.ReadFrame()
			if err != nil {
				t.Error(err)
				return
			}
			if fault {
				if mode == "reply" {
					close(reached)
				}
				result <- string(reply)
				return
			}
			if err := writeAll(front, reply); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		l.Close()
		select {
		case release <- struct{}{}:
		default:
		}
		select {
		case <-result:
		case <-time.After(7 * time.Second):
			t.Error("pending relay leaked")
		}
	})
	return l.Addr().String(), reached, release, result
}

// Run in an actual fresh process for the targeted applier SIGKILL test.
func TestSyncPendingChild(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_PENDING_CHILD") != "isolated" {
		t.Skip("isolated pending child only")
	}
	j, err := openSyncJournal(os.Getenv("GATEWAY_TEST_PENDING_CHILD_JOURNAL"), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	target := syncColdTarget{address: os.Getenv("GATEWAY_TEST_PENDING_CHILD_ADDRESS"), timeout: 5 * time.Second}
	if _, err := target.applyPending(context.Background(), j); err != nil {
		t.Fatal(err)
	}
}

func kvCommitPendingExperiments(t *testing.T, addr string, c *kvCommitClient) {
	dir := os.Getenv("GATEWAY_TEST_SYNC_FIXTURE_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("explicit fixture directory required")
	}
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || len(b) > 4<<20 {
			t.Fatal("invalid fixture", err)
		}
		return b
	}
	one, two := read("phase1.log"), read("phase2.log")
	openFixture := func(t *testing.T, data []byte) (*syncJournal, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "capture.log")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		j, err := openSyncJournal(path, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { j.close() })
		return j, path
	}
	first, _ := openFixture(t, one)
	second, _ := openFixture(t, two)
	client := &syncColdClient{conn: c.c, reader: c.r, timeout: 5 * time.Second}
	keys, err := client.keys()
	if err != nil || len(keys) != 0 {
		t.Fatal("pending experiment requires new empty DB", err)
	}
	owned := []string{syncPendingCPKey, syncPendingKey, syncAnchorKey, "seed-counter", "seed-delete", "binary\x00key", "after-snapshot", "during-restart", "", "binary\xffkey", "unrelated"}
	for i := 0; i < 270; i++ {
		owned = append(owned, fmt.Sprintf("growth-%d", i))
	}
	reset := func() {
		raw, err := c.request(append([]string{"DEL"}, owned...)...)
		if err != nil || len(raw) == 0 || raw[0] != ':' {
			t.Fatal("fixture cleanup failed", err)
		}
	}
	target := syncColdTarget{address: addr, timeout: 5 * time.Second}
	apply := func(t *testing.T, j *syncJournal) {
		t.Helper()
		if p, err := target.applyPending(context.Background(), j); err != nil || p != j.durablePosition() {
			t.Fatal("pending apply", p, err)
		}
	}
	for _, phase := range []string{"bootstrap", "prepare", "apply", "control", "delete"} {
		for _, mode := range []string{"before", "reply", "late"} {
			t.Run(phase+"_"+mode, func(t *testing.T) {
				reset()
				relay, reached, release, done := pendingFaultRelay(t, addr, phase, mode)
				faulty := target
				faulty.address = relay
				faulty.timeout = 250 * time.Millisecond
				_, err := faulty.applyPending(context.Background(), first)
				if !errors.Is(err, errSyncPendingUncertain) {
					t.Fatal("expected injected uncertain EXEC", err)
				}
				select {
				case <-reached:
				case <-time.After(time.Second):
					t.Fatal("fault point not reached")
				}
				apply(t, first)
				kvCommitGet(t, c, "seed-counter", "42")
				if mode == "late" {
					release <- struct{}{}
					reply := <-done
					if reply != "*-1\r\n" && reply != "$-1\r\n" {
						t.Fatalf("stale EXEC was not aborted: %q", reply)
					}
				} else {
					<-done
				}
				apply(t, first)
				kvCommitGet(t, c, "seed-counter", "42")
				apply(t, second)
				kvCommitGet(t, c, "seed-counter", "43")
				t.Log("recovery and duplicate replay preserved counter; stale transaction cannot overwrite new checkpoint/intent")
			})
		}
	}
	t.Run("applier_sigkill_with_pending", func(t *testing.T) {
		reset()
		// The child must own its journal lock independently of the parent.
		path := filepath.Join(t.TempDir(), "child.log")
		if err := os.WriteFile(path, one, 0600); err != nil {
			t.Fatal(err)
		}
		relay, reached, release, done := pendingFaultRelay(t, addr, "apply", "late")
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, "-test.run", "^TestSyncPendingChild$", "-test.timeout=15s")
		cmd.Env = append(os.Environ(), "GATEWAY_TEST_PENDING_CHILD=isolated", "GATEWAY_TEST_PENDING_CHILD_JOURNAL="+path, "GATEWAY_TEST_PENDING_CHILD_ADDRESS="+relay)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cmd.Process.Kill() })
		select {
		case <-reached:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatal("child did not reach prepared intent")
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Wait(); err == nil {
			t.Fatal("child was not killed")
		}
		j, err := openSyncJournal(path, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer j.close()
		apply(t, j)
		release <- struct{}{}
		reply := <-done
		if reply != "*-1\r\n" && reply != "$-1\r\n" {
			t.Fatalf("late killed-client EXEC: %q", reply)
		}
		kvCommitGet(t, c, "seed-counter", "42")
		t.Log("applier process SIGKILL after intent persistence; reopened journal and recovered one INCR; no Kvrocks kill")
	})
	for _, kind := range []string{"data_ahead", "checkpoint_ahead"} {
		t.Run(kind, func(t *testing.T) {
			reset()
			relay, _, _, done := pendingFaultRelay(t, addr, "apply", "before")
			faulty := target
			faulty.address = relay
			if _, err := faulty.applyPending(context.Background(), first); !errors.Is(err, errSyncPendingUncertain) {
				t.Fatal(err)
			}
			<-done
			meta, err := client.pendingMeta()
			if err != nil || meta.pending == nil {
				t.Fatal(err)
			}
			if kind == "data_ahead" {
				c.expect(t, ":42\r\n", "INCR", "seed-counter")
			} else {
				c.expect(t, "+OK\r\n", "SET", syncPendingCPKey, meta.pending.Next.encode())
			}
			if _, err := target.applyPending(context.Background(), first); !errors.Is(err, errSyncTargetDrift) {
				t.Fatal("split state accepted", err)
			}
			want := "41"
			if kind == "data_ahead" {
				want = "42"
			}
			kvCommitGet(t, c, "seed-counter", want)
		})
	}
	for _, kind := range []string{"missing", "wrongtype", "ttl", "after_tamper"} {
		t.Run("metadata_"+kind, func(t *testing.T) {
			reset()
			apply(t, first)
			switch kind {
			case "missing":
				c.expect(t, ":1\r\n", "DEL", syncPendingKey)
			case "wrongtype":
				c.expect(t, ":1\r\n", "DEL", syncPendingKey)
				c.expect(t, ":1\r\n", "HSET", syncPendingKey, "x", "y")
			case "ttl":
				c.expect(t, ":1\r\n", "PEXPIRE", syncPendingKey, "60000")
			case "after_tamper":
				m, err := client.pendingMeta()
				if err != nil {
					t.Fatal(err)
				}
				m.pending.After = syncPendingValue{true, []byte("bad")}
				c.expect(t, "+OK\r\n", "SET", syncPendingKey, m.pending.encode())
			}
			if _, err := target.applyPending(context.Background(), second); !errors.Is(err, errSyncTargetDrift) {
				t.Fatal("invalid pending metadata accepted", err)
			}
			kvCommitGet(t, c, "seed-counter", "42")
		})
	}
	t.Run("binary_empty_control_and_delete", func(t *testing.T) {
		reset()
		j, _ := openFixture(t, two)
		for _, cmd := range [][]string{{"SET", "", ""}, {"DEL", ""}, {"DEL", ""}, {"SET", "binary\xffkey", "\xfe\xff\x00"}, {"PING"}} {
			appendSyncTestCommand(t, j, cmd...)
		}
		apply(t, j)
		apply(t, j)
		kvCommitGet(t, c, "binary\xffkey", "\xfe\xff\x00")
		c.expect(t, "$-1\r\n", "GET", "")
		m, err := client.pendingMeta()
		if err != nil || m.pending == nil {
			t.Fatal(err)
		}
		cmd, err := syncPendingCommand(m.pending.Record.Data)
		if err != nil || len(cmd) != 0 {
			t.Fatal("control intent missing", err)
		}
	})
	t.Run("resume_without_global_scan_or_model", func(t *testing.T) {
		reset()
		j, _ := openFixture(t, two)
		for i := 0; i < 270; i++ {
			appendSyncTestCommand(t, j, "SET", fmt.Sprintf("growth-%d", i), "v")
		}
		apply(t, j)
		// More live keys than the old full-state check permits. Unrelated drift
		// is outside this protocol's proof; it must not claim a full audit.
		c.expect(t, "+OK\r\n", "SET", "unrelated", "outside-proof")
		apply(t, j)
		kvCommitGet(t, c, "growth-269", "v")
		kvCommitGet(t, c, "unrelated", "outside-proof")
		t.Log("resumed with >256 keys; checked only last intent, not unrelated cold data")
	})
	reset()
}
