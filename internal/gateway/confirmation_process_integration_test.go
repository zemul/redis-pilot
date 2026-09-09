package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestConfirmationGatewayProcess(t *testing.T) {
	raw := os.Getenv("GATEWAY_CONFIRMATION_CHILD_OPTIONS")
	if raw == "" {
		t.Skip("isolated Gateway client child only")
	}
	var options IsolatedConfirmationGatewayOptions
	if json.Unmarshal([]byte(raw), &options) != nil {
		t.Fatal("invalid child options")
	}
	g, err := OpenIsolatedConfirmationGateway(options)
	if err != nil {
		t.Fatal("child recovery failed")
	}
	defer g.Close()
	if err := g.Confirm(context.Background()); err != nil {
		t.Fatal("child confirmation failed")
	}
}

// Real source capture, a separately executed redis-syncer binary and an HTTP
// Gateway adapter with its OWN files. The parent never opens the source log
// while the child owns it. Requires new empty, isolated backends.
func TestConfirmationProcessKvrocks(t *testing.T) {
	testConfirmationProcessKvrocks(t, false)
}

func TestLiveConfirmationProcessKvrocks(t *testing.T) {
	testConfirmationProcessKvrocks(t, true)
}

func testConfirmationProcessKvrocks(t *testing.T, live bool) {
	binary := os.Getenv("GATEWAY_TEST_CONFIRM_SYNCER_BINARY")
	if binary == "" {
		t.Skip("requires explicit redis-syncer executable and isolated backends")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute syncer binary required")
	}
	f := newEvictionColdFixturePrepared(t, confirmationPrivateDir(t), true, !live)
	id := syncTaskIdentity{TaskID: f.target.taskID, Source: os.Getenv("GATEWAY_TEST_EVICTION_SOURCE"), Cold: f.target.address}
	binding := confirmationTestBinding(id, f.intent)
	if err := f.j.close(); err != nil {
		t.Fatal(err)
	}
	// This fixture's original op is not shared with either transport endpoint.
	f.op.close()
	token := secretFile(t, strings.Repeat("rpc-test-", 8))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	o := IsolatedConfirmationOptions{Binding: binding, JournalDir: filepath.Join(f.root, "segments"), OperationDir: confirmationPrivateDir(t), TokenFile: token,
		AllowUnauthenticated: true, SegmentBytes: 1 << 20, JournalBytes: 4 << 20, Timeout: 3 * time.Second}
	config := map[string]any{"mode": "isolated-confirmation", "listen": address, "confirm_exclusive_cold": true, "confirmation": o}
	if live {
		config["mode"] = "isolated-confirmation-live"
	}
	raw, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(confirmationPrivateDir(t), "confirmation.yaml")
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + address
	client := confirmationHTTPClient(time.Second)
	defer client.CloseIdleConnections()
	secret, _ := confirmationToken(token)
	var process *exec.Cmd
	var output bytes.Buffer
	var done chan error
	stop := func(kill bool) {
		if process == nil {
			return
		}
		if kill {
			process.Process.Kill()
		} else {
			process.Process.Signal(syscall.SIGTERM)
		}
		select {
		case err := <-done:
			if !kill && err != nil {
				t.Error("syncer stop", err, output.String())
			}
		case <-time.After(12 * time.Second):
			process.Process.Kill()
			<-done
			t.Error("syncer shutdown timeout")
		}
		process = nil
	}
	t.Cleanup(func() { stop(true) })
	start := func() confirmationSession {
		output.Reset()
		process = exec.Command(binary, "-confirmation-config", configPath)
		process.Stdout = &output
		process.Stderr = &output
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		done = make(chan error, 1)
		go func(cmd *exec.Cmd) { done <- cmd.Wait() }(process)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			var session confirmationSession
			if confirmationCall(context.Background(), client, endpoint, secret, "GET", nil, &session) == nil {
				return session
			}
			select {
			case err := <-done:
				process = nil
				t.Fatal("syncer failed to start", err, output.String())
			default:
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("syncer startup timeout")
		return confirmationSession{}
	}
	session := start()
	liveStatus := func() (confirmationLiveStatus, error) {
		var result confirmationLiveStatus
		req, _ := http.NewRequest("GET", endpoint+confirmationRPCPath+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+secret)
		resp, err := client.Do(req)
		if err != nil {
			return result, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return result, errConfirmationRPC
		}
		err = decodeConfirmation(resp.Body, &result)
		return result, err
	}
	waitLive := func(predicate func(confirmationLiveStatus) bool) confirmationLiveStatus {
		deadline := time.Now().Add(6 * time.Second)
		var last confirmationLiveStatus
		for time.Now().Before(deadline) {
			status, err := liveStatus()
			if err == nil {
				last = status
				if predicate(status) {
					return status
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("live state timeout: %+v", last)
		return last
	}
	var stopWriter func()
	if live {
		waitLive(func(s confirmationLiveStatus) bool { return s.Barrier == "none" && s.SourceAttempts > 0 })
		kvCommitGet(t, f.cold, "counter", "42")
		f.hot.expect(t, ":1\r\n", "EVAL", evictionPrepareProbeScript, "2", "counter", f.intent.prepareKey(), "42", "prepare:"+f.intent.encode())
		writer := kvCommitConnect(t, id.Source)
		writeCtx, cancel := context.WithCancel(context.Background())
		writerDone := make(chan error, 1)
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-writeCtx.Done():
					writerDone <- nil
					return
				case <-ticker.C:
					if _, err := writer.request("SET", "after-barrier", "continuous-suffix"); err != nil {
						writerDone <- err
						return
					}
				}
			}
		}()
		stopped := false
		stopWriter = func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			select {
			case err := <-writerDone:
				if err != nil {
					t.Error("suffix writer", err)
				}
			case <-time.After(3 * time.Second):
				writer.c.Close()
				<-writerDone
				t.Error("writer join timeout")
			}
		}
		t.Cleanup(stopWriter)
		waitLive(func(s confirmationLiveStatus) bool {
			return s.Barrier == "waiting_confirmation" && s.CapturedOffset > s.AppliedOffset
		})
		// Restart BEFORE receiving any confirmation. The marker and local intent
		// must recover while PSYNC continues capturing the concurrent suffix.
		stop(true)
		session = start()
		waitLive(func(s confirmationLiveStatus) bool { return s.Barrier == "waiting_confirmation" })
	}
	// The runtime holds the real capture lock; it cannot race ordinary replay.
	if j, err := openSyncSegmentsForTask(o.JournalDir, o.SegmentBytes, o.JournalBytes, defaultSyncResourcePolicy(), &id); err == nil {
		j.close()
		t.Fatal("journal double ownership")
	}
	goOptions := IsolatedConfirmationGatewayOptions{binding, endpoint, token, confirmationPrivateDir(t), 5 * time.Second}
	g, err := openConfirmationGateway(goOptions, &f.intent, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	// Forward one real cold commit, then destroy only its HTTP reply. The
	// syncer must recover its own receipt on the client's explicit retry.
	var dropped atomic.Bool
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forward, err := http.NewRequestWithContext(r.Context(), r.Method, endpoint+confirmationRPCPath, r.Body)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		forward.Header = r.Header.Clone()
		response, err := client.Do(forward)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, confirmationRPCMaxBytes+1))
		if err != nil {
			w.WriteHeader(502)
			return
		}
		if r.Method == "POST" && response.StatusCode == 200 && dropped.CompareAndSwap(false, true) {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		w.WriteHeader(response.StatusCode)
		w.Write(body)
	}))
	g.options.Endpoint = relay.URL
	if err := g.Confirm(context.Background()); err == nil || !dropped.Load() {
		t.Fatal("lost real reply returned success")
	}
	state, err := g.op.snapshot()
	if err != nil || state.Receipt != nil {
		t.Fatal("lost reply created Gateway receipt")
	}
	g.options.Endpoint = endpoint
	relay.Close()
	for n := 0; n < 2; n++ {
		if err := g.Confirm(context.Background()); err != nil {
			t.Fatal("real RPC confirmation", n, err)
		}
	}
	if live {
		first := waitLive(func(s confirmationLiveStatus) bool { return s.Barrier == "confirmed_held" })
		waitLive(func(s confirmationLiveStatus) bool {
			return s.CapturedOffset > first.CapturedOffset && s.AppliedOffset == first.AppliedOffset
		})
		stopWriter()
	}
	s, err := g.op.snapshot()
	if err != nil || s.Receipt == nil {
		t.Fatal("Gateway receipt missing", err)
	}
	kvCommitGet(t, f.cold, "counter", "42")
	kvCommitGet(t, f.hot, "counter", "42")
	f.cold.expect(t, "$-1\r\n", "GET", "after-barrier")
	if _, err := g.barriers.enter("counter"); err == nil {
		t.Fatal("success unblocked key")
	}
	// Actual Gateway adapter processes reopen their own file, never the source
	// journal. Starting twice also verifies recovery after their process exits.
	g.Close()
	encodedOptions, _ := json.Marshal(goOptions)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 2; n++ {
		childCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		child := exec.CommandContext(childCtx, self, "-test.run=^TestConfirmationGatewayProcess$", "-test.count=1")
		child.Env = append(os.Environ(), "GATEWAY_CONFIRMATION_CHILD_OPTIONS="+string(encodedOptions))
		out, err := child.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatal("Gateway child", err, string(out))
		}
	}
	g, err = OpenIsolatedConfirmationGateway(goOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Confirm(context.Background()); err != nil {
		t.Fatal("Gateway recovery", err)
	}
	stop(true)
	if err := g.Confirm(context.Background()); err == nil {
		t.Fatal("offline syncer accepted local receipt")
	}
	newSession := start()
	if live {
		waitLive(func(s confirmationLiveStatus) bool { return s.Barrier == "recorded_requires_revalidation" })
	}
	if newSession.BootID == session.BootID {
		t.Fatal("boot not renewed")
	}
	old := confirmationRequest{session, f.intent, false}
	data, _ := json.Marshal(old)
	req, _ := http.NewRequest("POST", endpoint+confirmationRPCPath, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 409 {
		t.Fatal("pre-restart request accepted", resp.StatusCode)
	}
	if err := g.Confirm(context.Background()); err != nil {
		t.Fatal("syncer restart revalidation", err)
	}
	// Conflicting intent cannot overwrite the one-operation recovery slot.
	conflict := f.intent
	conflict.OperationID = strings.Repeat("b", 32)
	var reply confirmationResponse
	if confirmationCall(context.Background(), client, endpoint, secret, "POST", confirmationRequest{newSession, conflict, false}, &reply) == nil {
		t.Fatal("conflicting intent accepted")
	}
	if live {
		// Fill only the held suffix, not the filesystem. The live budget is
		// bounded below the journal file cap, so capture must pause, not prune.
		payload := strings.Repeat("x", 8192)
		for n := 0; n < 100; n++ {
			f.hot.expect(t, "+OK\r\n", "SET", "after-barrier", payload)
		}
		paused := waitLive(func(s confirmationLiveStatus) bool { return s.CapturePaused })
		time.Sleep(150 * time.Millisecond)
		stable := waitLive(func(s confirmationLiveStatus) bool { return s.CapturePaused })
		if paused.CapturedOffset != stable.CapturedOffset || paused.AppliedOffset != stable.AppliedOffset || paused.SourceAttempts != stable.SourceAttempts {
			t.Fatal("pressure did not stop capture/retries")
		}
		f.cold.expect(t, "$-1\r\n", "GET", "after-barrier")
		kvCommitGet(t, f.hot, "counter", "42")
		stop(false)
		start()
		waitLive(func(s confirmationLiveStatus) bool {
			return s.Barrier == "recorded_requires_revalidation" && s.CapturePaused
		})
		stop(false)
		t.Log("live mode: concurrent capture, first-marker hold, unconfirmed/confirmed process recovery, lost reply, source-log retention, bounded pressure and graceful held stop passed")
		return
	}
	// Source lineage changes: old receipt must not become a cached success.
	f.hot.expect(t, "+OK\r\n", "REPLICAOF", "127.0.0.1", "1")
	if err := g.Confirm(context.Background()); err == nil {
		t.Fatal("source role change accepted")
	}
	f.hot.expect(t, "+OK\r\n", "REPLICAOF", "NO", "ONE")
	if err := g.Confirm(context.Background()); err == nil {
		t.Fatal("new source history accepted")
	}
	stop(false)
	t.Log("real syncer process: duplicate, Gateway recovery, syncer kill/restart, old boot, conflict and source history rejection passed; no hot deletion")
}
