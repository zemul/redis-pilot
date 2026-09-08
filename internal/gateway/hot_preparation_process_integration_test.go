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

// Opt-in isolated Redis + Kvrocks + independently executed live syncer. Unlike
// the older RPC test, preparation and confirmation enter through Service HTTP
// and ordinary mutations through its RESP listener. No hot deletion occurs.
func TestHotPreparationServiceKvrocks(t *testing.T) {
	binary := os.Getenv("GATEWAY_TEST_CONFIRM_SYNCER_BINARY")
	if binary == "" {
		t.Skip("requires explicit syncer executable and isolated backends")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute syncer executable required")
	}
	f := newEvictionColdFixturePrepared(t, confirmationPrivateDir(t), true, false)
	id := syncTaskIdentity{f.target.taskID, os.Getenv("GATEWAY_TEST_EVICTION_SOURCE"), f.target.address}
	binding := confirmationTestBinding(id, f.intent)
	if err := f.j.close(); err != nil {
		t.Fatal(err)
	}
	f.op.close()
	tokenFile := secretFile(t, strings.Repeat("syncer-rpc-", 6))
	token, err := confirmationToken(tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	address := freeAddress(t)
	options := IsolatedConfirmationOptions{Binding: binding, JournalDir: filepath.Join(f.root, "segments"), OperationDir: confirmationPrivateDir(t),
		TokenFile: tokenFile, AllowUnauthenticated: true, SegmentBytes: 1 << 20, JournalBytes: 4 << 20, Timeout: 3 * time.Second}
	raw, err := yaml.Marshal(map[string]any{"mode": "isolated-confirmation-live", "listen": address, "confirm_exclusive_cold": true, "confirmation": options})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(confirmationPrivateDir(t), "syncer.yaml")
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + address
	httpClient := confirmationHTTPClient(time.Second)
	defer httpClient.CloseIdleConnections()
	var cmd *exec.Cmd
	var output bytes.Buffer
	var done chan error
	stopSyncer := func(kill bool) {
		if cmd == nil {
			return
		}
		if kill {
			cmd.Process.Kill()
		} else {
			cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case err := <-done:
			if !kill && err != nil {
				t.Error("syncer stop", err, output.String())
			}
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			<-done
			t.Error("syncer join timeout")
		}
		cmd = nil
	}
	t.Cleanup(func() { stopSyncer(true) })
	startSyncer := func() {
		output.Reset()
		cmd = exec.Command(binary, "-confirmation-config", configPath)
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done = make(chan error, 1)
		go func(c *exec.Cmd) { done <- c.Wait() }(cmd)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			var session confirmationSession
			if confirmationCall(context.Background(), httpClient, endpoint, token, "GET", nil, &session) == nil {
				return
			}
			select {
			case err := <-done:
				cmd = nil
				t.Fatal("syncer startup", err, output.String())
			default:
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("syncer startup timeout")
	}
	waitLive := func(predicate func(confirmationLiveStatus) bool) confirmationLiveStatus {
		deadline := time.Now().Add(6 * time.Second)
		var last confirmationLiveStatus
		for time.Now().Before(deadline) {
			r, _ := http.NewRequest("GET", endpoint+confirmationRPCPath+"/status", nil)
			r.Header.Set("Authorization", "Bearer "+token)
			resp, err := httpClient.Do(r)
			if err == nil {
				err = decodeConfirmation(resp.Body, &last)
				resp.Body.Close()
				if resp.StatusCode == 200 && err == nil && predicate(last) {
					return last
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("live state timeout: %+v", last)
		return last
	}
	startSyncer()
	waitLive(func(s confirmationLiveStatus) bool {
		return s.Barrier == "none" && s.SourceAttempts > 0 && s.AppliedValid
	})
	kvCommitGet(t, f.cold, "counter", "42")

	// Drop the first real successful cold-confirmation reply, after the syncer
	// has actually committed it. Gateway must report failure and retry online.
	var dropped atomic.Bool
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forward, err := http.NewRequestWithContext(r.Context(), r.Method, endpoint+r.URL.Path, r.Body)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		forward.Header = r.Header.Clone()
		resp, err := httpClient.Do(forward)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, confirmationRPCMaxBytes+1))
		if err != nil {
			w.WriteHeader(502)
			return
		}
		if r.Method == "POST" && resp.StatusCode == 200 && dropped.CompareAndSwap(false, true) {
			c, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				c.Close()
			}
			return
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
	}))
	defer relay.Close()
	dir := confirmationPrivateDir(t)
	adminTokenFile := secretFile(t, strings.Repeat("admin-token-", 6))
	adminToken, _ := confirmationToken(adminTokenFile)
	boot := admissionRunOptions{dir, binding, true, 16, 4096, 32}
	preparation := hotPreparationOptions{IsolatedConfirmationGatewayOptions{binding, relay.URL, tokenFile, dir, 3 * time.Second}, adminTokenFile, true}
	cfg := passConfig(id.Source)
	cfg.GatewayID = binding.GatewayID
	cfg.RequestTimeout = time.Second
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admissionBoot = &boot
	s.preparationOptions = &preparation
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx, cancel := context.WithCancel(context.Background())
	gatewayDone := make(chan error, 1)
	go func() { gatewayDone <- s.serve(ctx, data, admin) }()
	joined := false
	t.Cleanup(func() {
		cancel()
		if !joined {
			select {
			case <-gatewayDone:
			case <-time.After(5 * time.Second):
				t.Error("gateway leaked")
			}
		}
	})
	front, err := net.DialTimeout("tcp", data.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer front.Close()
	front.SetDeadline(time.Now().Add(30 * time.Second))
	replies, _ := NewReader(front, cfg.Limits)
	if err := writeAll(front, encodeCommand("INCR", "counter")); err != nil {
		t.Fatal(err)
	}
	expect(t, replies, ":43\r\n")
	request := hotPreparationRequest{f.intent.OperationID, []byte("counter")}
	post := func(path string, req hotPreparationRequest) int {
		body, _ := json.Marshal(req)
		r, _ := http.NewRequest("POST", "http://"+admin.Addr().String()+path, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+adminToken)
		client := confirmationHTTPClient(5 * time.Second)
		defer client.CloseIdleConnections()
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal("Gateway management request", err)
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	if code := post(hotPreparationPath, request); code != 200 {
		t.Fatal("prepare", code)
	}
	if code := post(hotPreparationPath, request); code != 200 {
		t.Fatal("prepare retry", code)
	}
	if err := writeAll(front, encodeCommand("INCR", "counter")); err != nil {
		t.Fatal(err)
	}
	expect(t, replies, "-TRYAGAIN hot key admission unavailable; command not sent\r\n")
	if err := writeAll(front, encodeCommand("SET", "after-barrier", "gateway-suffix")); err != nil {
		t.Fatal(err)
	}
	expect(t, replies, "+OK\r\n")
	waitLive(func(s confirmationLiveStatus) bool {
		return s.Barrier == "waiting_confirmation" && s.CapturedOffset > s.AppliedOffset
	})
	kvCommitGet(t, f.cold, "counter", "43")
	stopSyncer(true)
	startSyncer()
	waitLive(func(s confirmationLiveStatus) bool { return s.Barrier == "waiting_confirmation" })
	if code := post(hotPreparationPath+"/confirm", request); code != 503 || !dropped.Load() {
		t.Fatal("lost cold reply fabricated success", code)
	}
	state, err := s.preparation.confirmation.op.snapshot()
	if err != nil || state.Receipt != nil {
		t.Fatal("lost reply saved Gateway receipt", err)
	}
	if code := post(hotPreparationPath+"/confirm", request); code != 200 {
		t.Fatal("confirm retry", code)
	}
	first := waitLive(func(s confirmationLiveStatus) bool { return s.Barrier == "confirmed_held" })
	if err := writeAll(front, encodeCommand("SET", "after-barrier", "another-suffix")); err != nil {
		t.Fatal(err)
	}
	expect(t, replies, "+OK\r\n")
	waitLive(func(s confirmationLiveStatus) bool {
		return s.CapturedOffset > first.CapturedOffset && s.AppliedOffset == first.AppliedOffset
	})
	f.cold.expect(t, "$-1\r\n", "GET", "after-barrier")
	kvCommitGet(t, f.hot, "counter", "43")
	kvCommitGet(t, f.cold, "counter", "43")
	stopSyncer(true)
	if code := post(hotPreparationPath+"/confirm", request); code != 503 {
		t.Fatal("offline syncer bypassed by receipt", code)
	}
	startSyncer()
	waitLive(func(s confirmationLiveStatus) bool { return s.Barrier == "recorded_requires_revalidation" })
	if code := post(hotPreparationPath+"/confirm", request); code != 200 {
		t.Fatal("syncer restart revalidation", code)
	}
	conflict := request
	conflict.OperationID = strings.Repeat("b", 32)
	if code := post(hotPreparationPath, conflict); code != 503 {
		t.Fatal("conflicting preparation accepted", code)
	}
	front.Close()
	cancel()
	select {
	case err := <-gatewayDone:
		joined = true
		if err != nil {
			t.Fatal("held Gateway shutdown failed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Gateway stop timeout")
	}
	boot.Create = false
	restarted, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	restarted.admissionBoot = &boot
	restarted.preparationOptions = &preparation
	assertRecoveredService(t, restarted, "counter", "-TRYAGAIN hot key admission unavailable; command not sent\r\n", "after-barrier", "$14\r\nanother-suffix\r\n")
	stopSyncer(false)
	t.Log("real Service RESP/HTTP -> live syncer process -> Kvrocks: prepare/retry, lost cold reply, syncer recovery, held suffix, unchanged hot key and Gateway restart with only the operation key blocked passed")
}
