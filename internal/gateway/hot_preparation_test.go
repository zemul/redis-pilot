package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type hotPreparationFixture struct {
	h                           *hotPreparation
	o                           hotPreparationOptions
	mu                          sync.Mutex
	value, ttl, history, policy string
	dir                         string
	drop                        bool
	evals                       int
	increments                  int
}

func newHotPreparationFixture(t *testing.T) *hotPreparationFixture {
	t.Helper()
	id, intent, _ := evictionOperationFixtureData()
	dir := confirmationPrivateDir(t)
	f := &hotPreparationFixture{value: "value", ttl: ":-1\r\n", history: intent.History, policy: "noeviction", dir: dir}
	addr := fakeBackend(t, func(c net.Conn, args [][]byte) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		var reply []byte
		switch string(args[0]) {
		case "INFO":
			if string(args[1]) == "all" {
				infoReply(c, "master")
				return true
			}
			reply = syncTransportBulk("role:master\r\nmaster_replid:" + f.history + "\r\nmaster_repl_offset:1000\r\n")
		case "CONFIG":
			reply = append([]byte("*2\r\n"), syncTransportBulk("maxmemory-policy")...)
			reply = append(reply, syncTransportBulk(f.policy)...)
		case "GET":
			reply = syncTransportBulk(f.value)
		case "INCR":
			n, err := strconv.Atoi(f.value)
			if err != nil {
				t.Error("invalid counter fixture", err)
				return false
			}
			f.value = strconv.Itoa(n + 1)
			f.increments++
			return false // Execute once, then drop the backend reply.
		case "PTTL":
			reply = []byte(f.ttl)
		case "EVAL":
			f.evals++
			// Verify the actual on-disk intent exists BEFORE the backend is
			// allowed to observe the mutating preparation script.
			data, err := os.ReadFile(filepath.Join(f.dir, "confirmation-operation.log"))
			if err != nil || !bytes.HasPrefix(data, []byte(evictionOperationMagic)) {
				t.Error("missing durable intent", err)
				return false
			}
			var state evictionOperationState
			if readSyncSegmentFrame(bytes.NewReader(data[len(evictionOperationMagic):]), &state, evictionOperationFrameLimit) != nil ||
				len(args) != 7 || string(args[1]) != hotPreparationScript || string(args[4]) != state.Intent.prepareKey() ||
				string(args[6]) != "prepare:"+state.Intent.encode() || string(args[5]) != f.value {
				t.Error("script/intent mismatch")
				return false
			}
			if f.drop {
				f.drop = false
				return false
			}
			reply = []byte(":2\r\n")
		default:
			t.Errorf("unexpected backend command %q", args)
			return false
		}
		return writeAll(c, reply) == nil
	})
	id.Source = addr
	binding := confirmationTestBinding(id, intent)
	syncer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := confirmationSession{1, evictionHash(binding), strings.Repeat("c", 32)}
		if r.Method == "GET" {
			json.NewEncoder(w).Encode(session)
			return
		}
		var req confirmationRequest
		if decodeConfirmation(r.Body, &req) != nil {
			t.Error("bad confirmation request")
			w.WriteHeader(400)
			return
		}
		before := syncPosition{Source: addr, History: intent.History, Offset: 100}
		p := evictionPreparation{before, evictionTestRecords(before, [][]byte{req.Intent.prepareFrame()})[0]}
		prev := syncApplyCheckpoint{2, before, strings.Repeat("b", 64)}
		ack := evictionColdReceipt{1, evictionHash(id), evictionHash(req.Intent), prev, nextPendingCheckpoint(prev, p.Record)}
		json.NewEncoder(w).Encode(confirmationResponse{session, evictionHash(req), p, ack})
	}))
	t.Cleanup(syncer.Close)
	f.o = hotPreparationOptions{IsolatedConfirmationGatewayOptions{binding, syncer.URL, secretFile(t, strings.Repeat("s", 32)), dir, 250 * time.Millisecond}, secretFile(t, strings.Repeat("a", 32)), true}
	cfg := passConfig(addr)
	cfg.GatewayID = binding.GatewayID
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admission = testKeyBarrierSet(t, 8, 1024, 8)
	f.h, err = openHotPreparation(s, f.o)
	if err != nil {
		t.Fatal(err)
	}
	s.preparation = f.h
	t.Cleanup(f.h.Close)
	return f
}

func hotPreparationPost(t *testing.T, h *hotPreparation, path string, req hotPreparationRequest) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	r.Header.Set("Authorization", "Bearer "+h.token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestHotPreparationLostReplyRetryAndRecovery(t *testing.T) {
	f := newHotPreparationFixture(t)
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	f.mu.Lock()
	f.drop = true
	f.mu.Unlock()
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	before, err := os.ReadFile(filepath.Join(f.o.Confirmation.OperationDir, "confirmation-operation.log"))
	if err != nil {
		t.Fatal(err)
	}
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	after, _ := os.ReadFile(filepath.Join(f.o.Confirmation.OperationDir, "confirmation-operation.log"))
	if !bytes.Equal(before, after) {
		t.Fatal("retry created or rewrote intent")
	}
	if w := hotPreparationPost(t, f.h, hotPreparationPath+"/confirm", req); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	state, err := f.h.confirmation.op.snapshot()
	if err != nil || state.Receipt == nil {
		t.Fatal("missing receipt", err)
	}
	_, err = f.h.service.admission.enter("key")
	requireBarrierError(t, err, errKeyBarrierBusy)
	c, r, _ := startSession(t, f.h.service)
	writeAll(c, encodeCommand("GET", "key"))
	expect(t, r, "-TRYAGAIN hot key admission unavailable; command not sent\r\n")
	writeAll(c, encodeCommand("MSET", "other", "v"))
	expect(t, r, "-ERR command outside hot preparation replay subset\r\n")
	f.h.Close()
	// Controller recovery restores only the durable operation's key barrier.
	s, err := NewService(f.h.service.cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admission = testKeyBarrierSet(t, 8, 1024, 8)
	restored, err := openHotPreparation(s, f.o)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.prepared {
		t.Fatal("local receipt restored online readiness")
	}
	_, err = s.admission.enter("key")
	requireBarrierError(t, err, errKeyBarrierBusy)
	if w := hotPreparationPost(t, restored, hotPreparationPath+"/confirm", req); w.Code != 200 {
		t.Fatal(w.Code)
	}
}

func TestHotPreparationAfterOrdinaryLostReply(t *testing.T) {
	for _, key := range []string{"counter", "other"} {
		t.Run(key, func(t *testing.T) {
			f := newHotPreparationFixture(t)
			f.mu.Lock()
			f.value = "41"
			f.mu.Unlock()
			c, r, _ := startSession(t, f.h.service)
			if err := writeAll(c, encodeCommand("INCR", "counter")); err != nil {
				t.Fatal(err)
			}
			if _, err := r.ReadFrame(); err == nil {
				t.Fatal("fabricated reply")
			}
			// Both the same key and a different key can start a NEW preparation.
			// This exercises durable intent + backend checks, not just reserve().
			req := hotPreparationRequest{strings.Repeat("1", 32), []byte(key)}
			if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
				t.Fatal("ordinary reply loss blocked new preparation", w.Code, w.Body.String())
			}
			if w := hotPreparationPost(t, f.h, hotPreparationPath+"/confirm", req); w.Code != 200 {
				t.Fatal("confirmation failed", w.Code, w.Body.String())
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.increments != 1 || f.value != "42" || f.evals != 1 {
				t.Fatal("replayed business write or skipped preparation", f.increments, f.value, f.evals)
			}
		})
	}
}

func TestHotPreparationPreconditionsAndConflict(t *testing.T) {
	for _, kind := range []string{"ttl", "history", "eviction", "changed_retry", "conflict"} {
		t.Run(kind, func(t *testing.T) {
			f := newHotPreparationFixture(t)
			req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
			if kind == "changed_retry" || kind == "conflict" {
				if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
					t.Fatal(w.Code)
				}
			}
			f.mu.Lock()
			switch kind {
			case "ttl":
				f.ttl = ":100\r\n"
			case "history":
				f.history = strings.Repeat("e", 40)
			case "eviction":
				f.policy = "allkeys-lru"
			case "changed_retry":
				f.value = "changed"
			case "conflict":
				req.OperationID = strings.Repeat("2", 32)
			}
			calls := f.evals
			f.mu.Unlock()
			if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 503 {
				t.Fatal(w.Code)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.evals != calls {
				t.Fatal("rejected operation dispatched script")
			}
		})
	}
}

func TestHotPreparationDrainsAndRetainsUnknown(t *testing.T) {
	f := newHotPreparationFixture(t)
	r := testKeyRequest(t, f.h.service.admission, "key")
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if f.h.confirmation != nil {
		t.Fatal("did not wait for in-flight request")
	}
	r.settle(false)
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 503 {
		t.Fatal(w.Code)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.evals != 0 {
		t.Fatal("unknown hot outcome allowed preparation")
	}
}

func TestHotPreparationAuthenticationAndClose(t *testing.T) {
	f := newHotPreparationFixture(t)
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	body, _ := json.Marshal(req)
	r := httptest.NewRequest("POST", hotPreparationPath, bytes.NewReader(body))
	w := httptest.NewRecorder()
	f.h.ServeHTTP(w, r)
	if w.Code != 401 || f.h.request != nil {
		t.Fatal("unauthorized mutation")
	}
	r = httptest.NewRequest("POST", hotPreparationPath, strings.NewReader(`{"operation_id":"bad","operation_id":"bad","key":"aw=="}`))
	r.Header.Set("Authorization", "Bearer "+f.h.token)
	w = httptest.NewRecorder()
	f.h.ServeHTTP(w, r)
	if w.Code != 400 || f.h.request != nil {
		t.Fatal("noncanonical mutation")
	}
	f.h.gate <- struct{}{}
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 409 {
		t.Fatal("unbounded queue", w.Code)
	}
	<-f.h.gate
	f.h.Close()
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if metricValue(f.h.service.bufferReserved) != 0 {
		t.Fatal("controller workspace leaked")
	}
}

func TestHotPreparationShutdownCancelsDrain(t *testing.T) {
	f := newHotPreparationFixture(t)
	testKeyRequest(t, f.h.service.admission, "key")
	done := make(chan int, 1)
	go func() {
		done <- hotPreparationPost(t, f.h, hotPreparationPath, hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}).Code
	}()
	deadline := time.After(time.Second)
	for {
		g := f.h.service.admission
		g.mu.Lock()
		reserved := g.entries["key"].barrier != nil
		g.mu.Unlock()
		if reserved {
			break
		}
		select {
		case <-deadline:
			t.Fatal("request never reserved key")
		case <-time.After(time.Millisecond):
		}
	}
	f.h.Close()
	select {
	case code := <-done:
		if code != 503 {
			t.Fatal(code)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel/join request")
	}
}

func TestHotPreparationCommandSubset(t *testing.T) {
	for _, args := range [][]string{{"SET", "k", "v"}, {"INCR", "k"}, {"DEL", "k"}, {"GET", "k"}, {"PING"}} {
		if !hotPreparationCommand(admissionArgs(args...)) {
			t.Fatal(args)
		}
	}
	for _, args := range [][]string{{"SET", "k", "v", "EX", "1"}, {"DEL", "a", "b"}, {"MGET", "a", "b"}, {"EXPIRE", "k", "1"}, {"EVAL", "x", "0"}, {"DECR", "k"}} {
		if hotPreparationCommand(admissionArgs(args...)) {
			t.Fatal(args)
		}
	}
	if _, err := hotPreparationValue([]byte("$-1\r\n")); err == nil {
		t.Fatal("missing value accepted")
	}
	if _, err := hotPreparationValue(syncTransportBulk(strings.Repeat("v", syncPendingValueLimit+1))); err == nil {
		t.Fatal("oversize value accepted")
	}
}

func TestHotPreparationIntentFailureRemainsProtected(t *testing.T) {
	f := newHotPreparationFixture(t)
	path := filepath.Join(f.o.Confirmation.OperationDir, "confirmation-operation.log")
	if err := os.WriteFile(path, []byte("unexpected-existing-state"), 0600); err != nil {
		t.Fatal(err)
	}
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	for i := 0; i < 2; i++ {
		if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 503 {
			t.Fatal(w.Code)
		}
	}
	if !f.h.poisoned {
		t.Fatal("failed intent creation not poisoned")
	}
	_, err := f.h.service.admission.enter("key")
	requireBarrierError(t, err, errKeyBarrierBusy)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.evals != 0 {
		t.Fatal("script dispatched without durable intent")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "unexpected-existing-state" {
		t.Fatal("existing state overwritten", err)
	}
}

func TestHotPreparationRejectsUnsafeConfiguration(t *testing.T) {
	f := newHotPreparationFixture(t)
	f.h.Close()
	for _, kind := range []string{"shared_token", "not_exclusive", "wrong_gateway", "wrong_source"} {
		o := f.o
		s, err := NewService(f.h.service.cfg)
		if err != nil {
			t.Fatal(err)
		}
		s.admission = testKeyBarrierSet(t, 8, 1024, 8)
		switch kind {
		case "shared_token":
			o.AdminTokenFile = o.Confirmation.TokenFile
		case "not_exclusive":
			o.ExclusiveHot = false
		case "wrong_gateway":
			o.Confirmation.Binding.GatewayID = "other-gateway"
		case "wrong_source":
			o.Confirmation.Binding.Source = "127.0.0.1:1"
		}
		h, err := openHotPreparation(s, o)
		if err == nil {
			h.Close()
			t.Fatal("unsafe config", kind)
		}
		if metricValue(s.bufferReserved) != 0 {
			t.Fatal("failed config leaked memory budget")
		}
	}
}

func TestHotPreparationServiceLifecycle(t *testing.T) {
	f := newHotPreparationFixture(t)
	f.h.Close()
	dir := confirmationPrivateDir(t)
	f.mu.Lock()
	f.dir = dir
	f.mu.Unlock()
	o := f.o
	o.Confirmation.OperationDir = dir
	boot := admissionRunOptions{dir, o.Confirmation.Binding, true, 8, 1024, 8}
	// Simulate an unclean previous boot with no operation. The real Service
	// must still execute a fresh preparation after resuming the dirty record.
	previous, err := openAdmissionRun(boot, nil)
	if err != nil {
		t.Fatal(err)
	}
	previous.close()
	boot.Create = false
	s, err := NewService(f.h.service.cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admissionBoot = &boot
	s.preparationOptions = &o
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
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, data, admin) }()
	joined := false
	t.Cleanup(func() {
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("service leaked")
			}
		}
	})
	client := &http.Client{Timeout: 2 * time.Second}
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	body, _ := json.Marshal(req)
	r, err := http.NewRequest("POST", "http://"+admin.Addr().String()+hotPreparationPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	resp, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal("Service preparation failed", resp.StatusCode)
	}
	c, err := net.DialTimeout("tcp", data.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if err := writeAll(c, encodeCommand("GET", "key")); err != nil {
		t.Fatal(err)
	}
	reader, _ := NewReader(c, s.cfg.Limits)
	expect(t, reader, "-TRYAGAIN hot key admission unavailable; command not sent\r\n")
	c.Close()
	cancel()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal("held operation must not make shutdown fail", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("service shutdown stuck")
	}
	if metricValue(s.bufferReserved) != 0 {
		t.Fatal("Service workspace leaked")
	}
	boot.Create = false
	restarted, err := NewService(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	restarted.admissionBoot = &boot
	restarted.preparationOptions = &o
	assertRecoveredService(t, restarted, "key", "-TRYAGAIN hot key admission unavailable; command not sent\r\n", "ordinary", "$5\r\nvalue\r\n")
	if restarted.preparation.prepared {
		t.Fatal("recovery fabricated online verification")
	}
}
