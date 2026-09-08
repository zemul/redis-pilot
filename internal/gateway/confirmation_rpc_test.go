package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func confirmationTestBinding(id syncTaskIdentity, i evictionIntent) ConfirmationBinding {
	return ConfirmationBinding{SyncerID: "syncer-probe", GatewayID: "gateway-probe", TaskID: id.TaskID, Source: id.Source, Cold: id.Cold, History: i.History, Epoch: i.Epoch}
}

func confirmationPrivateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestConfirmationGatewayProtocol(t *testing.T) {
	for _, scenario := range []string{"success", "lost_reply", "bad_request_hash", "bad_boot", "bad_binding", "bad_prepare", "bad_receipt", "restart_during_reply", "redirect", "oversize", "duplicate_field", "unauthorized", "local_fsync"} {
		t.Run(scenario, func(t *testing.T) {
			id, intent, p := evictionOperationFixtureData()
			binding := confirmationTestBinding(id, intent)
			session := confirmationSession{1, evictionHash(binding), strings.Repeat("a", 32)}
			prev := syncApplyCheckpoint{2, p.Before, strings.Repeat("b", 64)}
			ack := evictionColdReceipt{1, evictionHash(id), evictionHash(intent), prev, nextPendingCheckpoint(prev, p.Record)}
			var gets, posts atomic.Int32
			token := strings.Repeat("secret", 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !confirmationAuthorized(r, token) {
					t.Error("client omitted authentication")
					w.WriteHeader(401)
					return
				}
				if r.Method == "GET" {
					n := gets.Add(1)
					s := session
					if scenario == "restart_during_reply" && n > 1 {
						s.BootID = strings.Repeat("c", 32)
					}
					json.NewEncoder(w).Encode(s)
					return
				}
				posts.Add(1)
				var req confirmationRequest
				if decodeConfirmation(r.Body, &req) != nil {
					t.Error("invalid request")
					w.WriteHeader(400)
					return
				}
				reply := confirmationResponse{session, evictionHash(req), p, ack}
				switch scenario {
				case "bad_request_hash":
					reply.RequestSHA256 = strings.Repeat("a", 64)
				case "bad_boot":
					reply.Session.BootID = strings.Repeat("c", 32)
				case "bad_binding":
					reply.Session.BindingSHA256 = strings.Repeat("c", 64)
				case "bad_prepare":
					reply.Prepare.Before.Offset++
				case "bad_receipt":
					reply.Receipt.Prepared.Chain = strings.Repeat("c", 64)
				case "lost_reply":
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
					return
				case "redirect":
					w.Header().Set("Location", "/not-followed")
					w.WriteHeader(307)
					return
				case "unauthorized":
					w.WriteHeader(401)
					return
				case "oversize":
					w.Write([]byte(strings.Repeat("x", confirmationRPCMaxBytes+1)))
					return
				case "duplicate_field":
					raw, _ := json.Marshal(reply)
					w.Write(append([]byte(`{"session":{},`), raw[1:]...))
					return
				}
				json.NewEncoder(w).Encode(reply)
			}))
			defer server.Close()
			o := IsolatedConfirmationGatewayOptions{binding, server.URL, secretFile(t, token), confirmationPrivateDir(t), time.Second}
			g, err := openConfirmationGateway(o, &intent, nil)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "local_fsync" {
				g.op.fault = func(stage string) error {
					if stage == "receipt_after_sync" {
						return errors.New("fault")
					}
					return nil
				}
			}
			err = g.Confirm(context.Background())
			if (err == nil) != (scenario == "success") {
				t.Fatal("result", err)
			}
			if _, err := g.barriers.enter(string(intent.Key)); err == nil {
				t.Fatal("key reopened")
			}
			if posts.Load() != 1 {
				t.Fatal("automatic retry or redirect", posts.Load())
			}
			s, snapErr := g.op.snapshot()
			if scenario != "local_fsync" && (snapErr != nil || (s.Receipt != nil) != (scenario == "success")) {
				t.Fatal("unexpected local receipt", snapErr)
			}
			g.Close()
			// Every restart restores the key gate, even with a durable receipt.
			g, err = OpenIsolatedConfirmationGateway(o)
			if err != nil {
				t.Fatal("reopen", err)
			}
			defer g.Close()
			if _, err := g.barriers.enter(string(intent.Key)); err == nil {
				t.Fatal("recovery reopened key")
			}
			if scenario == "success" {
				if err := g.Confirm(context.Background()); err != nil {
					t.Fatal("online revalidation", err)
				}
				if posts.Load() != 2 {
					t.Fatal("local receipt bypassed server")
				}
			}
		})
	}
}

func TestConfirmationGatewayDrainsAndQuarantines(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		id, i, _ := evictionOperationFixtureData()
		var calls atomic.Int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) }))
		o := IsolatedConfirmationGatewayOptions{confirmationTestBinding(id, i), s.URL, secretFile(t, strings.Repeat("s", 32)), confirmationPrivateDir(t), 50 * time.Millisecond}
		barriers, _ := newKeyBarrierSet(2, 1<<20, 2)
		active, _ := barriers.enter(string(i.Key))
		g, err := openConfirmationGateway(o, &i, barriers)
		if err != nil {
			t.Fatal(err)
		}
		if g.Confirm(context.Background()) == nil || calls.Load() != 0 {
			t.Fatal("did not wait for drain")
		}
		active.settle(!uncertain)
		if g.Confirm(context.Background()) == nil {
			t.Fatal("unavailable server allowed")
		}
		if uncertain && calls.Load() != 0 {
			t.Fatal("uncertain hot command allowed dispatch")
		}
		g.Close()
		s.Close()
	}
}

func TestConfirmationBindingPersistence(t *testing.T) {
	id, i, _ := evictionOperationFixtureData()
	b := confirmationTestBinding(id, i)
	dir := confirmationPrivateDir(t)
	f, err := openConfirmationBinding(dir, b)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := openConfirmationBinding(dir, b); err == nil {
		other.Close()
		t.Fatal("double owner")
	}
	f.Close()
	for _, mutate := range []func(*ConfirmationBinding){func(b *ConfirmationBinding) { b.Epoch++ }, func(b *ConfirmationBinding) { b.SyncerID = "other" }, func(b *ConfirmationBinding) { b.History = strings.Repeat("c", 40) }, func(b *ConfirmationBinding) { b.GatewayID = "other" }} {
		other := b
		mutate(&other)
		if f, err := openConfirmationBinding(dir, other); err == nil {
			f.Close()
			t.Fatal("binding changed without reconciliation")
		}
	}
	f, err = openConfirmationBinding(dir, b)
	if err != nil {
		t.Fatal("same binding recovery", err)
	}
	f.Close()
	if err := os.Chmod(filepath.Join(dir, "confirmation-binding.json"), 0644); err != nil {
		t.Fatal(err)
	}
	if f, err := openConfirmationBinding(dir, b); err == nil {
		f.Close()
		t.Fatal("public binding accepted")
	}
}

func TestConfirmationMissingBindingCannotAdoptOperation(t *testing.T) {
	id, i, _ := evictionOperationFixtureData()
	dir := confirmationPrivateDir(t)
	op, err := openEvictionOperation(filepath.Join(dir, "confirmation-operation.log"), id, &i, nil)
	if err != nil {
		t.Fatal(err)
	}
	op.close()
	if binding, err := openConfirmationBinding(dir, confirmationTestBinding(id, i)); err == nil {
		binding.Close()
		t.Fatal("existing operation adopted without binding")
	}
}

func TestConfirmationGatewayRejectsUnsafeEndpoint(t *testing.T) {
	id, i, _ := evictionOperationFixtureData()
	for _, endpoint := range []string{"http://localhost:19301", "http://192.0.2.1:19301", "http://127.0.0.1:19301/path", "http://127.0.0.1:19301?x=y", "http://user@127.0.0.1:19301", "http://" + id.Source, "http://" + id.Cold} {
		o := IsolatedConfirmationGatewayOptions{confirmationTestBinding(id, i), endpoint, secretFile(t, strings.Repeat("s", 32)), confirmationPrivateDir(t), time.Second}
		if g, err := openConfirmationGateway(o, &i, nil); err == nil {
			g.Close()
			t.Fatal("unsafe endpoint accepted")
		}
	}
}

func TestConfirmationHTTPAdmission(t *testing.T) {
	id, i, _ := evictionOperationFixtureData()
	b := confirmationTestBinding(id, i)
	s := &IsolatedConfirmationServer{options: IsolatedConfirmationOptions{Binding: b}, session: confirmationSession{1, evictionHash(b), strings.Repeat("a", 32)}, token: strings.Repeat("s", 32), gate: make(chan struct{}, 1)}
	for _, test := range []struct {
		method, token, body string
		want                int
	}{
		{"GET", "", "", 401}, {"PUT", s.token, "", 405}, {"POST", s.token, `{}`, 409},
		{"POST", s.token, strings.Repeat("x", confirmationRPCMaxBytes+1), 400},
		{"POST", s.token, `{"session":{},"session":{},"intent":{}}`, 400},
	} {
		r := httptest.NewRequest(test.method, confirmationRPCPath, strings.NewReader(test.body))
		r.Header.Set("Authorization", "Bearer "+test.token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		// Non-canonical empty request is rejected as malformed, before binding checks.
		want := test.want
		if test.body == `{}` {
			want = 400
		}
		if w.Code != want {
			t.Fatalf("%s: %d != %d", test.method, w.Code, want)
		}
	}
	req := confirmationRequest{Session: s.session, Intent: i}
	req.Session.BootID = strings.Repeat("b", 32)
	data, _ := json.Marshal(req)
	r := httptest.NewRequest("POST", confirmationRPCPath, bytes.NewReader(data))
	r.Header.Set("Authorization", "Bearer "+s.token)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 409 {
		t.Fatal("old boot accepted", w.Code)
	}
	s.gate <- struct{}{}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	<-s.gate
	if w.Code != 503 {
		t.Fatal("unbounded handler queue")
	}
}
