package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"golang.org/x/sys/unix"
)

const reportingTestToken = "syncer-observation-test-token"

func reportingConfig(t *testing.T, endpoint string) Config {
	t.Helper()
	c := validMultiConfig(t, 2)
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(reportingTestToken), 0600); err != nil {
		t.Fatal(err)
	}
	c.ControlPlane = ControlPlaneConfig{ServerEndpoint: endpoint, TokenFile: file, SyncerID: "worker-1", NodeID: "node-1", HeartbeatInterval: time.Second, RequestTimeout: 200 * time.Millisecond}
	return c
}

func reportingService(t *testing.T, c Config) (*Service, []*fakeTask) {
	t.Helper()
	fakes := make([]*fakeTask, len(c.Tasks))
	tasks := make([]isolatedTask, len(c.Tasks))
	for i := range fakes {
		fakes[i] = newFakeTask()
		fakes[i].taskStatus = gateway.IsolatedSyncStatus{State: "running", Running: true}
		tasks[i] = fakes[i]
	}
	s, err := newMultiService(c, tasks, net.Listen)
	if err != nil {
		t.Fatal(err)
	}
	return s, fakes
}

func receiveReport(t *testing.T, ch <-chan apitypes.SyncerHeartbeat) apitypes.SyncerHeartbeat {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("missing heartbeat")
		return apitypes.SyncerHeartbeat{}
	}
}

func TestSyncerReportingPersistenceRestartAndJoin(t *testing.T) {
	d, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	reports := make(chan apitypes.SyncerHeartbeat, 32)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/syncer/heartbeat/worker-1" || r.Header.Get("Authorization") != "Bearer "+reportingTestToken {
			t.Error("unexpected reporting request")
			w.WriteHeader(401)
			return
		}
		var v apitypes.SyncerHeartbeat
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if err := d.SaveSyncerHeartbeat(v); err != nil {
			t.Error(err)
			w.WriteHeader(409)
			return
		}
		reports <- v
		json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"accepted_sequence": v.Sequence}})
	}))
	defer httpServer.Close()
	c := reportingConfig(t, httpServer.URL)
	var priorBoot, registered string
	for run := 0; run < 2; run++ {
		s, fakes := reportingService(t, c)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		joined := false
		defer func() {
			cancel()
			if !joined {
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("service cleanup did not join")
				}
			}
		}()
		first := receiveReport(t, reports)
		if len(first.Tasks) != 2 || first.SyncerID != "worker-1" || first.BootID == priorBoot {
			cancel()
			t.Fatal("bad stable/boot identity")
		}
		priorBoot = first.BootID
		cancel()
		runErr := awaitRun(t, done)
		joined = true
		if err := runErr; err != nil {
			t.Fatal(err)
		}
		last := receiveReport(t, reports)
		if last.State != "stopped" || last.Running || last.Sequence <= first.Sequence || last.BootID != first.BootID {
			t.Fatalf("final report: %+v", last)
		}
		for _, task := range last.Tasks {
			if task.Running || task.State != "stopped" {
				t.Fatal("reported stopped before task join", task)
			}
		}
		for _, task := range fakes {
			if task.runs.Load() != 1 {
				t.Fatal("reporter controlled execution")
			}
		}
		status := s.Status()
		if status.Reporting.LastResult != "acknowledged" || status.Reporting.LastAckAt.IsZero() {
			t.Fatal("missing bounded final ack", status.Reporting)
		}
		runtime, err := d.ListSyncerRuntime()
		if err != nil || len(runtime.Bindings) != 2 || len(runtime.Observations) != run+1 {
			t.Fatal("runtime mismatch", err)
		}
		if run == 0 {
			registered = runtime.Bindings[0].RegisteredAt
		} else if runtime.Bindings[0].RegisteredAt != registered {
			t.Fatal("restart replaced binding")
		}
		for _, v := range runtime.Observations {
			if v.Status != "stopped" {
				t.Fatal("clean restart falsely conflicted", v.Status)
			}
		}
		if _, err := os.Stat(c.Tasks[0].JournalDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("fake-task reporting created a journal")
		}
	}
}

func TestSyncerReportingResponseHandlingAndNoRedirect(t *testing.T) {
	var redirects atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirects.Add(1) }))
	defer target.Close()
	for _, scenario := range []string{"ack", "wrong_ack", "unauthorized", "conflict", "failure", "oversize", "redirect", "malformed"} {
		t.Run(scenario, func(t *testing.T) {
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var v apitypes.SyncerHeartbeat
				json.NewDecoder(r.Body).Decode(&v)
				switch scenario {
				case "ack":
					fmt.Fprintf(w, `{"success":true,"data":{"accepted_sequence":%d}}`, v.Sequence)
				case "wrong_ack":
					fmt.Fprint(w, `{"success":true,"data":{"accepted_sequence":999}}`)
				case "unauthorized":
					w.WriteHeader(401)
				case "conflict":
					w.WriteHeader(409)
				case "failure":
					w.WriteHeader(503)
				case "oversize":
					fmt.Fprint(w, strings.Repeat("x", 8192))
				case "redirect":
					http.Redirect(w, r, target.URL, 307)
				case "malformed":
					fmt.Fprint(w, "SENSITIVE response")
				}
			}))
			defer httpServer.Close()
			c := reportingConfig(t, httpServer.URL)
			s, _ := reportingService(t, c)
			h, err := newSyncerReporter(c)
			if err != nil {
				t.Fatal(err)
			}
			defer h.client.CloseIdleConnections()
			h.send(context.Background(), s)
			h.send(context.Background(), s)
			want := "error"
			if scenario == "ack" {
				want = "acknowledged"
			}
			if scenario == "unauthorized" || scenario == "conflict" {
				want = scenario
			}
			status := s.Status()
			if status.Reporting.LastResult != want || h.report.Sequence != 2 || (scenario == "ack") == status.Reporting.LastAckAt.IsZero() {
				t.Fatal("response result", status.Reporting)
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
			if !strings.Contains(w.Body.String(), `redis_pilot_syncer_heartbeat_total{result="`+want+`"} 2`) {
				t.Fatal("report metrics missing", w.Body.String())
			}
			if strings.Contains(w.Body.String(), reportingTestToken) || strings.Contains(w.Body.String(), "SENSITIVE") {
				t.Fatal("report metrics leaked secrets")
			}
		})
	}
	if redirects.Load() != 0 {
		t.Fatal("credential-bearing request followed redirect")
	}
}

func TestSyncerReportingUnavailableDoesNotStopTasks(t *testing.T) {
	requests := make(chan struct{}, 8)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests <- struct{}{}; w.WriteHeader(503) }))
	defer httpServer.Close()
	c := reportingConfig(t, httpServer.URL)
	s, fakes := reportingService(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	awaitSignal(t, requests)
	select {
	case err := <-done:
		t.Fatal("Server failure stopped tasks", err)
	default:
	}
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatal(err)
	}
	if s.Status().Reporting.LastResult != "error" {
		t.Fatal("Server outage hidden")
	}
	for _, f := range fakes {
		if f.runs.Load() != 1 {
			t.Fatal("task execution changed")
		}
	}
}

func TestSyncerReportingTimeoutAndCancellation(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer httpServer.Close()
	defer close(release)
	c := reportingConfig(t, httpServer.URL)
	c.ControlPlane.RequestTimeout = 50 * time.Millisecond
	s, _ := reportingService(t, c)
	h, err := newSyncerReporter(c)
	if err != nil {
		t.Fatal(err)
	}
	defer h.client.CloseIdleConnections()
	done := make(chan struct{})
	go func() { defer close(done); h.send(context.Background(), s) }()
	awaitSignal(t, entered)
	awaitSignal(t, done)
	if s.Status().Reporting.LastResult != "error" {
		t.Fatal("timeout acknowledged")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done = make(chan struct{})
	go func() { defer close(done); h.send(ctx, s) }()
	awaitSignal(t, done)
}

func TestSyncerReportingConfigAndIdentity(t *testing.T) {
	c := reportingConfig(t, "https://control.example")
	for _, endpoint := range []string{"http://example.com", "http://localhost", "https://user:secret@example.com", "https://example.com/path", "https://example.com?secret", "https://example.com#fragment", "https://example.com#", "https://example.com:", "https://example.com:0", "https://example.com:65536"} {
		bad := c
		bad.ControlPlane.ServerEndpoint = endpoint
		if err := bad.Validate(); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("bad endpoint accepted or leaked", endpoint, err)
		}
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.ControlPlane.SyncerID = "bad/id" }, func(c *Config) { c.ControlPlane.NodeID = "" }, func(c *Config) { c.ControlPlane.TokenFile = "relative" }, func(c *Config) { c.ControlPlane.HeartbeatInterval = 16 * time.Second }, func(c *Config) { c.ControlPlane.RequestTimeout = 4 * time.Second }, func(c *Config) { c.BuildVersion = "bad\nversion" }, func(c *Config) { c.ControlPlane.ServerEndpoint = "" },
	} {
		bad := c
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatal("invalid reporter config")
		}
	}
	legacy := validConfig(t)
	legacy.ControlPlane = c.ControlPlane
	if err := legacy.Validate(); err == nil {
		t.Fatal("legacy journal silently promoted to named reporting")
	}
	original := c.Tasks[0]
	changed := original
	changed.SourceAddress = "[::ffff:127.0.0.1]:016400"
	changed.JournalDir = "/different/private/path"
	changed.SourcePasswordFile = "/secret"
	changed.JournalBytes++
	if taskIdentityDigest(original) != taskIdentityDigest(changed) {
		t.Fatal("equivalent identity changed digest")
	}
	changed = original
	changed.ColdAddress = "127.0.0.1:33333"
	if taskIdentityDigest(original) == taskIdentityDigest(changed) {
		t.Fatal("cold identity omitted")
	}
	yaml := multiYAML(t) + "control_plane:\n  server_endpoint: https://control.example\n  token_file: /private/token\n  syncer_id: worker\n  node_id: node\n"
	decoded, err := DecodeConfig(strings.NewReader(yaml))
	if err != nil || decoded.ControlPlane.HeartbeatInterval != 10*time.Second {
		t.Fatal("reporter defaults", err)
	}
	if _, err := DecodeConfig(strings.NewReader(yaml + "  token: SENSITIVE\n")); err == nil || strings.Contains(err.Error(), "SENSITIVE") {
		t.Fatal("inline token accepted or leaked", err)
	}
}

func TestSyncerReportingCredentialSafetyAndNoIOConstruction(t *testing.T) {
	c := reportingConfig(t, "https://control.example")
	for name, setup := range map[string]func(string){
		"public":    func(p string) { os.WriteFile(p, []byte(reportingTestToken), 0644) },
		"short":     func(p string) { os.WriteFile(p, []byte("short"), 0600) },
		"multiline": func(p string) { os.WriteFile(p, []byte(reportingTestToken+"\nSENSITIVE"), 0600) },
		"large":     func(p string) { os.WriteFile(p, []byte(strings.Repeat("a", 4097)), 0600) },
		"fifo": func(p string) {
			if err := unix.Mkfifo(p, 0600); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(p string) {
			if err := os.Symlink(c.ControlPlane.TokenFile, p); err != nil {
				t.Fatal(err)
			}
		},
		"hardlink": func(p string) {
			if err := os.Link(c.ControlPlane.TokenFile, p); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "invalid")
			setup(p)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, err := readReportingToken(p)
				if err == nil || strings.Contains(err.Error(), "SENSITIVE") {
					t.Error("unsafe token accepted", err)
				}
			}()
			awaitSignal(t, done)
		})
	}
	// Nonexistent token is not opened until explicit credential check / Run.
	c.ControlPlane.TokenFile = filepath.Join(t.TempDir(), "missing")
	s, fakes := reportingService(t, c)
	if err := s.CheckCredentials(); err != errCredentials {
		t.Fatal("missing token not checked", err)
	}
	if err := s.Run(context.Background()); err != errCredentials {
		t.Fatal("run ignored missing token", err)
	}
	for _, task := range fakes {
		if task.runs.Load() != 0 {
			t.Fatal("task started before reporter preflight")
		}
	}
	for _, task := range c.Tasks {
		if _, err := os.Stat(task.JournalDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("check created journal")
		}
	}
}
