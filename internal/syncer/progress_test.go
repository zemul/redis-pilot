package syncer

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type fakeProgressTask struct {
	*fakeTask
	sample func(context.Context, time.Duration) apitypes.SyncProgress
	probes atomic.Int32
}

func (t *fakeProgressTask) SampleProgress(ctx context.Context, timeout time.Duration) apitypes.SyncProgress {
	t.probes.Add(1)
	return t.sample(ctx, timeout)
}

func sampledProgress() apitypes.SyncProgress {
	at := time.Now()
	pos := func(n int64) *apitypes.SyncProgressPosition {
		return &apitypes.SyncProgressPosition{History: strings.Repeat("a", 40), Offset: n}
	}
	return apitypes.SyncProgress{Local: &apitypes.SyncLocalProgress{SampledAt: at, Captured: pos(900), Applied: pos(800), GCConfirmed: pos(700), LiveJournalBytes: 1, JournalLimitBytes: 128 << 20}, Source: apitypes.SyncSourceProgress{SampledAt: at, Position: pos(1000), Error: "none"}}
}

func progressService(t *testing.T, c Config, fn func(context.Context, time.Duration) apitypes.SyncProgress) (*Service, []*fakeProgressTask) {
	t.Helper()
	fakes := make([]*fakeProgressTask, len(c.Tasks))
	tasks := make([]isolatedTask, len(c.Tasks))
	for i := range fakes {
		fakes[i] = &fakeProgressTask{fakeTask: newFakeTask(), sample: fn}
		fakes[i].taskStatus = gateway.IsolatedSyncStatus{State: "running", Running: true}
		tasks[i] = fakes[i]
	}
	s, err := newMultiService(c, tasks, net.Listen)
	if err != nil {
		t.Fatal(err)
	}
	return s, fakes
}

func TestSyncerProgressOptInAndReadOnlyStatus(t *testing.T) {
	c := validMultiConfig(t, 2)
	s, fakes := progressService(t, c, func(context.Context, time.Duration) apitypes.SyncProgress {
		t.Error("disabled probe called")
		return sampledProgress()
	})
	stop := s.startProgress(context.Background())
	stop()
	if err := s.CheckCredentials(); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"/status", "/metrics"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", route, nil))
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	for _, f := range fakes {
		if f.probes.Load() != 0 {
			t.Fatal("read-only endpoint probed source")
		}
	}
	if s.Status().Tasks[0].Progress != nil || s.Status().Tasks[0].ProgressView != nil {
		t.Fatal("disabled telemetry changed task contract")
	}
	c.Progress.Enabled = true
	s, fakes = progressService(t, c, func(context.Context, time.Duration) apitypes.SyncProgress { return sampledProgress() })
	if err := s.CheckCredentials(); err != nil {
		t.Fatal(err)
	}
	for _, f := range fakes {
		if f.probes.Load() != 0 {
			t.Fatal("check-config probed source")
		}
	}
	for i := range s.tasks {
		p := sampledProgress()
		s.progress[i] = &p
	}
	for _, route := range []string{"/status", "/metrics"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", route, nil))
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		if route == "/metrics" && !strings.Contains(w.Body.String(), `redis_pilot_syncer_progress_lag_bytes{kind="source_capture",task_id="shard-1"} 100`) {
			t.Fatal("known gap missing", w.Body.String())
		}
	}
	for _, f := range fakes {
		if f.probes.Load() != 0 {
			t.Fatal("scrape performed I/O")
		}
	}
	view := s.Status()
	view.Tasks[0].Progress.Local.Captured.Offset = 0
	if s.Status().Tasks[0].Progress.Local.Captured.Offset != 900 {
		t.Fatal("external mutation of cache")
	}
	old := sampledProgress()
	old.Local.SampledAt = time.Now().Add(-time.Minute)
	old.Source.SampledAt = old.Local.SampledAt
	s.progress[0] = &old
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(w.Body.String(), `redis_pilot_syncer_progress_lag_bytes{kind="source_capture",task_id="shard-1"}`) || !strings.Contains(w.Body.String(), `redis_pilot_syncer_progress_lag_known{kind="source_capture",task_id="shard-1"} 0`) {
		t.Fatal("stale lag emitted", w.Body.String())
	}
}

func TestSyncerProgressParallelCancellationAndTaskExecution(t *testing.T) {
	c := validMultiConfig(t, 3)
	c.Progress.Enabled = true
	entered := make(chan struct{}, 3)
	exited := make(chan struct{}, 3)
	s, tasks := progressService(t, c, func(ctx context.Context, _ time.Duration) apitypes.SyncProgress {
		entered <- struct{}{}
		<-ctx.Done()
		exited <- struct{}{}
		return apitypes.SyncProgress{Source: apitypes.SyncSourceProgress{SampledAt: time.Now(), Error: "canceled"}}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			awaitRun(t, done)
		}
	}()
	go func() { done <- s.Run(ctx) }()
	for i := 0; i < 3; i++ {
		awaitSignal(t, entered)
	}
	// Every source probe is independently in flight; none occupies task.Run.
	cancel()
	err := awaitRun(t, done)
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		awaitSignal(t, exited)
	}
	for _, task := range tasks {
		if task.runs.Load() != 1 || task.probes.Load() != 1 {
			t.Fatal("probe changed execution or repeated after stop")
		}
	}
	for _, task := range s.Status().Tasks {
		if task.ProgressView.Condition != "stopped" || task.ProgressView.SourceCaptureBytes != nil {
			t.Fatal("stopped task advertised current lag", task.ProgressView)
		}
	}
}

func TestSyncerProgressReportPersistenceAndNoFalseFreshness(t *testing.T) {
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v apitypes.SyncerHeartbeat
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if err := db.SaveSyncerHeartbeat(v); err != nil {
			t.Error(err)
			w.WriteHeader(409)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"accepted_sequence": v.Sequence}})
	}))
	defer server.Close()
	c := reportingConfig(t, server.URL)
	c.Progress.Enabled = true
	s, _ := progressService(t, c, func(context.Context, time.Duration) apitypes.SyncProgress { return sampledProgress() })
	s.setState("running", true)
	for i := range s.tasks {
		p := sampledProgress()
		s.progress[i] = &p
	}
	h, err := newSyncerReporter(c)
	if err != nil {
		t.Fatal(err)
	}
	defer h.client.CloseIdleConnections()
	h.send(context.Background(), s)
	runtime, err := db.ListSyncerRuntime()
	if err != nil || len(runtime.Observations) != 1 || runtime.Observations[0].TaskProgress["shard-1"].Condition != "catching_up" {
		t.Fatal("missing progress report", runtime, err)
	}
	// A newer heartbeat carries updated age, not a fake fresh probe timestamp.
	s.progress[0].Local.SampledAt = time.Now().Add(-time.Minute)
	s.progress[0].Source.SampledAt = s.progress[0].Local.SampledAt
	h.send(context.Background(), s)
	if s.Status().Reporting.LastResult != "acknowledged" {
		t.Fatal("progress update confused with identity change")
	}
	runtime, err = db.ListSyncerRuntime()
	if err != nil || runtime.Observations[0].TaskProgress["shard-1"].Condition != "observation_stale" || runtime.Observations[0].TaskProgress["shard-2"].Condition != "catching_up" {
		t.Fatal("new heartbeat refreshed stale sample", runtime, err)
	}
}

func TestSyncerProgressConcurrentCacheAndValidation(t *testing.T) {
	c := validMultiConfig(t, 1)
	c.Progress.Enabled = true
	s, _ := progressService(t, c, func(context.Context, time.Duration) apitypes.SyncProgress { return sampledProgress() })
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				p := sampledProgress()
				s.mu.Lock()
				s.progress[0] = &p
				s.mu.Unlock()
				status := s.Status()
				if status.Tasks[0].Progress.Validate() != nil {
					t.Error("torn progress snapshot")
				}
				if _, err := s.registry.Gather(); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	for _, mutate := range []func(*Config){func(c *Config) { c.Tasks = nil }, func(c *Config) { c.Progress.Interval = 0 }, func(c *Config) { c.Progress.Interval = 11 * time.Second }, func(c *Config) { c.Progress.Timeout = 0 }, func(c *Config) { c.Progress.Timeout = 3 * time.Second }} {
		bad := c
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("unbounded observer config accepted")
		}
	}
	yaml := multiYAML(t) + "progress:\n  enabled: true\n"
	decoded, err := DecodeConfig(strings.NewReader(yaml))
	if err != nil || !decoded.Progress.Enabled || decoded.Progress.Interval != 5*time.Second || decoded.Progress.Timeout != time.Second {
		t.Fatal("progress config defaults", err)
	}
	if _, err := DecodeConfig(strings.NewReader(yaml + "  command: SENSITIVE\n")); err == nil || strings.Contains(err.Error(), "SENSITIVE") {
		t.Fatal("arbitrary probe command allowed or leaked", err)
	}
}
