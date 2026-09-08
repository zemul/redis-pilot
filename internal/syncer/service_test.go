package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
)

type fakeTask struct {
	checkErr   error
	run        func(context.Context) error
	checks     atomic.Int32
	runs       atomic.Int32
	collector  prometheus.Collector
	taskStatus gateway.IsolatedSyncStatus
}

func newFakeTask() *fakeTask {
	return &fakeTask{
		collector:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "syncer_test_task", Help: "Test-only task metric."}),
		taskStatus: gateway.IsolatedSyncStatus{State: "idle"},
		run:        func(ctx context.Context) error { <-ctx.Done(); return nil },
	}
}

func (f *fakeTask) CheckCredentials() error            { f.checks.Add(1); return f.checkErr }
func (f *fakeTask) Run(ctx context.Context) error      { f.runs.Add(1); return f.run(ctx) }
func (f *fakeTask) Collector() prometheus.Collector    { return f.collector }
func (f *fakeTask) Status() gateway.IsolatedSyncStatus { return f.taskStatus }

func testService(t *testing.T, task *fakeTask) *Service {
	t.Helper()
	s, err := newService(validConfig(t), task, net.Listen)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for deterministic lifecycle boundary")
	}
}

func awaitRun(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("service did not finish")
		return nil
	}
}

func TestServiceReadOnlyAdminRoutes(t *testing.T) {
	fake := newFakeTask()
	fake.taskStatus = gateway.IsolatedSyncStatus{State: "running", LastError: "", Running: true, UpdatedAt: time.Unix(100, 0).UTC()}
	s := testService(t, fake)
	if fake.checks.Load() != 0 || fake.runs.Load() != 0 {
		t.Fatal("construction performed task I/O")
	}
	h := s.Handler()
	for _, route := range []string{"/livez", "/status", "/metrics"} {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions} {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(method, route, nil))
			if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "GET, HEAD" {
				t.Fatalf("write method allowed on %s", route)
			}
		}
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(method, route, nil))
			want := http.StatusOK
			if route == "/livez" {
				want = http.StatusServiceUnavailable
			}
			if w.Code != want {
				t.Fatalf("%s %s status=%d", method, route, w.Code)
			}
			if method == http.MethodHead && w.Body.Len() != 0 {
				t.Fatalf("HEAD %s emitted a body", route)
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("status should not be cached")
			}
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusNotFound {
		t.Fatal("isolated syncer must not advertise backend readiness")
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status", nil))
	var status Status
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Mode != "isolated" || status.State != "created" || status.Running || !status.Task.Running || status.Task.State != "running" || status.Task.LastError != "none" || status.LastError != "none" {
		t.Fatalf("service liveness and task state were conflated: %+v", status)
	}
	for _, forbidden := range []string{s.cfg.SourceAddress, s.cfg.ColdAddress, s.cfg.JournalDir, "password"} {
		if strings.Contains(w.Body.String(), forbidden) {
			t.Fatal("status exposed configuration")
		}
	}
	if fake.checks.Load() != 0 || fake.runs.Load() != 0 {
		t.Fatal("scrape performed task I/O")
	}
}

func TestServiceUsesPrivateMetricsRegistry(t *testing.T) {
	first := testService(t, newFakeTask())
	second := testService(t, newFakeTask())
	for _, s := range []*Service{first, second} {
		families, err := s.registry.Gather()
		if err != nil || len(families) != 1 || families[0].GetName() != "syncer_test_task" {
			t.Fatalf("private registry: %v %v", families, err)
		}
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "syncer_test_task" {
			t.Fatal("task registered global metrics")
		}
	}
}

func TestServiceStatusAndErrorClassesAreRedacted(t *testing.T) {
	fake := newFakeTask()
	fake.taskStatus.State = "SENSITIVE_STATE"
	fake.taskStatus.LastError = "SENSITIVE_BACKEND_RESPONSE"
	s := testService(t, fake)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status", nil))
	if strings.Contains(w.Body.String(), "SENSITIVE") || s.Status().Task.State != "unknown" || s.Status().Task.LastError != "unknown" {
		t.Fatal("task status was not restricted to known categories")
	}
	for _, reason := range []string{"credential_file", "journal_open", "journal_close", "history_gap", "shutdown_timeout"} {
		if taskError(reason) != reason {
			t.Fatalf("lost actionable fixed category %s", reason)
		}
	}
	if ErrorClass(errors.New("SENSITIVE_ERROR")) != "unknown" {
		t.Fatal("error class exposed arbitrary error")
	}
}

func TestServiceCancellationWaitsForTaskCleanup(t *testing.T) {
	started, stopping, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	fake := newFakeTask()
	fake.run = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(stopping)
		<-release
		return nil
	}
	s := testService(t, fake)
	bound := make(chan net.Addr, 1)
	s.listen = func(network, address string) (net.Listener, error) {
		l, err := net.Listen(network, address)
		if err == nil {
			bound <- l.Addr()
		}
		return l, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	awaitSignal(t, started)
	addr := <-bound
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + addr.String() + "/livez")
	if err != nil {
		close(release)
		cancel()
		awaitRun(t, done)
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("live service status=%d", resp.StatusCode)
	}
	cancel()
	awaitSignal(t, stopping)
	select {
	case err := <-done:
		t.Fatalf("returned before task cleanup: %v", err)
	default:
	}
	close(release)
	if err := awaitRun(t, done); err != nil {
		t.Fatal(err)
	}
	if s.Status().State != "stopped" || s.Status().Running {
		t.Fatal("missing stopped state")
	}
	if _, err := client.Get("http://" + addr.String() + "/livez"); err == nil {
		t.Fatal("admin listener still accepting after Run")
	}
}

func TestServicePortConflictDoesNotStartTask(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	fake := newFakeTask()
	s := testService(t, fake)
	s.cfg.AdminListen = occupied.Addr().String()
	if err := s.Run(context.Background()); err != errAdminListen {
		t.Fatalf("expected sanitized bind error: %v", err)
	}
	if fake.runs.Load() != 0 || s.Status().State != "failed" {
		t.Fatal("bind failure started task or lost state")
	}
	if s.Status().LastError != "admin_listen" {
		t.Fatal("bind failure category unavailable after admin exit")
	}
}

func TestServiceCredentialsFailureDoesNotBindOrRun(t *testing.T) {
	fake := newFakeTask()
	fake.checkErr = errors.New("SENSITIVE_CREDENTIAL_FAILURE")
	s := testService(t, fake)
	s.listen = func(string, string) (net.Listener, error) {
		t.Error("credential failure attempted bind")
		return nil, errors.New("not expected")
	}
	if err := s.Run(context.Background()); err != errCredentials || strings.Contains(err.Error(), "SENSITIVE") {
		t.Fatalf("unsafe credential error: %v", err)
	}
	if fake.runs.Load() != 0 {
		t.Fatal("credential failure started task")
	}
	if s.Status().LastError != "credentials" {
		t.Fatal("credential failure category unavailable after exit")
	}
}

func TestServiceTaskExitIsSanitizedAndClosesAdmin(t *testing.T) {
	for _, taskErr := range []error{nil, errors.New("SENSITIVE_BACKEND_FAILURE"), context.Canceled} {
		fake := newFakeTask()
		fake.run = func(context.Context) error { return taskErr }
		s := testService(t, fake)
		err := s.Run(context.Background())
		want := errTaskRun
		if taskErr == nil {
			want = errTaskExited
		}
		if err != want || strings.Contains(err.Error(), "SENSITIVE") {
			t.Fatalf("unexpected task error: %v", err)
		}
		if s.Status().State != "failed" || s.Status().Running {
			t.Fatal("task exit did not fail service")
		}
	}
}

type brokenAdminListener struct {
	fail, closed chan struct{}
	once         sync.Once
}

func (l *brokenAdminListener) Accept() (net.Conn, error) {
	select {
	case <-l.fail:
		return nil, errors.New("SENSITIVE_LISTENER_FAILURE")
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *brokenAdminListener) Close() error { l.once.Do(func() { close(l.closed) }); return nil }
func (l *brokenAdminListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19300}
}

func TestServiceHTTPFailureCancelsAndJoinsTask(t *testing.T) {
	started, stopping, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	fake := newFakeTask()
	fake.run = func(ctx context.Context) error { close(started); <-ctx.Done(); close(stopping); <-release; return nil }
	s := testService(t, fake)
	l := &brokenAdminListener{fail: make(chan struct{}), closed: make(chan struct{})}
	s.listen = func(string, string) (net.Listener, error) { return l, nil }
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	awaitSignal(t, started)
	close(l.fail)
	awaitSignal(t, stopping)
	select {
	case err := <-done:
		t.Fatalf("HTTP failure leaked unfinished task: %v", err)
	default:
	}
	close(release)
	if err := awaitRun(t, done); err != errAdminServe {
		t.Fatalf("unexpected HTTP failure: %v", err)
	}
}

type acceptedListener struct {
	net.Listener
	accepted chan struct{}
	once     sync.Once
}

func (l *acceptedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.once.Do(func() { close(l.accepted) })
	}
	return c, err
}

func TestServiceHTTPShutdownHasBoundedForceClose(t *testing.T) {
	fake := newFakeTask()
	started := make(chan struct{})
	fake.run = func(ctx context.Context) error { close(started); <-ctx.Done(); return nil }
	s := testService(t, fake)
	s.cfg.ShutdownTimeout = 50 * time.Millisecond
	accepted := make(chan struct{})
	bound := make(chan net.Addr, 1)
	s.listen = func(network, address string) (net.Listener, error) {
		l, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}
		bound <- l.Addr()
		return &acceptedListener{Listener: l, accepted: accepted}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	awaitSignal(t, started)
	conn, err := net.DialTimeout("tcp", (<-bound).String(), time.Second)
	if err != nil {
		cancel()
		awaitRun(t, done)
		t.Fatal(err)
	}
	defer conn.Close()
	awaitSignal(t, accepted)
	// An accepted client that never supplies headers must not consume the
	// server's 3s header timeout during a shorter shutdown budget.
	stopAt := time.Now()
	cancel()
	if err := awaitRun(t, done); err != errAdminShutdown {
		t.Fatalf("expected bounded force-close category: %v", err)
	}
	if time.Since(stopAt) > time.Second {
		t.Fatal("HTTP shutdown exceeded its bounded fallback")
	}
	if s.Status().LastError != "admin_shutdown" {
		t.Fatal("missing final HTTP shutdown reason")
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("force-close left accepted client open")
	}
}

func TestServiceRunIsSingleUseAndCanceledStartIsInert(t *testing.T) {
	fake := newFakeTask()
	s := testService(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if fake.checks.Load() != 0 || fake.runs.Load() != 0 {
		t.Fatal("canceled start performed I/O")
	}
	if err := s.Run(context.Background()); err != errRunOnce {
		t.Fatalf("second run accepted: %v", err)
	}
	started := make(chan struct{})
	fake = newFakeTask()
	fake.run = func(ctx context.Context) error { close(started); <-ctx.Done(); return nil }
	s = testService(t, fake)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	awaitSignal(t, started)
	if err := s.Run(context.Background()); err != errRunOnce {
		t.Fatalf("concurrent run accepted: %v", err)
	}
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err != errRunOnce {
		t.Fatalf("completed service restarted: %v", err)
	}
}
