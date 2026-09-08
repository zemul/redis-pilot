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
	"sync"
	"testing"
	"time"
)

func validMultiConfig(t *testing.T, count int) Config {
	t.Helper()
	c := DefaultConfig()
	c.AdminListen = "127.0.0.1:0"
	c.Tasks = make([]TaskConfig, count)
	dir := t.TempDir()
	for i := range c.Tasks {
		task := DefaultTaskConfig()
		task.TaskID = fmt.Sprintf("shard-%d", i+1)
		task.ConfirmExclusiveCold, task.AllowUnauthenticated = true, true
		task.SourceAddress = fmt.Sprintf("127.0.0.1:%d", 16400+2*i)
		task.ColdAddress = fmt.Sprintf("127.0.0.1:%d", 16401+2*i)
		task.JournalDir = filepath.Join(dir, task.TaskID)
		c.Tasks[i] = task
	}
	return c
}

func multiYAML(t *testing.T) string {
	t.Helper()
	return "mode: isolated\ntasks:\n  - task_id: shard-a\n    source_address: 127.0.0.1:16400\n    cold_address: 127.0.0.1:16401\n    journal_dir: " + filepath.Join(t.TempDir(), "journal") + "\n    confirm_exclusive_cold: true\n    allow_unauthenticated: true\n"
}

func TestMultiConfigStrictDefaultsAndLegacyMixing(t *testing.T) {
	base := multiYAML(t)
	c, err := DecodeConfig(strings.NewReader(base))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Tasks) != 1 || c.Tasks[0].SegmentBytes != 4<<20 || c.Tasks[0].JournalBytes != 128<<20 || c.taskOptions().TaskID != "shard-a" || c.taskOptions().IOTimeout != 30*time.Second {
		t.Fatal("named task defaults or common timeouts were lost")
	}
	for _, suffix := range []string{
		"source_address: ''\n", "confirm_exclusive_cold: false\n", "allow_unauthenticated: false\n", "segment_bytes: 4194304\n", "journal_bytes: 0\n",
	} {
		if _, err := DecodeConfig(strings.NewReader(base + suffix)); err == nil {
			t.Fatalf("mixed legacy field accepted: %s", suffix)
		}
	}
	for _, suffix := range []string{"    segment_bytes: 0\n", "    journal_bytes: 0\n", "    io_timeout: 1s\n", "    unexpected: true\n"} {
		if _, err := DecodeConfig(strings.NewReader(base + suffix)); err == nil {
			t.Fatalf("invalid task field accepted: %s", suffix)
		}
	}
	for _, input := range []string{"mode: isolated\ntasks: []\n", "mode: isolated\ntasks: null\n", base + "<<: {source_address: secret}\n"} {
		if _, err := DecodeConfig(strings.NewReader(input)); err == nil {
			t.Fatal("empty tasks or YAML merge accepted")
		}
	}
	legacy, err := DecodeConfig(strings.NewReader(validYAML(t)))
	if err != nil || legacy.Tasks != nil || legacy.taskOptions().TaskID != "" {
		t.Fatal("legacy task was silently assigned an ID")
	}
}

func TestMultiConfigRejectsConflictsAndUnsafeIdentity(t *testing.T) {
	tests := map[string]func(*Config){
		"empty_tasks":          func(c *Config) { c.Tasks = []TaskConfig{} },
		"too_many_tasks":       func(c *Config) { c.Tasks = append(c.Tasks, make([]TaskConfig, maxTasks)...) },
		"empty_id":             func(c *Config) { c.Tasks[0].TaskID = "" },
		"duplicate_id":         func(c *Config) { c.Tasks[1].TaskID = c.Tasks[0].TaskID },
		"unsafe_id":            func(c *Config) { c.Tasks[0].TaskID = "bad{SENSITIVE}" },
		"unicode_id":           func(c *Config) { c.Tasks[0].TaskID = "分片" },
		"large_id":             func(c *Config) { c.Tasks[0].TaskID = strings.Repeat("x", 65) },
		"source_alias":         func(c *Config) { c.Tasks[1].SourceAddress = "[::ffff:127.0.0.1]:016400" },
		"cold_duplicate":       func(c *Config) { c.Tasks[1].ColdAddress = c.Tasks[0].ColdAddress },
		"source_is_other_cold": func(c *Config) { c.Tasks[1].SourceAddress = c.Tasks[0].ColdAddress },
		"admin_alias":          func(c *Config) { c.AdminListen = "[::ffff:127.0.0.1]:016403" },
		"journal_duplicate":    func(c *Config) { c.Tasks[1].JournalDir = c.Tasks[0].JournalDir },
		"journal_nested":       func(c *Config) { c.Tasks[1].JournalDir = filepath.Join(c.Tasks[0].JournalDir, "child") },
		"journal_parent":       func(c *Config) { c.Tasks[0].JournalDir = filepath.Join(c.Tasks[1].JournalDir, "child") },
		"journal_unclean":      func(c *Config) { c.Tasks[0].JournalDir += "/../other" },
		"journal_relative":     func(c *Config) { c.Tasks[0].JournalDir = "relative" },
		"journal_root":         func(c *Config) { c.Tasks[0].JournalDir = "/" },
		"missing_cold_consent": func(c *Config) { c.Tasks[1].ConfirmExclusiveCold = false },
		"missing_auth_consent": func(c *Config) { c.Tasks[1].AllowUnauthenticated = false },
		"combined_budget": func(c *Config) {
			for i := range c.Tasks {
				c.Tasks[i].JournalBytes = 400 << 20
			}
		},
		"missing_task_budget":   func(c *Config) { c.Tasks[1].JournalBytes = 0 },
		"mixed_legacy_endpoint": func(c *Config) { c.SourceAddress = "127.0.0.1:17000" },
		"mixed_legacy_budget":   func(c *Config) { c.JournalBytes = 256 << 20 },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			c := validMultiConfig(t, 3)
			change(&c)
			if err := c.Validate(); err == nil || strings.Contains(err.Error(), "SENSITIVE") {
				t.Fatalf("unsafe config result: %v", err)
			}
		})
	}
	c := validMultiConfig(t, maxTasks)
	if err := c.Validate(); err != nil {
		t.Fatalf("eight default journals should exactly fit the declared budget: %v", err)
	}
	if _, err := NewService(c); err != nil {
		t.Fatal(err)
	}
	for _, task := range c.Tasks {
		if _, err := os.Stat(task.JournalDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("constructing named tasks created storage")
		}
	}
}

func TestMultiConfigJournalAliasesAndRuntimeRevalidation(t *testing.T) {
	c := validMultiConfig(t, 3)
	realDir := filepath.Join(t.TempDir(), "real")
	if err := os.Mkdir(realDir, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(realDir, alias); err != nil {
		t.Fatal(err)
	}
	c.Tasks[0].JournalDir = filepath.Join(realDir, "journal")
	c.Tasks[1].JournalDir = filepath.Join(alias, "journal")
	if err := c.Validate(); err == nil {
		t.Fatal("symlink journal alias accepted")
	}
	c.Tasks[1].JournalDir = filepath.Join(alias, "journal", "child")
	if err := c.Validate(); err == nil {
		t.Fatal("symlink nested journal accepted")
	}
	c.Tasks[1].JournalDir = filepath.Join(alias, "separate")
	if err := c.Validate(); err != nil {
		t.Fatalf("non-overlapping host ancestor alias rejected: %v", err)
	}
	fakes := []isolatedTask{newFakeTask(), newFakeTask(), newFakeTask()}
	s, err := newMultiService(c, fakes, net.Listen)
	if err != nil {
		t.Fatal(err)
	}
	// A previously absent journal becomes an alias after construction. Run
	// must reject it before binding or starting even a different task.
	if err := os.Symlink(filepath.Join(realDir, "journal"), c.Tasks[1].JournalDir); err != nil {
		t.Fatal(err)
	}
	s.listen = func(string, string) (net.Listener, error) {
		t.Error("unsafe paths reached listener")
		return nil, errors.New("unexpected")
	}
	if err := s.Run(context.Background()); err != errConfig {
		t.Fatalf("runtime path change was not rejected: %v", err)
	}
	for _, fake := range fakes {
		if fake.(*fakeTask).runs.Load() != 0 {
			t.Fatal("runtime path rejection started a task")
		}
	}
}

func newTestMultiService(t *testing.T, fakes []*fakeTask) *Service {
	t.Helper()
	tasks := make([]isolatedTask, len(fakes))
	for i, fake := range fakes {
		tasks[i] = fake
	}
	s, err := newMultiService(validMultiConfig(t, len(fakes)), tasks, net.Listen)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func awaitServiceState(t *testing.T, s *Service, expected string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Status().State == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("service did not reach %s: %+v", expected, s.Status())
}

func TestMultiServiceCancellationStopsAllBeforeJoining(t *testing.T) {
	fakes := make([]*fakeTask, 3)
	started, stopping := make([]chan struct{}, 3), make([]chan struct{}, 3)
	release := make(chan struct{})
	for i := range fakes {
		started[i], stopping[i] = make(chan struct{}), make(chan struct{})
		fakes[i] = newFakeTask()
		fakes[i].run = func(ctx context.Context) error {
			close(started[i])
			<-ctx.Done()
			close(stopping[i])
			<-release
			return nil
		}
	}
	s := newTestMultiService(t, fakes)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	for _, signal := range started {
		awaitSignal(t, signal)
	}
	cancel()
	for _, signal := range stopping {
		awaitSignal(t, signal)
	}
	select {
	case err := <-done:
		t.Fatalf("returned before all cleanup: %v", err)
	default:
	}
	close(release)
	if err := awaitRun(t, done); err != nil {
		t.Fatal(err)
	}
	status := s.Status()
	if status.State != "stopped" || status.Running || len(status.Tasks) != 3 || status.Task.TaskID != "" {
		t.Fatalf("bad stopped state: %+v", status)
	}
	for _, task := range status.Tasks {
		if task.Running || task.State != "stopped" || task.LastError != "none" {
			t.Fatalf("bad task stop: %+v", task)
		}
	}
}

func TestMultiServiceFailureIsIsolatedAndRetained(t *testing.T) {
	fail := make(chan struct{})
	started := make([]chan struct{}, 3)
	canceled := make(chan struct{}, 2)
	fakes := make([]*fakeTask, 3)
	for i := range fakes {
		started[i] = make(chan struct{})
		fakes[i] = newFakeTask()
		fakes[i].run = func(ctx context.Context) error {
			close(started[i])
			if i == 0 {
				<-fail
				return errors.New("SENSITIVE_SOURCE_ERROR")
			}
			<-ctx.Done()
			canceled <- struct{}{}
			return nil
		}
	}
	s := newTestMultiService(t, fakes)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	for _, signal := range started {
		awaitSignal(t, signal)
	}
	close(fail)
	awaitServiceState(t, s, "degraded")
	select {
	case <-canceled:
		t.Fatal("failed task canceled healthy sibling")
	default:
	}
	select {
	case err := <-done:
		t.Fatalf("degraded process exited: %v", err)
	default:
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if w.Code != http.StatusOK {
		t.Fatal("degraded but running process lost liveness")
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status", nil))
	var status Status
	if json.Unmarshal(w.Body.Bytes(), &status) != nil || strings.Contains(w.Body.String(), "SENSITIVE") || status.LastError != "task_failure" || status.Tasks[0].State != "failed" || status.Tasks[0].TaskID != "shard-1" || status.Tasks[0].LastError != "task_failure" {
		t.Fatalf("failure was not safely retained: %s", w.Body.String())
	}
	cancel()
	if err := awaitRun(t, done); err != errTaskRun {
		t.Fatalf("cancellation erased earlier failure: %v", err)
	}
	if s.Status().LastError != "task_failure" || s.Status().State != "failed" || s.Status().Tasks[0].LastError != "task_failure" {
		t.Fatal("final state erased failed task")
	}
	for _, fake := range fakes {
		if fake.runs.Load() != 1 {
			t.Fatal("task was restarted automatically")
		}
	}
}

func TestMultiServiceAllExitsCloseService(t *testing.T) {
	for _, failErr := range []error{nil, errors.New("SENSITIVE_FAILURE")} {
		fakes := []*fakeTask{newFakeTask(), newFakeTask(), newFakeTask()}
		for _, fake := range fakes {
			fake.run = func(context.Context) error { return failErr }
		}
		s := newTestMultiService(t, fakes)
		if err := s.Run(context.Background()); err == nil {
			t.Fatal("all unexpected exits succeeded")
		}
		if status := s.Status(); status.State != "failed" || status.Running || len(status.Tasks) != 3 {
			t.Fatalf("bad final state: %+v", status)
		}
		for _, task := range s.Status().Tasks {
			if task.State != "failed" || task.Running {
				t.Fatal("task final state missing")
			}
		}
	}
}

func TestMultiServicePreflightChecksAllBeforeAnyStart(t *testing.T) {
	fakes := []*fakeTask{newFakeTask(), newFakeTask(), newFakeTask()}
	fakes[1].checkErr = errors.New("SENSITIVE_CREDENTIAL")
	s := newTestMultiService(t, fakes)
	s.listen = func(string, string) (net.Listener, error) {
		t.Error("failed credential preflight bound listener")
		return nil, errors.New("unexpected")
	}
	if err := s.Run(context.Background()); err != errCredentials {
		t.Fatal(err)
	}
	for _, fake := range fakes {
		if fake.runs.Load() != 0 || fake.checks.Load() != 1 {
			t.Fatal("preflight did not check all tasks before starting")
		}
	}
}

func TestMultiServiceCollectorLabelsAreTaskScoped(t *testing.T) {
	for _, count := range []int{1, 3} {
		fakes := make([]*fakeTask, count)
		for i := range fakes {
			fakes[i] = newFakeTask()
		}
		s := newTestMultiService(t, fakes)
		families, err := s.registry.Gather()
		if err != nil || len(families) != 1 || len(families[0].Metric) != count {
			t.Fatalf("independent task metrics missing: %v %v", families, err)
		}
		ids := map[string]bool{}
		for _, metric := range families[0].Metric {
			if len(metric.Label) != 1 || metric.Label[0].GetName() != "task_id" {
				t.Fatal("task metric labels not bounded to task_id")
			}
			ids[metric.Label[0].GetValue()] = true
		}
		if len(ids) != count {
			t.Fatal("task metric identity collision")
		}
	}
	legacy := testService(t, newFakeTask())
	families, err := legacy.registry.Gather()
	if err != nil || len(families[0].Metric[0].Label) != 0 || legacy.Status().Tasks != nil {
		t.Fatal("legacy metrics/status compatibility was changed")
	}
}

func TestMultiServiceHTTPFailureCancelsAllTasks(t *testing.T) {
	fakes := make([]*fakeTask, 3)
	started, stopping := make([]chan struct{}, 3), make([]chan struct{}, 3)
	release := make(chan struct{})
	for i := range fakes {
		started[i], stopping[i] = make(chan struct{}), make(chan struct{})
		fakes[i] = newFakeTask()
		fakes[i].run = func(ctx context.Context) error {
			close(started[i])
			<-ctx.Done()
			close(stopping[i])
			<-release
			return nil
		}
	}
	s := newTestMultiService(t, fakes)
	l := &brokenAdminListener{fail: make(chan struct{}), closed: make(chan struct{})}
	s.listen = func(string, string) (net.Listener, error) { return l, nil }
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	for _, signal := range started {
		awaitSignal(t, signal)
	}
	close(l.fail)
	for _, signal := range stopping {
		awaitSignal(t, signal)
	}
	close(release)
	if err := awaitRun(t, done); err != errAdminServe {
		t.Fatalf("unexpected HTTP failure: %v", err)
	}
}

func TestMultiServiceConcurrentCredentialAndStatusReaders(t *testing.T) {
	fail := make(chan struct{})
	fakes := []*fakeTask{newFakeTask(), newFakeTask(), newFakeTask()}
	for _, fake := range fakes {
		fake.run = func(context.Context) error { <-fail; return errors.New("SENSITIVE_FAILURE") }
	}
	s := newTestMultiService(t, fakes)
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	awaitServiceState(t, s, "running")
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 30; i++ {
				if err := s.CheckCredentials(); err != nil {
					t.Error(err)
					return
				}
				if len(s.Status().Tasks) != 3 {
					t.Error("concurrent status lost tasks")
					return
				}
			}
		}()
	}
	close(fail)
	if err := awaitRun(t, done); err != errTaskRun {
		t.Fatal(err)
	}
	readers.Wait()
}

func TestMultiServiceCopiesTaskConfiguration(t *testing.T) {
	c := validMultiConfig(t, 1)
	fake := newFakeTask()
	s, err := newMultiService(c, []isolatedTask{fake}, net.Listen)
	if err != nil {
		t.Fatal(err)
	}
	c.Tasks[0].TaskID = "changed"
	c.Tasks[0].JournalDir = "relative"
	if err := s.CheckCredentials(); err != nil {
		t.Fatalf("caller mutation changed service configuration: %v", err)
	}
	if s.Status().Tasks[0].TaskID != "shard-1" {
		t.Fatal("caller mutation changed task identity")
	}
}
