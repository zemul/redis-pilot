package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

func isolatedSyncOptionsForTest(t *testing.T) IsolatedSyncOptions {
	t.Helper()
	return IsolatedSyncOptions{
		SourceAddress: "127.0.0.1:12345", ColdAddress: "127.0.0.1:12346",
		JournalDir: filepath.Join(t.TempDir(), "journal"), AllowUnauthenticated: true,
		IOTimeout: time.Second, ShutdownTimeout: time.Second,
		SegmentBytes: 1 << 20, JournalBytes: 4 << 20,
	}
}

func isolatedSyncTaskForTest(t *testing.T, o IsolatedSyncOptions) *IsolatedSyncTask {
	t.Helper()
	task, err := NewIsolatedSyncTask(o)
	if err != nil {
		t.Fatal(err)
	}
	task.runner.config = syncRunnerForTest().config
	task.runner.ops = syncRunnerForTest().ops
	return task
}

func TestIsolatedSyncOptions(t *testing.T) {
	for name, mutate := range map[string]func(*IsolatedSyncOptions){
		"source_dns":          func(o *IsolatedSyncOptions) { o.SourceAddress = "localhost:6379" },
		"source_remote":       func(o *IsolatedSyncOptions) { o.SourceAddress = "192.0.2.1:6379" },
		"cold_remote":         func(o *IsolatedSyncOptions) { o.ColdAddress = "192.0.2.2:6379" },
		"source_port_zero":    func(o *IsolatedSyncOptions) { o.SourceAddress = "127.0.0.1:0" },
		"cold_same":           func(o *IsolatedSyncOptions) { o.ColdAddress = o.SourceAddress },
		"cold_alias":          func(o *IsolatedSyncOptions) { o.ColdAddress = "[::ffff:127.0.0.1]:12345" },
		"relative_journal":    func(o *IsolatedSyncOptions) { o.JournalDir = "./journal" },
		"root_journal":        func(o *IsolatedSyncOptions) { o.JournalDir = "/" },
		"unclean_journal":     func(o *IsolatedSyncOptions) { o.JournalDir += "/../journal" },
		"no_auth_consent":     func(o *IsolatedSyncOptions) { o.AllowUnauthenticated = false },
		"relative_credential": func(o *IsolatedSyncOptions) { o.ColdPasswordFile = "secret" },
		"zero_io":             func(o *IsolatedSyncOptions) { o.IOTimeout = 0 },
		"long_io":             func(o *IsolatedSyncOptions) { o.IOTimeout = 31 * time.Second },
		"zero_shutdown":       func(o *IsolatedSyncOptions) { o.ShutdownTimeout = 0 },
		"long_shutdown":       func(o *IsolatedSyncOptions) { o.ShutdownTimeout = 31 * time.Second },
		"small_segment":       func(o *IsolatedSyncOptions) { o.SegmentBytes = 1024 },
		"big_segment":         func(o *IsolatedSyncOptions) { o.SegmentBytes = 17 << 20 },
		"small_journal":       func(o *IsolatedSyncOptions) { o.JournalBytes = o.SegmentBytes },
		"big_journal":         func(o *IsolatedSyncOptions) { o.JournalBytes = (1 << 30) + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			o := isolatedSyncOptionsForTest(t)
			journal := o.JournalDir
			mutate(&o)
			if _, err := NewIsolatedSyncTask(o); err == nil {
				t.Fatal("invalid options accepted")
			}
			if _, err := os.Stat(journal); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("validation touched journal", err)
			}
		})
	}
}

func TestIsolatedSyncConstructAndCheckAreReadOnly(t *testing.T) {
	o := isolatedSyncOptionsForTest(t)
	task, err := NewIsolatedSyncTask(o)
	if err != nil || task.CheckCredentials() != nil {
		t.Fatal("cannot validate isolated task", err)
	}
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(task.Collector()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
	if s := task.Status(); s.State != "idle" || s.Running {
		t.Fatal("construction started task", s)
	}
	if _, err := os.Stat(o.JournalDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read-only operations created journal", err)
	}
}

func TestIsolatedSyncCredentialFiles(t *testing.T) {
	for _, mode := range []string{"valid", "crlf", "empty", "multiline", "oversized", "public", "symlink", "fifo", "directory", "missing", "hardlink"} {
		t.Run(mode, func(t *testing.T) {
			o := isolatedSyncOptionsForTest(t)
			path := filepath.Join(t.TempDir(), "sensitive-file-name")
			data := "sensitive-password\n"
			perm := os.FileMode(0600)
			switch mode {
			case "crlf":
				data = "sensitive-password\r\n"
			case "empty":
				data = ""
			case "multiline":
				data += "more\n"
			case "oversized":
				data = strings.Repeat("x", 4097)
			case "public":
				perm = 0644
			}
			var err error
			switch mode {
			case "symlink":
				err = os.Symlink("missing", path)
			case "fifo":
				err = unix.Mkfifo(path, 0600)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "missing":
			default:
				err = os.WriteFile(path, []byte(data), perm)
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "hardlink" {
				if err := os.Link(path, path+"-alias"); err != nil {
					t.Fatal(err)
				}
			}
			o.SourcePasswordFile = path
			task, err := NewIsolatedSyncTask(o)
			if err != nil {
				t.Fatal(err)
			}
			err = task.CheckCredentials()
			if (err == nil) != (mode == "valid" || mode == "crlf") {
				t.Fatal("credential validation outcome", err)
			}
			if err != nil && strings.Contains(err.Error(), "sensitive") {
				t.Fatal("credential error leaked details", err)
			}
			if _, err := os.Stat(o.JournalDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("credential check opened journal", err)
			}
		})
	}
}

func TestIsolatedSyncPreCanceledDoesNotOpenJournal(t *testing.T) {
	o := isolatedSyncOptionsForTest(t)
	task := isolatedSyncTaskForTest(t, o)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(o.JournalDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pre-canceled task opened journal", err)
	}
	if s := task.Status(); s.State != "stopped" || s.Running {
		t.Fatal(s)
	}
}

func TestIsolatedSyncStartupErrorsAreRedactedAndObservable(t *testing.T) {
	for _, mode := range []string{"journal", "credential", "runner"} {
		t.Run(mode, func(t *testing.T) {
			o := isolatedSyncOptionsForTest(t)
			want := "journal_open"
			switch mode {
			case "journal":
				o.JournalDir = filepath.Join(t.TempDir(), "private-missing-parent", "journal")
			case "credential":
				o.SourcePasswordFile = filepath.Join(t.TempDir(), "private-missing-secret")
				want = "credential_file"
			case "runner":
				want = "cold_outcome_uncertain"
			}
			task := isolatedSyncTaskForTest(t, o)
			if mode == "runner" {
				task.runner.ops.drain = func(context.Context, *syncSegments) (syncPosition, error) {
					return syncPosition{}, errors.Join(errSyncPendingUncertain, errors.New("private-response"))
				}
			}
			err := task.Run(context.Background())
			if err == nil || strings.Contains(err.Error(), "private") || !strings.Contains(err.Error(), want) {
				t.Fatal("unsafe or missing failure", err)
			}
			if s := task.Status(); s.State != "failed" || s.Running || s.LastError != want {
				t.Fatal(s)
			}
			if s := task.runner.snapshot(); s.State != "failed" || s.LastError != want {
				t.Fatal(s)
			}
			registry := prometheus.NewPedanticRegistry()
			registry.MustRegister(task.Collector())
			if _, err := registry.Gather(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIsolatedSyncGracefulStopJoinsCaptureThenClosesJournal(t *testing.T) {
	o := isolatedSyncOptionsForTest(t)
	task := isolatedSyncTaskForTest(t, o)
	captured := make(chan *syncSegments, 1)
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	task.runner.ops.capture = func(ctx context.Context, j *syncSegments) error {
		record := captureSnapshot(o.SourceAddress)
		record.Data = syncTestRDB()
		if err := j.append(record); err != nil {
			return err
		}
		captured <- j
		<-ctx.Done()
		// Capture may finish a durable record after stop is requested.
		if err := syncShutdownAppendForTest(j, "last-durable-frame"); err != nil {
			return err
		}
		close(canceled)
		<-release
		return ctx.Err()
	}
	task.runner.ops.drain = func(_ context.Context, j *syncSegments) (syncPosition, error) {
		return syncShutdownConfirmForTest(j), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	t.Cleanup(func() { cancel(); unblock() })
	var j *syncSegments
	select {
	case j = <-captured:
	case <-time.After(3 * time.Second):
		t.Fatal("capture not started")
	}
	if !task.Status().Running {
		t.Fatal("task not observed as running")
	}
	if err := task.Run(context.Background()); err == nil {
		t.Fatal("concurrent Run accepted")
	}
	cancel()
	syncShutdownSignalForTest(t, canceled)
	select {
	case err := <-done:
		t.Fatal("task returned before capture joined", err)
	default:
	}
	if duplicate, err := openSyncSegments(o.JournalDir, o.SegmentBytes, o.JournalBytes); err == nil {
		duplicate.close()
		t.Fatal("journal lock released before capture joined")
	}
	unblock()
	if err := syncRunnerResultForTest(t, done); err != nil {
		t.Fatal(err)
	}
	if s := task.Status(); s.State != "drained" || s.Running {
		t.Fatal(s)
	}
	want := task.runner.snapshot().DrainTarget
	if task.runner.snapshot().Confirmed != want || want.History == "" {
		t.Fatal("wrong frozen tail")
	}
	j.mu.Lock()
	closed := j.dir == nil && j.lock == nil && len(j.files) == 0
	j.mu.Unlock()
	if !closed {
		t.Fatal("task leaked journal handles")
	}
	// A new process/object can reopen the persistent capture, not an empty log.
	reopened, err := openSyncSegments(o.JournalDir, o.SegmentBytes, o.JournalBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if reopened.durablePosition() != want {
		t.Fatal("durable tail lost across reopen")
	}
	if err := task.Run(context.Background()); err == nil {
		t.Fatal("completed task reused")
	}
}

func TestIsolatedSyncShutdownTimeoutIsFailure(t *testing.T) {
	o := isolatedSyncOptionsForTest(t)
	o.ShutdownTimeout = 30 * time.Millisecond
	task := isolatedSyncTaskForTest(t, o)
	entered := make(chan struct{})
	task.runner.ops.drain = func(ctx context.Context, _ *syncSegments) (syncPosition, error) {
		close(entered)
		<-ctx.Done()
		return syncPosition{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	syncShutdownSignalForTest(t, entered)
	cancel()
	if err := syncRunnerResultForTest(t, done); err == nil || !strings.Contains(err.Error(), "shutdown_timeout") {
		t.Fatal("shutdown timeout hidden", err)
	}
	if s := task.Status(); s.State != "failed" || s.LastError != "shutdown_timeout" {
		t.Fatal(s)
	}
}

func TestIsolatedSyncStatusAndMetricsDuringRun(t *testing.T) {
	task := isolatedSyncTaskForTest(t, isolatedSyncOptionsForTest(t))
	var capturing atomic.Bool
	task.runner.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		capturing.Store(true)
		<-ctx.Done()
		return ctx.Err()
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(task.Collector())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	syncRunnerWaitForTest(t, capturing.Load)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				_ = task.Status()
				if _, err := registry.Gather(); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	cancel()
	if err := syncRunnerResultForTest(t, done); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestIsolatedSyncCredentialReReadsRejectReplacement(t *testing.T) {
	for _, replacement := range []string{"fifo", "symlink", "public"} {
		t.Run(replacement, func(t *testing.T) {
			o := isolatedSyncOptionsForTest(t)
			path := filepath.Join(t.TempDir(), "credential")
			if err := os.WriteFile(path, []byte("secret-before-rotation"), 0600); err != nil {
				t.Fatal(err)
			}
			o.SourcePasswordFile, o.ColdPasswordFile = path, path
			task := isolatedSyncTaskForTest(t, o)
			if err := task.CheckCredentials(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			var err error
			switch replacement {
			case "fifo":
				err = unix.Mkfifo(path, 0600)
			case "symlink":
				err = os.Symlink("missing", path)
			case "public":
				err = os.WriteFile(path, []byte("secret-after-rotation"), 0644)
			}
			if err != nil {
				t.Fatal(err)
			}
			j, err := openSyncSegments(o.JournalDir, o.SegmentBytes, o.JournalBytes)
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			for _, operation := range []func() error{
				func() error { return task.runner.validate(j) },
				func() error { return task.runner.source.capture(context.Background(), j) },
				func() error {
					_, closeClient, err := task.runner.target.open(context.Background())
					if closeClient != nil {
						closeClient()
					}
					return err
				},
			} {
				result := make(chan error, 1)
				go func() { result <- operation() }()
				select {
				case err := <-result:
					if !errors.Is(err, errSyncCredentialFile) || syncRunnerReason(err) != "credential_file" {
						t.Fatal("unsafe replacement accepted by actual sync read", err)
					}
				case <-time.After(time.Second):
					t.Fatal("replacement blocked synchronous credential read")
				}
			}
		})
	}
}

func TestIsolatedSyncAbortBeforePublicationIsAtomic(t *testing.T) {
	r := syncRunnerForTest()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	aboutToAbort, allowAbort, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var publishSawCanceled atomic.Bool
	done := make(chan bool, 1)
	go func() {
		done <- abortSyncBeforePublication(r, func() {
			close(aboutToAbort)
			<-allowAbort
			cancel()
		})
	}()
	syncShutdownSignalForTest(t, aboutToAbort)
	go func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.control = &syncRunControl{}
		publishSawCanceled.Store(ctx.Err() != nil)
		close(published)
	}()
	select {
	case <-published:
		t.Fatal("control published before atomic abort completed")
	default:
	}
	close(allowAbort)
	if !<-done {
		t.Fatal("unpublished run was not canceled")
	}
	syncShutdownSignalForTest(t, published)
	if !publishSawCanceled.Load() {
		t.Fatal("publication escaped cancellation")
	}
	var unexpectedAbort atomic.Bool
	if abortSyncBeforePublication(r, func() { unexpectedAbort.Store(true) }) || unexpectedAbort.Load() {
		t.Fatal("published run bypassed graceful shutdown")
	}
}

func TestIsolatedSyncCancellationPreservesTerminalFailure(t *testing.T) {
	task := isolatedSyncTaskForTest(t, isolatedSyncOptionsForTest(t))
	entered, release := make(chan struct{}), make(chan struct{})
	task.runner.ops.drain = func(ctx context.Context, _ *syncSegments) (syncPosition, error) {
		close(entered)
		<-release
		return syncPosition{}, errors.Join(errSyncPendingUncertain, context.Canceled)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	syncShutdownSignalForTest(t, entered)
	cancel()
	syncRunnerWaitForTest(t, func() bool { return task.runner.snapshot().StopRequested })
	close(release)
	if err := syncRunnerResultForTest(t, done); err == nil || !strings.Contains(err.Error(), "cold_outcome_uncertain") {
		t.Fatal("cancel hid cold outcome", err)
	}
}
