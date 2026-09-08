package gateway

// Isolated service adapter for the existing replication prototype. Keeping this
// narrow boundary here avoids changing the capture/apply protocol while the
// executable and its lifecycle move into the independent syncer service.

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

// IsolatedSyncOptions is deliberately single-source, single-target and local.
// The caller must dedicate the entire cold DB to this task; directory locking
// is not distributed fencing and does not protect a shared cold database.
type IsolatedSyncOptions struct {
	TaskID                            string // empty is the legacy single-task format, not an auto-generated identity
	SourceAddress, SourcePasswordFile string
	ColdAddress, ColdPasswordFile     string
	JournalDir                        string
	AllowUnauthenticated              bool
	IOTimeout, ShutdownTimeout        time.Duration
	SegmentBytes, JournalBytes        int64
}

var errSyncCredentialFile = errors.New("replication credential file unavailable or unsafe")

func syncLiteralEndpoint(address string) (net.IP, int, bool) {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	n, pe := strconv.Atoi(port)
	return ip, n, err == nil && pe == nil && ip != nil && ip.IsLoopback() && n > 0 && n <= 65535
}

func syncAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && filepath.Dir(path) != path && !strings.ContainsRune(path, 0)
}

// ValidateIsolatedSyncOptions performs no file, network or registry operations.
func ValidateIsolatedSyncOptions(o IsolatedSyncOptions) error {
	if o.TaskID != "" {
		if err := ValidateIsolatedSyncTaskID(o.TaskID); err != nil {
			return err
		}
	}
	sourceIP, sourcePort, sourceOK := syncLiteralEndpoint(o.SourceAddress)
	coldIP, coldPort, coldOK := syncLiteralEndpoint(o.ColdAddress)
	if !sourceOK || !coldOK || (sourceIP.Equal(coldIP) && sourcePort == coldPort) {
		return errors.New("isolated sync requires distinct literal loopback backends")
	}
	if !syncAbsolutePath(o.JournalDir) {
		return errors.New("isolated sync requires an absolute clean journal directory")
	}
	for _, path := range []string{o.SourcePasswordFile, o.ColdPasswordFile} {
		if path == "" {
			if !o.AllowUnauthenticated {
				return errors.New("isolated sync requires password files or explicit unauthenticated consent")
			}
		} else if !syncAbsolutePath(path) {
			return errors.New("isolated sync password files require absolute clean paths")
		}
	}
	if o.IOTimeout <= 0 || o.IOTimeout > 30*time.Second || o.ShutdownTimeout <= 0 || o.ShutdownTimeout > 30*time.Second {
		return errors.New("isolated sync timeouts must be positive and at most 30 seconds")
	}
	if o.SegmentBytes < 1<<20 || o.SegmentBytes > 16<<20 || o.JournalBytes < 2*o.SegmentBytes || o.JournalBytes > 1<<30 {
		return errors.New("isolated sync requires bounded segment and journal capacities")
	}
	return nil
}

// IsolatedSyncStatus is a cached, redacted lifecycle view, not a health probe,
// ownership lease, or proof that cold data is current or safe for eviction.
type IsolatedSyncStatus struct {
	State     string    `json:"state"`
	LastError string    `json:"last_error,omitempty"`
	Running   bool      `json:"running"`
	UpdatedAt time.Time `json:"updated_at"`
}

type IsolatedSyncTask struct {
	options IsolatedSyncOptions
	runner  *syncRunner
	mu      sync.Mutex
	started bool // one Run per object; restart creates a new object and reopens the journal
	status  IsolatedSyncStatus
}

// NewIsolatedSyncTask only constructs a task. In particular -check-config must
// never create a journal or acquire its lock, nor contact Redis or Kvrocks.
func NewIsolatedSyncTask(o IsolatedSyncOptions) (*IsolatedSyncTask, error) {
	if err := ValidateIsolatedSyncOptions(o); err != nil {
		return nil, err
	}
	if o.TaskID != "" {
		// Stable representation avoids treating equivalent IP/port spellings as
		// a source-history change. Legacy capture strings remain unchanged.
		o.SourceAddress = syncCanonicalEndpoint(o.SourceAddress)
		o.ColdAddress = syncCanonicalEndpoint(o.ColdAddress)
	}
	r := &syncRunner{
		source: syncSource{address: o.SourceAddress, passwordFile: o.SourcePasswordFile, timeout: o.IOTimeout, maxSnapshot: 1 << 20},
		target: syncColdTarget{address: o.ColdAddress, passwordFile: o.ColdPasswordFile, timeout: o.IOTimeout, taskID: o.TaskID},
		config: defaultSyncRunnerConfig(),
	}
	return &IsolatedSyncTask{options: o, runner: r, status: IsolatedSyncStatus{State: "idle"}}, nil
}

// CheckCredentials is read-only and deliberately returns no paths or content.
// Refuse links, devices and FIFOs before reading; credentials must be private
// regular files belonging to the service account, in a trusted parent directory.
func (t *IsolatedSyncTask) CheckCredentials() error {
	for _, path := range []string{t.options.SourcePasswordFile, t.options.ColdPasswordFile} {
		if path == "" {
			continue
		}
		if _, err := syncReadPassword(path); err != nil {
			return errors.New("isolated sync credential file validation failed")
		}
	}
	return nil
}

// Used by every synchronous credential read on capture/reconnect/recovery too,
// not just a one-time check. In particular replacing a file with a FIFO must
// never turn a bounded service shutdown into an uninterruptible credential read.
func syncReadPassword(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", errSyncCredentialFile
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	st, err := f.Stat()
	var raw unix.Stat_t
	statErr := unix.Fstat(fd, &raw)
	if err != nil || statErr != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || raw.Uid != uint32(os.Geteuid()) || raw.Nlink != 1 || st.Size() > 4096 {
		return "", errSyncCredentialFile
	}
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(b) > 4096 {
		return "", errSyncCredentialFile
	}
	password := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	if password == "" || strings.ContainsAny(password, "\r\n") {
		return "", errSyncCredentialFile
	}
	return password, nil
}

func (t *IsolatedSyncTask) Collector() prometheus.Collector {
	return newSyncRunnerCollector(t.runner)
}

func (t *IsolatedSyncTask) Status() IsolatedSyncStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	status := t.status
	if status.Running {
		runner := t.runner.snapshot()
		if runner.State != "" {
			status.State = syncRunnerFixedMetricLabel(runner.State, "idle", syncRunnerMetricStates)
			status.LastError = syncRunnerFixedMetricLabel(runner.LastError, "", syncRunnerMetricErrors)
			status.UpdatedAt = runner.UpdatedAt
		}
	}
	return status
}

// Run opens/owns/closes the journal and joins all work before returning. Context
// cancellation requests bounded graceful shutdown, NOT immediate runner abort.
// Safety errors remain errors; an operator must explicitly restart/reconcile.
func (t *IsolatedSyncTask) Run(ctx context.Context) (result error) {
	if ctx == nil {
		return errors.New("isolated sync run context is unavailable")
	}
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return errors.New("isolated sync task can only run once")
	}
	t.started = true
	t.status = IsolatedSyncStatus{State: "starting", Running: true, UpdatedAt: time.Now()}
	t.mu.Unlock()
	t.runner.update(func(s *syncRunnerStatus) {
		s.State, s.StartedAt = "starting", time.Now()
	})
	reason := ""
	defer func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		state := t.runner.snapshot().State
		if state == "" || state == "starting" {
			state = "stopped"
		}
		if result != nil {
			state = "failed"
			if reason == "" {
				reason = syncRunnerReason(result)
			}
			// Never expose wrapped socket errors, journal paths or backend replies.
			result = errors.New("isolated sync task failed: " + reason)
		}
		t.runner.update(func(s *syncRunnerStatus) {
			s.State, s.LastError, s.FinishedAt = state, reason, time.Now()
		})
		t.status = IsolatedSyncStatus{State: state, LastError: reason, UpdatedAt: time.Now()}
	}()
	if ctx.Err() != nil {
		return nil // nothing opened or captured; stopped is distinct from drained
	}
	if err := t.CheckCredentials(); err != nil {
		reason = "credential_file"
		return err
	}
	j, err := openSyncSegmentsForTask(t.options.JournalDir, t.options.SegmentBytes, t.options.JournalBytes, defaultSyncResourcePolicy(), t.options.identity())
	if err != nil {
		reason = "journal_open"
		if errors.Is(err, errSyncTaskIdentity) {
			reason = "task_identity"
		}
		return err
	}
	defer func() {
		if err := j.close(); err != nil {
			if result == nil {
				reason = "journal_close"
			}
			result = errors.Join(result, err)
		}
	}()
	if ctx.Err() != nil {
		return nil
	}
	// Parent cancellation is translated below; passing it directly to the
	// runner would abort cold transactions before graceful drain can happen.
	runCtx, abort := context.WithCancel(context.Background())
	defer abort()
	done := make(chan error, 1)
	go func() { done <- t.runner.run(runCtx, j) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	graceCtx, cancelGrace := context.WithTimeout(context.Background(), t.options.ShutdownTimeout)
	defer cancelGrace()
	if abortSyncBeforePublication(t.runner, abort) {
		// Cancellation was serialized with publication, so no cold operation
		// or capture can start with a live context after this decision.
		err := <-done
		if syncOnlyCancellation(err) {
			return nil
		}
		return err
	}
	shutdownErr := t.runner.shutdown(graceCtx)
	if shutdownErr != nil {
		abort() // also handles a deadline expiring before shutdown was accepted
	}
	err = <-done
	if shutdownErr != nil && (err == nil || syncOnlyCancellation(err)) {
		return shutdownErr
	}
	return err
}

// The same lock guards publishing control before any cold/capture work. A
// separate shutdown()->NotRunning check followed by abort would have a race:
// the runner could start between those operations and skip graceful draining.
func abortSyncBeforePublication(r *syncRunner, abort context.CancelFunc) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.control != nil {
		return false
	}
	abort()
	return true
}
