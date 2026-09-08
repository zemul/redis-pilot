package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var (
	errRunOnce       = errors.New("syncer service can only run once")
	errConfig        = errors.New("syncer configuration check failed")
	errCredentials   = errors.New("syncer credential check failed")
	errAdminListen   = errors.New("syncer admin listener could not start")
	errAdminServe    = errors.New("syncer admin listener stopped unexpectedly")
	errAdminShutdown = errors.New("syncer admin shutdown did not complete gracefully")
	errTaskRun       = errors.New("syncer isolated task failed")
	errTaskExited    = errors.New("syncer isolated task stopped unexpectedly")
)

type isolatedTask interface {
	CheckCredentials() error
	Run(context.Context) error
	Collector() prometheus.Collector
	Status() gateway.IsolatedSyncStatus
}

// TaskStatus describes cached runner state, not backend availability or readiness.
type TaskStatus struct {
	TaskID       string                     `json:"task_id,omitempty"`
	State        string                     `json:"state"`
	LastError    string                     `json:"last_error"`
	Running      bool                       `json:"running"`
	UpdatedAt    time.Time                  `json:"updated_at"`
	Progress     *apitypes.SyncProgress     `json:"progress,omitempty"`
	ProgressView *apitypes.SyncProgressView `json:"progress_view,omitempty"`
}

type Status struct {
	Mode      string          `json:"mode"`
	State     string          `json:"state"`
	Running   bool            `json:"running"`
	LastError string          `json:"last_error"`
	Task      TaskStatus      `json:"task"`
	Tasks     []TaskStatus    `json:"tasks,omitempty"`
	Reporting ReportingStatus `json:"reporting"`
}

type serviceTask struct {
	id         string
	task       isolatedTask
	finished   bool // protected by Service.mu
	exitReason string
	finishedAt time.Time
}

// Service owns bounded isolated tasks and a loopback, read-only admin server.
// Construction does not open credentials, storage, sockets, or global collectors.
type Service struct {
	cfg           Config
	tasks         []serviceTask
	registry      *prometheus.Registry
	listen        func(string, string) (net.Listener, error)
	mu            sync.Mutex
	started       bool
	state         string
	running       bool
	lastError     string
	reporting     ReportingStatus // protected by mu
	reportCount   *prometheus.CounterVec
	lastReportAck prometheus.Gauge
	progress      []*apitypes.SyncProgress // immutable samples published under mu
}

func NewService(c Config) (*Service, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	tasks := make([]isolatedTask, 0, len(c.taskConfigs()))
	for _, config := range c.taskConfigs() {
		task, err := gateway.NewIsolatedSyncTask(config.options(c))
		if err != nil {
			return nil, errors.New("syncer isolated task could not be configured")
		}
		tasks = append(tasks, task)
	}
	return newMultiService(c, tasks, net.Listen)
}

func newService(c Config, task isolatedTask, listen func(string, string) (net.Listener, error)) (*Service, error) {
	return newMultiService(c, []isolatedTask{task}, listen)
}

func newMultiService(c Config, tasks []isolatedTask, listen func(string, string) (net.Listener, error)) (*Service, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	configs := c.taskConfigs()
	if len(tasks) != len(configs) || listen == nil {
		return nil, errors.New("syncer service dependencies are unavailable")
	}
	r := prometheus.NewRegistry()
	entries := make([]serviceTask, len(tasks))
	for i, task := range tasks {
		if task == nil {
			return nil, errors.New("syncer service dependencies are unavailable")
		}
		if c.Progress.Enabled {
			if _, ok := task.(progressSampler); !ok {
				return nil, errors.New("syncer task progress sampling is unavailable")
			}
		}
		collector := task.Collector()
		if collector == nil {
			return nil, errors.New("syncer task metrics are unavailable")
		}
		var registerer prometheus.Registerer = r
		if c.Tasks != nil {
			registerer = prometheus.WrapRegistererWith(prometheus.Labels{"task_id": configs[i].TaskID}, r)
		}
		if err := registerer.Register(collector); err != nil {
			return nil, errors.New("syncer task metrics could not be registered")
		}
		entries[i] = serviceTask{id: configs[i].TaskID, task: task}
	}
	if c.Tasks != nil {
		c.Tasks = append([]TaskConfig(nil), c.Tasks...)
	}
	reports, ack := newReportingMetrics()
	reporting := ReportingStatus{Enabled: c.ControlPlane.ServerEndpoint != "", LastResult: "disabled"}
	if reporting.Enabled {
		reporting.LastResult = "pending"
		if err := r.Register(reports); err != nil {
			return nil, errors.New("syncer reporting metrics could not be registered")
		}
		if err := r.Register(ack); err != nil {
			return nil, errors.New("syncer reporting metrics could not be registered")
		}
	}
	s := &Service{cfg: c, tasks: entries, registry: r, listen: listen, state: "created", lastError: "none", reporting: reporting, reportCount: reports, lastReportAck: ack, progress: make([]*apitypes.SyncProgress, len(tasks))}
	if c.Progress.Enabled {
		if err := r.Register(newProgressCollector(s)); err != nil {
			return nil, errors.New("syncer progress metrics could not be registered")
		}
	}
	return s, nil
}

// CheckCredentials only reads credential files; it does not contact either
// backend or create the journal. It is separate from NewService for check-config.
func (s *Service) CheckCredentials() error {
	if err := s.cfg.Validate(); err != nil {
		return errConfig
	}
	s.mu.Lock()
	tasks := make([]isolatedTask, len(s.tasks))
	for i := range s.tasks {
		tasks[i] = s.tasks[i].task
	}
	s.mu.Unlock()
	var result error
	if s.cfg.ControlPlane.ServerEndpoint != "" {
		if _, err := readReportingToken(s.cfg.ControlPlane.TokenFile); err != nil {
			result = errCredentials
		}
	}
	for _, task := range tasks {
		if err := task.CheckCredentials(); err != nil {
			result = errCredentials
		}
	}
	return result
}

func (s *Service) Status() Status {
	s.mu.Lock()
	state, running, lastError := s.state, s.running, s.lastError
	reporting := s.reporting
	entries := append([]serviceTask(nil), s.tasks...)
	progress := append([]*apitypes.SyncProgress(nil), s.progress...)
	s.mu.Unlock()
	status := Status{Mode: "isolated", State: state, Running: running, LastError: lastError, Reporting: reporting}
	for i, entry := range entries {
		t := entry.task.Status()
		view := TaskStatus{TaskID: entry.id, State: taskState(t.State), LastError: taskError(t.LastError), Running: t.Running, UpdatedAt: t.UpdatedAt}
		if entry.finished {
			view.Running, view.UpdatedAt = false, entry.finishedAt
			if entry.exitReason != "none" {
				view.State = "failed"
				if view.LastError == "none" || view.LastError == "canceled" {
					view.LastError = entry.exitReason
				}
			} else if view.State != "drained" {
				view.State = "stopped"
			}
		}
		if s.cfg.Tasks == nil {
			status.Task = view
		} else {
			if s.cfg.Progress.Enabled {
				view.Progress = progress[i].At(time.Now())
				derived := apitypes.DeriveSyncProgress(view.Progress, view.State, view.LastError, view.Running, 0)
				if state == "stopping" {
					derived = apitypes.SyncProgressView{Condition: "stopping"}
				}
				view.ProgressView = &derived
			}
			status.Tasks = append(status.Tasks, view)
		}
	}
	return status
}

func (s *Service) setState(state string, running bool) {
	s.mu.Lock()
	s.state, s.running = state, running
	s.mu.Unlock()
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		live := (s.state == "running" || s.state == "degraded") && s.running
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !live {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, "syncer not running\n")
			return
		}
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Status())
	})
	mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Method == http.MethodHead {
			mux.ServeHTTP(headWriter{w}, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type headWriter struct{ http.ResponseWriter }

func (w headWriter) Write(p []byte) (int, error) { return len(p), nil }

// Run is single-use. Cancellation stops capture through the task's graceful
// shutdown contract and waits for task cleanup before returning. No signal
// handlers are installed here; the executable supplies the cancellation context.
func (s *Service) Run(ctx context.Context) (result error) {
	if ctx == nil {
		return errors.New("syncer run context is unavailable")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errRunOnce
	}
	s.started, s.state = true, "starting"
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.lastError = ErrorClass(result)
		s.mu.Unlock()
	}()
	if ctx.Err() != nil {
		s.setState("stopped", false)
		return nil
	}
	if err := s.CheckCredentials(); err != nil {
		s.setState("failed", false)
		return err
	}
	if ctx.Err() != nil {
		s.setState("stopped", false)
		return nil
	}
	admin, err := s.listen("tcp", s.cfg.AdminListen)
	if err != nil {
		s.setState("failed", false)
		return errAdminListen
	}
	defer admin.Close()
	reporter, err := newSyncerReporter(s.cfg)
	if err != nil {
		s.setState("failed", false)
		return errCredentials
	}
	// Bind before starting a task: an unavailable admin port must not leave a
	// hidden synchronizer writing to the cold database.
	if ctx.Err() != nil {
		s.setState("stopped", false)
		return nil
	}
	server := &http.Server{
		Handler: s.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192,
		ErrorLog: log.New(io.Discard, "", 0),
	}
	// Each adapter independently translates cancellation into its graceful
	// drain budget. Cancel all first, then join, never N sequential budgets.
	type taskExit struct {
		index int
		err   error
	}
	httpDone, taskDone := make(chan error, 1), make(chan taskExit, len(s.tasks))
	cancels := make([]context.CancelFunc, len(s.tasks))
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()
	s.setState("running", true)
	if reporter != nil {
		reportCtx, cancelReport := context.WithCancel(context.Background())
		reportDone := make(chan struct{})
		go func() { defer close(reportDone); reporter.run(reportCtx, s) }()
		defer func() {
			s.mu.Lock()
			s.lastError = ErrorClass(result)
			s.mu.Unlock()
			cancelReport()
			<-reportDone
		}()
	}
	go func() { httpDone <- server.Serve(admin) }()
	stopProgress := s.startProgress(ctx)
	defer stopProgress() // runs before final control-plane report, after all task joins
	for i, entry := range s.tasks {
		taskCtx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel
		go func(index int, task isolatedTask, runCtx context.Context) {
			err := task.Run(runCtx)
			taskDone <- taskExit{index: index, err: taskExitError(err, runCtx.Err() != nil)}
		}(i, entry.task, taskCtx)
	}
	httpJoined, stopping, remaining := false, false, len(s.tasks)
	recordExit := func(exit taskExit) {
		remaining--
		s.mu.Lock()
		entry := &s.tasks[exit.index]
		entry.finished, entry.exitReason, entry.finishedAt = true, ErrorClass(exit.err), time.Now().UTC()
		if exit.err != nil {
			if result == nil {
				result = exit.err
			}
			s.lastError = ErrorClass(result)
			if !stopping {
				s.state = "degraded"
			}
		}
		s.mu.Unlock()
	}
	for remaining > 0 && !stopping {
		select {
		case <-ctx.Done():
			stopping = true
		case err := <-httpDone:
			httpJoined, stopping = true, true
			if ctx.Err() == nil || !normalHTTPStop(err) {
				if result == nil {
					result = errAdminServe
				}
			}
		case exit := <-taskDone:
			recordExit(exit)
		}
	}
	stopping = true
	s.setState("stopping", true)
	for _, cancel := range cancels {
		cancel()
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	if err := server.Shutdown(shutdown); err != nil {
		server.Close()
		if result == nil {
			result = errAdminShutdown
		}
	}
	cancelShutdown()
	if !httpJoined {
		if err := <-httpDone; !normalHTTPStop(err) && result == nil {
			result = errAdminServe
		}
	}
	for remaining > 0 {
		recordExit(<-taskDone)
	}
	if result != nil {
		s.setState("failed", false)
	} else {
		s.setState("stopped", false)
	}
	return result
}

func normalHTTPStop(err error) bool {
	return errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed)
}

func taskExitError(err error, stopping bool) error {
	if stopping && (err == nil || err == context.Canceled) {
		return nil
	}
	if err == nil {
		return errTaskExited
	}
	return errTaskRun
}

// ErrorClass returns a fixed, safe category for executable exit logging. Task
// details are separately available in Status().Tasks (or legacy Task).
func ErrorClass(err error) string {
	switch err {
	case nil:
		return "none"
	case errRunOnce:
		return "run_once"
	case errConfig:
		return "config"
	case errCredentials:
		return "credentials"
	case errAdminListen:
		return "admin_listen"
	case errAdminServe:
		return "admin_serve"
	case errAdminShutdown:
		return "admin_shutdown"
	case errTaskRun:
		return "task_failure"
	case errTaskExited:
		return "task_exited"
	default:
		return "unknown"
	}
}

// Defense in depth at the HTTP/log boundary. Unknown future runner values stay
// observable as unknown without accidentally exposing an upstream error string.
func taskState(state string) string {
	if state == "" {
		return "idle"
	}
	if apitypes.ValidSyncTaskState(state) {
		return state
	}
	return "unknown"
}

func taskError(reason string) string {
	if reason == "" {
		return "none"
	}
	if apitypes.ValidSyncTaskError(reason) {
		return reason
	}
	return "unknown"
}
