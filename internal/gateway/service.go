package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Service supports the isolated skeleton and single-backend G2 passthrough.
type Service struct {
	// Immutable after sessions start. Kept internal until durable startup and
	// preparation orchestration can safely install and restore the same set.
	admission          *keyBarrierSet
	admissionBoot      *admissionRunOptions   // Internal, immutable before serve.
	preparationOptions *hotPreparationOptions // Internal only; no YAML switch.
	preparation        *hotPreparation
	reporter           *heartbeatReporter
	heartbeats         *prometheus.CounterVec
	lastHeartbeatAck   prometheus.Gauge
	cfg                Config
	rejected           prometheus.Counter
	registry           *prometheus.Registry
	draining           atomic.Bool
	backendReady       atomic.Bool
	serving            atomic.Bool
	frontendPassword   string
	backendPassword    string
	uncertain          prometheus.Counter
	sessionsMu         sync.Mutex
	sessions           map[*sessionControl]struct{}
	budgetMu           sync.Mutex
	reserved           int64
	active             prometheus.Gauge
	bufferReserved     prometheus.Gauge
	inflight           prometheus.Gauge
	rejects            *prometheus.CounterVec
	errors             *prometheus.CounterVec
	completed          *prometheus.CounterVec
	duration           prometheus.Histogram
	batchSize          prometheus.Histogram
	drainDuration      prometheus.Histogram
}

func NewService(c Config) (*Service, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	s := &Service{cfg: c, registry: prometheus.NewRegistry(), sessions: make(map[*sessionControl]struct{})}
	var err error
	if c.Mode == "passthrough" {
		s.frontendPassword, err = readPassword(c.FrontendPasswordFile)
		if err != nil {
			return nil, err
		}
		s.backendPassword, err = readPassword(c.BackendPasswordFile)
		if err != nil {
			return nil, err
		}
	}
	s.rejected = prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_pilot_gateway_skeleton_rejected_connections_total", Help: "Connections rejected because forwarding is not implemented."})
	s.registry.MustRegister(s.rejected)
	s.uncertain = prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_pilot_gateway_upstream_result_unknown_total", Help: "Requests whose backend result is unknown after dispatch; includes reads."})
	s.registry.MustRegister(s.uncertain)
	s.initMetrics()
	s.reporter, err = newHeartbeatReporter(c)
	if err != nil {
		return nil, err
	}
	return s, nil
}
func (s *Service) Handler() http.Handler {
	m := http.NewServeMux()
	if s.preparation != nil {
		m.Handle(hotPreparationPath, s.preparation)
		m.Handle(hotPreparationPath+"/confirm", s.preparation)
	}
	m.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() {
			http.Error(w, "draining", 503)
			return
		}
		io.WriteString(w, "ok\n")
	})
	m.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.Mode == "passthrough" && s.serving.Load() && !s.draining.Load() && s.backendReady.Load() {
			io.WriteString(w, "ok\n")
			return
		}
		http.Error(w, "gateway not ready", http.StatusServiceUnavailable)
	})
	m.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	return m
}
func (s *Service) Run(ctx context.Context) error {
	data, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	defer data.Close()
	admin, err := net.Listen("tcp", s.cfg.AdminListen)
	if err != nil {
		return err
	}
	defer admin.Close()
	return s.serve(ctx, data, admin)
}
func (s *Service) serve(ctx context.Context, data, admin net.Listener) error {
	if s.preparationOptions != nil && s.admissionBoot == nil {
		return errAdmissionRun
	}
	var boot *admissionRun
	if s.admissionBoot != nil {
		if s.cfg.Mode != "passthrough" || s.admission != nil || s.cfg.RedisAddress != s.admissionBoot.Binding.Source ||
			s.cfg.GatewayID != s.admissionBoot.Binding.GatewayID {
			return errAdmissionRun
		}
		var err error
		boot, err = openAdmissionRun(*s.admissionBoot, nil)
		if err != nil {
			return err
		}
		defer boot.close() // Early exits never record clean.
		s.admission = boot.barriers
		if s.preparationOptions == nil {
			// Disabling preparation must not silently skip a surviving operation.
			if _, err := os.Lstat(filepath.Join(s.admissionBoot.Dir, "confirmation-operation.log")); !errors.Is(err, os.ErrNotExist) {
				return errAdmissionRun
			}
		}
	}
	if s.preparationOptions != nil {
		o := s.preparationOptions.Confirmation
		if o.OperationDir != s.admissionBoot.Dir || o.Binding != s.admissionBoot.Binding {
			return errAdmissionRun
		}
		var err error
		s.preparation, err = openHotPreparation(s, *s.preparationOptions)
		if err != nil {
			return err
		}
		defer s.preparation.Close()
	}
	work, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	var workers sync.WaitGroup
	probeCtx, cancelProbe := context.WithCancel(work)
	defer cancelProbe()
	slots := make(chan struct{}, s.cfg.MaxConnections)
	s.serving.Store(true)
	defer s.serving.Store(false)
	if s.cfg.Mode == "passthrough" {
		probeBytes := s.cfg.probeWorkspaceBytes()
		if !s.acquireWorkspace(probeBytes) {
			return errors.New("insufficient probe buffer budget")
		}
		workers.Add(1)
		go func() { defer workers.Done(); defer s.releaseWorkspace(probeBytes); s.probe(probeCtx) }()
	}
	if s.reporter != nil {
		workers.Add(1)
		go func() { defer workers.Done(); s.reporter.run(probeCtx, s) }()
	}
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan error, 2)
	go func() { done <- server.Serve(admin) }()
	go func() {
		for {
			c, e := data.Accept()
			if e != nil {
				done <- e
				return
			}
			if s.cfg.Mode == "passthrough" {
				select {
				case slots <- struct{}{}:
					workers.Add(1)
					go func() { defer workers.Done(); defer func() { <-slots }(); s.session(work, c) }()
				default:
					s.rejects.WithLabelValues("connections").Inc()
					c.Close()
				}
				continue
			}
			// Sequential, bounded rejection: no unbounded per-connection workers.
			c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			io.WriteString(c, "-ERR gateway skeleton: forwarding unavailable\r\n")
			c.Close()
			s.rejected.Inc()
		}
	}()
	var cause error
	consumed := 0
	select {
	case <-ctx.Done():
	case cause = <-done:
		consumed = 1
	}
	drainStart := time.Now()
	s.sessionsMu.Lock()
	s.draining.Store(true)
	for session := range s.sessions {
		session.stopReading()
	}
	s.sessionsMu.Unlock()
	data.Close()
	if s.preparation != nil {
		s.preparation.stopping.Store(true)
		s.preparation.cancel()
	}
	cancelProbe()
	shutdown, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		server.Close()
		if cause == nil {
			cause = err
		}
	}
	for consumed < 2 {
		<-done
		consumed++
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-shutdown.Done():
		s.errors.WithLabelValues("drain_timeout").Inc()
		cancelWork()
		<-finished
	}
	cancelWork()
	s.drainDuration.Observe(time.Since(drainStart).Seconds())
	if s.preparation != nil {
		s.preparation.Close()
	}
	if errors.Is(cause, http.ErrServerClosed) || errors.Is(cause, net.ErrClosed) {
		cause = nil
	}
	if boot != nil {
		cause = errors.Join(cause, boot.finish())
	}
	return cause
}
