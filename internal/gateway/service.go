package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Service supports the isolated skeleton and single-backend G2 passthrough.
type Service struct {
	cfg              Config
	rejected         prometheus.Counter
	registry         *prometheus.Registry
	draining         atomic.Bool
	backendReady     atomic.Bool
	serving          atomic.Bool
	frontendPassword string
	backendPassword  string
	uncertain        prometheus.Counter
}

func NewService(c Config) (*Service, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	s := &Service{cfg: c, registry: prometheus.NewRegistry()}
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
	return s, nil
}
func (s *Service) Handler() http.Handler {
	m := http.NewServeMux()
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
	work, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	var workers sync.WaitGroup
	slots := make(chan struct{}, s.cfg.MaxConnections)
	s.serving.Store(true)
	defer s.serving.Store(false)
	if s.cfg.Mode == "passthrough" {
		workers.Add(1)
		go func() { defer workers.Done(); s.probe(work) }()
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
	s.draining.Store(true)
	data.Close()
	// G2 closes active sessions on stop; in-flight graceful draining is G3.
	cancelWork()
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
	workers.Wait()
	if errors.Is(cause, http.ErrServerClosed) || errors.Is(cause, net.ErrClosed) {
		return nil
	}
	return cause
}
