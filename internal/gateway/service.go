package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Service is the G1 startup skeleton. It never connects to a storage backend.
type Service struct {
	cfg      Config
	rejected prometheus.Counter
	registry *prometheus.Registry
	draining atomic.Bool
}

func NewService(c Config) (*Service, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	s := &Service{cfg: c, registry: prometheus.NewRegistry()}
	s.rejected = prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_pilot_gateway_skeleton_rejected_connections_total", Help: "Connections rejected because forwarding is not implemented."})
	s.registry.MustRegister(s.rejected)
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
		http.Error(w, "G1 skeleton: forwarding unavailable", http.StatusServiceUnavailable)
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
	if errors.Is(cause, http.ErrServerClosed) || errors.Is(cause, net.ErrClosed) {
		return nil
	}
	return cause
}
