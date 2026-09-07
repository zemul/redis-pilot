package gateway

import "github.com/prometheus/client_golang/prometheus"

func (s *Service) initMetrics() {
	const ns = "redis_pilot_gateway"
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help})
		s.registry.MustRegister(g)
		return g
	}
	counter := func(name, help, label string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, []string{label})
		s.registry.MustRegister(c)
		return c
	}
	hist := func(name, help string, buckets []float64) prometheus.Histogram {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets})
		s.registry.MustRegister(h)
		return h
	}
	s.active = gauge("active_sessions", "Sessions holding a reserved workspace.")
	s.heartbeats = counter("control_heartbeats_total", "Control-plane reports by fixed result; no effect on data admission.", "result")
	s.lastHeartbeatAck = gauge("control_last_ack_timestamp_seconds", "Last validated Server heartbeat acknowledgement, zero until first success.")
	s.bufferReserved = gauge("reserved_buffer_bytes", "Conservative admitted workspace bytes, not actual heap or RSS.")
	s.inflight = gauge("inflight_requests", "Requests in batches currently being processed, including local replies.")
	s.rejects = counter("admission_rejected_total", "Rejected connections by fixed reason.", "reason")
	s.errors = counter("transport_errors_total", "Transport errors by fixed stage.", "stage")
	s.completed = counter("responses_delivered_total", "Complete responses delivered by origin.", "origin")
	s.duration = hist("request_duration_seconds", "Time from complete request decoding to complete response delivery.", prometheus.DefBuckets)
	s.batchSize = hist("batch_requests", "Number of requests in each processed batch.", []float64{1, 2, 4, 8, 16, 32, 64})
	s.drainDuration = hist("drain_duration_seconds", "Time spent stopping admission and draining sessions.", prometheus.DefBuckets)
}
func (s *Service) acquireWorkspace(n int64) bool {
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	if n > s.cfg.BufferBudgetBytes-s.reserved {
		return false
	}
	s.reserved += n
	s.bufferReserved.Set(float64(s.reserved))
	return true
}
func (s *Service) releaseWorkspace(n int64) {
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	s.reserved -= n
	s.bufferReserved.Set(float64(s.reserved))
}
