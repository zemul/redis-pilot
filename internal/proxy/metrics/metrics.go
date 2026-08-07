// Package metrics 使用 prometheus/client_golang 提供 proxy 指标（§14）。
//
// 指标按 group / backend 维度打标签，通过标准 promhttp.Handler 暴露在 /metrics。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Metrics 收集 proxy 运行时指标。
type Metrics struct {
	// Gauges
	ActiveClientConns  *prometheus.GaugeVec
	ActiveUpstreamConns *prometheus.GaugeVec
	ExclusiveConns      *prometheus.GaugeVec
	BackendHealth      *prometheus.GaugeVec

	// Counters
	ReadFallbackMaster *prometheus.CounterVec
	ErrorCount          *prometheus.CounterVec
	ClientConnTotal     *prometheus.CounterVec
	CommandCount        *prometheus.CounterVec
	BackendDownCount    *prometheus.CounterVec

	registry *prometheus.Registry
}

// New 创建 Metrics 并注册到私有 registry。
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		ActiveClientConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "proxy_active_client_conns",
			Help: "Active client connections to proxy",
		}, []string{"group"}),

		ActiveUpstreamConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "proxy_active_upstream_conns",
			Help: "Active upstream connections from proxy to backends",
		}, []string{"group", "backend", "role"}),

		ExclusiveConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "proxy_exclusive_conns",
			Help: "Exclusive (pinned) upstream connections (blocking/txn/subscribe)",
		}, []string{"group"}),

		BackendHealth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "proxy_backend_health",
			Help: "Backend health status (0=OK, 1=DEGRADED, 2=SYNCING, 3=DOWN)",
		}, []string{"group", "backend"}),

		ReadFallbackMaster: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_read_fallback_master_total",
			Help: "Read requests that fell back to master (D10)",
		}, []string{"group"}),

		ErrorCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_error_total",
			Help: "Total errors",
		}, []string{"group"}),

		ClientConnTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_client_conn_total",
			Help: "Total client connections accepted",
		}, []string{"group"}),

		CommandCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_command_total",
			Help: "Total commands processed by group",
		}, []string{"group"}),

		BackendDownCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_backend_down_total",
			Help: "Backend DOWN events",
		}, []string{"group"}),
	}

	m.registry.MustRegister(
		m.ActiveClientConns,
		m.ActiveUpstreamConns,
		m.ExclusiveConns,
		m.BackendHealth,
		m.ReadFallbackMaster,
		m.ErrorCount,
		m.ClientConnTotal,
		m.CommandCount,
		m.BackendDownCount,
	)

	return m
}

// Handler 返回 /metrics 的 HTTP handler。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// IncClientConn / DecClientConn 客户端连接增减。
func (m *Metrics) IncClientConn(group string) {
	m.ActiveClientConns.WithLabelValues(group).Inc()
}
func (m *Metrics) DecClientConn(group string) {
	m.ActiveClientConns.WithLabelValues(group).Dec()
}

// SetUpstreamConns 设置指定后端的上游连接数。
func (m *Metrics) SetUpstreamConns(group, backend, role string, n int) {
	m.ActiveUpstreamConns.WithLabelValues(group, backend, role).Set(float64(n))
}

// IncExclusive / DecExclusive 独占连接增减。
func (m *Metrics) IncExclusive(group string) {
	m.ExclusiveConns.WithLabelValues(group).Inc()
}
func (m *Metrics) DecExclusive(group string) {
	m.ExclusiveConns.WithLabelValues(group).Dec()
}

// IncReadFallback 读回落 master。
func (m *Metrics) IncReadFallback(group string) {
	m.ReadFallbackMaster.WithLabelValues(group).Inc()
}

// IncError 错误计数。
func (m *Metrics) IncError(group string) {
	m.ErrorCount.WithLabelValues(group).Inc()
}

// IncClientConnTotal 客户端连接总数计数。
func (m *Metrics) IncClientConnTotal(group string) {
	m.ClientConnTotal.WithLabelValues(group).Inc()
}

// IncCommand 命令处理计数（per-group）。
func (m *Metrics) IncCommand(group string) {
	m.CommandCount.WithLabelValues(group).Inc()
}

// IncBackendDown 后端 DOWN 事件。
func (m *Metrics) IncBackendDown(group string) {
	m.BackendDownCount.WithLabelValues(group).Inc()
}
