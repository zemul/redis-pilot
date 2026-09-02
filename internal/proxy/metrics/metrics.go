// Package metrics 使用 prometheus/client_golang 提供 proxy 指标（§14）。
//
// 指标按 group / backend 维度打标签，通过标准 promhttp.Handler 暴露在 /metrics。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Metrics 收集 proxy 运行时指标。
type Metrics struct {
	// Gauges
	ActiveClientConns  *prometheus.GaugeVec
	ActiveUpstreamConns *prometheus.GaugeVec
	ExclusiveConns      *prometheus.GaugeVec
	SnapshotAppliedAt   *prometheus.GaugeVec

	// Counters
	ReadFallbackMaster *prometheus.CounterVec
	ErrorCount          *prometheus.CounterVec
	ClientConnTotal     *prometheus.CounterVec
	CommandCount        *prometheus.CounterVec
	BackendDownCount    *prometheus.CounterVec
	SlowCommand         *prometheus.CounterVec
	SnapshotFetch       *prometheus.CounterVec // labels: result=success|failure
	ServerDisconnect    prometheus.Counter

	// Histograms
	CommandDuration *prometheus.HistogramVec

	registry *prometheus.Registry
}

// namespace 是所有 Proxy 自有指标的统一前缀。最终指标名为
// redis_pilot_proxy_<name>，用产品+组件命名，避免与其他 exporter（尤其
// redis_exporter、通用 proxy/sidecar）的指标混淆。Go/Process collector 的
// go_*/process_* 是标准约定，不加此前缀。
const namespace = "redis_pilot_proxy"

// New 创建 Metrics 并注册到私有 registry。
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		ActiveClientConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_client_conns",
			Help:      "Active client connections to proxy",
		}, []string{"group"}),

		ActiveUpstreamConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_upstream_conns",
			Help:      "Active upstream connections from proxy to backends",
		}, []string{"group", "backend", "role"}),

		ExclusiveConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exclusive_conns",
			Help:      "Exclusive (pinned) upstream connections (blocking/txn/subscribe)",
		}, []string{"group"}),

		SnapshotAppliedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "snapshot_applied_timestamp_seconds",
			Help: "Unix timestamp (seconds) when the currently applied discovery snapshot was applied. " +
				"Staleness = time() - this; a frozen value means the discovery/long-poll link is stuck.",
		}, []string{"version"}),

		ReadFallbackMaster: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "read_fallback_master_total",
			Help:      "Read requests that fell back to master (D10)",
		}, []string{"group"}),

		ErrorCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "error_total",
			Help:      "Total errors",
		}, []string{"group"}),

		ClientConnTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "client_conn_total",
			Help:      "Total client connections accepted",
		}, []string{"group"}),

		CommandCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "command_total",
			Help: "Total commands processed, by group and command. " +
				"Command names are normalized against a known-command whitelist; unknown names map to OTHER to bound cardinality. " +
				"Only forwarded business commands are counted; locally-terminated (AUTH/PING/CLIENT/INFO/CONFIG…) and denied commands are excluded.",
		}, []string{"group", "cmd"}),

		BackendDownCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "backend_down_total",
			Help:      "Backend DOWN events",
		}, []string{"group"}),

		SlowCommand: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "slow_command_total",
			Help: "Commands whose backend round-trip exceeded the slow threshold, by command. " +
				"Command names are normalized against a known-command whitelist; unknown names map to OTHER to bound cardinality.",
		}, []string{"group", "cmd"}),

		SnapshotFetch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "snapshot_fetch_total",
			Help: "Discovery snapshot long-poll fetch attempts, by result (success|failure). " +
				"A rising failure rate signals the Proxy -> Server discovery link is degraded.",
		}, []string{"result"}),

		ServerDisconnect: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "server_disconnect_total",
			Help: "Number of times a discovery snapshot fetch failed (Proxy lost contact with Server). " +
				"Equivalent to the failure stream of redis_pilot_proxy_snapshot_fetch_total, exposed as a scalar for easy alerting.",
		}),

		CommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "command_duration_seconds",
			Help: "Backend round-trip latency in seconds, by group and kind (read/write/other). " +
				"Observes one backend round-trip; a pipelined batch is recorded as a single observation, " +
				"so this is round-trip latency, not strictly per-command latency.",
			Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}, []string{"group", "kind"}),
	}

	m.registry.MustRegister(
		m.ActiveClientConns,
		m.ActiveUpstreamConns,
		m.ExclusiveConns,
		m.SnapshotAppliedAt,
		m.ReadFallbackMaster,
		m.ErrorCount,
		m.ClientConnTotal,
		m.CommandCount,
		m.BackendDownCount,
		m.SlowCommand,
		m.SnapshotFetch,
		m.ServerDisconnect,
		m.CommandDuration,
		// 进程级指标：私有 registry 不会自动注册默认 collector，显式加入。
		// 覆盖 goroutine 数、GC、堆内存、FD、CPU 等（proxy 高并发长驻进程的兜底观测）。
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
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

// IncCommand 命令处理计数（per-group, per-command）。
// cmd 需由 route.NormalizeCommand 归一化后传入（白名单外归 OTHER）。
func (m *Metrics) IncCommand(group, cmd string) {
	m.CommandCount.WithLabelValues(group, cmd).Inc()
}

// IncBackendDown 后端 DOWN 事件。
func (m *Metrics) IncBackendDown(group string) {
	m.BackendDownCount.WithLabelValues(group).Inc()
}

// ObserveCommandDuration 记录一次后端 round-trip 耗时（秒），kind ∈ {read,write,other}。
// pipeline 批调用一次，记为单次观测。
func (m *Metrics) ObserveCommandDuration(group, kind string, seconds float64) {
	m.CommandDuration.WithLabelValues(group, kind).Observe(seconds)
}

// IncSlowCommand 慢命令计数（超过阈值），cmd 需由 route.NormalizeCommand 归一化后传入。
func (m *Metrics) IncSlowCommand(group, cmd string) {
	m.SlowCommand.WithLabelValues(group, cmd).Inc()
}

// IncSnapshotFetchSuccess 记录一次成功的快照拉取（拿到新版本）。
func (m *Metrics) IncSnapshotFetchSuccess() {
	m.SnapshotFetch.WithLabelValues("success").Inc()
}

// IncSnapshotFetchFailure 记录一次失败的快照拉取，并累加 Server 断连计数。
func (m *Metrics) IncSnapshotFetchFailure() {
	m.SnapshotFetch.WithLabelValues("failure").Inc()
	m.ServerDisconnect.Inc()
}

// SetSnapshotApplied 记录当前已应用快照的版本与应用时刻（Unix 秒）。
// 每次只暴露最新版本的时间戳；旧版本 series 会被重置为 0 以避免陈旧标签堆积。
func (m *Metrics) SetSnapshotApplied(version string, appliedAtUnix float64) {
	m.SnapshotAppliedAt.Reset()
	m.SnapshotAppliedAt.WithLabelValues(version).Set(appliedAtUnix)
}
