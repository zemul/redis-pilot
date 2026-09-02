// Package metrics 以 Prometheus exposition format 暴露 Server 的可观测指标（§4.2）。
//
// 设计要点：
//   - 前缀统一 redis_pilot_server_，与 Proxy 的 redis_pilot_proxy_ 对齐，避免与
//     其他 exporter（尤其 redis_exporter、通用 proxy/sidecar）混淆。
//   - 异常指标不自建判定，直接读平台已沉淀的 health_incidents 表（open 记录），
//     由一个 custom collector 在抓取时现算，保证与平台异常判定一致、无第二套逻辑。
//   - 标签只用有限枚举（severity/incident_type/object_type），绝不使用
//     instance_name/object_id 等高基数字段，避免 series 无限累积。
//   - Go/Process collector 的 go_*/process_* 是通用约定，不加产品前缀。
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

const namespace = "redis_pilot_server"

// IncidentLister 由 Server 注入（= s.db.ListOpenIncidents），使 metrics 包无需
// 反向依赖 server 包，也避免导入循环。返回当前所有未恢复（open）异常。
type IncidentLister func() ([]store.Incident, error)

// Metrics 汇总 Server 的指标与私有 registry。
type Metrics struct {
	// reconcile 存活（控制面看门狗）
	ReconcileRuns   *prometheus.CounterVec
	reconcileLastTS prometheus.Gauge

	incidentColl *incidentCollector
	registry     *prometheus.Registry
}

// New 创建 Metrics，注入异常数据源并注册全部 collector 到私有 registry。
func New(lister IncidentLister) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		ReconcileRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "reconcile_runs_total",
			Help:      "Total reconcile loop runs, by result (success|error).",
		}, []string{"result"}),
		reconcileLastTS: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "reconcile_last_timestamp_seconds",
			Help: "Unix timestamp (seconds) when the last reconcile loop finished. " +
				"Staleness = time() - this; a frozen value means the reconcile loop is stuck.",
		}),
	}

	m.incidentColl = newIncidentCollector(lister)

	m.registry.MustRegister(
		m.ReconcileRuns,
		m.reconcileLastTS,
		m.incidentColl,
		// 进程级兜底指标：私有 registry 不会自动注册默认 collector，显式加入。
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Handler 返回 /metrics 的 HTTP handler。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ObserveReconcile 记录一次 reconcile 运行结果与完成时刻。
func (m *Metrics) ObserveReconcile(success bool) {
	if success {
		m.ReconcileRuns.WithLabelValues("success").Inc()
	} else {
		m.ReconcileRuns.WithLabelValues("error").Inc()
	}
	m.reconcileLastTS.SetToCurrentTime()
}
