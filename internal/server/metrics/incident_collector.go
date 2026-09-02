package metrics

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// incidentCollector 在每次抓取时读取 open incidents 并现算指标。用 custom
// collector 而非常驻 gauge，是因为异常的真相在 health_incidents 表，抓取时读
// 一次即与平台判定强一致，无需维护第二套状态、也不会有陈旧/漂移。
//
// 只用有限枚举标签（severity/incident_type/object_type）。instance_name/object_id
// 等高基数字段绝不入标签——具体是哪个对象出问题，告警后去 incidents API 查。
type incidentCollector struct {
	lister IncidentLister

	openDesc      *prometheus.Desc
	oldestAgeDesc *prometheus.Desc
	scrapeErrDesc *prometheus.Desc
}

func newIncidentCollector(lister IncidentLister) *incidentCollector {
	return &incidentCollector{
		lister: lister,
		openDesc: prometheus.NewDesc(
			namespace+"_open_incidents",
			"Number of currently open (unresolved) health incidents, by severity/type/object_type. "+
				"Sourced directly from the platform's health_incidents table (single source of truth).",
			[]string{"severity", "incident_type", "object_type"}, nil,
		),
		oldestAgeDesc: prometheus.NewDesc(
			namespace+"_oldest_open_incident_age_seconds",
			"Age in seconds of the oldest currently-open incident, by severity. "+
				"0 (absent) when there is no open incident of that severity.",
			[]string{"severity"}, nil,
		),
		scrapeErrDesc: prometheus.NewDesc(
			namespace+"_incident_scrape_error",
			"1 if the last incident scrape failed to read the store, 0 otherwise.",
			nil, nil,
		),
	}
}

func (c *incidentCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.openDesc
	ch <- c.oldestAgeDesc
	ch <- c.scrapeErrDesc
}

func (c *incidentCollector) Collect(ch chan<- prometheus.Metric) {
	open, err := c.lister()
	if err != nil {
		slog.Warn("metrics: list open incidents failed", "err", err)
		ch <- prometheus.MustNewConstMetric(c.scrapeErrDesc, prometheus.GaugeValue, 1)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.scrapeErrDesc, prometheus.GaugeValue, 0)

	// 按 (severity,type,object_type) 计数。
	type key struct{ sev, typ, obj string }
	counts := make(map[key]int)
	// 每个 severity 的最老 first_observed_at。
	oldest := make(map[string]time.Time)
	now := time.Now()

	for i := range open {
		inc := &open[i]
		sev := normalizeSeverity(inc.Severity)
		counts[key{sev, inc.IncidentType, inc.ObjectType}]++

		if t, perr := time.Parse(time.RFC3339, inc.FirstObservedAt); perr == nil {
			if cur, ok := oldest[sev]; !ok || t.Before(cur) {
				oldest[sev] = t
			}
		}
	}

	for k, n := range counts {
		ch <- prometheus.MustNewConstMetric(
			c.openDesc, prometheus.GaugeValue, float64(n),
			k.sev, k.typ, k.obj,
		)
	}
	for sev, t := range oldest {
		age := now.Sub(t).Seconds()
		if age < 0 {
			age = 0
		}
		ch <- prometheus.MustNewConstMetric(
			c.oldestAgeDesc, prometheus.GaugeValue, age, sev,
		)
	}
}

// normalizeSeverity 兜底空值。平台约定 severity ∈ {critical, warning}，
// 空串按 warning 处理（与 incident_service 的默认一致）。
func normalizeSeverity(s string) string {
	switch s {
	case "critical", "warning":
		return s
	default:
		return "warning"
	}
}

var _ prometheus.Collector = (*incidentCollector)(nil)
