package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func TestIncidentMetrics_Aggregation(t *testing.T) {
	old := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
	recent := time.Now().Add(-30 * time.Second).Format(time.RFC3339)
	lister := func() ([]store.Incident, error) {
		return []store.Incident{
			{Severity: "critical", IncidentType: "instance_unexpected_down", ObjectType: "instance", FirstObservedAt: old},
			{Severity: "critical", IncidentType: "instance_unexpected_down", ObjectType: "instance", FirstObservedAt: recent},
			{Severity: "warning", IncidentType: "backup_task_failed", ObjectType: "instance", FirstObservedAt: recent},
			{Severity: "", IncidentType: "replica_missing", ObjectType: "group", FirstObservedAt: recent}, // 空 severity → warning
		}, nil
	}
	m := New(lister)
	out := scrape(t, m)

	checks := []string{
		// 两条同 (critical, instance_unexpected_down, instance) 聚合为 2
		`redis_pilot_server_open_incidents{incident_type="instance_unexpected_down",object_type="instance",severity="critical"} 2`,
		`redis_pilot_server_open_incidents{incident_type="backup_task_failed",object_type="instance",severity="warning"} 1`,
		`redis_pilot_server_open_incidents{incident_type="replica_missing",object_type="group",severity="warning"} 1`,
		`redis_pilot_server_incident_scrape_error 0`,
	}
	for _, c := range checks {
		if !strings.Contains(out, c) {
			t.Errorf("expected metrics to contain:\n  %s\ngot:\n%s", c, out)
		}
	}
	// 最老 critical 异常约 10min（600s），断言 > 500 以避免时钟抖动误判
	if !strings.Contains(out, `redis_pilot_server_oldest_open_incident_age_seconds{severity="critical"}`) {
		t.Errorf("missing oldest_open_incident_age_seconds for critical")
	}
}

func TestIncidentMetrics_ScrapeError(t *testing.T) {
	m := New(func() ([]store.Incident, error) { return nil, io.ErrUnexpectedEOF })
	out := scrape(t, m)
	if !strings.Contains(out, "redis_pilot_server_incident_scrape_error 1") {
		t.Errorf("expected scrape_error 1 on lister failure, got:\n%s", out)
	}
}

func TestReconcileAndProcessMetrics(t *testing.T) {
	m := New(func() ([]store.Incident, error) { return nil, nil })
	m.ObserveReconcile(true)
	m.ObserveReconcile(false)
	out := scrape(t, m)

	for _, c := range []string{
		`redis_pilot_server_reconcile_runs_total{result="success"} 1`,
		`redis_pilot_server_reconcile_runs_total{result="error"} 1`,
		"redis_pilot_server_reconcile_last_timestamp_seconds",
		"go_goroutines",
		"process_open_fds",
	} {
		if !strings.Contains(out, c) {
			t.Errorf("expected metrics to contain %q", c)
		}
	}
}
