package server

import (
	"net/http"
	"strings"
	"testing"
)

// TestMetricsEndpointPublic 验证 /metrics 为公开路由（无需鉴权）、返回 Prometheus
// 文本格式，且暴露 Server 自有指标与进程指标。
func TestMetricsEndpointPublic(t *testing.T) {
	s := newTestServer(t, "") // 空 token 也不影响：/metrics 在公开路由区
	// 不带任何 Authorization 头，直接请求
	w := doRequest(s.Router(), http.MethodGet, "/metrics", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /metrics: got %d, want 200 (should be public, no auth)", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{
		"redis_pilot_server_incident_scrape_error",      // custom collector 始终输出
		"redis_pilot_server_reconcile_last_timestamp_seconds", // 无标签 Gauge，始终输出
		"go_goroutines",                                 // GoCollector
		"process_open_fds",                              // ProcessCollector
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}
