package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandlerExposesProcessAndSnapshotMetrics 验证 /metrics 暴露了 Go/process
// collector 指标与新增的发现链路指标，且不再暴露已删除的 backend_health。
func TestHandlerExposesProcessAndSnapshotMetrics(t *testing.T) {
	m := New()
	// 触发一次快照链路指标写入，确保 series 存在
	m.IncSnapshotFetchSuccess()
	m.IncSnapshotFetchFailure()
	m.SetSnapshotApplied("v1", 1700000000)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	mustContain := []string{
		"go_goroutines",                                      // GoCollector
		"process_open_fds",                                   // ProcessCollector
		"redis_pilot_proxy_snapshot_fetch_total",             // ③
		"redis_pilot_proxy_server_disconnect_total",          // ③
		"redis_pilot_proxy_snapshot_applied_timestamp_seconds", // ③
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("expected /metrics to contain %q, not found", s)
		}
	}
	// 已删除的假指标不应再出现
	if strings.Contains(out, "backend_health") {
		t.Errorf("backend_health should have been removed but is still exposed")
	}
	// 旧的裸 proxy_ 前缀不应再出现（已统一为 redis_pilot_proxy_）
	if strings.Contains(out, "\nproxy_") || strings.HasPrefix(out, "proxy_") {
		t.Errorf("legacy bare proxy_ prefix should no longer be exposed")
	}
}
