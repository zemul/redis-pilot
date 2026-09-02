package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestProxyHeartbeatAndStatus(t *testing.T) {
	s := newInternalTokenTestServer(t, "internal-secret")
	r := s.Router()
	heartbeat := apitypes.ProxyHeartbeat{
		ProxyID:           "proxy-a",
		Hostname:          "host-a",
		StartedAt:         "2026-09-01T00:00:00Z",
		SnapshotVersion:   "v12",
		SnapshotAppliedAt: "2026-09-01T00:01:00Z",
		ListenerCount:     4,
		BackendCount:      6,
	}

	if w := doRequest(r, http.MethodPost, "/api/v1/proxy/heartbeat", heartbeat); w.Code != http.StatusUnauthorized {
		t.Fatalf("heartbeat without internal token = %d, want 401", w.Code)
	}
	if w := doRequestWithAuth(r, http.MethodPost, "/api/v1/proxy/heartbeat", "internal-secret", heartbeat); w.Code != http.StatusOK {
		t.Fatalf("heartbeat = %d: %s", w.Code, w.Body.String())
	}

	w := doAuthRequest(r, http.MethodGet, "/proxy/status", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var envelope struct {
		Success bool                        `json:"success"`
		Data    apitypes.ProxyStatusSummary `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Success || envelope.Data.Online != 1 || envelope.Data.Total != 1 {
		t.Fatalf("unexpected summary: %+v", envelope.Data)
	}
	got := envelope.Data.Proxies[0]
	if got.ProxyID != "proxy-a" || got.Status != "online" || got.SnapshotVersion != "v12" || got.ListenerCount != 4 {
		t.Fatalf("unexpected proxy status: %+v", got)
	}
}

func TestProxyStatusDerivesStaleAndOfflineFromLastSeen(t *testing.T) {
	s := newTestServer(t, "")
	s.proxyRuntime["stale"] = proxyRuntimeObservation{
		heartbeat: apitypes.ProxyHeartbeat{ProxyID: "stale"},
		received:  time.Now().Add(-45 * time.Second),
	}
	s.proxyRuntime["offline"] = proxyRuntimeObservation{
		heartbeat: apitypes.ProxyHeartbeat{ProxyID: "offline"},
		received:  time.Now().Add(-90 * time.Second),
	}

	w := doAuthRequest(s.Router(), http.MethodGet, "/proxy/status", nil)
	var envelope struct {
		Data apitypes.ProxyStatusSummary `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data.Online != 0 || envelope.Data.Total != 2 {
		t.Fatalf("unexpected counts: %+v", envelope.Data)
	}
	statuses := map[string]string{}
	for _, proxy := range envelope.Data.Proxies {
		statuses[proxy.ProxyID] = proxy.Status
	}
	if statuses["stale"] != "stale" || statuses["offline"] != "offline" {
		t.Fatalf("unexpected statuses: %+v", statuses)
	}
}

func TestProxyHeartbeatRejectsMissingID(t *testing.T) {
	s := newTestServer(t, "")
	w := doRequest(s.Router(), http.MethodPost, "/proxy/heartbeat", apitypes.ProxyHeartbeat{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing proxy_id = %d, want 400", w.Code)
	}
}
