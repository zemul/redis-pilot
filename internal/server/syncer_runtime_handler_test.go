package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func serverSyncerFixture() apitypes.SyncerHeartbeat {
	return apitypes.SyncerHeartbeat{SyncerID: "syncer-1", NodeID: "node-1", BootID: "boot-1", Sequence: 1, BuildVersion: "test", State: "running", LastError: "none", Running: true,
		Tasks: []apitypes.SyncTaskReport{{TaskID: "task-1", IdentityDigest: strings.Repeat("a", 64), State: "running", LastError: "none", Running: true}}}
}

func TestSyncerHeartbeatAPIAuthAndPersistence(t *testing.T) {
	s := newTestServer(t, "")
	const token = "syncer-dedicated-token-long-enough"
	s.cfg.SyncerTokens = map[string]string{"syncer-1": token, "other": "other-syncer-dedicated-token"}
	r := s.Router()
	path := "/api/v1/syncer/heartbeat/syncer-1"
	v := serverSyncerFixture()
	before, err := s.buildProxySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, credential := range []string{"", "wrong", heartbeatToken, "admin"} {
		if w := doRequestWithAuth(r, "POST", path, credential, v); w.Code != 401 {
			t.Fatalf("machine auth bypass: %d", w.Code)
		}
	}
	if w := doAuthRequest(r, "POST", path, v); w.Code != 401 {
		t.Fatal("JWT accepted for machine report")
	}
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 200 {
		t.Fatal("duplicate rejected")
	}
	v.Sequence = 2
	v.State = "degraded"
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	v.Sequence = 1
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 409 {
		t.Fatal("stale report accepted")
	}
	v = serverSyncerFixture()
	v.SyncerID = "other"
	if w := doRequestWithAuth(r, "POST", "/api/v1/syncer/heartbeat/other", token, v); w.Code != 401 {
		t.Fatal("token escaped identity")
	}
	if w := doRequestWithAuth(r, "POST", "/api/v1/syncer/heartbeat/other", s.cfg.SyncerTokens["other"], v); w.Code != 409 {
		t.Fatal("task reassigned")
	}
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 400 {
		t.Fatal("body identity mismatch accepted")
	}
	if w := doRequest(r, "GET", "/api/v1/syncer/runtime", nil); w.Code != 401 {
		t.Fatal("public runtime")
	}
	viewer, err := s.jwtSign("viewer", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	w := doRequestWithAuth(r, "GET", "/api/v1/syncer/runtime", viewer, nil)
	var reply struct {
		Data apitypes.SyncerRuntime `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || w.Code != 200 || len(reply.Data.Bindings) != 1 || len(reply.Data.Observations) != 1 || reply.Data.Observations[0].Status != "degraded" {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	if strings.Contains(w.Body.String(), token) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("unsafe runtime response")
	}
	after, err := s.buildProxySnapshot()
	if err != nil || before.Version != after.Version {
		t.Fatal("sync observation altered routing", err)
	}
	s.cfg.SyncerTokens = nil
	if w := doRequestWithAuth(r, "POST", path, token, serverSyncerFixture()); w.Code != 401 {
		t.Fatal("empty token map bypassed auth")
	}
}

func TestSyncerHeartbeatAPIRejectsMalformedReports(t *testing.T) {
	s := newTestServer(t, "")
	const token = "syncer-test-credential-long-enough"
	s.cfg.SyncerTokens = map[string]string{"syncer-1": token}
	r := s.Router()
	valid, _ := json.Marshal(serverSyncerFixture())
	for _, body := range []string{"null", "{}", string(valid) + " {}", strings.Replace(string(valid), `"state":"running"`, `"state":"SENSITIVE"`, 1), strings.Replace(string(valid), `"task_id":"task-1"`, `"task_id":"task-1","password":"SENSITIVE"`, 1), `{"padding":"` + strings.Repeat("x", 17000) + `"}`} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/syncer/heartbeat/syncer-1", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, request)
		if w.Code != 400 || strings.Contains(w.Body.String(), "SENSITIVE") {
			t.Fatal("invalid/redaction", w.Code, w.Body.String())
		}
	}
	runtime, err := s.db.ListSyncerRuntime()
	if err != nil || len(runtime.Bindings) != 0 || len(runtime.Observations) != 0 {
		t.Fatal("invalid reports wrote state", err)
	}
}
