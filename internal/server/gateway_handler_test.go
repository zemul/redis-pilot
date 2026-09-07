package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestGatewayDraftAPI(t *testing.T) {
	s := newTestServer(t, "")
	if err := s.db.UpsertServer("node", &apitypes.NodeServer{Endpoint: "127.0.0.1", AgentToken: "node-secret"}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.UpsertGroup("group", &apitypes.InstanceGroupState{Engine: "redis", Type: "standalone", Password: "redis-secret", CurrentMaster: "group-0", Proxy: &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500}}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.UpsertInstance("group-0", &apitypes.Instance{Group: "group", Server: "node", Role: "master", Port: 6389, DesiredState: "running", ObservedStatus: "running"}); err != nil {
		t.Fatal(err)
	}
	r := s.Router()
	base := "/api/v1/gateway/drafts"
	spec := apitypes.GatewayDraftSpec{Node: "node", Group: "group", DataPort: 17379, AdminPort: 19200}
	create := map[string]any{"name": "gw", "spec": spec}
	if w := doRequest(r, http.MethodGet, base, nil); w.Code != 401 {
		t.Fatalf("unauthenticated: %d", w.Code)
	}
	viewer, err := s.jwtSign("viewer", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	if w := doRequestWithAuth(r, http.MethodPost, base, viewer, create); w.Code != 403 {
		t.Fatalf("viewer write: %d", w.Code)
	}
	before, err := s.buildProxySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Listeners) == 0 {
		t.Fatal("snapshot regression fixture has no active routes")
	}
	w := doAuthRequest(r, http.MethodPost, base, create)
	if w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	var response struct {
		Data apitypes.GatewayDraft `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	v := response.Data
	if v.Status != "draft" || v.Revision == "" {
		t.Fatalf("bad response %s", w.Body.String())
	}
	if w := doAuthRequest(r, http.MethodPost, base, create); w.Code != 409 {
		t.Fatalf("duplicate %d %s", w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, http.MethodGet, base, viewer, nil); w.Code != 200 || strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("viewer read leaked/failed %s", w.Body.String())
	}
	spec.DataPort = 17380
	update := map[string]any{"spec": spec, "revision": v.Revision}
	if w := doAuthRequest(r, http.MethodPut, base+"/gw", update); w.Code != 200 {
		t.Fatalf("update: %d %s", w.Code, w.Body.String())
	}
	if w := doAuthRequest(r, http.MethodPut, base+"/gw", update); w.Code != 409 {
		t.Fatalf("stale update %d", w.Code)
	}
	if w := doAuthRequest(r, http.MethodDelete, base+"/gw", map[string]any{"revision": v.Revision}); w.Code != 409 {
		t.Fatalf("stale delete %d", w.Code)
	}
	current, err := s.db.GetGatewayDraft("gw")
	if err != nil {
		t.Fatal(err)
	}
	if w := doAuthRequest(r, http.MethodDelete, base+"/gw", map[string]any{"revision": current.Revision}); w.Code != 200 {
		t.Fatalf("delete %d", w.Code)
	}
	if w := doAuthRequest(r, http.MethodGet, base+"/gw", nil); w.Code != 404 {
		t.Fatalf("missing %d", w.Code)
	}
	after, err := s.buildProxySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if before.Version != after.Version {
		t.Fatal("draft CRUD changed proxy snapshot")
	}
	records, _, err := s.db.QueryAudit(store.AuditFilter{Action: "gateway.draft.update", Limit: 10})
	if err != nil || len(records) != 2 {
		t.Fatalf("audit: %d %v", len(records), err)
	}
	for _, record := range records {
		if record.Operator != "admin" {
			t.Fatalf("wrong audit actor: %+v", record)
		}
	}
}

func TestGatewayDraftRejectsUnsupportedInput(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	base := "/api/v1/gateway/drafts"
	for _, body := range []any{
		map[string]any{"name": "gw", "password": "must-not-be-accepted"},
		map[string]any{"name": "gw", "spec": map[string]any{"backend_url": "http://example.test"}},
		map[string]any{"name": strings.Repeat("x", 9000)},
		nil,
	} {
		w := doAuthRequest(r, http.MethodPost, base, body)
		if w.Code != 400 {
			t.Fatalf("invalid body accepted: %d %s", w.Code, w.Body.String())
		}
	}
	if w := doAuthRequest(r, http.MethodPut, base+"/gw", map[string]any{}); w.Code != 400 {
		t.Fatal("missing revision accepted")
	}
}
