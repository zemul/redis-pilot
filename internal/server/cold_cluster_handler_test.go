package server

import (
	"net/http"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestColdClusterAPI(t *testing.T) {
	s := newTestServer(t, "")
	s.cfg.GatewayTokens = map[string]string{"gw": heartbeatToken, "other": "other-gateway-token-long"}
	if err := s.db.UpsertServer("node", &apitypes.NodeServer{Endpoint: "127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"hot", "a", "b"} {
		engine := "kvrocks"
		if i == 0 {
			engine = "redis"
		}
		if err := s.db.UpsertGroup(name, &apitypes.InstanceGroupState{Engine: engine, Type: "standalone", Password: "proxy-only-secret"}); err != nil {
			t.Fatal(err)
		}
		if err := s.db.UpsertInstance(name, &apitypes.Instance{Group: name, Server: "node", Role: "master", Port: 17000 + i, DesiredState: "running"}); err != nil {
			t.Fatal(err)
		}
	}
	r := s.Router()
	base := "/api/v1/cold-clusters"
	spec := apitypes.ColdClusterSpec{ID: "cluster", GatewayID: "gw", NodeID: "node", HotInstance: "hot", Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}, {ID: "b", Instance: "b"}}}
	viewer, _ := s.jwtSign("viewer", "viewer")
	if w := doRequest(r, "GET", base, nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := doRequestWithAuth(r, "POST", base, viewer, spec); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if w := doAuthRequest(r, "POST", base, spec); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, "GET", base, viewer, nil); w.Code != 200 || strings.Contains(w.Body.String(), "secret") {
		t.Fatal(w.Code, w.Body.String())
	}
	path := "/api/v1/gateway/control/gw/clusters/cluster"
	if w := doAuthRequest(r, "GET", path, nil); w.Code != 401 {
		t.Fatal("human credential accepted", w.Code)
	}
	if w := doRequestWithAuth(r, "GET", path, heartbeatToken, nil); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, "GET", "/api/v1/gateway/control/other/clusters/cluster", "other-gateway-token-long", nil); w.Code != 403 {
		t.Fatal(w.Code)
	}
	task := apitypes.ColdTaskRequest{ID: "move", ExpectedRevision: 1, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}
	for i := 0; i < 2; i++ {
		if w := doAuthRequest(r, "POST", base+"/cluster/tasks", task); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := doRequestWithAuth(r, "POST", path+"/report", heartbeatToken, apitypes.ColdTaskReport{TaskID: "move", ToRevision: 2, Phase: "published"}); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := doAuthRequest(r, http.MethodPost, "/instance/stop", map[string]any{"name": "b", "force": true}); w.Code != 409 {
		t.Fatal("reserved shard stop must be rejected before contacting Agent", w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, "POST", path+"/report", heartbeatToken, map[string]any{"task_id": "move", "phase": "complete", "to_revision": 2, "password": "x"}); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := doRequestWithAuth(r, "POST", path+"/report", heartbeatToken, apitypes.ColdTaskReport{TaskID: "move", ToRevision: 2, Phase: "complete"}); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}
