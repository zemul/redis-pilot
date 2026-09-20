package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestColdClusterAPI(t *testing.T) {
	for _, cacheEnabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "cache", false: "without-cache"}[cacheEnabled], func(t *testing.T) {
			testCustomClusterAPI(t, cacheEnabled)
		})
	}
}

func testCustomClusterAPI(t *testing.T, cacheEnabled bool) {
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
	if !cacheEnabled {
		base = "/api/v1/custom-clusters"
		spec.CacheEnabled, spec.HotInstance = &cacheEnabled, ""
	}
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
	ingress := apitypes.ColdIngressRequest{GatewayAddress: "127.0.0.1:19555", Password: "ingress-business-secret"}
	if w := doRequestWithAuth(r, "POST", base+"/cluster/ingress", viewer, ingress); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if w := doAuthRequest(r, "POST", base+"/cluster/ingress", ingress); w.Code != 200 || strings.Contains(w.Body.String(), ingress.Password) {
		t.Fatal(w.Code, w.Body.String())
	}
	before, err := s.buildProxySnapshotForNode("node")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range before.Listeners {
		if l.Group == "cold:cluster" {
			t.Fatal("unconfirmed ingress published")
		}
	}
	if w := doRequestWithAuth(r, "POST", "/api/v1/gateway/control/gw/clusters/cluster/ingress-ready", heartbeatToken, map[string]string{"gateway_address": ingress.GatewayAddress}); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, node := range []string{"", "other", "node"} {
		snapshot, err := s.buildProxySnapshotForNode(node)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, l := range snapshot.Listeners {
			if l.Group == "cold:cluster" {
				found = true
				if l.Password != ingress.Password || l.AccessMode != "standard" || l.ReadCluster != "" {
					t.Fatal(l)
				}
			}
		}
		if found != (node == "node") {
			t.Fatal("wrong placement", node, snapshot)
		}
		for _, c := range snapshot.Clusters {
			if c.Name == "cold:cluster" && (c.Password != "" || len(c.Endpoints) != 1 || c.Endpoints[0].Port != 19555) {
				t.Fatal(c)
			}
		}
	}
	if w := doRequestWithAuth(r, "GET", base+"/cluster/ingress", viewer, nil); w.Code != 200 || strings.Contains(w.Body.String(), ingress.Password) {
		t.Fatal(w.Code, w.Body.String())
	}
	st, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	ports, err := allocProxyPorts(s.cfg.Ports, st, false)
	if err != nil || ports.MasterPort == st.ReservedProxyPorts[0] {
		t.Fatal(ports, err)
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
	} else {
		var response struct {
			Data apitypes.ColdCluster `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Data.Primaries) != 2 || strings.Contains(w.Body.String(), "secret") {
			t.Fatal(w.Body.String())
		}
		for _, p := range response.Data.Primaries {
			if p.Generation == 0 || p.Group != p.Instance || p.ShardID != p.Instance || p.Address == "" {
				t.Fatal(p)
			}
		}
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
