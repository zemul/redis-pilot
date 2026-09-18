package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func clusterSpecFixture(t *testing.T) (*Server, apitypes.ClusterCreateRequest, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/instance/ensure-profile" && r.URL.Path != "/instance/verify-profile" {
			t.Errorf("unexpected mutation %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
			return
		}
		var req apitypes.EnsureProfileRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: map[string]any{"operation_id": req.OperationID, "container_id": "container-" + req.Name, "database_ready": true, "empty": true}})
	}))
	t.Cleanup(a.Close)
	s := newTestServer(t, "")
	s.cfg.GatewayTokens = map[string]string{"gateway": "0123456789abcdef"}
	host, port := fakeAgentHostPort(a.Listener.Addr().String())
	if err := s.db.UpsertServer("node", &apitypes.NodeServer{Endpoint: host, AgentPort: port, Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi", Disk: "500Gi"}}); err != nil {
		t.Fatal(err)
	}
	r := apitypes.ClusterCreateRequest{ID: "orders", GatewayID: "gateway", NodeID: "node", Cold: apitypes.ClusterColdSpec{Count: 2, ClusterResourceSpec: apitypes.ClusterResourceSpec{Memory: "2Gi", Disk: "50Gi"}}}
	return s, r, &calls
}

func TestClusterSpecAtomicCreateReplayAndRestart(t *testing.T) {
	for _, policy := range []string{"", "invalidate", "update_existing"} {
		t.Run("policy-"+policy, func(t *testing.T) {
			s, req, calls := clusterSpecFixture(t)
			if policy != "" {
				req.Cache = &apitypes.ClusterCacheSpec{ClusterResourceSpec: apitypes.ClusterResourceSpec{Memory: "4Gi"}, WritePolicy: policy}
			}
			router := s.Router()
			var wg sync.WaitGroup
			ids := make(chan string, 6)
			for i := 0; i < 6; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					w := doAuthRequest(router, "POST", "/api/v1/custom-clusters/create", req)
					if w.Code != 202 {
						t.Errorf("%d %s", w.Code, w.Body.String())
						return
					}
					var response struct {
						Data struct {
							TaskID string `json:"task_id"`
						} `json:"data"`
					}
					if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
						t.Error(err)
					}
					ids <- response.Data.TaskID
				}()
			}
			wg.Wait()
			close(ids)
			parentID := ""
			for id := range ids {
				if parentID != "" && parentID != id {
					t.Fatal("duplicate parent tasks")
				}
				parentID = id
			}
			if parentID == "" {
				t.Fatal("missing parent")
			}
			if calls.Load() != 0 {
				t.Fatal("Agent contacted during reservation")
			}
			expected := 2
			if req.Cache != nil {
				expected++
			}
			st, _ := s.db.ReadInstances()
			tasks, _, _ := s.db.QueryTasks(store.TaskFilter{Limit: 100})
			if len(st.Instances) != expected || len(st.Groups) != expected || len(tasks) != expected+1 {
				t.Fatalf("partial or duplicate plan: instances=%d tasks=%d", len(st.Instances), len(tasks))
			}
			ports := map[int]bool{}
			for name, i := range st.Instances {
				if ports[i.Port] {
					t.Fatal("duplicate ports")
				}
				ports[i.Port] = true
				if st.Groups[i.Group].Proxy != nil || i.Lock == nil {
					t.Fatal("resource not protected", name)
				}
			}
			changed := req
			changed.Cold.Count = 3
			w := doAuthRequest(router, "POST", "/api/v1/custom-clusters/create", changed)
			if w.Code != 409 {
				t.Fatalf("changed spec accepted: %d %s", w.Code, w.Body.String())
			}
			if err := s.db.Close(); err != nil {
				t.Fatal(err)
			}
			resumed, err := New(s.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer resumed.db.Close()
			w = doAuthRequest(resumed.Router(), "POST", "/api/v1/custom-clusters/create", req)
			if w.Code != 202 {
				t.Fatalf("replay after restart failed: %d %s", w.Code, w.Body.String())
			}
			children, err := resumed.db.ClusterBuildChildren(parentID)
			if err != nil {
				t.Fatal(err)
			}
			for _, child := range children {
				resumed.trackProvisionTask(child)
			}
			parent, _ := resumed.db.GetTask(parentID)
			resumed.trackClusterBuildTask(*parent)
			cluster, err := resumed.db.GetColdCluster(req.ID)
			if err != nil {
				t.Fatal(err)
			}
			if cluster.Ingress != nil || cluster.WritePolicy != policy || cluster.UsesCache() != (policy != "") {
				t.Fatalf("invalid result %+v", cluster)
			}
			bootstrap := apitypes.GatewayBootstrapRequest{ServerEndpoint: "http://127.0.0.1:8080", Listen: "127.0.0.1:17579", AdminListen: "127.0.0.1:19500", StateDir: "/var/lib/redis-gateway/orders", TokenFile: "/etc/redis-gateway/orders.secret"}
			path := "/api/v1/custom-clusters/orders/gateway-config"
			viewer, _ := resumed.jwtSign("viewer", "viewer")
			if w := doRequestWithAuth(resumed.Router(), "POST", path, viewer, bootstrap); w.Code != 403 {
				t.Fatal(w.Code)
			}
			if w := doRequest(resumed.Router(), "POST", path, bootstrap); w.Code != 401 {
				t.Fatal(w.Code)
			}
			countBefore := calls.Load()
			w = doAuthRequest(resumed.Router(), "POST", path, bootstrap)
			if w.Code != 200 || strings.Contains(w.Body.String(), resumed.cfg.GatewayTokens[req.GatewayID]) {
				t.Fatal(w.Code, w.Body.String())
			}
			var exported struct {
				Data apitypes.GatewayBootstrap `json:"data"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &exported); err != nil {
				t.Fatal(err)
			}
			cfg, err := gateway.DecodeConfig(strings.NewReader(exported.Data.YAML))
			if err != nil || cfg.ServiceID != req.ID || len(cfg.Cache.Sharding.Nodes) != 2 {
				t.Fatal("invalid generated config", err)
			}
			after, err := resumed.db.GetColdCluster(req.ID)
			if err != nil || after.Ingress != nil || calls.Load() != countBefore {
				t.Fatal("config export mutated deployment")
			}
			count := calls.Load()
			w = doAuthRequest(resumed.Router(), "POST", "/api/v1/custom-clusters/create", req)
			if w.Code != 202 || calls.Load() != count {
				t.Fatal("completed replay did new work")
			}
		})
	}
}

func TestClusterSpecFailureRollsBackAllReservations(t *testing.T) {
	for _, failure := range []string{"capacity", "late-name-conflict", "gateway-conflict", "bad-placement"} {
		t.Run(failure, func(t *testing.T) {
			s, req, calls := clusterSpecFixture(t)
			switch failure {
			case "capacity":
				req.Cache = &apitypes.ClusterCacheSpec{ClusterResourceSpec: apitypes.ClusterResourceSpec{Memory: "64Gi"}}
			case "late-name-conflict":
				if err := s.db.UpsertGroup("orders-cold-2", &apitypes.InstanceGroupState{Type: "standalone"}); err != nil {
					t.Fatal(err)
				}
			case "gateway-conflict":
				other := req
				other.ID = "first"
				w := doAuthRequest(s.Router(), "POST", "/api/v1/custom-clusters/create", other)
				if w.Code != 202 {
					t.Fatal(w.Body.String())
				}
			case "bad-placement":
				req.Cold.Servers = []string{"node", "missing"}
			}
			before, _ := s.db.ReadInstances()
			beforeTasks, _, _ := s.db.QueryTasks(store.TaskFilter{Limit: 100})
			w := doAuthRequest(s.Router(), "POST", "/api/v1/custom-clusters/create", req)
			if w.Code < 400 {
				t.Fatal("invalid plan accepted")
			}
			after, _ := s.db.ReadInstances()
			afterTasks, _, _ := s.db.QueryTasks(store.TaskFilter{Limit: 100})
			if len(before.Instances) != len(after.Instances) || len(before.Groups) != len(after.Groups) || len(beforeTasks) != len(afterTasks) || calls.Load() != 0 {
				t.Fatal("failed plan left partial resources or Agent side effects")
			}
		})
	}
}
