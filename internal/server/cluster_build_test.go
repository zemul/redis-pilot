package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestClusterBuildChildrenRestartAndAtomicHandoff(t *testing.T) {
	for _, cache := range []bool{false, true} {
		t.Run(map[bool]string{false: "without-cache", true: "with-cache"}[cache], func(t *testing.T) {
			var failB atomic.Bool
			failB.Store(true)
			agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/instance/ensure-profile" && r.URL.Path != "/instance/verify-profile" {
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.Error(w, "unexpected", 500)
					return
				}
				var req apitypes.EnsureProfileRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				if req.Name == "b" && failB.Load() {
					http.Error(w, "temporarily unavailable", 503)
					return
				}
				json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: map[string]any{"operation_id": req.OperationID, "container_id": "container-" + req.Name, "database_ready": true, "empty": true}})
			}))
			defer agent.Close()
			s := newTestServer(t, "")
			s.cfg.GatewayTokens = map[string]string{"gateway": "0123456789abcdef"}
			host, port := fakeAgentHostPort(agent.Listener.Addr().String())
			if err := s.db.UpsertServer("node", &apitypes.NodeServer{Endpoint: host, AgentPort: port, Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}}); err != nil {
				t.Fatal(err)
			}
			names := []string{"a", "b"}
			if cache {
				names = append(names, "hot")
			}
			var children []string
			for _, name := range names {
				r := apitypes.CreateInstanceRequest{Name: name, Group: name, Profile: "kvrocks-storage", Engine: "kvrocks", Category: "persistent", Type: "standalone", Memory: "256Mi", Server: "node", DisableProxy: true}
				if name == "hot" {
					r.Profile, r.Engine, r.Category = "redis-cache", "redis", "cache"
				}
				w := doAuthRequest(s.Router(), "POST", "/instance/create", r)
				if w.Code != 202 {
					t.Fatalf("%d %s", w.Code, w.Body.String())
				}
				tasks, _, err := s.db.QueryTasks(store.TaskFilter{Instance: name})
				if err != nil || len(tasks) != 1 {
					t.Fatal("missing child", err)
				}
				children = append(children, tasks[0].ID)
			}
			spec := apitypes.ColdClusterSpec{ID: "build", GatewayID: "gateway", NodeID: "node", CacheEnabled: &cache, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}, {ID: "b", Instance: "b"}}}
			if cache {
				spec.HotInstance = "hot"
			}
			w := doAuthRequest(s.Router(), "POST", "/api/v1/custom-clusters/provision", map[string]any{"spec": spec, "child_tasks": children})
			if w.Code != 202 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			parents, _, _ := s.db.QueryTasks(store.TaskFilter{Kind: "cluster.provision"})
			if len(parents) != 1 {
				t.Fatal("missing parent")
			}
			parent := parents[0]
			if _, err := s.db.CreateColdCluster(spec); err == nil {
				t.Fatal("direct registration bypassed parent")
			}
			for _, id := range children {
				child, _ := s.db.GetTask(id)
				s.trackProvisionTask(*child)
			}
			s.trackClusterBuildTask(parent)
			if _, err := s.db.GetColdCluster(spec.ID); err == nil {
				t.Fatal("published before all children ready")
			}
			if err := s.db.DeleteInstance("a"); err == nil {
				t.Fatal("completed child lost parent protection")
			}
			if err := s.db.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := New(s.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.db.Close()
			failB.Store(false)
			for _, id := range children {
				child, _ := s.db.GetTask(id)
				s.trackProvisionTask(*child)
			}
			s.trackClusterBuildTask(parent)
			cluster, err := s.db.GetColdCluster(spec.ID)
			if err != nil {
				t.Fatal(err)
			}
			if cluster.UsesCache() != cache || len(cluster.Shards) != 2 || cluster.Ingress != nil {
				t.Fatalf("invalid registration %+v", cluster)
			}
			finished, _ := s.db.GetTask(parent.ID)
			if finished.Status != "success" {
				t.Fatalf("%+v", finished)
			}
			if err := s.db.DeleteInstance("a"); err == nil {
				t.Fatal("cluster resource protection did not take over")
			}
			s.trackClusterBuildTask(parent) // stale task snapshot must not redeliver
		})
	}
}
