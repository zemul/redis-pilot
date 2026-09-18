package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestDedicatedCreateAndOldAgentRefusal(t *testing.T) {
	for _, supported := range []bool{true, false} {
		t.Run(map[bool]string{true: "supported", false: "old-agent"}[supported], func(t *testing.T) {
			deleted := false
			var received apitypes.EnsureProfileRequest
			agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/instance/delete" {
					deleted = true
				}
				if (r.URL.Path != "/instance/ensure-profile" && r.URL.Path != "/instance/verify-profile") || !supported {
					http.NotFound(w, r)
					return
				}
				json.NewDecoder(r.Body).Decode(&received)
				json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: map[string]any{"operation_id": received.OperationID, "container_id": "container-1", "database_ready": true, "empty": true}})
			}))
			defer agent.Close()
			s := newTestServer(t, "")
			host, port := fakeAgentHostPort(agent.Listener.Addr().String())
			s.db.UpsertServer("srv1", &apitypes.NodeServer{Endpoint: host, AgentPort: port, Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}})
			req := apitypes.CreateInstanceRequest{Name: "dedicated", Group: "dedicated", Profile: "redis-cache", DisableProxy: true, Engine: "redis", Category: "cache", Type: "standalone", Memory: "256Mi", Server: "srv1"}
			w := doAuthRequest(s.Router(), "POST", "/instance/create", req)
			if w.Code != 202 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			s.trackProvisionTasksOnce()
			tasks, _, err := s.db.QueryTasks(store.TaskFilter{Kind: "profile.create"})
			if err != nil || len(tasks) != 1 {
				t.Fatal("missing task", err)
			}
			if supported && tasks[0].Status != "success" {
				t.Fatalf("task not completed: %+v", tasks[0])
			}
			if !supported && (tasks[0].Status != "running" || tasks[0].Phase != "retry_wait") {
				t.Fatal("old Agent must retain retryable reservation")
			}
			if deleted {
				t.Fatal("uncertain create deleted dedicated data")
			}
			if supported {
				if received.Profile != req.Profile {
					t.Fatal("profile not delivered")
				}
				st, _ := s.db.ReadInstances()
				i := st.Instances[req.Name]
				if i.Profile != req.Profile || i.Persistence.RDB || i.Persistence.AOF {
					t.Fatal("incorrect stored profile or persistence")
				}
			}
		})
	}
}
