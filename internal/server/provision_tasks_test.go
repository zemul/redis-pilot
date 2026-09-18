package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestProvisionTaskRestartRetryAndProtection(t *testing.T) {
	requests := make(chan apitypes.EnsureProfileRequest, 4)
	var calls atomic.Int32
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/instance/ensure-profile" && r.URL.Path != "/instance/verify-profile" {
			t.Errorf("unexpected Agent mutation %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
			return
		}
		var req apitypes.EnsureProfileRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if r.URL.Path == "/instance/ensure-profile" {
			requests <- req
		}
		if r.URL.Path == "/instance/ensure-profile" && calls.Add(1) == 1 {
			http.Error(w, "creation may already have completed", 503)
			return
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: map[string]any{"operation_id": req.OperationID, "container_id": "same-container", "database_ready": true, "empty": true}})
	}))
	defer agent.Close()
	s := newTestServer(t, "")
	host, port := fakeAgentHostPort(agent.Listener.Addr().String())
	if err := s.db.UpsertServer("node", &apitypes.NodeServer{Endpoint: host, AgentPort: port, Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}}); err != nil {
		t.Fatal(err)
	}
	r := apitypes.CreateInstanceRequest{Name: "task-cache", Group: "task-cache", Profile: "redis-cache", Engine: "redis", Type: "standalone", Category: "cache", Memory: "256Mi", Server: "node", DisableProxy: true}
	w := doAuthRequest(s.Router(), "POST", "/instance/create", r)
	if w.Code != 202 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatal("Agent called before asynchronous task dispatch")
	}
	w = doAuthRequest(s.Router(), "POST", "/instance/create", r)
	if w.Code != 409 {
		t.Fatal("duplicate create did not return conflict")
	}
	tasks, _, _ := s.db.QueryTasks(store.TaskFilter{Kind: "profile.create"})
	if len(tasks) != 1 {
		t.Fatal("missing durable task")
	}
	id := tasks[0].ID
	// Simulate a stale lock and an observation that the container is absent.
	if err := s.db.WithInstances(func(st *apitypes.InstancesState) error {
		i := st.Instances[r.Name]
		i.Lock.AcquiredAt = time.Now().Add(-time.Hour).Format(time.RFC3339)
		i.ObservedStatus = apitypes.ObservedMissing
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	st, _ := s.db.ReadInstances()
	if err := state.TryAcquireLock(st.Instances[r.Name], id, "delete", 300); err == nil {
		t.Fatal("task ID bypassed durable lock")
	}
	mem, cpu, _ := computeAllocated(st, "node")
	if mem == 0 || cpu == 0 {
		t.Fatal("pending resources stopped consuming capacity")
	}
	for _, path := range []string{"/instance/stop", "/instance/delete"} {
		w := doAuthRequest(s.Router(), "POST", path, map[string]any{"name": r.Name, "clean_data": true})
		if w.Code < 400 {
			t.Fatalf("%s allowed during provisioning", path)
		}
	}
	s.trackProvisionTasksOnce()
	first := <-requests
	task, _ := s.db.GetTask(id)
	if task.Phase != "retry_wait" || task.Status != "running" {
		t.Fatalf("%+v", task)
	}
	s.trackProvisionTasksOnce()
	if calls.Load() != 1 {
		t.Fatal("retry delay ignored")
	}
	// Restart Server from the same SQLite database, without reconstructing a request.
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := New(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.db.Close()
	task, err = resumed.db.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	// Direct invocation avoids a 30-second wall-clock wait in the unit test.
	resumed.trackProvisionTask(*task)
	second := <-requests
	b1, _ := json.Marshal(first)
	b2, _ := json.Marshal(second)
	if string(b1) != string(b2) {
		t.Fatal("retry changed operation identity or allocated resources")
	}
	task, _ = resumed.db.GetTask(id)
	st, _ = resumed.db.ReadInstances()
	if task.Status != "success" || st.Instances[r.Name].Lock != nil || st.Instances[r.Name].ObservedStatus != apitypes.ObservedRunning {
		t.Fatalf("task=%+v instance=%+v", task, st.Instances[r.Name])
	}
	resumed.trackProvisionTasksOnce()
	if calls.Load() != 2 {
		t.Fatal("completed task was redelivered")
	}
	w = doAuthRequest(resumed.Router(), "GET", "/tasks/"+id, nil)
	if w.Code != 200 {
		t.Fatal("task not queryable")
	}
}
