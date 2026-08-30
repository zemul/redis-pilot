package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestOperationTrackerPersistsRestoreResultAndReleasesLock(t *testing.T) {
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: map[string]interface{}{
			"id": "restore-1", "instance": "redis-1", "status": "success", "backup": "2026-08-27T01:02:03",
		}})
	}))
	defer agent.Close()
	host, port := fakeAgentHostPort(agent.Listener.Addr().String())
	s := newTestServer(t, "")
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: host, AgentPort: port}}})
	instances := &apitypes.InstancesState{Groups: map[string]*apitypes.InstanceGroupState{"g": testGroup("redis-1")}, Instances: map[string]*apitypes.Instance{
		"redis-1": {Group: "g", Server: "srv1", DesiredState: "running", ObservedStatus: "running"},
	}}
	if err := state.TryAcquireLock(instances.Instances["redis-1"], "restore-1", "restore", 300); err != nil {
		t.Fatal(err)
	}
	if err := s.db.WriteInstances(instances); err != nil {
		t.Fatal(err)
	}
	payload := operationTaskPayload{Instance: "redis-1", Server: "srv1", Operator: "admin", BackupTs: "2026-08-27T01:02:03", LockSessionID: "restore-1", LockInstances: []string{"redis-1"}}
	if err := s.db.CreateTask(store.Task{ID: "restore-1", Kind: "backup.restore", Status: "running", Payload: marshalTaskPayload(payload)}); err != nil {
		t.Fatal(err)
	}

	s.trackOperationTasksOnce()
	task, err := s.db.GetTask("restore-1")
	if err != nil || task.Status != "success" {
		t.Fatalf("task not completed: task=%+v err=%v", task, err)
	}
	got, _ := s.db.ReadInstances()
	if got.Instances["redis-1"].Lock != nil {
		t.Fatalf("restore lock was not released: %+v", got.Instances["redis-1"].Lock)
	}
	w := doAuthRequest(s.Router(), http.MethodGet, "/backup/status?name=redis-1&task_id=restore-1", nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"success"`) {
		t.Fatalf("persisted status query failed: code=%d body=%s", w.Code, w.Body.String())
	}
	records, _, err := s.audit.Query(store.AuditFilter{Action: "backup.restore"})
	if err != nil || len(records) != 1 || records[0].Result != "success" {
		t.Fatalf("final audit missing: records=%+v err=%v", records, err)
	}
}

func TestOperationTrackerPersistsCanceledBigkeysPartialResult(t *testing.T) {
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: map[string]interface{}{
			"id": "scan-1", "instance": "redis-1", "status": "canceled", "progress": 42,
			"result": map[string]interface{}{"scanned": 42, "incomplete": true, "keys": []interface{}{}},
		}})
	}))
	defer agent.Close()
	host, port := fakeAgentHostPort(agent.Listener.Addr().String())
	s := newTestServer(t, "")
	if err := s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: host, AgentPort: port}}}); err != nil {
		t.Fatal(err)
	}
	payload := operationTaskPayload{Instance: "redis-1", Server: "srv1", Group: "g", Operator: "admin", Source: "manual", Trigger: "manual"}
	if err := s.db.CreateTask(store.Task{ID: "scan-1", Kind: "diagnostic.bigkeys", Status: "running", Cancelable: true, Payload: marshalTaskPayload(payload)}); err != nil {
		t.Fatal(err)
	}
	s.trackOperationTasksOnce()
	task, err := s.db.GetTask("scan-1")
	if err != nil || task.Status != "canceled" || task.Progress != 42 || !strings.Contains(task.Result, `"incomplete":true`) || task.Cancelable {
		t.Fatalf("partial result not persisted: task=%+v err=%v", task, err)
	}
}
