package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestTaskQueryAndDetail(t *testing.T) {
	s := newTestServer(t, "")
	if err := s.db.CreateTask(store.Task{
		ID: "backup-order-1", Kind: "backup.create", Status: "running",
		Operator: "admin", Source: "manual", Trigger: "manual",
		GroupName: "order", InstanceName: "order-1", ServerName: "srv-a",
	}); err != nil {
		t.Fatal(err)
	}

	w := doAuthRequest(s.Router(), http.MethodGet, "/tasks?status=active&q=order", nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"total":1`) || !strings.Contains(w.Body.String(), `"instance_name":"order-1"`) {
		t.Fatalf("task query failed: code=%d body=%s", w.Code, w.Body.String())
	}
	w = doAuthRequest(s.Router(), http.MethodGet, "/tasks/backup-order-1", nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"operator":"admin"`) {
		t.Fatalf("task detail failed: code=%d body=%s", w.Code, w.Body.String())
	}
	w = doAuthRequest(s.Router(), http.MethodGet, "/tasks/missing", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing task code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestTaskCancelForwardsToAgentAndMarksRequest(t *testing.T) {
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/instance/bigkeys/cancel" || r.Method != http.MethodPost {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: map[string]string{"status": "canceling"}})
	}))
	defer agent.Close()
	host, port := fakeAgentHostPort(agent.Listener.Addr().String())
	s := newTestServer(t, "")
	if err := s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{"srv-a": {Endpoint: host, AgentPort: port}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.CreateTask(store.Task{ID: "scan-1", Kind: "diagnostic.bigkeys", Status: "running", Cancelable: true, ServerName: "srv-a"}); err != nil {
		t.Fatal(err)
	}
	w := doAuthRequest(s.Router(), http.MethodPost, "/tasks/scan-1/cancel", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("cancel code=%d body=%s", w.Code, w.Body.String())
	}
	task, err := s.db.GetTask("scan-1")
	if err != nil || !task.CancelRequested {
		t.Fatalf("cancel not persisted: task=%+v err=%v", task, err)
	}
}
