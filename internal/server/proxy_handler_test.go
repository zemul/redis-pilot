package server

import (
	"net/http"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestProxySnapshot(t *testing.T) {
	s := newTestServer(t, "")
	if err := s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: "10.0.0.1"},
			"srv2": {Endpoint: "10.0.0.2"},
		},
	}); err != nil {
		t.Fatalf("WriteNode() error = %v", err)
	}
	if err := s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"cache": {
				Type:          "replication",
				CurrentMaster: "cache-1",
				Envoy: &apitypes.EnvoyConfig{
					AutoPort:   16379,
					MasterPort: 16500,
				},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Group:    "cache",
				Role:     "master",
				Server:   "srv1",
				Port:     6379,
				Password: "secret",
				Status:   "running",
			},
			"cache-2": {
				Group:  "cache",
				Role:   "replica",
				Server: "srv2",
				Port:   6379,
				Status: "running",
			},
		},
	}); err != nil {
		t.Fatalf("WriteInstances() error = %v", err)
	}

	snapshot, err := s.buildProxySnapshot()
	if err != nil {
		t.Fatalf("buildProxySnapshot() error = %v", err)
	}
	if snapshot.Version == "" {
		t.Fatal("snapshot version should not be empty")
	}
	if len(snapshot.Listeners) != 2 {
		t.Fatalf("expected 2 listeners, got %d", len(snapshot.Listeners))
	}
	if len(snapshot.Clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(snapshot.Clusters))
	}

	w := doRequest(s.Router(), http.MethodGet, "/api/v1/proxy/snapshot", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}
