package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type replicaAgentTransport struct{}

func (replicaAgentTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var req apitypes.EnsureProfileRequest
	if r.Body != nil {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
	}
	b, _ := json.Marshal(apitypes.APIResponse{Success: true, Data: map[string]any{"operation_id": req.OperationID, "container_id": "owned-" + req.Name, "database_ready": true, "empty": true}})
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(b))), Header: make(http.Header)}, nil
}

func TestClusterReplicaCreationAndSentinelAssignment(t *testing.T) {
	old, oldLong := agentHTTPClient, agentHTTPClientLong
	agentHTTPClient = &http.Client{Transport: replicaAgentTransport{}}
	agentHTTPClientLong = agentHTTPClient
	defer func() { agentHTTPClient, agentHTTPClientLong = old, oldLong }()
	for _, engine := range []string{"redis", "kvrocks"} {
		t.Run(engine, func(t *testing.T) {
			s, req, _ := clusterSpecFixture(t)
			s.cfg.Sentinel.Enabled = true
			for i := 1; i <= 2; i++ {
				if err := s.db.UpsertServer(fmt.Sprintf("replica%d", i), &apitypes.NodeServer{Endpoint: fmt.Sprintf("10.0.0.%d", i), AgentPort: 18080, Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi", Disk: "500Gi"}}); err != nil {
					t.Fatal(err)
				}
			}
			req.Engine, req.Cold.Count, req.Cold.Replicas = engine, 1, 2
			req.Cold.Servers, req.Cold.ReplicaServers = []string{"node"}, [][]string{{"replica1", "replica2"}}
			w := doAuthRequest(s.Router(), "POST", "/api/v1/custom-clusters/create", req)
			if w.Code != 202 {
				t.Fatal(w.Code, w.Body.String())
			}
			var reply struct {
				Data struct {
					TaskID string `json:"task_id"`
				} `json:"data"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			st, _ := s.db.ReadInstances()
			nodes, _ := s.db.ReadNode()
			if len(st.Instances) != 3 || len(s.buildSentinelMasters(nodes, st)) != 0 {
				t.Fatal("partial creation monitored")
			}
			children, err := s.db.ClusterBuildChildren(reply.Data.TaskID)
			if err != nil || len(children) != 3 {
				t.Fatal(children, err)
			}
			for _, child := range children {
				var p profileTaskPayload
				if err := json.Unmarshal([]byte(child.Payload), &p); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(child.InstanceName, "replica") && (p.ReplicaOf == "" || p.Type != "replication") {
					t.Fatal("missing replica configuration")
				}
				s.trackProvisionTask(child)
				updated, _ := s.db.GetTask(child.ID)
				if updated.Status != "success" {
					t.Fatal(updated)
				}
			}
			s.cfg.Sentinel.Enabled = false // no external Sentinel I/O in this unit test
			parent, _ := s.db.GetTask(reply.Data.TaskID)
			s.trackClusterBuildTask(*parent)
			parent, _ = s.db.GetTask(parent.ID)
			if parent.Status != "success" {
				t.Fatal(parent)
			}
			before, err := s.db.ColdAssignment("gateway", req.ID)
			if err != nil || len(before.Primaries) != 1 || before.Primaries[0].Replicas != 2 {
				t.Fatal(before, err)
			}
			st, _ = s.db.ReadInstances()
			if len(s.buildSentinelMasters(nodes, st)) != 1 {
				t.Fatal("registered shard not monitored")
			}
			replica := st.Instances["orders-cold-1-replica-1"]
			if err := s.handleSentinelFailover("orders-cold-1", fmt.Sprintf("10.0.0.1:%d", replica.Port), "test", "test"); err != nil {
				t.Fatal(err)
			}
			after, err := s.db.ColdAssignment("gateway", req.ID)
			if err != nil || after.Primaries[0].Instance != "orders-cold-1-replica-1" || after.Primaries[0].Generation <= before.Primaries[0].Generation {
				t.Fatal(after, err)
			}
			if err := s.db.DeleteInstance("orders-cold-1-replica-2"); err == nil {
				t.Fatal("reserved replica deleted")
			}
		})
	}
}
