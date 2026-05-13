package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestSelectSentinelNodes_ConfiguredOnly(t *testing.T) {
	s := newTestServer(t, "")
	s.cfg.Sentinel = SentinelConfig{
		Enabled: true,
		Nodes:   []string{"srv-c", "srv-a", "srv-c", "missing"},
		Quorum:  2,
	}
	pool := &apitypes.PoolState{Servers: map[string]*apitypes.PoolServer{
		"srv-a": {Status: "healthy", Labels: map[string]string{"zone": "az-1", "role": "production"}},
		"srv-b": {Status: "healthy", Labels: map[string]string{"zone": "az-1", "role": "production"}},
		"srv-c": {Status: "healthy", Labels: map[string]string{"zone": "az-2", "role": "production"}},
		"srv-d": {Status: "healthy", Labels: map[string]string{"zone": "az-3", "role": "standby"}},
	}}
	got := s.selectSentinelNodes(pool)
	want := []string{"srv-c", "srv-a"}
	if len(got) != len(want) {
		t.Fatalf("expected configured sentinel nodes %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected configured sentinel nodes %v, got %v", want, got)
		}
	}
}

func TestBuildSentinelMasters(t *testing.T) {
	s := &Server{cfg: &Config{Sentinel: SentinelConfig{
		DownAfterMilliseconds: 6000,
		FailoverTimeout:       40000,
		ParallelSyncs:         2,
	}}}
	pool := &apitypes.PoolState{Servers: map[string]*apitypes.PoolServer{
		"srv-a": {Endpoint: "10.0.1.10"},
		"srv-b": {Endpoint: "10.0.1.11"},
	}}
	instances := &apitypes.InstancesState{Instances: map[string]*apitypes.Instance{
		"order-master": {
			Type: "replication", Group: "order", Role: "master", Status: "running", Server: "srv-a", Port: 6379, Password: "secret",
		},
		"order-replica": {
			Type: "replication", Group: "order", Role: "replica", Status: "running", Server: "srv-b", Port: 6379,
		},
		"cache-1": {
			Type: "standalone", Role: "standalone", Status: "running", Server: "srv-a", Port: 6380,
		},
	}}
	masters := s.buildSentinelMasters(pool, instances)
	if len(masters) != 1 {
		t.Fatalf("expected one monitored master, got %#v", masters)
	}
	m := masters[0]
	if m.Group != "order" || m.Host != "10.0.1.10" || m.Port != 6379 || m.Password != "secret" {
		t.Fatalf("unexpected master: %#v", m)
	}
	if m.DownAfterMilliseconds != 6000 || m.FailoverTimeout != 40000 || m.ParallelSyncs != 2 {
		t.Fatalf("sentinel timing config not propagated: %#v", m)
	}
}

func TestSentinelEventEndpoint(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WritePool(&apitypes.PoolState{Servers: map[string]*apitypes.PoolServer{
		"srv-a": {Endpoint: "10.0.1.10"},
		"srv-b": {Endpoint: "10.0.1.11"},
	}})
	s.state.WriteInstances(&apitypes.InstancesState{Instances: map[string]*apitypes.Instance{
		"order-master": {
			Type: "replication", Group: "order", Role: "master", Status: "running", Server: "srv-a", Port: 6379,
			Replicas: []string{"order-replica"},
			Envoy:    &apitypes.EnvoyConfig{ReadWritePort: 16379, ReadOnlyPort: 16500},
		},
		"order-replica": {
			Type: "replication", Group: "order", Role: "replica", Status: "running", Server: "srv-b", Port: 6379,
			ReplicaOf: "order-master",
		},
	}})
	w := doRequest(s.Router(), "POST", "/sentinel/event", map[string]interface{}{
		"event":      "+switch-master",
		"group":      "order",
		"old_master": "10.0.1.10:6379",
		"new_master": "10.0.1.11:6379",
	})
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleSentinelFailover_UpdatesState(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WritePool(&apitypes.PoolState{Servers: map[string]*apitypes.PoolServer{
		"srv-a": {Endpoint: "10.0.1.10"},
		"srv-b": {Endpoint: "10.0.1.11"},
		"srv-c": {Endpoint: "10.0.1.12"},
	}})
	s.state.WriteInstances(&apitypes.InstancesState{Instances: map[string]*apitypes.Instance{
		"order-master": {
			Type: "replication", Group: "order", Role: "master", Status: "running", Server: "srv-a", Port: 6379,
			Replicas: []string{"order-replica", "order-replica-2"},
			Envoy:    &apitypes.EnvoyConfig{ReadWritePort: 16379, ReadOnlyPort: 16500},
		},
		"order-replica": {
			Type: "replication", Group: "order", Role: "replica", Status: "running", Server: "srv-b", Port: 6379,
			ReplicaOf: "order-master",
		},
		"order-replica-2": {
			Type: "replication", Group: "order", Role: "replica", Status: "running", Server: "srv-c", Port: 6379,
			ReplicaOf: "order-master",
		},
	}})
	if err := s.handleSentinelFailover("order", "10.0.1.11:6379", "test", "sentinel"); err != nil {
		t.Fatal(err)
	}
	state, err := s.state.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	oldMaster := state.Instances["order-master"]
	newMaster := state.Instances["order-replica"]
	otherReplica := state.Instances["order-replica-2"]
	if oldMaster.Status != "failed" || oldMaster.Role != "replica" || oldMaster.ReplicaOf != "order-replica" {
		t.Fatalf("old master not marked failed replica: %#v", oldMaster)
	}
	if newMaster.Role != "master" || newMaster.ReplicaOf != "" {
		t.Fatalf("new master not promoted in state: %#v", newMaster)
	}
	if newMaster.Envoy == nil || newMaster.Envoy.ReadWritePort != 16379 || newMaster.Envoy.ReadOnlyPort != 16500 {
		t.Fatalf("new master did not inherit envoy config: %#v", newMaster.Envoy)
	}
	if len(newMaster.Replicas) != 1 || newMaster.Replicas[0] != "order-replica-2" {
		t.Fatalf("unexpected new master replicas: %#v", newMaster.Replicas)
	}
	if otherReplica.ReplicaOf != "order-replica" {
		t.Fatalf("other replica should point at new master: %#v", otherReplica)
	}
	if oldMaster.Group != "order" || newMaster.Group != "order" || otherReplica.Group != "order" {
		t.Fatalf("group should remain stable after failover")
	}
}
