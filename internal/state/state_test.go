package state

import (
	"sort"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// newTestManager 创建一个使用临时目录的 Manager（测试辅助）
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// ---------- Manager 读写测试 ----------

func TestReadNode_Empty(t *testing.T) {
	m := newTestManager(t)
	ps, err := m.ReadNode()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps.Servers == nil {
		t.Fatal("Servers map should be initialized, got nil")
	}
	if len(ps.Servers) != 0 {
		t.Fatalf("expected 0 servers, got %d", len(ps.Servers))
	}
}

func TestWriteNode_ThenRead(t *testing.T) {
	m := newTestManager(t)
	want := &apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {
				Endpoint:  "10.0.0.1",
				AgentPort: 8400,
				Status:    "healthy",
				Capacity:  apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi", Disk: "500Gi"},
				Instances: []string{"redis-1", "redis-2"},
			},
		},
	}
	if err := m.WriteNode(want); err != nil {
		t.Fatalf("write error: %v", err)
	}
	got, err := m.ReadNode()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	srv := got.Servers["srv1"]
	if srv == nil {
		t.Fatal("expected srv1")
	}
	if srv.Endpoint != "10.0.0.1" || srv.AgentPort != 8400 || srv.Status != "healthy" {
		t.Fatalf("basic fields mismatch: %+v", srv)
	}
	if srv.Capacity.CPUCores != 8 || srv.Capacity.Memory != "32Gi" {
		t.Fatalf("capacity mismatch: %+v", srv.Capacity)
	}
	if len(srv.Instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(srv.Instances))
	}
}

func TestReadInstances_Empty(t *testing.T) {
	m := newTestManager(t)
	is, err := m.ReadInstances()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if is.Instances == nil {
		t.Fatal("Instances map should be initialized, got nil")
	}
	if len(is.Instances) != 0 {
		t.Fatalf("expected 0 instances, got %d", len(is.Instances))
	}
}

func TestWriteInstances_ThenRead(t *testing.T) {
	m := newTestManager(t)
	want := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis": {
				Type:          "replication",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "redis-1",
			},
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Role:         "master",
				Group:        "redis",
				Server:       "srv1",
				Port:         6379,
				Memory:       "4Gi",
				CPUs:         2,
				Password:     "secret",
				DesiredState: "running", ObservedStatus: "running",
			},
			"redis-1-rep": {
				Role:         "replica",
				Group:        "redis",
				Server:       "srv2",
				Port:         6380,
				ReplicaOf:    "redis-1",
				DesiredState: "running", ObservedStatus: "running",
			},
		},
	}
	if err := m.WriteInstances(want); err != nil {
		t.Fatalf("write error: %v", err)
	}
	got, err := m.ReadInstances()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if len(got.Instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(got.Instances))
	}
	master := got.Instances["redis-1"]
	if master.Role != "master" || got.Groups["redis"].CurrentMaster != "redis-1" {
		t.Fatalf("master mismatch: %+v", master)
	}
	replica := got.Instances["redis-1-rep"]
	if replica.Role != "replica" || replica.ReplicaOf != "redis-1" {
		t.Fatalf("replica mismatch: %+v", replica)
	}
}

func TestWithInstances_Atomic(t *testing.T) {
	m := newTestManager(t)

	err := m.WithInstances(func(st *apitypes.InstancesState) error {
		st.Groups["g1"] = &apitypes.InstanceGroupState{Type: "standalone", Engine: "redis"}
		st.Instances["inst1"] = &apitypes.Instance{Group: "g1", Role: "master", DesiredState: "running", ObservedStatus: "running"}
		return nil
	})
	if err != nil {
		t.Fatalf("WithInstances: %v", err)
	}
	got, _ := m.ReadInstances()
	if got.Instances["inst1"] == nil {
		t.Fatal("inst1 should exist after WithInstances")
	}
}

// ---------- TryAcquireLock 测试 ----------

func TestTryAcquireLock_Success(t *testing.T) {
	inst := &apitypes.Instance{}
	if err := TryAcquireLock(inst, "session-1", "create", 60); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inst.Lock == nil {
		t.Fatal("lock should be set")
	}
	if inst.Lock.HeldBy != "session-1" || inst.Lock.Operation != "create" || inst.Lock.Timeout != 60 {
		t.Fatalf("lock fields mismatch: %+v", inst.Lock)
	}
}

func TestTryAcquireLock_Reentrant(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60) //nolint:errcheck
	if err := TryAcquireLock(inst, "session-1", "create", 60); err != nil {
		t.Fatalf("reentrant lock should succeed: %v", err)
	}
}

func TestTryAcquireLock_Conflict(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60) //nolint:errcheck
	err := TryAcquireLock(inst, "session-2", "delete", 60)
	if err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestTryAcquireLock_Expired(t *testing.T) {
	inst := &apitypes.Instance{
		Lock: &apitypes.Lock{
			HeldBy:     "session-1",
			Operation:  "create",
			AcquiredAt: time.Now().Add(-10 * time.Minute).Format(time.RFC3339),
			Timeout:    1,
		},
	}
	if err := TryAcquireLock(inst, "session-2", "delete", 60); err != nil {
		t.Fatalf("expired lock should be acquirable: %v", err)
	}
	if inst.Lock.HeldBy != "session-2" {
		t.Fatalf("expected session-2, got %s", inst.Lock.HeldBy)
	}
}

func TestTryAcquireLock_DefaultTimeout(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 0) //nolint:errcheck
	if inst.Lock.Timeout != 300 {
		t.Fatalf("expected default timeout 300, got %d", inst.Lock.Timeout)
	}
}

// ---------- ReleaseLock 测试 ----------

func TestReleaseLock_ByHolder(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60) //nolint:errcheck
	ReleaseLock(inst, "session-1")
	if inst.Lock != nil {
		t.Fatal("lock should be nil after release")
	}
}

func TestReleaseLock_ByEmpty(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60) //nolint:errcheck
	ReleaseLock(inst, "")
	if inst.Lock != nil {
		t.Fatal("empty heldBy should force release")
	}
}

func TestReleaseLock_ByOther(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60) //nolint:errcheck
	ReleaseLock(inst, "session-2")
	if inst.Lock == nil {
		t.Fatal("non-holder should not release lock")
	}
}

// ---------- InstanceGroup 测试 ----------

func TestInstanceGroup_Standalone(t *testing.T) {
	st := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Role: "master"},
		},
	}
	group := InstanceGroup(st, "redis-1")
	if len(group) != 1 || group[0] != "redis-1" {
		t.Fatalf("expected [redis-1], got %v", group)
	}
}

func TestInstanceGroup_Master(t *testing.T) {
	st := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1":    {Role: "master", Group: "redis"},
			"redis-1-r1": {Role: "replica", Group: "redis", ReplicaOf: "redis-1"},
			"redis-1-r2": {Role: "replica", Group: "redis", ReplicaOf: "redis-1"},
		},
	}
	group := InstanceGroup(st, "redis-1")
	sort.Strings(group)
	expected := []string{"redis-1", "redis-1-r1", "redis-1-r2"}
	sort.Strings(expected)
	if len(group) != 3 {
		t.Fatalf("expected 3 members, got %v", group)
	}
	for i := range expected {
		if group[i] != expected[i] {
			t.Fatalf("expected %v, got %v", expected, group)
		}
	}
}

func TestInstanceGroup_Replica(t *testing.T) {
	st := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1":    {Role: "master", Group: "redis"},
			"redis-1-r1": {Role: "replica", Group: "redis", ReplicaOf: "redis-1"},
		},
	}
	group := InstanceGroup(st, "redis-1-r1")
	sort.Strings(group)
	expected := []string{"redis-1", "redis-1-r1"}
	sort.Strings(expected)
	if len(group) != 2 {
		t.Fatalf("expected 2 members, got %v", group)
	}
	for i := range expected {
		if group[i] != expected[i] {
			t.Fatalf("expected %v, got %v", expected, group)
		}
	}
}

func TestInstanceGroup_StableGroupField(t *testing.T) {
	st := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"order-master":  {Role: "master", Group: "order"},
			"order-replica": {Role: "replica", Group: "order", ReplicaOf: "order-master"},
			"cache-1":       {Role: "master", Group: "cache"},
		},
	}
	group := InstanceGroup(st, "order-replica")
	sort.Strings(group)
	expected := []string{"order-master", "order-replica"}
	if len(group) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, group)
	}
	for i := range expected {
		if group[i] != expected[i] {
			t.Fatalf("expected %v, got %v", expected, group)
		}
	}
}

func TestInstanceGroup_NotExist(t *testing.T) {
	st := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{},
	}
	group := InstanceGroup(st, "nonexistent")
	if len(group) != 1 || group[0] != "nonexistent" {
		t.Fatalf("expected [nonexistent], got %v", group)
	}
}

func TestRecalculateGroupTopology_StandaloneHealthyWithMasterOnly(t *testing.T) {
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"cache": {Type: "standalone", CurrentMaster: "cache-1"},
		},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {Group: "cache", Role: "master", DesiredState: "running", ObservedStatus: "running"},
		},
	}
	RecalculateGroupTopology(st, "cache")
	if st.Groups["cache"].TopologyStatus != "healthy" {
		t.Fatalf("expected standalone healthy, got %s", st.Groups["cache"].TopologyStatus)
	}
}

func TestRecalculateGroupTopology_ReplicationDegradedWithoutReplica(t *testing.T) {
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", CurrentMaster: "order-master"},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running"},
		},
	}
	RecalculateGroupTopology(st, "order")
	if st.Groups["order"].TopologyStatus != "degraded" {
		t.Fatalf("expected replication degraded without replica, got %s", st.Groups["order"].TopologyStatus)
	}
}

func TestRecalculateGroupTopology_ReplicationHealthyWithReplica(t *testing.T) {
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", CurrentMaster: "order-master"},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master":  {Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running"},
			"order-replica": {Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", ReplicaOf: "order-master"},
		},
	}
	RecalculateGroupTopology(st, "order")
	if st.Groups["order"].TopologyStatus != "healthy" {
		t.Fatalf("expected replication healthy with replica, got %s", st.Groups["order"].TopologyStatus)
	}
}
