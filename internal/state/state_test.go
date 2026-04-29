package state

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// ---------- Manager 读写测试 ----------

func TestReadPool_FileNotExist(t *testing.T) {
	m := NewManager(t.TempDir())
	ps, err := m.ReadPool()
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

func TestReadPool_ValidYAML(t *testing.T) {
	dir := t.TempDir()
	yaml := `servers:
  srv1:
    endpoint: "10.0.0.1"
    agent_port: 8400
    status: healthy
`
	if err := os.WriteFile(filepath.Join(dir, "pool-state.yaml"), []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(dir)
	ps, err := m.ReadPool()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	srv, ok := ps.Servers["srv1"]
	if !ok {
		t.Fatal("expected srv1 in servers")
	}
	if srv.Endpoint != "10.0.0.1" {
		t.Fatalf("expected endpoint 10.0.0.1, got %s", srv.Endpoint)
	}
	if srv.AgentPort != 8400 {
		t.Fatalf("expected agent_port 8400, got %d", srv.AgentPort)
	}
	if srv.Status != "healthy" {
		t.Fatalf("expected status healthy, got %s", srv.Status)
	}
}

func TestWritePool_ThenRead(t *testing.T) {
	m := NewManager(t.TempDir())
	want := &apitypes.PoolState{
		Servers: map[string]*apitypes.PoolServer{
			"srv1": {
				Endpoint:  "10.0.0.1",
				AgentPort: 8400,
				Status:    "healthy",
				Capacity:  apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi", Disk: "500Gi"},
				Instances: []string{"redis-1", "redis-2"},
			},
		},
	}
	if err := m.WritePool(want); err != nil {
		t.Fatalf("write error: %v", err)
	}
	got, err := m.ReadPool()
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

func TestReadInstances_FileNotExist(t *testing.T) {
	m := NewManager(t.TempDir())
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

func TestReadInstances_ValidYAML(t *testing.T) {
	dir := t.TempDir()
	yaml := `instances:
  redis-1:
    engine: redis
    role: master
    port: 6379
    status: running
`
	if err := os.WriteFile(filepath.Join(dir, "instances-state.yaml"), []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(dir)
	is, err := m.ReadInstances()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	inst, ok := is.Instances["redis-1"]
	if !ok {
		t.Fatal("expected redis-1")
	}
	if inst.Engine != "redis" || inst.Role != "master" || inst.Port != 6379 || inst.Status != "running" {
		t.Fatalf("fields mismatch: %+v", inst)
	}
}

func TestWriteInstances_ThenRead(t *testing.T) {
	m := NewManager(t.TempDir())
	want := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine:   "redis",
				Category: "cache",
				Type:     "replication",
				Role:     "master",
				Server:   "srv1",
				Port:     6379,
				Memory:   "4Gi",
				CPUs:     2,
				Password: "secret",
				Status:   "running",
				Replicas: []string{"redis-1-rep"},
			},
			"redis-1-rep": {
				Engine:    "redis",
				Role:      "replica",
				Server:    "srv2",
				Port:      6380,
				ReplicaOf: "10.0.0.1:6379",
				Status:    "running",
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
	if master.Role != "master" || len(master.Replicas) != 1 {
		t.Fatalf("master mismatch: %+v", master)
	}
	replica := got.Instances["redis-1-rep"]
	if replica.Role != "replica" || replica.ReplicaOf != "10.0.0.1:6379" {
		t.Fatalf("replica mismatch: %+v", replica)
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
	TryAcquireLock(inst, "session-1", "create", 60)
	if err := TryAcquireLock(inst, "session-1", "create", 60); err != nil {
		t.Fatalf("reentrant lock should succeed: %v", err)
	}
}

func TestTryAcquireLock_Conflict(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60)
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
			Timeout:    1, // 1 秒超时，早已过期
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
	TryAcquireLock(inst, "session-1", "create", 0)
	if inst.Lock.Timeout != 300 {
		t.Fatalf("expected default timeout 300, got %d", inst.Lock.Timeout)
	}
}

// ---------- ReleaseLock 测试 ----------

func TestReleaseLock_ByHolder(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60)
	ReleaseLock(inst, "session-1")
	if inst.Lock != nil {
		t.Fatal("lock should be nil after release")
	}
}

func TestReleaseLock_ByEmpty(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60)
	ReleaseLock(inst, "")
	if inst.Lock != nil {
		t.Fatal("empty heldBy should force release")
	}
}

func TestReleaseLock_ByOther(t *testing.T) {
	inst := &apitypes.Instance{}
	TryAcquireLock(inst, "session-1", "create", 60)
	ReleaseLock(inst, "session-2")
	if inst.Lock == nil {
		t.Fatal("non-holder should not release lock")
	}
}

// ---------- InstanceGroup 测试 ----------

func TestInstanceGroup_Standalone(t *testing.T) {
	state := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Role: "standalone"},
		},
	}
	group := InstanceGroup(state, "redis-1")
	if len(group) != 1 || group[0] != "redis-1" {
		t.Fatalf("expected [redis-1], got %v", group)
	}
}

func TestInstanceGroup_Master(t *testing.T) {
	state := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1":     {Role: "master", Replicas: []string{"redis-1-r1", "redis-1-r2"}},
			"redis-1-r1":  {Role: "replica", ReplicaOf: "10.0.0.1:6379"},
			"redis-1-r2":  {Role: "replica", ReplicaOf: "10.0.0.1:6379"},
		},
	}
	group := InstanceGroup(state, "redis-1")
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
	state := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1":    {Role: "master", Replicas: []string{"redis-1-r1"}},
			"redis-1-r1": {Role: "replica", ReplicaOf: "10.0.0.1:6379"},
		},
	}
	group := InstanceGroup(state, "redis-1-r1")
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

func TestInstanceGroup_NotExist(t *testing.T) {
	state := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{},
	}
	group := InstanceGroup(state, "nonexistent")
	if len(group) != 1 || group[0] != "nonexistent" {
		t.Fatalf("expected [nonexistent], got %v", group)
	}
}
