package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func makeNode(servers map[string]*apitypes.NodeServer) *apitypes.NodeState {
	return &apitypes.NodeState{Servers: servers}
}

func emptyInstances() *apitypes.InstancesState {
	return &apitypes.InstancesState{Instances: map[string]*apitypes.Instance{}}
}

func TestSelectServer_SingleHealthy(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: "10.0.0.1", Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
	})
	name, err := selectServer(node, emptyInstances(), "4Gi", 2, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if name != "srv1" {
		t.Fatalf("expected srv1, got %s", name)
	}
}

func TestSelectServer_FilterUnhealthy(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Status: "unhealthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
		"srv2": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
	})
	name, err := selectServer(node, emptyInstances(), "4Gi", 2, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if name != "srv2" {
		t.Fatalf("expected srv2, got %s", name)
	}
}

func TestSelectServer_FilterInsufficientMemory(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
		"srv2": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
	})
	instances := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"big": {Server: "srv1", Memory: "30Gi", CPUs: 1, Status: "running"},
		},
	}
	name, err := selectServer(node, instances, "4Gi", 2, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if name != "srv2" {
		t.Fatalf("expected srv2, got %s", name)
	}
}

func TestSelectServer_FilterInsufficientCPU(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 4, Memory: "32Gi"}},
		"srv2": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
	})
	instances := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"heavy": {Server: "srv1", Memory: "1Gi", CPUs: 3, Status: "running"},
		},
	}
	name, err := selectServer(node, instances, "4Gi", 2, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if name != "srv2" {
		t.Fatalf("expected srv2, got %s", name)
	}
}

func TestSelectServer_PreferMoreResources(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "16Gi"}},
		"srv2": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "64Gi"}},
	})
	name, err := selectServer(node, emptyInstances(), "4Gi", 2, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if name != "srv2" {
		t.Fatalf("expected srv2 (more memory), got %s", name)
	}
}

func TestSelectServer_ReplicaAvoidMasterServer(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: "10.0.0.1", Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 16, Memory: "64Gi"}},
		"srv2": {Endpoint: "10.0.0.2", Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
	})
	instances := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-m": {Role: "master", Server: "srv1", Port: 6379},
		},
	}
	// 从库应避开主库所在的 srv1，选 srv2
	name, err := selectServer(node, instances, "4Gi", 2, "", "10.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	if name != "srv2" {
		t.Fatalf("expected srv2 (different from master), got %s", name)
	}
}

func TestSelectServer_ReplicaPreferDifferentZone(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: "10.0.0.1", Status: "healthy", Labels: map[string]string{"zone": "az-1"}, Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
		"srv2": {Endpoint: "10.0.0.2", Status: "healthy", Labels: map[string]string{"zone": "az-1"}, Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "64Gi"}},
		"srv3": {Endpoint: "10.0.0.3", Status: "healthy", Labels: map[string]string{"zone": "az-2"}, Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"}},
	})
	instances := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-m": {Role: "master", Server: "srv1", Port: 6379},
		},
	}
	// srv2 资源更多但和主库同 zone，应选 srv3（不同 zone）
	name, err := selectServer(node, instances, "4Gi", 2, "", "10.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	if name != "srv3" {
		t.Fatalf("expected srv3 (different zone), got %s", name)
	}
}

func TestSelectServer_NoResourcesAvailable(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{
		"srv1": {Status: "healthy", Capacity: apitypes.ResourceSpec{CPUCores: 2, Memory: "4Gi"}},
	})
	instances := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"full": {Server: "srv1", Memory: "4Gi", CPUs: 2, Status: "running"},
		},
	}
	_, err := selectServer(node, instances, "4Gi", 2, "", "")
	if err == nil {
		t.Fatal("expected error for no resources")
	}
}

func TestSelectServer_EmptyNode(t *testing.T) {
	node := makeNode(map[string]*apitypes.NodeServer{})
	_, err := selectServer(node, emptyInstances(), "4Gi", 2, "", "")
	if err == nil {
		t.Fatal("expected error for empty node")
	}
}
