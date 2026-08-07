package server

import (
	"net/http"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestProxySnapshot(t *testing.T) {
	// 主场景：master + replica 都在跑，auto listener 有读写分离
	t.Run("master_and_replica_running", func(t *testing.T) {
		s := newTestServer(t, "")
		s.db.WriteNode(&apitypes.NodeState{
			Servers: map[string]*apitypes.NodeServer{
				"srv1": {Endpoint: "10.0.0.1"},
				"srv2": {Endpoint: "10.0.0.2"},
			},
		})
		s.db.WriteInstances(&apitypes.InstancesState{
			Groups: map[string]*apitypes.InstanceGroupState{
				"cache": {
					Type:          "replication",
					CurrentMaster: "cache-1",
					Proxy:         &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500},
				},
			},
			Instances: map[string]*apitypes.Instance{
				"cache-1": {
					Group: "cache", Role: "master", Server: "srv1", Port: 6379, Password: "secret",
					DesiredState: "running", ObservedStatus: "running",
				},
				"cache-2": {
					Group: "cache", Role: "replica", Server: "srv2", Port: 6379,
					DesiredState: "running", ObservedStatus: "running",
				},
			},
		})

		snapshot, err := s.buildProxySnapshot()
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Listeners) != 2 {
			t.Fatalf("expected 2 listeners (master+auto), got %d", len(snapshot.Listeners))
		}
		if len(snapshot.Clusters) != 2 {
			t.Fatalf("expected 2 clusters (master+replica), got %d", len(snapshot.Clusters))
		}
		// auto listener 应该有 ReadCluster 指向 replica cluster
		var autoListener *apitypes.ProxyListener
		for i := range snapshot.Listeners {
			if snapshot.Listeners[i].Mode == "auto" {
				autoListener = &snapshot.Listeners[i]
			}
		}
		if autoListener == nil {
			t.Fatal("auto listener not found")
		}
		if autoListener.ReadCluster == "" {
			t.Error("auto listener should have ReadCluster when replicas are running")
		}
	})

	// 降级场景：所有 replica 都停了，auto listener 保留但 ReadCluster 留空，
	// 读请求降级到 master，不中断服务。
	t.Run("all_replicas_stopped_degrades_to_master", func(t *testing.T) {
		s := newTestServer(t, "")
		s.db.WriteNode(&apitypes.NodeState{
			Servers: map[string]*apitypes.NodeServer{
				"srv1": {Endpoint: "10.0.0.1"},
				"srv2": {Endpoint: "10.0.0.2"},
			},
		})
		s.db.WriteInstances(&apitypes.InstancesState{
			Groups: map[string]*apitypes.InstanceGroupState{
				"cache": {
					Type:          "replication",
					CurrentMaster: "cache-1",
					Proxy:         &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500},
				},
			},
			Instances: map[string]*apitypes.Instance{
				"cache-1": {
					Group: "cache", Role: "master", Server: "srv1", Port: 6379, Password: "secret",
					DesiredState: "running", ObservedStatus: "running",
				},
				"cache-2": {
					Group: "cache", Role: "replica", Server: "srv2", Port: 6379,
					DesiredState: "stopped", ObservedStatus: "stopped",
				},
			},
		})

		snapshot, err := s.buildProxySnapshot()
		if err != nil {
			t.Fatal(err)
		}
		// auto listener 必须仍然存在
		var autoListener *apitypes.ProxyListener
		for i := range snapshot.Listeners {
			if snapshot.Listeners[i].Mode == "auto" {
				autoListener = &snapshot.Listeners[i]
			}
		}
		if autoListener == nil {
			t.Fatal("auto listener should exist even when all replicas are stopped (degrade to master)")
		}
		// ReadCluster 留空 -> Proxy 不做读写分离，所有命令走 master
		if autoListener.ReadCluster != "" {
			t.Errorf("ReadCluster = %q, want empty (degrade reads to master)", autoListener.ReadCluster)
		}
		// replica cluster 不应存在（没有 replica 端点）
		for _, c := range snapshot.Clusters {
			if c.Name == "redis-cache-replica-cluster" {
				t.Error("replica cluster should not exist when no replicas are serving")
			}
		}
		// master cluster 必须存在
		hasMasterCluster := false
		for _, c := range snapshot.Clusters {
			if c.Name == "redis-cache-master-cluster" {
				hasMasterCluster = true
			}
		}
		if !hasMasterCluster {
			t.Error("master cluster must exist")
		}
	})

	// HTTP 端点冒烟
	t.Run("http_endpoint", func(t *testing.T) {
		s := newTestServer(t, "")
		s.db.WriteNode(&apitypes.NodeState{
			Servers: map[string]*apitypes.NodeServer{
				"srv1": {Endpoint: "10.0.0.1"},
			},
		})
		s.db.WriteInstances(&apitypes.InstancesState{
			Groups: map[string]*apitypes.InstanceGroupState{
				"cache": {
					Type:          "standalone",
					CurrentMaster: "cache-1",
					Proxy:         &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500},
				},
			},
			Instances: map[string]*apitypes.Instance{
				"cache-1": {
					Group: "cache", Role: "master", Server: "srv1", Port: 6379, Password: "secret",
					DesiredState: "running", ObservedStatus: "running",
				},
			},
		})
		w := doRequest(s.Router(), http.MethodGet, "/api/v1/proxy/snapshot", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
}
