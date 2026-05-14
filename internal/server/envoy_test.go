package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func newTestServerWithEnvoy(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		DataDir:  dir,
		EnvoyDir: filepath.Join(dir, "envoy"),
		Log:      LogConfig{Dir: dir + "/log", Stdout: false},
	}
	return New(cfg)
}

func TestEnvoyConfig_Empty(t *testing.T) {
	s := newTestServerWithEnvoy(t)
	r := s.Router()
	w := doRequest(r, "GET", "/envoy/config", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestEnvoyConfig_StandaloneInstance(t *testing.T) {
	s := newTestServerWithEnvoy(t)
	s.state.WritePool(&apitypes.PoolState{
		Servers: map[string]*apitypes.PoolServer{
			"srv1": {Endpoint: "10.0.0.1"},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"cache": {
				Type:          "standalone",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "cache-1",
				Envoy:         &apitypes.EnvoyConfig{MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Group: "cache", Role: "master", Server: "srv1", Port: 6379, Status: "running",
			},
		},
	})

	config, err := s.generateEnvoyConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(config, "port_value: 16500") {
		t.Error("expected master listener port 16500")
	}
	if !strings.Contains(config, "address: 10.0.0.1") {
		t.Error("expected endpoint 10.0.0.1")
	}
	if !strings.Contains(config, "redis-cache-master-cluster") {
		t.Error("expected cluster name")
	}
}

func TestEnvoyConfig_ReplicationGroup(t *testing.T) {
	s := newTestServerWithEnvoy(t)
	s.state.WritePool(&apitypes.PoolState{
		Servers: map[string]*apitypes.PoolServer{
			"srv1": {Endpoint: "10.0.0.1"},
			"srv2": {Endpoint: "10.0.0.2"},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type:          "replication",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "order-master",
				Envoy:         &apitypes.EnvoyConfig{AutoPort: 16379, MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "master", Server: "srv1", Port: 6379, Status: "running",
			},
			"order-replica": {
				Group: "order", Role: "replica", Server: "srv2", Port: 6379, Status: "running",
				ReplicaOf: "order-master",
			},
		},
	})

	config, err := s.generateEnvoyConfig()
	if err != nil {
		t.Fatal(err)
	}
	// 应有 AUTO listener 和 MASTER listener
	if !strings.Contains(config, "port_value: 16379") {
		t.Error("expected auto listener port 16379")
	}
	if !strings.Contains(config, "port_value: 16500") {
		t.Error("expected master listener port 16500")
	}
	// cluster 应包含主从 endpoint（分布在不同 cluster）
	if !strings.Contains(config, "address: 10.0.0.1") {
		t.Error("expected master endpoint")
	}
	if !strings.Contains(config, "address: 10.0.0.2") {
		t.Error("expected replica endpoint")
	}
	if !strings.Contains(config, "read_command_policy:") {
		t.Error("expected auto listener read_command_policy")
	}
	if !strings.Contains(config, "cluster: redis-order-replica-cluster") {
		t.Error("expected read commands to use replica cluster")
	}
}

func TestEnvoyConfig_SkipNonRunning(t *testing.T) {
	s := newTestServerWithEnvoy(t)
	s.state.WritePool(&apitypes.PoolState{
		Servers: map[string]*apitypes.PoolServer{
			"srv1": {Endpoint: "10.0.0.1"},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"stopped": {
				Type:          "standalone",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "stopped-1",
				Envoy:         &apitypes.EnvoyConfig{MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"stopped-1": {
				Group: "stopped", Role: "master", Server: "srv1", Port: 6379, Status: "stopped",
			},
		},
	})

	config, err := s.generateEnvoyConfig()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(config, "16381") {
		t.Error("stopped instance should not appear in envoy config")
	}
}

func TestEnvoyRouteUpdate_WritesFile(t *testing.T) {
	s := newTestServerWithEnvoy(t)
	s.state.WritePool(&apitypes.PoolState{
		Servers: map[string]*apitypes.PoolServer{
			"srv1": {Endpoint: "10.0.0.1"},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"cache": {
				Type:          "standalone",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "cache-1",
				Envoy:         &apitypes.EnvoyConfig{MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Group: "cache", Role: "master", Server: "srv1", Port: 6379, Status: "running",
			},
		},
	})

	r := s.Router()
	w := doRequest(r, "POST", "/envoy/route/update", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证文件写入
	data, err := os.ReadFile(filepath.Join(s.cfg.EnvoyDir, "envoy.yaml"))
	if err != nil {
		t.Fatalf("envoy config file not written: %v", err)
	}
	if !strings.Contains(string(data), "port_value: 16500") {
		t.Error("expected listener in written file")
	}
}

func TestEnvoyConfig_NoEnvoyField(t *testing.T) {
	s := newTestServerWithEnvoy(t)
	s.state.WritePool(&apitypes.PoolState{
		Servers: map[string]*apitypes.PoolServer{
			"srv1": {Endpoint: "10.0.0.1"},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"no-envoy": {
				Type:          "standalone",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "no-envoy",
			},
		},
		Instances: map[string]*apitypes.Instance{
			"no-envoy": {
				Group: "no-envoy", Role: "master", Server: "srv1", Port: 6379, Status: "running",
			},
		},
	})

	config, err := s.generateEnvoyConfig()
	if err != nil {
		t.Fatal(err)
	}
	// 没有 Envoy 配置的实例不应出现
	if strings.Contains(config, "no-envoy") {
		t.Error("instance without envoy config should not appear")
	}
}

func TestEnvoyRouteUpdate_Response(t *testing.T) {
	s := newTestServerWithEnvoy(t)
	r := s.Router()
	w := doRequest(r, "POST", "/envoy/route/update", nil)
	resp := parseResponse(t, w)
	if !resp.Success {
		t.Fatalf("expected success: %s", resp.Error)
	}
	// Data 应包含 config 字段
	data, _ := json.Marshal(resp.Data)
	var result map[string]string
	json.Unmarshal(data, &result)
	if _, ok := result["config"]; !ok {
		t.Error("expected config in response data")
	}
}
