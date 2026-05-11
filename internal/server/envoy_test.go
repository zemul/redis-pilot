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
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Engine: "redis", Group: "cache", Role: "standalone", Server: "srv1", Port: 6379, Status: "running",
				Envoy: &apitypes.EnvoyConfig{ReadWritePort: 16381},
			},
		},
	})

	config, err := s.generateEnvoyConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(config, "port_value: 16381") {
		t.Error("expected listener port 16381")
	}
	if !strings.Contains(config, "address: 10.0.0.1") {
		t.Error("expected endpoint 10.0.0.1")
	}
	if !strings.Contains(config, "redis-cache-cluster") {
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
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Engine: "redis", Group: "order", Role: "master", Server: "srv1", Port: 6379, Status: "running",
				Replicas: []string{"order-replica"},
				Envoy:    &apitypes.EnvoyConfig{ReadWritePort: 16379, WriteOnlyPort: 16400},
			},
			"order-replica": {
				Engine: "redis", Group: "order", Role: "replica", Server: "srv2", Port: 6379, Status: "running",
				ReplicaOf: "10.0.0.1:6379",
				Envoy:     &apitypes.EnvoyConfig{ReadWritePort: 16379},
			},
		},
	})

	config, err := s.generateEnvoyConfig()
	if err != nil {
		t.Fatal(err)
	}
	// 应有读写 listener 和仅写 listener
	if !strings.Contains(config, "port_value: 16379") {
		t.Error("expected rw listener port 16379")
	}
	if !strings.Contains(config, "port_value: 16400") {
		t.Error("expected wo listener port 16400")
	}
	// cluster 应包含主从两个 endpoint
	if !strings.Contains(config, "address: 10.0.0.1") {
		t.Error("expected master endpoint")
	}
	if !strings.Contains(config, "address: 10.0.0.2") {
		t.Error("expected replica endpoint")
	}
	// 读写分离策略
	if !strings.Contains(config, "read_policy: REPLICA") {
		t.Error("expected REPLICA read policy")
	}
	if !strings.Contains(config, "read_policy: MASTER") {
		t.Error("expected MASTER read policy for write-only")
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
		Instances: map[string]*apitypes.Instance{
			"stopped-1": {
				Engine: "redis", Role: "standalone", Server: "srv1", Port: 6379, Status: "stopped",
				Envoy: &apitypes.EnvoyConfig{ReadWritePort: 16381},
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
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Engine: "redis", Role: "standalone", Server: "srv1", Port: 6379, Status: "running",
				Envoy: &apitypes.EnvoyConfig{ReadWritePort: 16381},
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
	if !strings.Contains(string(data), "port_value: 16381") {
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
		Instances: map[string]*apitypes.Instance{
			"no-envoy": {
				Engine: "redis", Role: "standalone", Server: "srv1", Port: 6379, Status: "running",
				// Envoy 字段为 nil
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
