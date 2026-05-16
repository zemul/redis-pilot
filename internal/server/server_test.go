package server

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// newTestServer 创建一个用于测试的 Server，dataDir 指向临时目录
func newTestServer(t *testing.T, token string) *Server {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		Port:    8080,
		Token:   token,
		DataDir: dir,
		Log:     LogConfig{Dir: dir + "/log", Stdout: false},
	}
	return New(cfg)
}

func doRequest(router http.Handler, method, path string, body interface{}) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func doRequestWithAuth(router http.Handler, method, path, token string, body interface{}) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func parseResponse(t *testing.T, w *httptest.ResponseRecorder) apitypes.APIResponse {
	t.Helper()
	var resp apitypes.APIResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v, body: %s", err, w.Body.String())
	}
	return resp
}

// ---------- Auth Middleware ----------

func TestAuthMiddleware_NoToken(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/node/list", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	s := newTestServer(t, "secret")
	r := s.Router()
	w := doRequestWithAuth(r, "GET", "/node/list", "secret", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	s := newTestServer(t, "secret")
	r := s.Router()
	w := doRequestWithAuth(r, "GET", "/node/list", "wrong", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_MissingToken(t *testing.T) {
	s := newTestServer(t, "secret")
	r := s.Router()
	w := doRequest(r, "GET", "/node/list", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestDashboard_NoAuthRequired(t *testing.T) {
	s := newTestServer(t, "secret")
	r := s.Router()
	w := doRequest(r, "GET", "/dashboard", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Fatalf("expected html content type, got %q", ct)
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("Redis Pilot")) {
		t.Fatalf("dashboard html should contain title, got body: %s", w.Body.String())
	}
}

// ---------- Node Handlers ----------

func TestNodeQuery_Empty(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/node/list", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := parseResponse(t, w)
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
}

func TestNodeAdd_Success(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	body := map[string]interface{}{
		"name": "srv1",
		"server": map[string]interface{}{
			"endpoint":   "10.0.0.1",
			"agent_port": 8400,
			"status":     "healthy",
		},
	}
	w := doRequest(r, "POST", "/node/add", body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证写入
	w = doRequest(r, "GET", "/node/list", nil)
	resp := parseResponse(t, w)
	data, _ := json.Marshal(resp.Data)
	var node apitypes.NodeState
	json.Unmarshal(data, &node)
	if _, ok := node.Servers["srv1"]; !ok {
		t.Fatal("srv1 should exist after add")
	}
}

func TestNodeAdd_Duplicate(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	body := map[string]interface{}{
		"name":   "srv1",
		"server": map[string]interface{}{"endpoint": "10.0.0.1", "agent_port": 8400},
	}
	doRequest(r, "POST", "/node/add", body)
	w := doRequest(r, "POST", "/node/add", body)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

func TestNodeRemove_Success(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	// 先添加
	doRequest(r, "POST", "/node/add", map[string]interface{}{
		"name":   "srv1",
		"server": map[string]interface{}{"endpoint": "10.0.0.1", "agent_port": 8400},
	})
	// 再删除
	w := doRequest(r, "POST", "/node/remove", map[string]interface{}{"name": "srv1"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestNodeRemove_NotFound(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "POST", "/node/remove", map[string]interface{}{"name": "nonexistent"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestNodeUpdate_Success(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	doRequest(r, "POST", "/node/add", map[string]interface{}{
		"name":   "srv1",
		"server": map[string]interface{}{"endpoint": "10.0.0.1", "agent_port": 8400, "status": "healthy"},
	})
	w := doRequest(r, "POST", "/node/update", map[string]interface{}{
		"name":   "srv1",
		"server": map[string]interface{}{"endpoint": "10.0.0.2", "agent_port": 8400, "status": "drain"},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestNodeUpdate_NotFound(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "POST", "/node/update", map[string]interface{}{
		"name":   "nonexistent",
		"server": map[string]interface{}{"endpoint": "10.0.0.1"},
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// ---------- Instance Handlers (不依赖 Agent 的部分) ----------

func TestInstanceList_Empty(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/instance/list", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := parseResponse(t, w)
	if !resp.Success {
		t.Fatalf("expected success: %s", resp.Error)
	}
}

func TestInstanceList_FilterCommonFields(t *testing.T) {
	s := newTestServer(t, "")
	if err := s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type:           "replication",
				Engine:         "redis",
				EngineVersion:  "7",
				Category:       "persistent",
				CurrentMaster:  "order-master",
				TopologyStatus: "healthy",
			},
			"session": {
				Type:           "standalone",
				Engine:         "kvrocks",
				EngineVersion:  "2.15.0",
				Category:       "cache",
				CurrentMaster:  "session-cache",
				TopologyStatus: "healthy",
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group:  "order",
				Role:   "master",
				Server: "redis01",
				Status: "running",
			},
			"order-replica": {
				Group:  "order",
				Role:   "replica",
				Server: "redis02",
				Status: "running",
			},
			"session-cache": {
				Group:  "session",
				Role:   "master",
				Server: "redis02",
				Status: "stopped",
			},
		},
	}); err != nil {
		t.Fatalf("write instances: %v", err)
	}

	w := doRequest(s.Router(), "GET", "/instance/list?server=redis02&engine=redis&role=replica&status=running&category=persistent&engine_version=7", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	resp := parseResponse(t, w)
	data, _ := json.Marshal(resp.Data)
	var got apitypes.InstancesState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(got.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(got.Instances))
	}
	if _, ok := got.Instances["order-replica"]; !ok {
		t.Fatalf("expected order-replica in filtered response, got %#v", got.Instances)
	}
	if len(got.Groups) != 1 || got.Groups["order"] == nil {
		t.Fatalf("expected only order group, got %#v", got.Groups)
	}
}

func TestInstanceStatus_NotFound(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/instance/status?name=nonexistent", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestInstanceStatus_MissingName(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/instance/status", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceCreate_DuplicateName(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()

	// 先写入一个已有实例
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Status: "running"},
		},
	})

	w := doRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:     "redis-1",
		Category: "cache",
		Group:    "cache",
		Engine:   "redis",
		Type:     "standalone",
		Server:   "srv1",
		Memory:   "4Gi",
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstanceCreate_ServerNotFound(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:     "redis-1",
		Category: "cache",
		Group:    "cache",
		Engine:   "redis",
		Type:     "standalone",
		Server:   "nonexistent",
		Memory:   "4Gi",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestInstanceCreate_WithFakeAgent 用 httptest 模拟 Agent，测试完整创建流程
func TestInstanceCreate_WithFakeAgent(t *testing.T) {
	var agentReq apitypes.CreateInstanceRequest
	// 启动 fake agent
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instance/create" {
			json.NewDecoder(r.Body).Decode(&agentReq)
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer fakeAgent.Close()

	s := newTestServer(t, "")
	r := s.Router()

	// 解析 fake agent 地址
	agentHost, agentPort := fakeAgentHostPort(fakeAgent.Listener.Addr().String())

	// 添加 server 到 node，指向 fake agent
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {
				Endpoint:  agentHost,
				AgentPort: agentPort,
				Status:    "healthy",
				Capacity:  apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"},
			},
		},
	})

	w := doRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:     "redis-1",
		Category: "cache",
		Group:    "cache",
		Engine:   "redis",
		Type:     "standalone",
		Server:   "srv1",
		Port:     6379,
		Memory:   "4Gi",
		CPUs:     2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证实例状态
	instances, _ := s.state.ReadInstances()
	inst := instances.Instances["redis-1"]
	if inst == nil {
		t.Fatal("redis-1 should exist")
	}
	if inst.Status != "running" {
		t.Fatalf("expected running, got %s", inst.Status)
	}
	if inst.Role != "master" {
		t.Fatalf("expected role master, got %s", inst.Role)
	}
	if instances.Groups["cache"].Type != "standalone" {
		t.Fatalf("expected group type standalone, got %s", instances.Groups["cache"].Type)
	}
	if instances.Groups["cache"].EngineVersion != "7" {
		t.Fatalf("expected default redis version 7, got group=%q", instances.Groups["cache"].EngineVersion)
	}
	if agentReq.EngineImage != "docker.io/redis:7" {
		t.Fatalf("expected Server to send redis:7 image to Agent, got %q", agentReq.EngineImage)
	}
}

func TestInstanceCreate_RoleMaster(t *testing.T) {
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer fakeAgent.Close()
	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(fakeAgent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort, Status: "healthy"},
		},
	})
	doRequest(s.Router(), "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name: "redis-m", Category: "persistent", Group: "redis", Engine: "redis", EngineVersion: "6.2", Type: "replication",
		Server: "srv1", Port: 6379, Memory: "4Gi", CPUs: 2,
	})
	instances, _ := s.state.ReadInstances()
	if instances.Instances["redis-m"].Role != "master" {
		t.Fatalf("expected role master, got %s", instances.Instances["redis-m"].Role)
	}
}

func TestInstanceCreate_RoleReplica(t *testing.T) {
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer fakeAgent.Close()
	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(fakeAgent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort, Status: "healthy"},
		},
	})
	doRequest(s.Router(), "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name: "redis-m", Category: "persistent", Group: "redis", Engine: "redis", EngineVersion: "6.2", Type: "replication",
		Server: "srv1", Port: 6379, Memory: "4Gi", CPUs: 2,
	})
	doRequest(s.Router(), "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name: "redis-r", Category: "persistent", Engine: "redis", Type: "replication",
		Server: "srv1", Port: 6380, Memory: "4Gi", CPUs: 2, ReplicaOf: "redis-m",
	})
	instances, _ := s.state.ReadInstances()
	if instances.Instances["redis-r"].Role != "replica" {
		t.Fatalf("expected role replica, got %s", instances.Instances["redis-r"].Role)
	}
	if instances.Instances["redis-r"].Group != "redis" {
		t.Fatalf("expected replica to inherit group redis, got %s", instances.Instances["redis-r"].Group)
	}
	if instances.Groups["redis"].EngineVersion != "6.2" {
		t.Fatalf("expected replica group redis version 6.2, got %q", instances.Groups["redis"].EngineVersion)
	}
}

func TestInstanceDelete_NotFound(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "POST", "/instance/delete", map[string]interface{}{"name": "nonexistent"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestInstanceDelete_WithFakeAgent(t *testing.T) {
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer fakeAgent.Close()

	s := newTestServer(t, "")
	r := s.Router()

	agentHost, agentPort := fakeAgentHostPort(fakeAgent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {
				Endpoint:  agentHost,
				AgentPort: agentPort,
				Status:    "healthy",
				Instances: []string{"redis-1"},
				Allocated: apitypes.ResourceSpec{CPUCores: 2, Memory: "4Gi"},
			},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Memory: "4Gi", CPUs: 2, Status: "running", Role: "master"},
		},
	})

	w := doRequest(r, "POST", "/instance/delete", map[string]interface{}{"name": "redis-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证实例已删除
	instances, _ := s.state.ReadInstances()
	if _, exists := instances.Instances["redis-1"]; exists {
		t.Fatal("redis-1 should be deleted")
	}
}

// ---------- Reconcile 测试 ----------

// newFakeAgentWithContainers 创建一个返回指定容器列表的 fake agent
func newFakeAgentWithContainers(containers []map[string]interface{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instance/list" {
			json.NewEncoder(w).Encode(apitypes.APIResponse{
				Success: true,
				Data:    map[string]interface{}{"containers": containers},
			})
		} else {
			json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
		}
	}))
}

func TestReconcile_AllConsistent(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-redis-1", "running": true},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", Status: "running"},
		},
	})

	w := doRequest(s.Router(), "POST", "/reconcile", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 状态不应变化
	instances, _ := s.state.ReadInstances()
	if instances.Instances["redis-1"].Status != "running" {
		t.Fatalf("expected running, got %s", instances.Instances["redis-1"].Status)
	}
}

func TestReconcile_CreatingButRunning(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-redis-1", "running": true},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", Status: "creating"},
		},
	})

	doRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.state.ReadInstances()
	if instances.Instances["redis-1"].Status != "running" {
		t.Fatalf("expected status updated to running, got %s", instances.Instances["redis-1"].Status)
	}
}

func TestReconcile_RunningButStopped(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-redis-1", "running": false},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", Status: "running"},
		},
	})

	doRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.state.ReadInstances()
	if instances.Instances["redis-1"].Status != "unexpected_stopped" {
		t.Fatalf("expected unexpected_stopped, got %s", instances.Instances["redis-1"].Status)
	}
}

func TestReconcile_RunningButMissing(t *testing.T) {
	// Agent 返回空容器列表
	agent := newFakeAgentWithContainers([]map[string]interface{}{})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", Status: "running"},
		},
	})

	doRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.state.ReadInstances()
	if instances.Instances["redis-1"].Status != "failed" {
		t.Fatalf("expected failed, got %s", instances.Instances["redis-1"].Status)
	}
}

func TestReconcile_FailedButRunning(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-redis-1", "running": true},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.state.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", Status: "failed"},
		},
	})

	doRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.state.ReadInstances()
	if instances.Instances["redis-1"].Status != "running" {
		t.Fatalf("expected status corrected to running, got %s", instances.Instances["redis-1"].Status)
	}
}

// ---------- 辅助函数 ----------

func fakeAgentHostPort(addr string) (string, int) {
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	return host, port
}
