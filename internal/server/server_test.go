package server

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// newTestServer 创建一个用于测试的 Server，dataDir 指向临时目录
// token 参数已废弃（保留签名兼容），内部自动初始化 admin 用户。
func newTestServer(t *testing.T, token string) *Server {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		Port:    8080,
		DataDir: dir,
		Admin:   AdminConfig{Username: "admin", Password: "test"},
		Log:     LogConfig{Dir: dir + "/log", Stdout: false},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := s.InitAdmin(); err != nil {
		t.Fatalf("InitAdmin: %v", err)
	}
	t.Cleanup(func() { s.db.Close() })
	return s
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

// doAuthRequest 带认证的请求 — 自动生成 admin JWT（适用于大多数测试）
// 签名匹配 doRequest，直接替换使用。内部使用默认 jwt secret。
func doAuthRequest(router http.Handler, method, path string, body interface{}) *httptest.ResponseRecorder {
	// 使用默认 jwt secret 生成 admin token（与 Server 的 jwtSecret() 一致）
	token := makeTestJWT("admin", []byte("redis-pilot-default-jwt-secret"))
	return doRequestWithAuth(router, method, path, token, body)
}

func makeTestJWT(username string, secret []byte) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := fmt.Sprintf(`{"sub":"%s","role":"admin","exp":%d,"iat":%d}`, username, time.Now().Add(72*time.Hour).Unix(), time.Now().Unix())
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))
	msg := header + "." + payload
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(msg))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return msg + "." + sig
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

// testJWT 为测试生成一个有效的 JWT
func testJWT(t *testing.T, s *Server) string {
	t.Helper()
	token, err := s.jwtSign("admin", "admin")
	if err != nil {
		t.Fatalf("jwtSign: %v", err)
	}
	return token
}

func TestAuthMiddleware_NoToken(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/node/list", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", w.Code)
	}
}

func TestAuthMiddleware_ValidJWT(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	jwt := testJWT(t, s)
	w := doRequestWithAuth(r, "GET", "/node/list", jwt, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequestWithAuth(r, "GET", "/node/list", "wrong", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_MissingToken(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/node/list", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestDashboard_NoAuthRequired(t *testing.T) {
	s := newTestServer(t, "")
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

func TestDashboard_RootRedirectsToDashboard(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doRequest(r, "GET", "/", nil)
	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307, got %d", w.Code)
	}
	if location := w.Header().Get("Location"); location != "/dashboard" {
		t.Fatalf("expected redirect to /dashboard, got %q", location)
	}
}

// ---------- Internal Token Middleware ----------

// newInternalTokenTestServer 创建配置了 internal_token 的 Server。
func newInternalTokenTestServer(t *testing.T, internalToken string) *Server {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		Port:          8080,
		DataDir:       dir,
		InternalToken: internalToken,
		Admin:         AdminConfig{Username: "admin", Password: "test"},
		Log:           LogConfig{Dir: dir + "/log", Stdout: false},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := s.InitAdmin(); err != nil {
		t.Fatalf("InitAdmin: %v", err)
	}
	t.Cleanup(func() { s.db.Close() })
	return s
}

func TestInternalToken_Required(t *testing.T) {
	s := newInternalTokenTestServer(t, "secret-internal")
	r := s.Router()

	// 无 token -> 401
	if w := doRequest(r, "GET", "/proxy/snapshot", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without internal token, got %d", w.Code)
	}
	// 错误 token -> 401
	if w := doRequestWithAuth(r, "GET", "/proxy/snapshot", "wrong", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong internal token, got %d", w.Code)
	}
	// 用户体系 JWT 不放行内部路由（人/机器凭证分离）
	jwt := testJWT(t, s)
	if w := doRequestWithAuth(r, "GET", "/proxy/snapshot", jwt, nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with user JWT on internal route, got %d", w.Code)
	}
	// 正确 internal token -> 200（两个别名路径行为一致）
	if w := doRequestWithAuth(r, "GET", "/proxy/snapshot", "secret-internal", nil); w.Code != http.StatusOK {
		t.Fatalf("expected 200 with correct internal token, got %d: %s", w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, "GET", "/api/v1/proxy/snapshot", "secret-internal", nil); w.Code != http.StatusOK {
		t.Fatalf("expected 200 on alias path, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInternalToken_EmptySkipsAuth(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	if w := doRequest(r, "GET", "/proxy/snapshot", nil); w.Code != http.StatusOK {
		t.Fatalf("expected 200 without token when internal_token empty, got %d", w.Code)
	}
}

// ---------- Node Handlers ----------

func TestNodeQuery_Empty(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doAuthRequest(r, "GET", "/node/list", nil)
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
	w := doAuthRequest(r, "POST", "/node/add", body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证写入
	w = doAuthRequest(r, "GET", "/node/list", nil)
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
	doAuthRequest(r, "POST", "/node/add", body)
	w := doAuthRequest(r, "POST", "/node/add", body)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

func TestNodeRemove_Success(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	// 先添加
	doAuthRequest(r, "POST", "/node/add", map[string]interface{}{
		"name":   "srv1",
		"server": map[string]interface{}{"endpoint": "10.0.0.1", "agent_port": 8400},
	})
	// 再删除
	w := doAuthRequest(r, "POST", "/node/remove", map[string]interface{}{"name": "srv1"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestNodeRemove_NotFound(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doAuthRequest(r, "POST", "/node/remove", map[string]interface{}{"name": "nonexistent"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestNodeRemove_RejectsManagedInstances(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: "10.0.0.1", AgentPort: 8400},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"cache": testGroup("cache")},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {Group: "cache", Server: "srv1", DesiredState: "stopped", ObservedStatus: "stopped"},
		},
	})

	w := doAuthRequest(s.Router(), "POST", "/node/remove", map[string]string{"name": "srv1"})
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 for node with managed instance, got %d: %s", w.Code, w.Body.String())
	}
	nodes, _ := s.db.ReadNode()
	if nodes.Servers["srv1"] == nil {
		t.Fatal("node must remain after rejected removal")
	}
}

func TestNodeUpdate_Success(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	doAuthRequest(r, "POST", "/node/add", map[string]interface{}{
		"name":   "srv1",
		"server": map[string]interface{}{"endpoint": "10.0.0.1", "agent_port": 8400, "status": "healthy"},
	})
	w := doAuthRequest(r, "POST", "/node/update", map[string]interface{}{
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
	w := doAuthRequest(r, "POST", "/node/update", map[string]interface{}{
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
	w := doAuthRequest(r, "GET", "/instance/list", nil)
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
	if err := s.db.WriteInstances(&apitypes.InstancesState{
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
				Group:        "order",
				Role:         "master",
				Server:       "redis01",
				DesiredState: "running", ObservedStatus: "running",
			},
			"order-replica": {
				Group:        "order",
				Role:         "replica",
				Server:       "redis02",
				DesiredState: "running", ObservedStatus: "running",
			},
			"session-cache": {
				Group:        "session",
				Role:         "master",
				Server:       "redis02",
				DesiredState: "stopped", ObservedStatus: "stopped",
			},
		},
	}); err != nil {
		t.Fatalf("write instances: %v", err)
	}

	w := doAuthRequest(s.Router(), "GET", "/instance/list?server=redis02&engine=redis&role=replica&status=running&category=persistent&engine_version=7", nil)
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
	w := doAuthRequest(r, "GET", "/instance/status?name=nonexistent", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestInstanceStatus_MissingName(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doAuthRequest(r, "GET", "/instance/status", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceCreate_DuplicateName(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()

	// 先写入一个已有实例
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", DesiredState: "running", ObservedStatus: "running"},
		},
	})

	w := doAuthRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
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
	w := doAuthRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
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
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			testInstanceCreateWithProxy(t, disabled)
		})
	}
}

func testInstanceCreateWithProxy(t *testing.T, disabled bool) {
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
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {
				Endpoint:  agentHost,
				AgentPort: agentPort,
				Status:    "healthy",
				Capacity:  apitypes.ResourceSpec{CPUCores: 8, Memory: "32Gi"},
			},
		},
	})

	w := doAuthRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:         "redis-1",
		DisableProxy: disabled,
		Category:     "cache",
		Group:        "cache",
		Engine:       "redis",
		Type:         "standalone",
		Server:       "srv1",
		Port:         6379,
		Memory:       "4Gi",
		CPUs:         2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证实例状态
	instances, _ := s.db.ReadInstances()
	inst := instances.Instances["redis-1"]
	if inst == nil {
		t.Fatal("redis-1 should exist")
	}
	if inst.Phase() != "running" {
		t.Fatalf("expected running, got %s", inst.Phase())
	}
	if inst.Role != "master" {
		t.Fatalf("expected role master, got %s", inst.Role)
	}
	if instances.Groups["cache"].Type != "standalone" {
		t.Fatalf("expected group type standalone, got %s", instances.Groups["cache"].Type)
	}
	if (instances.Groups["cache"].Proxy == nil) != disabled {
		t.Fatalf("unexpected Proxy registration: disabled=%v group=%+v", disabled, instances.Groups["cache"])
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
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort, Status: "healthy"},
		},
	})
	doAuthRequest(s.Router(), "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name: "redis-m", Category: "persistent", Group: "redis", Engine: "redis", EngineVersion: "6.2", Type: "replication",
		Server: "srv1", Port: 6379, Memory: "4Gi", CPUs: 2,
	})
	instances, _ := s.db.ReadInstances()
	if instances.Instances["redis-m"].Role != "master" {
		t.Fatalf("expected role master, got %s", instances.Instances["redis-m"].Role)
	}
}

func TestInstanceCreate_RoleReplica(t *testing.T) {
	var createRequests []apitypes.CreateInstanceRequest
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instance/create" {
			var req apitypes.CreateInstanceRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			createRequests = append(createRequests, req)
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer fakeAgent.Close()
	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(fakeAgent.Listener.Addr().String())
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort, Status: "healthy"},
		},
	})
	doAuthRequest(s.Router(), "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name: "redis-m", Category: "persistent", Group: "redis", Engine: "redis", EngineVersion: "6.2", Type: "replication",
		Server: "srv1", Port: 6379, Memory: "4Gi", CPUs: 2, Password: "group-secret",
	})
	doAuthRequest(s.Router(), "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name: "redis-r", Category: "cache", Engine: "redis", Type: "standalone",
		Server: "srv1", Port: 6380, Memory: "4Gi", CPUs: 2, ReplicaOf: "redis-m", Password: "wrong-secret",
	})
	instances, _ := s.db.ReadInstances()
	if instances.Instances["redis-r"].Role != "replica" {
		t.Fatalf("expected role replica, got %s", instances.Instances["redis-r"].Role)
	}
	if instances.Instances["redis-r"].Group != "redis" {
		t.Fatalf("expected replica to inherit group redis, got %s", instances.Instances["redis-r"].Group)
	}
	if instances.Groups["redis"].EngineVersion != "6.2" {
		t.Fatalf("expected replica group redis version 6.2, got %q", instances.Groups["redis"].EngineVersion)
	}
	if got := instances.Groups["redis"].Password; got != "group-secret" {
		t.Fatalf("group must store front password, got %q", got)
	}
	if got := instances.Instances["redis-r"].Persistence.AOF; !got {
		t.Fatal("replica must inherit persistent category rather than caller category")
	}
	// 实例无密码：Agent 收到的 create 请求不带密码（前端密码只留在 Server 用于 Proxy 快照）
	if len(createRequests) != 2 || createRequests[1].Password != "" ||
		createRequests[1].Category != "persistent" || createRequests[1].Type != "replication" {
		t.Fatalf("agent replica request did not inherit group settings: %#v", createRequests)
	}
}

func TestInstanceReplicate_MovingSingletonDeletesOldGroup(t *testing.T) {
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer fakeAgent.Close()
	agentHost, agentPort := fakeAgentHostPort(fakeAgent.Listener.Addr().String())

	s := newTestServer(t, "")
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: agentHost, AgentPort: agentPort, Status: "healthy"},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"source": {Type: "standalone", Engine: "redis", EngineVersion: "7", Category: "cache", Password: "pw", CurrentMaster: "source-1", Proxy: &apitypes.ProxyConfig{MasterPort: 16500}},
			"target": {Type: "standalone", Engine: "redis", EngineVersion: "7", Category: "cache", Password: "pw", CurrentMaster: "target-1", Proxy: &apitypes.ProxyConfig{MasterPort: 16501}},
		},
		Instances: map[string]*apitypes.Instance{
			"source-1": {Group: "source", Role: "master", Server: "srv1", Port: 6379, DesiredState: "running", ObservedStatus: "running"},
			"target-1": {Group: "target", Role: "master", Server: "srv1", Port: 6380, DesiredState: "running", ObservedStatus: "running"},
		},
	})

	w := doAuthRequest(s.Router(), "POST", "/instance/replicate", map[string]string{
		"name": "source-1", "replica_of": "target-1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	got, _ := s.db.ReadInstances()
	if got.Groups["source"] != nil {
		t.Fatalf("empty source group must be removed, got %#v", got.Groups["source"])
	}
	if i := got.Instances["source-1"]; i.Group != "target" || i.Role != "replica" || i.ReplicaOf != "target-1" {
		t.Fatalf("unexpected migrated instance state: %#v", i)
	}
}

func TestInstanceDelete_NotFound(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()
	w := doAuthRequest(r, "POST", "/instance/delete", map[string]interface{}{"name": "nonexistent"})
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
	s.db.WriteNode(&apitypes.NodeState{
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
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Memory: "4Gi", CPUs: 2, DesiredState: "running", ObservedStatus: "running", Role: "master"},
		},
	})

	w := doAuthRequest(r, "POST", "/instance/delete", map[string]interface{}{"name": "redis-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证实例已删除
	instances, _ := s.db.ReadInstances()
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
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", DesiredState: "running", ObservedStatus: "running"},
		},
	})

	w := doAuthRequest(s.Router(), "POST", "/reconcile", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 状态不应变化
	instances, _ := s.db.ReadInstances()
	if instances.Instances["redis-1"].Phase() != "running" {
		t.Fatalf("expected running, got %s", instances.Instances["redis-1"].Phase())
	}
}

func TestReconcile_CreatingButRunning(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-redis-1", "running": true},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", DesiredState: "running", ObservedStatus: ""},
		},
	})

	doAuthRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.db.ReadInstances()
	if instances.Instances["redis-1"].Phase() != "running" {
		t.Fatalf("expected status updated to running, got %s", instances.Instances["redis-1"].Phase())
	}
}

func TestReconcile_RunningButStopped(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-redis-1", "running": false},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", DesiredState: "running", ObservedStatus: "running"},
		},
	})

	doAuthRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.db.ReadInstances()
	if instances.Instances["redis-1"].Phase() != "unexpected_stopped" {
		t.Fatalf("expected unexpected_stopped, got %s", instances.Instances["redis-1"].Phase())
	}
}

func TestReconcile_RunningButMissing(t *testing.T) {
	// Agent 返回空容器列表
	agent := newFakeAgentWithContainers([]map[string]interface{}{})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", DesiredState: "running", ObservedStatus: "running"},
		},
	})

	doAuthRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.db.ReadInstances()
	if instances.Instances["redis-1"].Phase() != "failed" {
		t.Fatalf("expected failed, got %s", instances.Instances["redis-1"].Phase())
	}
}

func TestReconcile_FailedButRunning(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-redis-1", "running": true},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Server: "srv1", Container: "redis-redis-1", DesiredState: "running", ObservedStatus: "missing"},
		},
	})

	doAuthRequest(s.Router(), "POST", "/reconcile", nil)

	instances, _ := s.db.ReadInstances()
	if instances.Instances["redis-1"].Phase() != "running" {
		t.Fatalf("expected status corrected to running, got %s", instances.Instances["redis-1"].Phase())
	}
}

// ---------- 辅助函数 ----------

func fakeAgentHostPort(addr string) (string, int) {
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	return host, port
}

// TestInstanceFailover_DirectFallback_DemotesOldMaster 验证 Sentinel 未启用时的
// fallback 路径（directFailover）会真正向旧 master 所在 Agent 下发 REPLICAOF，
// 让它追随新 master，而不仅仅是更新状态存储里的角色标记。
// 回归背景：此前 instancePromote 只调用了新 master 的 /instance/promote，
// 没有调用旧 master 的 /instance/replicate，导致旧 master 在运行时仍然是
// 独立 master（脑裂风险），即使状态存储显示它已经是 replica。
func TestInstanceFailover_DirectFallback_DemotesOldMaster(t *testing.T) {
	var newMasterPromoteCalled bool
	var oldMasterReplicateReq struct {
		Name      string `json:"name"`
		ReplicaOf string `json:"replica_of"`
	}
	var oldMasterReplicateCalled bool
	var newMasterConfigCalled bool
	var oldMasterConfigCalled bool

	// 新 master (order-replica) 所在的 fake agent
	newMasterAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instance/promote" {
			newMasterPromoteCalled = true
		}
		if r.URL.Path == "/instance/config" {
			newMasterConfigCalled = true
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer newMasterAgent.Close()

	// 旧 master (order-master) 所在的 fake agent
	oldMasterAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instance/replicate" {
			oldMasterReplicateCalled = true
			json.NewDecoder(r.Body).Decode(&oldMasterReplicateReq)
		}
		if r.URL.Path == "/instance/config" {
			oldMasterConfigCalled = true
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer oldMasterAgent.Close()

	s := newTestServer(t, "")
	r := s.Router()

	oldHost, oldPort := fakeAgentHostPort(oldMasterAgent.Listener.Addr().String())
	newHost, newPort := fakeAgentHostPort(newMasterAgent.Listener.Addr().String())

	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-old": {Endpoint: oldHost, AgentPort: oldPort, Status: "healthy"},
		"srv-new": {Endpoint: newHost, AgentPort: newPort, Status: "healthy"},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type:          "replication",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "order-master",
				Proxy:         &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-old", Port: 6379,
			},
			"order-replica": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-new", Port: 6380,
				ReplicaOf: "order-master",
			},
		},
	})

	w := doAuthRequest(r, "POST", "/instance/failover", map[string]string{"name": "order-replica"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if !newMasterPromoteCalled {
		t.Error("expected /instance/promote to be called on new master's agent")
	}
	if !oldMasterReplicateCalled {
		t.Fatal("expected /instance/replicate to be called on old master's agent to avoid split-brain")
	}
	if oldMasterReplicateReq.Name != "order-master" {
		t.Errorf("expected replicate request for order-master, got %q", oldMasterReplicateReq.Name)
	}
	wantAddr := newHost + ":6380"
	if oldMasterReplicateReq.ReplicaOf != wantAddr {
		t.Errorf("expected old master to replicate from %q, got %q", wantAddr, oldMasterReplicateReq.ReplicaOf)
	}

	instances, _ := s.db.ReadInstances()
	if instances.Instances["order-replica"].Role != "master" {
		t.Errorf("expected order-replica to become master")
	}
	if instances.Instances["order-master"].Role != "replica" {
		t.Errorf("expected order-master to become replica")
	}
	if instances.Instances["order-master"].ReplicaOf != "order-replica" {
		t.Errorf("expected order-master.ReplicaOf = order-replica, got %q", instances.Instances["order-master"].ReplicaOf)
	}

	// directFailover 必须把新角色推到磁盘配置文件，避免容器重启后角色回退（脑裂）。
	if !newMasterConfigCalled {
		t.Error("expected /instance/config to be called on new master's agent (persistGroupRoles)")
	}
	if !oldMasterConfigCalled {
		t.Error("expected /instance/config to be called on old master's agent (persistGroupRoles)")
	}
}

// TestInstanceFailover_DirectFallback_OldMasterUnreachable 验证 Sentinel 未启用时，
// 旧 master 不可达（故障转移的典型触发场景）时，failover 仍然能成功完成状态切换，
// 不会卡在中间态。
func TestInstanceFailover_DirectFallback_OldMasterUnreachable(t *testing.T) {
	newMasterAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	defer newMasterAgent.Close()

	s := newTestServer(t, "")
	r := s.Router()

	newHost, newPort := fakeAgentHostPort(newMasterAgent.Listener.Addr().String())

	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		// 旧 master 指向一个不可达端口，模拟旧 master 宕机/网络不可达。
		"srv-old": {Endpoint: "127.0.0.1", AgentPort: 1, Status: "healthy"},
		"srv-new": {Endpoint: newHost, AgentPort: newPort, Status: "healthy"},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type:          "replication",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "order-master",
				Proxy:         &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "missing", Server: "srv-old", Port: 6379,
			},
			"order-replica": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-new", Port: 6380,
				ReplicaOf: "order-master",
			},
		},
	})

	w := doAuthRequest(r, "POST", "/instance/failover", map[string]string{"name": "order-replica"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected promote to succeed even if old master is unreachable, got %d: %s", w.Code, w.Body.String())
	}

	instances, _ := s.db.ReadInstances()
	if instances.Instances["order-replica"].Role != "master" {
		t.Errorf("expected order-replica to become master despite old master being unreachable")
	}
}

// fakeSentinel 是一个极简的 RESP 协议 mock，只处理测试需要的两条命令：
// SENTINEL FAILOVER <group> 和 SENTINEL GET-MASTER-ADDR-BY-NAME <group>。
// masterAddr 用 atomic 存储，模拟 failover 完成后地址发生变化。
type fakeSentinel struct {
	listener      net.Listener
	masterAddr    atomic.Value // string, "host:port"
	failoverCalls atomic.Int32
	// failoverEffect 在收到 FAILOVER 命令时执行，用于模拟"过一会儿地址才变化"。
	failoverEffect func(fs *fakeSentinel)

	// masters 记录 SENTINEL MASTERS/MONITOR/REMOVE 维护的 group -> "host:port"。
	// 用于测试 syncSentinelNode 的行为。
	masters      sync.Map
	monitorCalls atomic.Int32
	removeCalls  atomic.Int32
	setCalls     atomic.Int32

	// cmdLog 记录收到的每条命令（原始大小写，空格连接），用于断言某条命令是否执行过。
	cmdMu  sync.Mutex
	cmdLog []string
	// failing 里的键形如 "MONITOR order" 或 "SET order auth-pass"，命中则回 -ERR。
	failing map[string]bool
}

// failCommand 让匹配的命令回复 -ERR，用于验证失败被正确隔离而不是中断整轮。
// parts 依次匹配命令动词之后的各个参数，例如 failCommand("MONITOR", "group-bad")。
func (fs *fakeSentinel) failCommand(parts ...string) {
	fs.cmdMu.Lock()
	defer fs.cmdMu.Unlock()
	if fs.failing == nil {
		fs.failing = map[string]bool{}
	}
	fs.failing[strings.Join(parts, " ")] = true
}

// sawCommand 判断是否收到过含全部 parts 的命令。
func (fs *fakeSentinel) sawCommand(parts ...string) bool {
	fs.cmdMu.Lock()
	defer fs.cmdMu.Unlock()
	for _, line := range fs.cmdLog {
		all := true
		for _, p := range parts {
			if !strings.Contains(line, p) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// shouldFail 记录命令并判断是否该注入失败。rawArgs 已去掉 "SENTINEL" 前缀。
func (fs *fakeSentinel) shouldFail(rawArgs []string) bool {
	line := strings.Join(rawArgs, " ")
	fs.cmdMu.Lock()
	defer fs.cmdMu.Unlock()
	fs.cmdLog = append(fs.cmdLog, line)
	for key := range fs.failing {
		parts := strings.Split(key, " ")
		all := true
		for _, p := range parts {
			if !strings.Contains(line, p) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// newFakeSentinelWithMasters 创建一个预置了若干 group -> "host:port" 映射的 fake sentinel，
// 用于测试 syncSentinelNode 是否会保留/重置这些映射。
func newFakeSentinelWithMasters(t *testing.T, initial map[string]string) *fakeSentinel {
	t.Helper()
	fs := newFakeSentinel(t, "")
	for group, addr := range initial {
		fs.masters.Store(group, addr)
	}
	return fs
}

func newFakeSentinel(t *testing.T, initialAddr string) *fakeSentinel {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake sentinel listen: %v", err)
	}
	fs := &fakeSentinel{listener: ln}
	fs.masterAddr.Store(initialAddr)
	go fs.serve()
	t.Cleanup(func() { ln.Close() })
	return fs
}

func (fs *fakeSentinel) addr() string {
	return fs.listener.Addr().String()
}

func (fs *fakeSentinel) serve() {
	for {
		conn, err := fs.listener.Accept()
		if err != nil {
			return
		}
		go fs.handleConn(conn)
	}
}

func (fs *fakeSentinel) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		reply, err := readRESP(r)
		if err != nil {
			return
		}
		args, _ := reply.([]interface{})
		// rawArgs 保留原始大小写（组名、host 等大小写敏感），strArgs 仅用于命令动词匹配。
		rawArgs := make([]string, 0, len(args))
		strArgs := make([]string, 0, len(args))
		for _, a := range args {
			if s, ok := a.(string); ok {
				rawArgs = append(rawArgs, s)
				strArgs = append(strArgs, strings.ToUpper(s))
			}
		}
		if len(strArgs) >= 2 && strArgs[0] == "SENTINEL" && strArgs[1] == "FAILOVER" {
			fs.failoverCalls.Add(1)
			if fs.failoverEffect != nil {
				fs.failoverEffect(fs)
			}
			fmt.Fprintf(conn, "+OK\r\n")
			continue
		}
		if len(strArgs) >= 2 && strArgs[0] == "SENTINEL" && strArgs[1] == "GET-MASTER-ADDR-BY-NAME" {
			addr, _ := fs.masterAddr.Load().(string)
			host, port, _ := net.SplitHostPort(addr)
			fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(host), host, len(port), port)
			continue
		}
		if len(strArgs) >= 2 && strArgs[0] == "SENTINEL" && strArgs[1] == "MASTERS" {
			var groups []string
			fs.masters.Range(func(k, v interface{}) bool {
				groups = append(groups, k.(string))
				return true
			})
			fmt.Fprintf(conn, "*%d\r\n", len(groups))
			for _, g := range groups {
				addr, _ := fs.masters.Load(g)
				host, port, _ := net.SplitHostPort(addr.(string))
				fmt.Fprintf(conn, "*6\r\n$4\r\nname\r\n$%d\r\n%s\r\n$2\r\nip\r\n$%d\r\n%s\r\n$4\r\nport\r\n$%d\r\n%s\r\n",
					len(g), g, len(host), host, len(port), port)
			}
			continue
		}
		if len(strArgs) >= 3 && strArgs[0] == "SENTINEL" && strArgs[1] == "MONITOR" {
			if fs.shouldFail(rawArgs[1:]) {
				fmt.Fprintf(conn, "-ERR injected failure\r\n")
				continue
			}
			fs.monitorCalls.Add(1)
			fs.masters.Store(rawArgs[2], rawArgs[3]+":"+rawArgs[4])
			fmt.Fprintf(conn, "+OK\r\n")
			continue
		}
		if len(strArgs) >= 3 && strArgs[0] == "SENTINEL" && strArgs[1] == "REMOVE" {
			if fs.shouldFail(rawArgs[1:]) {
				fmt.Fprintf(conn, "-ERR injected failure\r\n")
				continue
			}
			fs.removeCalls.Add(1)
			fs.masters.Delete(rawArgs[2])
			fmt.Fprintf(conn, "+OK\r\n")
			continue
		}
		if len(strArgs) >= 3 && strArgs[0] == "SENTINEL" && strArgs[1] == "SET" {
			if fs.shouldFail(rawArgs[1:]) {
				fmt.Fprintf(conn, "-ERR injected failure\r\n")
				continue
			}
			fs.setCalls.Add(1)
			fmt.Fprintf(conn, "+OK\r\n")
			continue
		}
		fmt.Fprintf(conn, "+OK\r\n")
	}
}

// TestInstanceFailover_SentinelManaged_TriggersRealFailoverAndSyncsState 验证
// Sentinel 托管的组上，instance failover 会真正发送 SENTINEL FAILOVER 命令，
// 而不是直接向节点下发 REPLICAOF；等待 Sentinel 完成切换后，再把结果被动同步到
// Server 自己的状态记录（复用 handleSentinelFailover）。
// 回归背景：Server 自动同步 Sentinel 的 failover 通知（handleSentinelFailover）只
// 更新状态，从不驱动节点；而 instancePromote 又绕开 Sentinel 直接操作节点——两个
// 独立的决策者可能同时改变同一份拓扑。方案改为 Sentinel 唯一权威：Server 主动请求
// 时也通过 SENTINEL FAILOVER，不再自己发 REPLICAOF。
func TestInstanceFailover_SentinelManaged_TriggersRealFailoverAndSyncsState(t *testing.T) {
	newMasterAddr := "127.0.0.1:7001"
	fs := newFakeSentinel(t, "127.0.0.1:7000") // 初始地址对应 order-master
	fs.failoverEffect = func(fs *fakeSentinel) {
		go func() {
			time.Sleep(50 * time.Millisecond)
			fs.masterAddr.Store(newMasterAddr)
		}()
	}
	sentinelHost, sentinelPortStr, _ := net.SplitHostPort(fs.addr())
	sentinelPort, _ := strconv.Atoi(sentinelPortStr)

	s := newTestServer(t, "")
	s.cfg.Sentinel = SentinelConfig{
		Enabled: true,
		Nodes:   []string{"srv-sentinel-a", "srv-sentinel-b", "srv-sentinel-c"},
		Quorum:  2,
		Port:    sentinelPort,
	}
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-sentinel-a": {Endpoint: sentinelHost, Status: "healthy"},
		"srv-sentinel-b": {Endpoint: sentinelHost, Status: "healthy"},
		"srv-sentinel-c": {Endpoint: sentinelHost, Status: "healthy"},
		"srv-old":        {Endpoint: "127.0.0.1", Status: "healthy"},
		"srv-new":        {Endpoint: "127.0.0.1", Status: "healthy"},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type:          "replication",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "order-master",
				Proxy:         &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-old", Port: 7000,
			},
			"order-replica": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-new", Port: 7001,
				ReplicaOf: "order-master",
			},
		},
	})

	r := s.Router()
	w := doAuthRequest(r, "POST", "/instance/failover", map[string]string{"name": "order-replica"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if fs.failoverCalls.Load() == 0 {
		t.Fatal("expected SENTINEL FAILOVER to be sent to the sentinel node")
	}

	var resp apitypes.APIResponse
	json.NewDecoder(w.Body).Decode(&resp)
	data, _ := resp.Data.(map[string]interface{})
	if data["new_master"] != "order-replica" {
		t.Errorf("expected new_master=order-replica in response, got %#v", data)
	}

	instances, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	if instances.Instances["order-replica"].Role != "master" {
		t.Errorf("expected order-replica to become master in state, got %#v", instances.Instances["order-replica"])
	}
	if instances.Instances["order-master"].Role != "replica" {
		t.Errorf("expected order-master to become replica in state, got %#v", instances.Instances["order-master"])
	}
	if instances.Groups["order"].CurrentMaster != "order-replica" {
		t.Errorf("expected group current master to be order-replica, got %q", instances.Groups["order"].CurrentMaster)
	}
}

// TestInstanceReplicate_RejectedOnSentinelManagedGroup 验证手动指定复制目标
// 在 Sentinel 托管的组上会被拒绝，避免与 Sentinel 的自动拓扑管理冲突。
func TestInstanceReplicate_RejectedOnSentinelManagedGroup(t *testing.T) {
	s := newTestServer(t, "")
	s.cfg.Sentinel = SentinelConfig{Enabled: true, Nodes: []string{"a", "b", "c"}, Quorum: 2}
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-old": {Endpoint: "127.0.0.1", Status: "healthy"},
		"srv-new": {Endpoint: "127.0.0.1", Status: "healthy"},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type:          "replication",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "order-master",
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-old", Port: 6379,
			},
			"order-replica": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-new", Port: 6380,
				ReplicaOf: "order-master",
			},
		},
	})

	r := s.Router()
	w := doAuthRequest(r, "POST", "/instance/replicate", map[string]string{
		"name":       "order-replica",
		"replica_of": "order-master",
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when group is Sentinel-managed, got %d: %s", w.Code, w.Body.String())
	}
}
