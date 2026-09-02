package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// parseMemoryGi 只认 Gi/G/Mi/M，其它单位/纯数字/空一律返回 0；而配额超卖校验都以
// reqXxx>0 为前置条件，0 会跳过检查。validateCapacitySpec 负责在入口拦掉这类值。
func TestValidateCapacitySpec(t *testing.T) {
	ok := []string{"4Gi", "512Mi", "10G", "100M", "1Gi"}
	for _, v := range ok {
		if err := validateCapacitySpec("memory", v); err != nil {
			t.Errorf("expected %q valid, got error: %v", v, err)
		}
	}
	bad := []string{"2GB", "2048", "", "2T", "0Gi", "abc", "1.5Gi"}
	for _, v := range bad {
		if err := validateCapacitySpec("memory", v); err == nil {
			t.Errorf("expected %q rejected, got nil error (would bypass quota)", v)
		}
	}
}

// createCallCapture 记录 fake agent 是否被调用，用于断言非法请求在 Server 侧就被拦下、
// 没有透传到 Agent。
type createCallCapture struct {
	mu     sync.Mutex
	called bool
}

func (c *createCallCapture) wasCalled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.called
}

func createTestSetup(t *testing.T, capMem string) (http.Handler, *createCallCapture) {
	t.Helper()
	cap := &createCallCapture{}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instance/create" {
			cap.mu.Lock()
			cap.called = true
			cap.mu.Unlock()
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	t.Cleanup(agent.Close)

	s := newTestServer(t, "")
	host, port := fakeAgentHostPort(agent.Listener.Addr().String())
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: host, AgentPort: port, Status: "healthy",
			Capacity: apitypes.ResourceSpec{CPUCores: 8, Memory: capMem, Disk: "500Gi"}},
	}})
	return s.Router(), cap
}

// 非法内存单位（会被 parseMemoryGi 解析成 0）必须在创建入口被拒，且不得透传到 Agent，
// 否则等于绕过内存超卖校验。
func TestInstanceCreate_BadMemoryUnit_RejectedNotForwarded(t *testing.T) {
	for _, mem := range []string{"2GB", "2048"} {
		r, cap := createTestSetup(t, "8Gi")
		w := doAuthRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
			Name: "redis-1", Category: "cache", Group: "cache", Engine: "redis",
			Type: "standalone", Server: "srv1", Port: 6379, Memory: mem, CPUs: 2,
		})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("memory=%q: expected 400, got %d: %s", mem, w.Code, w.Body.String())
		}
		if cap.wasCalled() {
			t.Fatalf("memory=%q: agent /instance/create should NOT be called (quota bypass)", mem)
		}
	}
}

// 非法磁盘单位同理需在入口被拒。
func TestInstanceCreate_BadDiskUnit_Rejected(t *testing.T) {
	r, cap := createTestSetup(t, "8Gi")
	w := doAuthRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name: "redis-1", Category: "cache", Group: "cache", Engine: "redis",
		Type: "standalone", Server: "srv1", Port: 6379, Memory: "4Gi", CPUs: 2, Disk: "50GB",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if cap.wasCalled() {
		t.Fatal("agent should NOT be called for invalid disk unit")
	}
}

// resize 路径同样要拦非法内存单位，不得透传 Agent。
func TestInstanceResize_BadMemoryUnit_RejectedNotForwarded(t *testing.T) {
	s, r, cap := resizeTestSetup(t, "16Gi", 8, "500Gi", "4Gi", 1, "10Gi")
	_ = s
	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "memory": "2GB",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if cap.wasCalled() {
		t.Fatal("agent /instance/resize should NOT be called for invalid memory unit")
	}
}

// resize 路径拦非法磁盘单位。
func TestInstanceResize_BadDiskUnit_Rejected(t *testing.T) {
	s, r, cap := resizeTestSetup(t, "16Gi", 8, "500Gi", "4Gi", 1, "10Gi")
	_ = s
	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "disk": "50GB",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if cap.wasCalled() {
		t.Fatal("agent /instance/resize should NOT be called for invalid disk unit")
	}
}
