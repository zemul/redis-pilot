package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// resizeTestSetup 起一个 fake agent 并 seed 一台节点 + 一个实例，返回 server、router
// 以及记录 /instance/resize 调用的回调数据。capMem/capCPU/capDisk 为节点容量，
// instMem/instCPU/instDisk 为实例当前规格。
func resizeTestSetup(t *testing.T, capMem string, capCPU float64, capDisk string,
	instMem string, instCPU float64, instDisk string) (*Server, http.Handler, *resizeCapture) {
	t.Helper()
	cap := &resizeCapture{}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instance/resize" {
			cap.mu.Lock()
			cap.called = true
			json.NewDecoder(r.Body).Decode(&cap.payload)
			cap.mu.Unlock()
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
	}))
	t.Cleanup(agent.Close)

	s := newTestServer(t, "")
	host, port := fakeAgentHostPort(agent.Listener.Addr().String())
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-1": {
			Endpoint: host, AgentPort: port, Status: "healthy",
			Capacity: apitypes.ResourceSpec{CPUCores: capCPU, Memory: capMem, Disk: capDisk},
		},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "standalone", Engine: "redis", Category: "cache", CurrentMaster: "order-1"},
		},
		Instances: map[string]*apitypes.Instance{
			"order-1": {
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running",
				Server: "srv-1", Port: 6379, Memory: instMem, CPUs: instCPU, Disk: instDisk,
			},
		},
	})
	return s, s.Router(), cap
}

type resizeCapture struct {
	mu      sync.Mutex
	called  bool
	payload map[string]interface{}
}

func (c *resizeCapture) wasCalled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.called
}

func readInst(t *testing.T, s *Server, name string) *apitypes.Instance {
	t.Helper()
	is, err := s.db.ReadInstances()
	if err != nil {
		t.Fatalf("ReadInstances: %v", err)
	}
	inst := is.Instances[name]
	if inst == nil {
		t.Fatalf("instance %s not found", name)
	}
	return inst
}

// CPU-only 变配：转发 Agent 且持久化，disk/memory 不变。
func TestInstanceResize_CPUOnly_ForwardsAndPersists(t *testing.T) {
	s, r, cap := resizeTestSetup(t, "16Gi", 8, "500Gi", "4Gi", 1, "10Gi")

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "cpus": 4.0,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !cap.wasCalled() {
		t.Fatal("expected agent /instance/resize to be called")
	}
	if got := cap.payload["cpus"]; got != 4.0 {
		t.Errorf("expected agent payload cpus=4, got %v", got)
	}
	if _, ok := cap.payload["memory"]; ok {
		t.Errorf("memory should not be sent when unchanged, got %v", cap.payload["memory"])
	}
	inst := readInst(t, s, "order-1")
	if inst.CPUs != 4 {
		t.Errorf("expected persisted cpus=4, got %g", inst.CPUs)
	}
	if inst.Memory != "4Gi" {
		t.Errorf("memory should be unchanged 4Gi, got %s", inst.Memory)
	}
}

// 仅改磁盘：不触碰 Agent，只更新控制面记账。
func TestInstanceResize_DiskOnly_NoAgentCall(t *testing.T) {
	s, r, cap := resizeTestSetup(t, "16Gi", 8, "500Gi", "4Gi", 1, "10Gi")

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "disk": "100Gi",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if cap.wasCalled() {
		t.Error("agent should NOT be called for disk-only resize")
	}
	if inst := readInst(t, s, "order-1"); inst.Disk != "100Gi" {
		t.Errorf("expected persisted disk=100Gi, got %s", inst.Disk)
	}
}

// 升配内存超过节点容量 → 409。
func TestInstanceResize_MemoryUpscale_InsufficientCapacity(t *testing.T) {
	// 节点仅 8Gi，实例当前 4Gi，请求升到 16Gi → 排除自身后 0+16 > 8 → 拒绝。
	_, r, cap := resizeTestSetup(t, "8Gi", 8, "500Gi", "4Gi", 1, "10Gi")

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "memory": "16Gi",
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
	if cap.wasCalled() {
		t.Error("agent should NOT be called when capacity check fails")
	}
}

// 容量校验排除自身：实例几乎占满节点，升到节点满容量仍应放行。
func TestInstanceResize_UpscaleExcludesSelf(t *testing.T) {
	// 节点 8Gi，实例当前占 6Gi（无其它实例）。升到 8Gi：排除自身后 0+8 <= 8 → 放行。
	s, r, cap := resizeTestSetup(t, "8Gi", 8, "500Gi", "6Gi", 1, "10Gi")

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "memory": "8Gi",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (self excluded from capacity), got %d: %s", w.Code, w.Body.String())
	}
	if !cap.wasCalled() {
		t.Error("expected agent to be called")
	}
	if got := cap.payload["memory"]; got != "8Gi" {
		t.Errorf("expected agent payload memory=8Gi, got %v", got)
	}
	if inst := readInst(t, s, "order-1"); inst.Memory != "8Gi" {
		t.Errorf("expected persisted memory=8Gi, got %s", inst.Memory)
	}
}

// 降配内存低于当前使用量 → OOM 护栏 400。
func TestInstanceResize_MemoryDownscale_OOMGuard(t *testing.T) {
	s, r, cap := resizeTestSetup(t, "16Gi", 8, "500Gi", "8Gi", 1, "10Gi")
	// 模拟 reconcile 采集：当前已用 3Gi。降到 2Gi < 3Gi*1.1 → 拒绝。
	s.memoryUsedMu.Lock()
	s.memoryUsed["order-1"] = 3 * 1024 * 1024 * 1024
	s.memoryUsedMu.Unlock()

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "memory": "2Gi",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 OOM guard, got %d: %s", w.Code, w.Body.String())
	}
	if cap.wasCalled() {
		t.Error("agent should NOT be called when OOM guard trips")
	}
}

// 新实例场景：reconcile 尚未采集，memoryUsed 缓存为空（used==0）。
// 此时 110% 余量护栏无从计算，必须由绝对下限（minResizeMemoryBytes）兜底，
// 否则过小的内存值会穿透到 agent 压死容器（线上实测过的 bug）。
func TestInstanceResize_MemoryDownscale_AbsoluteFloor_NoUsageCache(t *testing.T) {
	_, r, cap := resizeTestSetup(t, "16Gi", 8, "500Gi", "8Gi", 1, "10Gi")
	// 刻意不预置 s.memoryUsed["order-1"]，模拟刚创建、未采集的新实例。

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "memory": "1Mi", // 远低于 128Mi 下限
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 absolute-floor guard, got %d: %s", w.Code, w.Body.String())
	}
	if cap.wasCalled() {
		t.Error("agent should NOT be called when the memory floor guard trips")
	}
}

// 无用量缓存但内存高于绝对下限 → 放行（不应误伤合法降配）。
func TestInstanceResize_MemoryDownscale_AboveFloor_NoUsageCache(t *testing.T) {
	s, r, _ := resizeTestSetup(t, "16Gi", 8, "500Gi", "8Gi", 1, "10Gi")
	// 同样不预置 memoryUsed。

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "memory": "512Mi", // 高于 128Mi 下限
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if inst := readInst(t, s, "order-1"); inst.Memory != "512Mi" {
		t.Errorf("expected persisted memory=512Mi, got %s", inst.Memory)
	}
}

// 降配内存但仍高于使用量 → 放行。
func TestInstanceResize_MemoryDownscale_WithinHeadroom(t *testing.T) {
	s, r, _ := resizeTestSetup(t, "16Gi", 8, "500Gi", "8Gi", 1, "10Gi")
	s.memoryUsedMu.Lock()
	s.memoryUsed["order-1"] = 3 * 1024 * 1024 * 1024 // 已用 3Gi
	s.memoryUsedMu.Unlock()

	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "memory": "4Gi", // 4Gi > 3Gi*1.1 ≈ 3.3Gi → 放行
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if inst := readInst(t, s, "order-1"); inst.Memory != "4Gi" {
		t.Errorf("expected persisted memory=4Gi, got %s", inst.Memory)
	}
}

// 未指定任何维度 → 400。
func TestInstanceResize_NoFields(t *testing.T) {
	_, r, _ := resizeTestSetup(t, "16Gi", 8, "500Gi", "4Gi", 1, "10Gi")
	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{"name": "order-1"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// cpus <= 0 → 400。
func TestInstanceResize_InvalidCPUs(t *testing.T) {
	_, r, _ := resizeTestSetup(t, "16Gi", 8, "500Gi", "4Gi", 1, "10Gi")
	w := doAuthRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "order-1", "cpus": 0.0,
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for cpus=0, got %d: %s", w.Code, w.Body.String())
	}
}
