package server

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func setupProxySnapshotServer(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t, "")
	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: "10.0.1.10", AgentPort: 8400},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// 三个条件缺一个快照就是空的（0 listeners / 0 clusters），"状态变化导致版本变化"
	// 的前提就不成立：必须是 replication 组、CurrentMaster 指向真实存在的实例、
	// 且配了 Proxy 端口（listener 的生成条件是 masterPort > 0 且有 master 端点）。
	group := testGroup("cache-1")
	group.Type = "replication"
	group.Proxy = &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"cache": group},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Group: "cache", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestProxySnapshot_NoParamsReturnsImmediately 不带长轮询参数时行为不变。
func TestProxySnapshot_NoParamsReturnsImmediately(t *testing.T) {
	s := setupProxySnapshotServer(t)
	start := time.Now()
	w := doRequest(s.Router(), "GET", "/proxy/snapshot", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("状态码 %d：%s", w.Code, w.Body.String())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("不带参数不应挂起，耗时 %v", elapsed)
	}
}

// TestProxySnapshot_StaleVersionReturnsImmediately 版本已过期时立即返回新快照，
// 不能因为带了 wait 就白等。
func TestProxySnapshot_StaleVersionReturnsImmediately(t *testing.T) {
	s := setupProxySnapshotServer(t)
	start := time.Now()
	w := doRequest(s.Router(), "GET",
		"/proxy/snapshot?known_version=obviously-stale&wait=10s", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("状态码 %d：%s", w.Code, w.Body.String())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("版本不匹配时应立即返回，耗时 %v", elapsed)
	}
}

// TestProxySnapshot_UnchangedWaitsThenReturns304 版本未变时挂起到超时并返回 304。
//
// 超时上限的意义不是省资源，而是保证这条路径永远会结束：任何"等待唤醒"的机制都有
// 漏唤醒的可能，没有超时就会让 Proxy 的配置永久停在旧值，且不报错。
func TestProxySnapshot_UnchangedWaitsThenReturns304(t *testing.T) {
	s := setupProxySnapshotServer(t)
	r := s.Router()

	// 先拿到当前版本
	w := doRequest(r, "GET", "/proxy/snapshot", nil)
	current := parseSnapshotVersion(t, w.Body.Bytes())

	start := time.Now()
	w = doRequest(r, "GET", "/proxy/snapshot?known_version="+current+"&wait=400ms", nil)
	elapsed := time.Since(start)

	if w.Code != http.StatusNotModified {
		t.Fatalf("期望 304，实际 %d：%s", w.Code, w.Body.String())
	}
	if elapsed < 300*time.Millisecond {
		t.Errorf("应当挂起接近 wait 时长，实际只有 %v（没有真正等待）", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("挂起时间远超 wait，耗时 %v", elapsed)
	}
}

// TestProxySnapshot_WakesOnStateChange 验证状态变化能把挂起的请求唤醒，
// 这是把故障转移传播延迟从"一个轮询周期"降到接近即时的关键。
func TestProxySnapshot_WakesOnStateChange(t *testing.T) {
	s := setupProxySnapshotServer(t)
	r := s.Router()

	w := doRequest(r, "GET", "/proxy/snapshot", nil)
	current := parseSnapshotVersion(t, w.Body.Bytes())

	// 挂起一个长轮询，随后改变状态
	type result struct {
		code    int
		body    []byte
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		w := doRequest(r, "GET", "/proxy/snapshot?known_version="+current+"&wait=20s", nil)
		done <- result{w.Code, w.Body.Bytes(), time.Since(start)}
	}()

	time.Sleep(200 * time.Millisecond)
	// 加一个实例，代理配置随之变化
	if err := s.db.WithInstances(func(st *apitypes.InstancesState) error {
		st.Instances["cache-2"] = &apitypes.Instance{
			Group: "cache", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "cache-1",
			DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-done:
		if res.code != http.StatusOK {
			t.Fatalf("期望 200，实际 %d：%s", res.code, res.body)
		}
		if v := parseSnapshotVersion(t, res.body); v == current {
			t.Error("返回的还是旧版本")
		}
		// 唤醒应当很快，远小于 wait
		if res.elapsed > 5*time.Second {
			t.Errorf("状态已变化却等了 %v 才返回", res.elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("状态已变化但长轮询没被唤醒")
	}
}

// TestProxySnapshot_WaitIsCapped 验证 wait 被截断到上限，
// 调用方传一个极大值不能让请求无限挂着。
func TestProxySnapshot_WaitIsCapped(t *testing.T) {
	if got := parseProxySnapshotWait("100h"); got != maxProxySnapshotWait {
		t.Errorf("parseProxySnapshotWait(100h) = %v, want %v", got, maxProxySnapshotWait)
	}
	for _, raw := range []string{"", "garbage", "0s", "-5s"} {
		if got := parseProxySnapshotWait(raw); got != 0 {
			t.Errorf("parseProxySnapshotWait(%q) = %v, want 0（关闭长轮询）", raw, got)
		}
	}
	if got := parseProxySnapshotWait("5s"); got != 5*time.Second {
		t.Errorf("parseProxySnapshotWait(5s) = %v", got)
	}
}

// TestProxySnapshot_AuditWriteDoesNotWake 审计日志写入不应唤醒长轮询：
// 它不影响代理配置，而审计写入很频繁，每条都唤醒会退化成高频重算。
func TestProxySnapshot_AuditWriteDoesNotWake(t *testing.T) {
	s := setupProxySnapshotServer(t)
	r := s.Router()

	w := doRequest(r, "GET", "/proxy/snapshot", nil)
	current := parseSnapshotVersion(t, w.Body.Bytes())

	done := make(chan int, 1)
	go func() {
		w := doRequest(r, "GET", "/proxy/snapshot?known_version="+current+"&wait=600ms", nil)
		done <- w.Code
	}()

	time.Sleep(100 * time.Millisecond)
	s.audit.Log(auditTestRecord())

	select {
	case code := <-done:
		if code != http.StatusNotModified {
			t.Errorf("期望 304（审计写入不改变代理配置），实际 %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("长轮询没有按 wait 超时返回")
	}
}

// TestProxySnapshot_DoesNotMissWriteRacingTheGenerationBaseline 锁住"代际基线必须早于
// 读状态"这个顺序。
//
// 顺序反了（先算快照、再取代际）会漏掉一次变更：落在"读完实例状态"和"取代际"之间的写入，
// 其代际已经被计入基线，而它的效果又没进这份快照，于是等待循环永远不会为它触发重算。这次
// 变更要等满整个 wait（默认 20s）才被下一次请求发现，比改造前 2 秒定期轮询还慢一个数量级
// ——故障转移后 Proxy 会在这 20 秒里持续把写流量发给已经降级的旧 master。
//
// 用注入点把写入精确放进那个窗口，而不是靠并发去撞几毫秒的时间差：撞不到的测试等于没有测试。
func TestProxySnapshot_DoesNotMissWriteRacingTheGenerationBaseline(t *testing.T) {
	s := setupProxySnapshotServer(t)
	r := s.Router()

	w := doRequest(r, "GET", "/proxy/snapshot", nil)
	current := parseSnapshotVersion(t, w.Body.Bytes())

	// 读完状态之后、快照算完之前做一次会改变代理配置的写入。只做一次：后续的重算必须
	// 看到一个稳定的新状态，否则永远追不上。
	var once sync.Once
	proxySnapshotAfterStateReadHook = func() {
		once.Do(func() {
			if err := s.db.WithInstances(func(st *apitypes.InstancesState) error {
				st.Instances["cache-2"] = &apitypes.Instance{
					Group: "cache", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "cache-1",
					DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				}
				return nil
			}); err != nil {
				t.Errorf("注入写入失败: %v", err)
			}
		})
	}
	t.Cleanup(func() { proxySnapshotAfterStateReadHook = nil })

	start := time.Now()
	w = doRequest(r, "GET", "/proxy/snapshot?known_version="+current+"&wait=3s", nil)
	elapsed := time.Since(start)

	if w.Code == http.StatusNotModified {
		t.Fatalf("变更被漏掉了：代际基线取在读状态之后，这次写入既不在基线里也不在快照里（耗时 %v）", elapsed)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("期望 200，实际 %d：%s", w.Code, w.Body.String())
	}
	if v := parseSnapshotVersion(t, w.Body.Bytes()); v == current {
		t.Error("返回的还是旧版本")
	}
	if elapsed > 2*time.Second {
		t.Errorf("变更应当在一个 100ms 检查周期内被发现，实际等了 %v", elapsed)
	}
}

// parseSnapshotVersion 从响应体里取出快照版本。
func parseSnapshotVersion(t *testing.T, body []byte) string {
	t.Helper()
	var envelope struct {
		Success bool `json:"success"`
		Data    struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("解析响应失败: %v (%s)", err, body)
	}
	if envelope.Data.Version == "" {
		t.Fatalf("响应里没有 version: %s", body)
	}
	return envelope.Data.Version
}

func auditTestRecord() store.AuditRecord {
	return store.AuditRecord{
		Operator: "test",
		Action:   "test.noop",
		Level:    audit.LevelNormal,
		Result:   "success",
	}
}
