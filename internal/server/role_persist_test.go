package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// recordingAgent 是能记录 /instance/config 与 /instance/replicate 调用的 fake Agent，
// 并可按实例返回磁盘配置文件里的角色（configured_replica_of）和运行时实际复制目标
// （runtime_replica_of）。
type recordingAgent struct {
	*httptest.Server
	mu              sync.Mutex
	configCalls     []map[string]interface{}
	replicateCalls  []map[string]interface{}
	containers      []map[string]interface{}
	configured      map[string]string
	runtime         map[string]string
	failConfig      bool
	failReplicate   bool
	hasRuntimeField bool
}

func newRecordingAgent(containers []map[string]interface{}, configured map[string]string) *recordingAgent {
	ra := &recordingAgent{containers: containers, configured: configured}
	ra.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instance/list":
			ra.mu.Lock()
			data := map[string]interface{}{"containers": ra.containers}
			if ra.configured != nil {
				data["configured_replica_of"] = ra.configured
			}
			if ra.hasRuntimeField {
				data["runtime_replica_of"] = ra.runtime
			}
			ra.mu.Unlock()
			json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: data})
		case "/instance/config":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			ra.mu.Lock()
			ra.configCalls = append(ra.configCalls, body)
			shouldFail := ra.failConfig
			ra.mu.Unlock()
			if shouldFail {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(apitypes.APIResponse{Success: false, Error: "injected config failure"})
				return
			}
			json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
		case "/instance/replicate":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			ra.mu.Lock()
			ra.replicateCalls = append(ra.replicateCalls, body)
			shouldFail := ra.failReplicate
			ra.mu.Unlock()
			if shouldFail {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(apitypes.APIResponse{Success: false, Error: "injected replicate failure"})
				return
			}
			json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
		default:
			json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true})
		}
	}))
	return ra
}

// setRuntimeReplicaOf 让 /instance/list 附带 runtime_replica_of 字段。
// 显式调用才带上，用于验证"旧 Agent 不上报该字段时 Server 不会误判"。
func (ra *recordingAgent) setRuntimeReplicaOf(m map[string]string) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.runtime = m
	ra.hasRuntimeField = true
}

func (ra *recordingAgent) setFailConfig(v bool) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.failConfig = v
}

func (ra *recordingAgent) setFailReplicate(v bool) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.failReplicate = v
}

func (ra *recordingAgent) calls() []map[string]interface{} {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	out := make([]map[string]interface{}, len(ra.configCalls))
	copy(out, ra.configCalls)
	return out
}

// replicateCallsFor 返回针对指定实例的所有 /instance/replicate 调用。
func (ra *recordingAgent) replicateCallsFor(name string) []map[string]interface{} {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	var out []map[string]interface{}
	for _, c := range ra.replicateCalls {
		if c["name"] == name {
			out = append(out, c)
		}
	}
	return out
}

// callFor 返回针对指定实例的最后一次 config 调用。
func (ra *recordingAgent) callFor(name string) map[string]interface{} {
	var found map[string]interface{}
	for _, c := range ra.calls() {
		if c["name"] == name {
			found = c
		}
	}
	return found
}

// setupReplicationGroup 建一个 master + replica 的复制组，两个实例都在同一台 fake Agent 上。
// 返回 Agent 的实际 host，供调用方构造期望的 ip:port（httptest 监听在 127.0.0.1，
// 不能硬编码地址）。
func setupReplicationGroup(t *testing.T, s *Server, agentAddr string, currentMaster string) string {
	t.Helper()
	agentHost, agentPort := fakeAgentHostPort(agentAddr)
	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	}); err != nil {
		t.Fatal(err)
	}
	group := testGroup("order")
	group.Type = "replication"
	group.Engine = "redis"
	group.CurrentMaster = currentMaster
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"order": group},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Server: "srv1", Port: 6379, DesiredState: "running", ObservedStatus: "running",
				Role: roleFor("order-master", currentMaster), Password: "pw",
				Container: "redis-order-master",
				ReplicaOf: replicaOfFor("order-master", currentMaster),
			},
			"order-replica": {
				Group: "order", Server: "srv1", Port: 6380, DesiredState: "running", ObservedStatus: "running",
				Role: roleFor("order-replica", currentMaster), Password: "pw",
				Container: "redis-order-replica",
				ReplicaOf: replicaOfFor("order-replica", currentMaster),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return agentHost
}

func roleFor(name, currentMaster string) string {
	if name == currentMaster {
		return "master"
	}
	return "replica"
}

func replicaOfFor(name, currentMaster string) string {
	if name == currentMaster {
		return ""
	}
	return currentMaster
}

// ---------------------------------------------------------------------------

func TestResolveReplicaOfAddr(t *testing.T) {
	node := &apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: "10.0.1.10"},
		},
	}
	instances := &apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"order-master":  {Server: "srv1", Port: 6379, Role: "master"},
			"order-replica": {Server: "srv1", Port: 6380, Role: "replica"},
		},
	}

	t.Run("master 名解析为地址", func(t *testing.T) {
		got, err := resolveReplicaOfAddr(node, instances, "order-master")
		if err != nil {
			t.Fatal(err)
		}
		if got != "10.0.1.10:6379" {
			t.Errorf("got %q", got)
		}
	})

	// 与 resolveReplicaOf 的关键区别：不断言目标必须是 master。角色落盘发生在故障转移
	// 前后，组内角色可能正处于中间状态，此时需要如实写出 Server 的记录而不是校验它。
	t.Run("目标非 master 也能解析", func(t *testing.T) {
		got, err := resolveReplicaOfAddr(node, instances, "order-replica")
		if err != nil {
			t.Fatalf("不应断言目标必须是 master：%v", err)
		}
		if got != "10.0.1.10:6380" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("空表示 master", func(t *testing.T) {
		got, err := resolveReplicaOfAddr(node, instances, "")
		if err != nil || got != "" {
			t.Errorf("got %q, err %v", got, err)
		}
	})

	t.Run("实例不存在报错", func(t *testing.T) {
		if _, err := resolveReplicaOfAddr(node, instances, "nope"); err == nil {
			t.Error("应当报错")
		}
	})
}

func TestDetectRoleDrift(t *testing.T) {
	node := &apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: "10.0.1.10"}},
	}
	instances := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", CurrentMaster: "order-replica"},
			"cache": {Type: "standalone"},
		},
		Instances: map[string]*apitypes.Instance{
			// 故障转移后：order-replica 成了 master，order-master 降为它的 replica
			"order-master":  {Group: "order", Server: "srv1", Port: 6379, Role: "replica", ReplicaOf: "order-replica"},
			"order-replica": {Group: "order", Server: "srv1", Port: 6380, Role: "master", ReplicaOf: ""},
			"cache-1":       {Group: "cache", Server: "srv1", Port: 6390, Role: "master"},
		},
	}
	names := []string{"order-master", "order-replica", "cache-1"}

	t.Run("检测出旧 master 的配置文件仍写着自己是 master", func(t *testing.T) {
		// 磁盘上 order-master 没有 replicaof（还是老的 master 配置），
		// 但 Server 记录它现在应该是 order-replica 的 replica
		drifts := detectRoleDrift(node, instances, "srv1", names, map[string]string{
			"order-master":  "",
			"order-replica": "",
			"cache-1":       "",
		})
		if len(drifts) != 1 {
			t.Fatalf("期望 1 处漂移，得到 %d: %+v", len(drifts), drifts)
		}
		d := drifts[0]
		if d.Instance != "order-master" {
			t.Errorf("漂移实例 = %s", d.Instance)
		}
		if d.Configured != "" || d.Expected != "10.0.1.10:6380" {
			t.Errorf("configured=%q expected=%q", d.Configured, d.Expected)
		}
	})

	t.Run("角色一致时无漂移", func(t *testing.T) {
		drifts := detectRoleDrift(node, instances, "srv1", names, map[string]string{
			"order-master":  "10.0.1.10:6380",
			"order-replica": "",
			"cache-1":       "",
		})
		if len(drifts) != 0 {
			t.Errorf("不应有漂移，得到 %+v", drifts)
		}
	})

	// standalone 组没有角色概念，不该被卷进来
	t.Run("standalone 组被跳过", func(t *testing.T) {
		drifts := detectRoleDrift(node, instances, "srv1", []string{"cache-1"}, map[string]string{
			"cache-1": "10.0.1.10:9999", // 就算有值也不该判定漂移
		})
		if len(drifts) != 0 {
			t.Errorf("standalone 不应判定漂移，得到 %+v", drifts)
		}
	})

	// 旧版本 Agent 不上报该字段时，不能把整组当成漂移去覆盖配置文件
	t.Run("未上报的实例被跳过", func(t *testing.T) {
		drifts := detectRoleDrift(node, instances, "srv1", names, map[string]string{})
		if len(drifts) != 0 {
			t.Errorf("未上报时不应判定漂移，得到 %+v", drifts)
		}
	})
}

// TestReconcile_CorrectsRoleDrift 是 Wave 1 自愈路径的端到端验证。
//
// 场景：Sentinel 已经把 order-replica 提为 master、order-master 降为 replica，Server 的
// 记录也同步了，但 order-master 的磁盘配置文件还写着旧角色（没有 replicaof）。这种状态下
// 容器一旦重启，order-master 就会以 master 身份起来，与 Sentinel 的认知冲突。
//
// reconcile 应当发现这处漂移并推一次配置重写，且不重启容器。
func TestReconcile_CorrectsRoleDrift(t *testing.T) {
	agent := newRecordingAgent(
		[]map[string]interface{}{
			{"name": "redis-order-master", "running": true},
			{"name": "redis-order-replica", "running": true},
		},
		// 磁盘上两个实例都没有 replicaof —— order-master 的这份是陈旧的
		map[string]string{"order-master": "", "order-replica": ""},
	)
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, agent.Listener.Addr().String(), "order-replica")
	wantAddr := agentHost + ":6380" // order-replica 是新 master

	if _, err := s.runReconcile(); err != nil {
		t.Fatal(err)
	}

	call := agent.callFor("order-master")
	if call == nil {
		t.Fatal("reconcile 应当为 order-master 推一次配置重写")
	}
	if call["rewrite_config_only"] != true {
		t.Errorf("必须使用 rewrite_config_only 模式，避免重启容器，实际载荷：%+v", call)
	}
	if call["replica_of"] != wantAddr {
		t.Errorf("replica_of = %v, want %s", call["replica_of"], wantAddr)
	}
	if call["restart"] == true {
		t.Error("角色落盘不得触发重启")
	}

	// order-replica 的磁盘角色（master，无 replicaof）与记录一致，不该被推
	if c := agent.callFor("order-replica"); c != nil {
		t.Errorf("order-replica 角色一致，不应推送配置重写：%+v", c)
	}
}

// TestReconcile_NoDriftNoPush 确认角色一致时 reconcile 不做多余的 Agent 调用。
// reconcile 每 30 秒跑一次，无条件推送会变成持续的写放大。
func TestReconcile_NoDriftNoPush(t *testing.T) {
	// 先建一个 Server 拿到 Agent 的真实 host，再据此构造"磁盘上已经正确"的角色
	probe := newRecordingAgent(nil, nil)
	agentHost, _ := fakeAgentHostPort(probe.Listener.Addr().String())
	probe.Close()

	agent := newRecordingAgent(
		[]map[string]interface{}{
			{"name": "redis-order-master", "running": true},
			{"name": "redis-order-replica", "running": true},
		},
		// order-master 是 master（无 replicaof），order-replica 指向它 —— 与记录一致
		map[string]string{"order-master": "", "order-replica": agentHost + ":6379"},
	)
	defer agent.Close()

	s := newTestServer(t, "")
	setupReplicationGroup(t, s, agent.Listener.Addr().String(), "order-master")

	if _, err := s.runReconcile(); err != nil {
		t.Fatal(err)
	}
	if calls := agent.calls(); len(calls) != 0 {
		t.Errorf("角色全部一致时不应推送配置重写，实际推了 %d 次：%+v", len(calls), calls)
	}
}
