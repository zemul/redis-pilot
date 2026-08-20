package server

import (
	"fmt"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// auditResultsFor 返回某个 action 下所有审计记录的 result，最近的在前。
func auditResultsFor(t *testing.T, s *Server, action string) []string {
	t.Helper()
	records, err := s.audit.Query(store.AuditFilter{Action: action, Limit: 50})
	if err != nil {
		t.Fatalf("query audit: %v", err)
	}
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, r.Result)
	}
	return out
}

// ---------------------------------------------------------------------------
// 角色漂移（磁盘配置文件）：修正必须基于锁内重新读取的状态
// ---------------------------------------------------------------------------

// TestReconcileRoleDrift_SkipsWhenTopologyChangedSinceDetection 验证拓扑在
// 「检测漂移」和「修正漂移」之间变化时，不会把过期的 replicaof 写进配置文件。
//
// 这是修正前的真实缺陷：reconcileRoleDrift 虽然持 topologyMu，但 node/instances/drifts
// 三个参数都是 runReconcile 在锁外、且在遍历所有节点 Agent 之前读的。那段窗口是所有
// Agent 往返之和（每个最多 10s），窗口里完成一次故障转移，就会把刚被 failover 改对的
// 角色按旧值写回磁盘，容器下次重启即按错角色起来——正是角色落盘机制要消除的那个回退。
func TestReconcileRoleDrift_SkipsWhenTopologyChangedSinceDetection(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")

	// 检测时刻认为 order-replica 应该跟随 6380（一个已经过期的 master 地址）；
	// 当前状态里 order-replica 的 master 是 order-master（6379）。
	stale := []roleDrift{{
		Instance:   "order-replica",
		Server:     "srv1",
		Configured: "",
		Expected:   fmt.Sprintf("%s:6380", agentHost),
	}}
	s.reconcileRoleDrift(stale)

	if got := ra.calls(); len(got) != 0 {
		t.Fatalf("拓扑已变，不应下发任何配置重写，实际下发了 %d 次：%v", len(got), got)
	}
	if results := auditResultsFor(t, s, "reconcile.role_drift"); len(results) != 1 || results[0] != "skipped" {
		t.Errorf("审计应记录 skipped，实际 %v", results)
	}
}

// TestReconcileRoleDrift_PushesWhenExpectedStillMatches 验证期望值仍然成立时会照常补推。
// 上一个测试的"跳过"不能是靠一律不干活实现的。
func TestReconcileRoleDrift_PushesWhenExpectedStillMatches(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")
	expected := fmt.Sprintf("%s:6379", agentHost)

	s.reconcileRoleDrift([]roleDrift{{
		Instance:   "order-replica",
		Server:     "srv1",
		Configured: "", // 磁盘上写成了 master
		Expected:   expected,
	}})

	call := ra.callFor("order-replica")
	if call == nil {
		t.Fatal("期望值未变，应当补推一次配置重写")
	}
	if call["replica_of"] != expected {
		t.Errorf("replica_of = %v, want %v", call["replica_of"], expected)
	}
	if call["rewrite_config_only"] != true {
		t.Errorf("补推必须只重写配置文件、不重启容器，实际 rewrite_config_only=%v", call["rewrite_config_only"])
	}
	if results := auditResultsFor(t, s, "reconcile.role_drift"); len(results) != 1 || results[0] != "corrected" {
		t.Errorf("审计应记录 corrected，实际 %v", results)
	}
}

// TestReconcileRoleDrift_AuditReflectsPushFailure 验证下发失败时审计不会声称修好了。
//
// 此前审计记录在 runReconcile 里对每个 drift 无条件写 "corrected"，而 pushConfigRewrite
// 失败只是 continue——排查故障时审计日志会把人引开。
func TestReconcileRoleDrift_AuditReflectsPushFailure(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()
	ra.setFailConfig(true)

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")

	s.reconcileRoleDrift([]roleDrift{{
		Instance: "order-replica", Server: "srv1",
		Configured: "", Expected: fmt.Sprintf("%s:6379", agentHost),
	}})

	results := auditResultsFor(t, s, "reconcile.role_drift")
	if len(results) != 1 || results[0] != "failed" {
		t.Errorf("下发失败时审计应记录 failed，实际 %v", results)
	}
}

// ---------------------------------------------------------------------------
// 运行时复制漂移：检测
// ---------------------------------------------------------------------------

func TestDetectReplicaDrift(t *testing.T) {
	node := &apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: "10.0.1.10"},
	}}
	instances := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", CurrentMaster: "order-master"},
			"cache": {Type: "standalone"},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master":  {Group: "order", Server: "srv1", Port: 6379, Role: "master"},
			"order-replica": {Group: "order", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "order-master"},
			"cache-1":       {Group: "cache", Server: "srv1", Port: 6390, Role: "master"},
		},
	}
	names := []string{"order-master", "order-replica", "cache-1"}

	t.Run("运行时与记录一致时无漂移", func(t *testing.T) {
		drifts := detectReplicaDrift(node, instances, "srv1", names, map[string]string{
			"order-master":  "",
			"order-replica": "10.0.1.10:6379",
		})
		if len(drifts) != 0 {
			t.Fatalf("不应有漂移，实际 %+v", drifts)
		}
	})

	t.Run("replica 挂在错误的 master 上时报漂移", func(t *testing.T) {
		drifts := detectReplicaDrift(node, instances, "srv1", names, map[string]string{
			"order-master":  "",
			"order-replica": "10.0.1.99:6379", // 还挂在已降级的旧 master 上
		})
		if len(drifts) != 1 || drifts[0].Instance != "order-replica" {
			t.Fatalf("应检出 order-replica 漂移，实际 %+v", drifts)
		}
		if drifts[0].Runtime != "10.0.1.99:6379" || drifts[0].Expected != "10.0.1.10:6379" {
			t.Errorf("漂移内容不对：%+v", drifts[0])
		}
		if drifts[0].Group != "order" {
			t.Errorf("Group = %q, want order", drifts[0].Group)
		}
	})

	t.Run("map 中缺失的实例跳过而不是当成 master", func(t *testing.T) {
		// Agent 这一轮没探到 order-replica（不可达 / 正在 loading / 观测过期）。
		// 当成 "" 会得出"它以为自己是 master"的结论，进而对一个正常 replica 动手。
		drifts := detectReplicaDrift(node, instances, "srv1", names, map[string]string{
			"order-master": "",
		})
		if len(drifts) != 0 {
			t.Fatalf("缺失观测不构成漂移证据，实际 %+v", drifts)
		}
	})

	t.Run("旧版 Agent 完全不上报该字段时不产生漂移", func(t *testing.T) {
		drifts := detectReplicaDrift(node, instances, "srv1", names, map[string]string{})
		if len(drifts) != 0 {
			t.Fatalf("空 map 不应被解读为全组漂移，实际 %+v", drifts)
		}
	})

	t.Run("standalone 组不检查", func(t *testing.T) {
		drifts := detectReplicaDrift(node, instances, "srv1", []string{"cache-1"}, map[string]string{
			"cache-1": "10.0.1.10:6379", // standalone 却在复制别人，也不属于本机制的职责
		})
		if len(drifts) != 0 {
			t.Fatalf("standalone 组不检查，实际 %+v", drifts)
		}
	})
}

// ---------------------------------------------------------------------------
// 运行时复制漂移：修正
// ---------------------------------------------------------------------------

// TestReconcileReplicaDrift_ReissuesReplicaof 验证漂移的 replica 会被重新指向组的 master。
//
// 这是整个机制存在的理由：一个实际挂在错误 master 上的 replica，在 proxy_group 眼里
// IsServing() 为真，照样进 replica cluster 接读流量；错误 master 一停，它就长期提供
// 陈旧数据，而所有监控都显示健康。此前 Server 侧三条自愈路径没有一条看运行时复制关系。
func TestReconcileReplicaDrift_ReissuesReplicaof(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")
	expected := fmt.Sprintf("%s:6379", agentHost)

	s.reconcileReplicaDrift([]replicaDrift{{
		Instance: "order-replica", Server: "srv1", Group: "order",
		Runtime: "10.0.1.99:6379", Expected: expected,
	}})

	calls := ra.replicateCallsFor("order-replica")
	if len(calls) != 1 {
		t.Fatalf("应下发一次 REPLICAOF，实际 %d 次", len(calls))
	}
	if calls[0]["replica_of"] != expected {
		t.Errorf("replica_of = %v, want %v", calls[0]["replica_of"], expected)
	}
	if results := auditResultsFor(t, s, "reconcile.replica_drift"); len(results) != 1 || results[0] != "corrected" {
		t.Errorf("审计应记录 corrected，实际 %v", results)
	}
}

// TestReconcileReplicaDrift_NeverPromotes 验证本机制绝不提升实例。
//
// 期望它是 master 而它实际在复制别人时，正确动作是告警而不是发 REPLICAOF NO ONE。
// 谁该当 master 是 Sentinel 的选举结果；Server 在这里自行提升就是制造第二个拓扑
// 决策者，而两个决策者同时操作同一份拓扑正是脑裂的来源。
func TestReconcileReplicaDrift_NeverPromotes(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")

	s.reconcileReplicaDrift([]replicaDrift{{
		Instance: "order-master", Server: "srv1", Group: "order",
		Runtime: "10.0.1.99:6379", Expected: "", // 记录说它该是 master
	}})

	if calls := ra.replicateCallsFor("order-master"); len(calls) != 0 {
		t.Fatalf("不应对期望为 master 的实例下发任何复制指令，实际 %v", calls)
	}
	if results := auditResultsFor(t, s, "reconcile.replica_drift"); len(results) != 1 || results[0] != "alert" {
		t.Errorf("审计应记录 alert（交给 Sentinel 决策），实际 %v", results)
	}
}

// TestReconcileReplicaDrift_SkipsWhenTopologyChangedSinceDetection 与角色漂移同理：
// 检测和修正之间拓扑变了，就不能按检测时的期望值下发。
func TestReconcileReplicaDrift_SkipsWhenTopologyChangedSinceDetection(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")

	s.reconcileReplicaDrift([]replicaDrift{{
		Instance: "order-replica", Server: "srv1", Group: "order",
		Runtime:  "10.0.1.99:6379",
		Expected: fmt.Sprintf("%s:6380", agentHost), // 已过期：当前 master 是 6379
	}})

	if calls := ra.replicateCallsFor("order-replica"); len(calls) != 0 {
		t.Fatalf("拓扑已变，不应下发，实际 %v", calls)
	}
	if results := auditResultsFor(t, s, "reconcile.replica_drift"); len(results) != 1 || results[0] != "skipped" {
		t.Errorf("审计应记录 skipped，实际 %v", results)
	}
}

// TestReconcileReplicaDrift_SkipsWhenSentinelDisagrees 验证 Sentinel 与 Server 记录
// 对该组 master 的认定不一致时，本机制什么都不做。
//
// 不一致意味着 Server 记录可能已经过期：照它下发会拿一个旧地址覆盖 Sentinel 刚配好的
// 复制关系。这种情况属于 reconcileSentinelOnce 的职责，两条路径同时动手会互相打架。
func TestReconcileReplicaDrift_SkipsWhenSentinelDisagrees(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")

	// Sentinel 认为 master 是 6380，而 Server 记录是 6379。
	sentinel := newFakeSentinel(t, fmt.Sprintf("%s:6380", agentHost))
	s.cfg.Sentinel.Enabled = true
	s.cfg.Sentinel.Nodes = []string{"srv1", "sentinel-2", "sentinel-3"}
	s.cfg.Sentinel.Port = fakeSentinelPort(t, sentinel.addr())
	// 让 sentinel-2/sentinel-3 也能解析到 fake sentinel 的 host
	sentinelHost, _ := fakeAgentHostPort(sentinel.addr())
	s.db.WithNode(func(ns *apitypes.NodeState) error {
		ns.Servers["sentinel-2"] = &apitypes.NodeServer{Endpoint: sentinelHost}
		ns.Servers["sentinel-3"] = &apitypes.NodeServer{Endpoint: sentinelHost}
		return nil
	})

	s.reconcileReplicaDrift([]replicaDrift{{
		Instance: "order-replica", Server: "srv1", Group: "order",
		Runtime: "10.0.1.99:6379", Expected: fmt.Sprintf("%s:6379", agentHost),
	}})

	if calls := ra.replicateCallsFor("order-replica"); len(calls) != 0 {
		t.Fatalf("Sentinel 与 Server 记录不一致时不应下发，实际 %v", calls)
	}
	if results := auditResultsFor(t, s, "reconcile.replica_drift"); len(results) != 1 || results[0] != "skipped" {
		t.Errorf("审计应记录 skipped，实际 %v", results)
	}
}

// TestReconcileReplicaDrift_AppliesWhenSentinelAgrees 是上一个测试的对照：
// Sentinel 确认后必须真的下发，"跳过"不能是靠一律不干活实现的。
func TestReconcileReplicaDrift_AppliesWhenSentinelAgrees(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")
	expected := fmt.Sprintf("%s:6379", agentHost)

	sentinel := newFakeSentinel(t, expected)
	s.cfg.Sentinel.Enabled = true
	s.cfg.Sentinel.Nodes = []string{"srv1", "sentinel-2", "sentinel-3"}
	s.cfg.Sentinel.Port = fakeSentinelPort(t, sentinel.addr())
	// 让 sentinel-2/sentinel-3 也能解析到 fake sentinel 的 host
	sentinelHost, _ := fakeAgentHostPort(sentinel.addr())
	s.db.WithNode(func(ns *apitypes.NodeState) error {
		ns.Servers["sentinel-2"] = &apitypes.NodeServer{Endpoint: sentinelHost}
		ns.Servers["sentinel-3"] = &apitypes.NodeServer{Endpoint: sentinelHost}
		return nil
	})

	s.reconcileReplicaDrift([]replicaDrift{{
		Instance: "order-replica", Server: "srv1", Group: "order",
		Runtime: "10.0.1.99:6379", Expected: expected,
	}})

	if calls := ra.replicateCallsFor("order-replica"); len(calls) != 1 {
		t.Fatalf("Sentinel 确认一致后应下发一次，实际 %d 次", len(calls))
	}
}

// TestReconcileReplicaDrift_AuditReflectsFailure 验证下发失败时审计不声称修好了。
func TestReconcileReplicaDrift_AuditReflectsFailure(t *testing.T) {
	ra := newRecordingAgent(nil, nil)
	defer ra.Close()
	ra.setFailReplicate(true)

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")

	s.reconcileReplicaDrift([]replicaDrift{{
		Instance: "order-replica", Server: "srv1", Group: "order",
		Runtime: "10.0.1.99:6379", Expected: fmt.Sprintf("%s:6379", agentHost),
	}})

	if results := auditResultsFor(t, s, "reconcile.replica_drift"); len(results) != 1 || results[0] != "failed" {
		t.Errorf("下发失败时审计应记录 failed，实际 %v", results)
	}
}

// ---------------------------------------------------------------------------
// 端到端：reconcile 一整轮能发现并纠正运行时复制漂移
// ---------------------------------------------------------------------------

func TestReconcile_CorrectsRuntimeReplicaDrift(t *testing.T) {
	containers := []map[string]interface{}{
		{"name": "redis-order-master", "running": true},
		{"name": "redis-order-replica", "running": true},
	}
	ra := newRecordingAgent(containers, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	agentHost := setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")
	expected := fmt.Sprintf("%s:6379", agentHost)

	// order-replica 实际还挂在一个已经不是 master 的地址上
	ra.setRuntimeReplicaOf(map[string]string{
		"order-master":  "",
		"order-replica": "10.0.1.99:6379",
	})

	if _, err := s.runReconcile(); err != nil {
		t.Fatalf("runReconcile: %v", err)
	}

	calls := ra.replicateCallsFor("order-replica")
	if len(calls) != 1 {
		t.Fatalf("reconcile 应纠正运行时复制漂移，实际下发 %d 次", len(calls))
	}
	if calls[0]["replica_of"] != expected {
		t.Errorf("replica_of = %v, want %v", calls[0]["replica_of"], expected)
	}
}

// TestReconcile_NoRuntimeFieldNoCorrection 验证 Agent 不上报 runtime_replica_of 时
// reconcile 不会凭空判定漂移。字段缺失是"没有观测"，不是"全部为 master"。
func TestReconcile_NoRuntimeFieldNoCorrection(t *testing.T) {
	containers := []map[string]interface{}{
		{"name": "redis-order-master", "running": true},
		{"name": "redis-order-replica", "running": true},
	}
	ra := newRecordingAgent(containers, nil)
	defer ra.Close()

	s := newTestServer(t, "")
	setupReplicationGroup(t, s, ra.Listener.Addr().String(), "order-master")

	if _, err := s.runReconcile(); err != nil {
		t.Fatalf("runReconcile: %v", err)
	}
	if calls := ra.replicateCallsFor("order-replica"); len(calls) != 0 {
		t.Fatalf("没有运行时观测就不该下发任何复制指令，实际 %v", calls)
	}
}

// fakeSentinelPort 从 fakeSentinel 的监听地址里取出端口号。
func fakeSentinelPort(t *testing.T, addr string) int {
	t.Helper()
	_, port := fakeAgentHostPort(addr)
	return port
}
