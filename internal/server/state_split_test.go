package server

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// TestFailover_UpdatesTopologyOfDownReplica 是 Wave 2 的核心回归测试。
//
// Bug 原貌：handleSentinelFailover 里有一道 `if inst.Status == "running"` 门禁，导致
// 故障转移发生时恰好不 running 的 replica 的 Role/ReplicaOf 不被更新，永久停留在旧
// master 上。
//
// 危害链条：
//  1. 三节点组 A(master)/B/C，C 的容器先崩了，reconcile 把它标为异常
//  2. Sentinel 把 B 提为新 master
//  3. C 因为"不 running"被跳过，ReplicaOf 仍是 A —— 而 A 此刻已经是 B 的 replica，
//     于是 Server 相信一条 Sentinel 从未创建过的级联复制 C→A→B
//  4. 没有任何自愈路径会纠正它：reconcileSentinelOnce 只比对 master 地址，
//     runReconcile 只管观测状态
//  5. Wave 1 的角色落盘会拿着这份错误记录，持续把 `replicaof A` 写进 C 的配置文件
//
// 根因是拿观测结果给意图写入把门：Role/ReplicaOf 表达"这个实例应该扮演什么角色"，
// 与"它现在活不活"无关。一个挂掉的 replica 恢复后当然应该复制新 master。
func TestFailover_UpdatesTopologyOfDownReplica(t *testing.T) {
	s := newTestServer(t, "")

	group := testGroup("order")
	group.Type = "replication"
	group.Engine = "redis"
	group.CurrentMaster = "order-a"

	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: "10.0.1.10", AgentPort: 8400},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"order": group},
		Instances: map[string]*apitypes.Instance{
			"order-a": {
				Group: "order", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-a",
			},
			"order-b": {
				Group: "order", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "order-a",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-b",
			},
			// order-c 的容器已经崩了：意图仍是 running，但观测到容器不存在
			"order-c": {
				Group: "order", Server: "srv1", Port: 6381, Role: "replica", ReplicaOf: "order-a",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedMissing,
				Container: "redis-order-c",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// 前置确认：order-c 此刻的派生状态确实是 failed，即命中了原 bug 的触发条件
	before, _ := s.db.ReadInstances()
	if got := before.Instances["order-c"].Phase(); got != "failed" {
		t.Fatalf("前置条件不成立：order-c 的 Phase = %q，期望 failed", got)
	}

	// Sentinel 把 order-b 提为新 master
	if err := s.handleSentinelFailover("order", "10.0.1.10:6380", "test", "test"); err != nil {
		t.Fatalf("handleSentinelFailover: %v", err)
	}

	after, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}

	if got := after.Groups["order"].CurrentMaster; got != "order-b" {
		t.Errorf("CurrentMaster = %q, want order-b", got)
	}
	if got := after.Instances["order-b"].Role; got != "master" {
		t.Errorf("order-b.Role = %q, want master", got)
	}
	// 旧 master 降级
	if got := after.Instances["order-a"]; got.Role != "replica" || got.ReplicaOf != "order-b" {
		t.Errorf("order-a: role=%q replicaOf=%q, want replica/order-b", got.Role, got.ReplicaOf)
	}
	// 关键断言：崩掉的 replica 的拓扑意图也必须被更新
	c := after.Instances["order-c"]
	if c.ReplicaOf != "order-b" {
		t.Errorf("order-c.ReplicaOf = %q, want order-b。"+
			"拓扑意图不得受观测状态门禁——一个挂掉的 replica 恢复后应当复制新 master，"+
			"而不是复制一个已经降级的旧 master", c.ReplicaOf)
	}
	if c.Role != "replica" {
		t.Errorf("order-c.Role = %q, want replica", c.Role)
	}
	// 观测状态不应被故障转移改写：C 确实还是挂着的
	if c.ObservedStatus != apitypes.ObservedMissing {
		t.Errorf("order-c.ObservedStatus = %q，故障转移不该改写观测字段（C 仍然是挂的）",
			c.ObservedStatus)
	}
}

// TestFailover_DoesNotGuessOldMasterObservedStatus 说明为什么不再写旧 master 的观测状态。
//
// handleSentinelFailover 同时服务两种触发：Sentinel 自动故障转移（旧 master 通常真的挂了）
// 和用户手动 instance failover（旧 master 可能完全健康）。把它一律标成异常是在猜测，
// 而不是观测。交给 reconcile 在 30 秒内如实观测。
func TestFailover_DoesNotGuessOldMasterObservedStatus(t *testing.T) {
	s := newTestServer(t, "")
	group := testGroup("order")
	group.Type = "replication"
	group.Engine = "redis"
	group.CurrentMaster = "order-a"

	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: "10.0.1.10", AgentPort: 8400}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"order": group},
		Instances: map[string]*apitypes.Instance{
			"order-a": {Group: "order", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-a"},
			"order-b": {Group: "order", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "order-a",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-b"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.handleSentinelFailover("order", "10.0.1.10:6380", "test", "test"); err != nil {
		t.Fatal(err)
	}

	after, _ := s.db.ReadInstances()
	a := after.Instances["order-a"]
	if a.ObservedStatus != apitypes.ObservedRunning {
		t.Errorf("order-a.ObservedStatus = %q，故障转移不应猜测旧 master 的观测状态"+
			"（手动 failover 时它可能完全健康），应保留上一次真实观测", a.ObservedStatus)
	}
	// 意图也不该被动过
	if a.DesiredState != apitypes.DesiredRunning {
		t.Errorf("order-a.DesiredState = %q，故障转移不得修改意图", a.DesiredState)
	}
}

// TestReconcile_NeverWritesDesiredState 锁定"reconcile 只写观测"这条约束。
//
// 场景：用户明确停掉了一个实例（DesiredState=stopped），容器确实不在了。
// reconcile 观测到 missing，必须只更新 ObservedStatus，不能把 DesiredState 改掉——
// 否则用户的意图会被观测结果覆盖，实例再也不会被认为"应该停着"。
func TestReconcile_NeverWritesDesiredState(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: agentHost, AgentPort: agentPort}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"cache": testGroup("cache")},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Group: "cache", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredStopped, ObservedStatus: apitypes.ObservedStopped,
				Container: "redis-cache-1",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	results, err := s.runReconcile()
	if err != nil {
		t.Fatal(err)
	}

	after, _ := s.db.ReadInstances()
	inst := after.Instances["cache-1"]
	if inst.DesiredState != apitypes.DesiredStopped {
		t.Errorf("DesiredState = %q，reconcile 不得修改意图", inst.DesiredState)
	}
	if inst.ObservedStatus != apitypes.ObservedMissing {
		t.Errorf("ObservedStatus = %q, want missing", inst.ObservedStatus)
	}
	// 用户要它停，容器不在 —— 这是符合意图的，不该告警
	if len(results) != 1 {
		t.Fatalf("results = %d", len(results))
	}
	if results[0].Action == "alert" {
		t.Errorf("意图是 stopped、容器不存在，符合预期，不该告警：%+v", results[0])
	}
	if got := inst.Phase(); got != "stopped" {
		t.Errorf("Phase = %q, want stopped", got)
	}
}

// TestReconcile_AgentUnreachableDoesNotOverwriteObservedStatus 锁定一条容易被改坏的约束。
//
// "Agent 不可达"是对 Agent 的观测，不是对实例的观测。Agent 重启或网络抖动时，它名下的
// Redis 实例通常完好无损。如果把 unreachable 写进实例的 ObservedStatus，IsServing() 会
// 变成 false，proxy_group 会把这些健康实例从 Proxy 端点里摘掉——一次 Agent 重启就造成
// 一次业务中断。
//
// 所以这种情况只告警，不写观测字段。
func TestReconcile_AgentUnreachableDoesNotOverwriteObservedStatus(t *testing.T) {
	s := newTestServer(t, "")
	// 指向一个没人监听的地址，制造 Agent 不可达
	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: "127.0.0.1", AgentPort: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"cache": testGroup("cache")},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Group: "cache", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-cache-1",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	results, err := s.runReconcile()
	if err != nil {
		t.Fatal(err)
	}

	after, _ := s.db.ReadInstances()
	inst := after.Instances["cache-1"]
	if inst.ObservedStatus != apitypes.ObservedRunning {
		t.Errorf("ObservedStatus = %q。Agent 不可达是对 Agent 的观测，不是对实例的观测；"+
			"覆盖实例观测状态会让 Proxy 把健康实例摘掉", inst.ObservedStatus)
	}
	if !inst.IsServing() {
		t.Error("实例应当仍被认为可以承接流量")
	}
	// 但必须告警，让人知道观测能力本身出了问题
	if len(results) != 1 || results[0].Action != "alert" {
		t.Errorf("Agent 不可达应当告警：%+v", results)
	}
}

// TestReconcile_DetectsUnexpectedStop 验证派生状态取代了此前存储的 unexpected_stopped。
func TestReconcile_DetectsUnexpectedStop(t *testing.T) {
	agent := newFakeAgentWithContainers([]map[string]interface{}{
		{"name": "redis-cache-1", "running": false},
	})
	defer agent.Close()

	s := newTestServer(t, "")
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())
	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: agentHost, AgentPort: agentPort}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"cache": testGroup("cache")},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {
				Group: "cache", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-cache-1",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	results, err := s.runReconcile()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Action != "alert" {
		t.Fatalf("没人要求它停它却停了，应当告警：%+v", results)
	}

	after, _ := s.db.ReadInstances()
	inst := after.Instances["cache-1"]
	if inst.DesiredState != apitypes.DesiredRunning {
		t.Errorf("DesiredState = %q，意图不该变", inst.DesiredState)
	}
	if inst.ObservedStatus != apitypes.ObservedStopped {
		t.Errorf("ObservedStatus = %q, want stopped", inst.ObservedStatus)
	}
	// unexpected_stopped 现在是推导出来的，不再是存储值
	if got := inst.Phase(); got != "unexpected_stopped" {
		t.Errorf("Phase = %q, want unexpected_stopped", got)
	}
	if inst.IsServing() {
		t.Error("停掉的实例不应被认为可以承接流量")
	}
}

// TestNoStoredDerivedStatus 是结构性约束测试：生产代码不得把派生状态当成存储值写回。
//
// 存储派生值会产生"派生值与来源不一致"的第三种坏状态——这正是 Status 单字段时代的
// 问题形态。unexpected_stopped / creating / failed 只能出现在 Phase() 的推导里。
func TestNoStoredDerivedStatus(t *testing.T) {
	forbidden := regexp.MustCompile(
		`(ObservedStatus|DesiredState)\s*[:=]\s*"?(unexpected_stopped|creating|failed)"?`)

	files := []string{
		"reconcile_handler.go",
		"sentinel.go",
		"instance_handler.go",
	}
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			code := line
			if idx := strings.Index(code, "//"); idx >= 0 {
				code = code[:idx] // 剥离注释，只检查真实代码
			}
			if forbidden.MatchString(code) {
				t.Errorf("%s:%d 把派生状态写进了存储字段：%s\n"+
					"unexpected_stopped/creating/failed 只能由 Phase() 推导",
					name, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// TestFailover_LocksReleasedAfterCompletion 验证故障转移完成后实例锁被正确释放。
//
// 此前锁在 handleSentinelFailover 的 WithInstances 回调内通过 defer 释放，
// 回调返回时 defer 触发、锁被置 nil，WithInstances 提交到 DB 时锁已经是"未持有"。
// 锁从未生效，并发的 instanceStop/Delete 不受阻止。
//
// 修复后锁在独立事务中加锁并提交（Phase 1），在函数返回时由 defer releaseLockGroup
// 释放（独立事务）。此测试验证修复后的锁生命周期：故障转移完成后所有实例锁应为 nil。
func TestFailover_LocksReleasedAfterCompletion(t *testing.T) {
	s := newTestServer(t, "")
	group := testGroup("order")
	group.Type = "replication"
	group.Engine = "redis"
	group.CurrentMaster = "order-a"

	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: "10.0.1.10", AgentPort: 8400},
		},
	})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"order": group},
		Instances: map[string]*apitypes.Instance{
			"order-a": {
				Group: "order", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-a",
			},
			"order-b": {
				Group: "order", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "order-a",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-b",
			},
		},
	})

	if err := s.handleSentinelFailover("order", "10.0.1.10:6380", "test", "test"); err != nil {
		t.Fatalf("handleSentinelFailover: %v", err)
	}

	after, _ := s.db.ReadInstances()
	for name, inst := range after.Instances {
		if inst.Lock != nil {
			t.Errorf("instance %s still has lock after failover completed: %+v"+
				" (locks must be released by defer releaseLockGroup)", name, inst.Lock)
		}
	}
}

// TestFailover_LockBlocksConcurrentStop 验证故障转移期间实例锁能阻止并发的 instanceStop。
//
// 这是 A2 修复的核心验证：锁必须在独立事务中加锁并提交，才能被并发的
// resolveAndLock 看到。如果锁仍在回调内通过 defer 释放（旧 bug），
// resolveAndLock 不会看到锁，instanceStop 会成功--与故障转移产生竞态。
func TestFailover_LockBlocksConcurrentStop(t *testing.T) {
	s := newTestServer(t, "")
	group := testGroup("order")
	group.Type = "replication"
	group.Engine = "redis"
	group.CurrentMaster = "order-a"

	s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: "10.0.1.10", AgentPort: 8400},
		},
	})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"order": group},
		Instances: map[string]*apitypes.Instance{
			"order-a": {
				Group: "order", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-a",
			},
			"order-b": {
				Group: "order", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "order-a",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
				Container: "redis-order-b",
			},
		},
	})

	// 手动模拟故障转移持有锁的状态：在独立事务中加锁并提交
	sessionID := "test-failover-lock"
	s.db.WithInstances(func(instances *apitypes.InstancesState) error {
		for _, name := range []string{"order-a", "order-b"} {
			if inst := instances.Instances[name]; inst != nil {
				state.TryAcquireLock(inst, sessionID, "sentinel-failover", 60)
			}
		}
		return nil
	})

	// 尝试对被锁的实例执行 resolveAndLock（instanceStop 的前置）
	_, _, unlock, err := s.resolveAndLockInternal("order-a", "stop")
	if err == nil {
		if unlock != nil {
			unlock()
		}
		t.Fatal("resolveAndLock should fail when instance is locked by failover")
	}

	// 释放锁后 resolveAndLock 应该成功
	s.releaseLockGroup([]string{"order-a", "order-b"}, sessionID)
	_, _, unlock2, err2 := s.resolveAndLockInternal("order-a", "stop")
	if err2 != nil {
		t.Fatalf("resolveAndLock should succeed after lock released: %v", err2)
	}
	if unlock2 != nil {
		unlock2()
	}
}
