package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// TestSyncSentinelNode_OneGroupFailureDoesNotBlockOthers 验证单组失败被隔离。
//
// 早期实现在组循环里直接 return err：任意一组出问题，排在它后面的组这一轮完全不会被
// 配置。最坏的一种是 REMOVE 已执行、MONITOR 失败时返回——该组处于"已删除且未重建"的
// 无监控状态，同时后面的组也没被处理，一次局部故障扩散成整个节点的配置停滞。
func TestSyncSentinelNode_OneGroupFailureDoesNotBlockOthers(t *testing.T) {
	// 让 group-bad 的 MONITOR 失败，group-good 应当照常配置完成
	sentinel := newFakeSentinelWithMasters(t, nil)
	sentinel.failCommand("MONITOR", "group-bad")

	desired := []apitypes.SentinelMaster{
		{Group: "group-bad", Host: "10.0.1.10", Port: 6379,
			DownAfterMilliseconds: 5000, FailoverTimeout: 60000, ParallelSyncs: 1},
		{Group: "group-good", Host: "10.0.1.11", Port: 6379,
			DownAfterMilliseconds: 5000, FailoverTimeout: 60000, ParallelSyncs: 1},
	}
	known := map[string]struct{}{"group-bad": {}, "group-good": {}}

	err := syncSentinelNode(sentinel.addr(), desired, known, 2)
	if err == nil {
		t.Fatal("期望返回错误，说明失败被上报而不是被吞掉")
	}
	if !strings.Contains(err.Error(), "group-bad") {
		t.Errorf("错误信息应指出是哪个组失败了：%v", err)
	}
	if strings.Contains(err.Error(), "group-good") {
		t.Errorf("group-good 应当成功，不该出现在错误里：%v", err)
	}

	// 关键断言：group-good 的 MONITOR 与三项 SET 都执行到了
	if !sentinel.sawCommand("MONITOR", "group-good") {
		t.Error("group-good 的 MONITOR 没有执行——单组失败中断了整轮")
	}
	for _, param := range []string{"down-after-milliseconds", "failover-timeout", "parallel-syncs"} {
		if !sentinel.sawCommand("SET", "group-good", param) {
			t.Errorf("group-good 的 SET %s 没有执行", param)
		}
	}
}

// TestFailover_SerializedByTopologyLock 验证并发故障转移被编排锁串行化。
//
// 缺这把锁时，两次故障转移会产生两个同步协程各自读到不同的状态快照，若后者先把
// Sentinel 的 MONITOR 指向新地址、前者随后改回旧地址，Sentinel 就被指向了一个已经
// 降级的实例；接着 reconcileSentinelOnce 发现不一致，而 Sentinel 是设计上的权威，
// 于是把 Server 的 CurrentMaster 也改回去——一次来回震荡，两边都错。
//
// 这里不去构造那个时序（时序竞态测试本身不可靠），而是验证结构性保证：
// 并发调用结束后，Server 状态与最后一次成功的转移一致，且不存在中间态残留。
func TestFailover_SerializedByTopologyLock(t *testing.T) {
	// 用 fake agent 而不是一个不存在的地址：persistGroupRoles 会向 Agent 推配置，
	// 指向黑洞地址会让测试白等满 HTTP 超时。
	agent := newFakeAgentWithContainers(nil)
	defer agent.Close()
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())

	s := newTestServer(t, "")

	group := testGroup("order")
	group.Type = "replication"
	group.Engine = "redis"
	group.CurrentMaster = "order-a"

	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{
			"srv1": {Endpoint: agentHost, AgentPort: agentPort},
		},
	}); err != nil {
		t.Fatal(err)
	}
	mk := func(port int, role, replicaOf string) *apitypes.Instance {
		return &apitypes.Instance{
			Group: "order", Server: "srv1", Port: port, Role: role, ReplicaOf: replicaOf,
			DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
			Container: "redis-order",
		}
	}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"order": group},
		Instances: map[string]*apitypes.Instance{
			"order-a": mk(6379, "master", ""),
			"order-b": mk(6380, "replica", "order-a"),
			"order-c": mk(6381, "replica", "order-a"),
		},
	}); err != nil {
		t.Fatal(err)
	}

	// 两次并发故障转移，分别指向 order-b 和 order-c
	var wg sync.WaitGroup
	for _, addr := range []string{
		fmt.Sprintf("%s:6380", agentHost),
		fmt.Sprintf("%s:6381", agentHost),
	} {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			_ = s.handleSentinelFailover("order", a, "test", "test")
		}(addr)
	}
	wg.Wait()

	after, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	master := after.Groups["order"].CurrentMaster
	if master != "order-b" && master != "order-c" {
		t.Fatalf("CurrentMaster = %q，应当是两次转移之一的结果", master)
	}

	// 结构一致性：恰好一个 master，其余都是指向它的 replica。
	// 若两次转移交错，会出现两个 master 或 ReplicaOf 指向非 master 的实例。
	var masters []string
	for name, inst := range after.Instances {
		if inst.Role == "master" {
			masters = append(masters, name)
			if inst.ReplicaOf != "" {
				t.Errorf("%s 是 master 却有 ReplicaOf=%q", name, inst.ReplicaOf)
			}
			continue
		}
		if inst.ReplicaOf != master {
			t.Errorf("%s.ReplicaOf = %q，应当指向当前 master %q（交错的转移会留下指向旧 master 的记录）",
				name, inst.ReplicaOf, master)
		}
	}
	if len(masters) != 1 {
		t.Errorf("master 数量 = %d（%v），应当恰好一个", len(masters), masters)
	}
	if len(masters) == 1 && masters[0] != master {
		t.Errorf("CurrentMaster=%q 与实际标记为 master 的实例 %q 不一致", master, masters[0])
	}
}

// TestSyncSentinel_NoDeadlockFromFailoverPath 验证故障转移路径不会自死锁。
//
// handleSentinelFailover 持 topologyMu，末尾还要下发 Sentinel 配置。Go 的 Mutex
// 不可重入，如果那里调的是会重新加锁的 syncSentinelMasters 而不是 Locked 变体，
// 整个 Server 会当场卡死。这个测试跑不完就说明退化了。
func TestSyncSentinel_NoDeadlockFromFailoverPath(t *testing.T) {
	agent := newFakeAgentWithContainers(nil)
	defer agent.Close()
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())

	s := newTestServer(t, "")
	group := testGroup("order")
	group.Type = "replication"
	group.Engine = "redis"
	group.CurrentMaster = "order-a"

	if err := s.db.WriteNode(&apitypes.NodeState{
		Servers: map[string]*apitypes.NodeServer{"srv1": {Endpoint: agentHost, AgentPort: agentPort}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{"order": group},
		Instances: map[string]*apitypes.Instance{
			"order-a": {Group: "order", Server: "srv1", Port: 6379, Role: "master",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning},
			"order-b": {Group: "order", Server: "srv1", Port: 6380, Role: "replica", ReplicaOf: "order-a",
				DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning},
		},
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.handleSentinelFailover("order", fmt.Sprintf("%s:6380", agentHost), "test", "test")
		// 紧接着走一次未持锁的入口，确认锁已释放
		_ = s.syncSentinelMasters()
		s.persistGroupRoles("order")
	}()
	<-done
}
