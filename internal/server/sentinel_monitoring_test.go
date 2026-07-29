package server

import (
	"net"
	"strconv"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// sentinelRegressionFixture 搭一套「Server + 3 个 Sentinel 节点（都指向同一个 fake）
// + 一台 Agent」的环境，供本文件的回归测试复用。
type sentinelRegressionFixture struct {
	s         *Server
	sentinel  *fakeSentinel
	agentHost string
}

func newSentinelRegressionFixture(t *testing.T, containers []map[string]interface{}, initialMasters map[string]string) *sentinelRegressionFixture {
	t.Helper()

	agent := newFakeAgentWithContainers(containers)
	t.Cleanup(agent.Close)
	agentHost, agentPort := fakeAgentHostPort(agent.Listener.Addr().String())

	fs := newFakeSentinelWithMasters(t, initialMasters)
	sentinelHost, sentinelPortStr, _ := net.SplitHostPort(fs.addr())
	sentinelPort, _ := strconv.Atoi(sentinelPortStr)

	s := newTestServer(t, "")
	s.cfg.Sentinel = SentinelConfig{
		Enabled: true,
		Nodes:   []string{"sn1", "sn2", "sn3"},
		Quorum:  2,
		Port:    sentinelPort,
	}
	s.state.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv1": {Endpoint: agentHost, AgentPort: agentPort, Status: "healthy"},
		"sn1":  {Endpoint: sentinelHost, Status: "healthy"},
		"sn2":  {Endpoint: sentinelHost, Status: "healthy"},
		"sn3":  {Endpoint: sentinelHost, Status: "healthy"},
	}})
	return &sentinelRegressionFixture{s: s, sentinel: fs, agentHost: agentHost}
}

// writeOrderGroup 写入一个标准的 order 主从组：master 在 7000，replica 在 7001。
func (f *sentinelRegressionFixture) writeOrderGroup(t *testing.T) {
	t.Helper()
	f.s.state.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type: "replication", Engine: "redis", Category: "cache",
				CurrentMaster: "order-master",
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "master", Status: "running",
				Server: "srv1", Port: 7000, Container: "redis-order-master",
			},
			"order-replica": {
				Group: "order", Role: "replica", Status: "running", ReplicaOf: "order-master",
				Server: "srv1", Port: 7001, Container: "redis-order-replica",
			},
		},
	})
}

// TestSyncSentinel_KeepsMonitoringWhenMasterCrashes 是本轮修复的核心回归测试。
//
// 回归背景（实测复现过的 P0）：buildSentinelMasters 曾经要求
// inst.Role == "master" && inst.Status == "running" 才把组放进 desired，而
// syncSentinelNode 会把「Sentinel 上存在但不在 desired 中」的组一律 SENTINEL REMOVE。
// 于是这条链会静默摘掉故障转移能力：
//
//	master 容器崩溃（Agent 仍存活，只是容器不在了）
//	  → 30s 定时容器 reconcile 把 master 标成 failed（无需任何人为操作）
//	  → 之后任意一次与该组无关的管理操作（instance create/delete）调 syncSentinel
//	  → 该组被剔出 desired → SENTINEL REMOVE → Sentinel 彻底忘记这个组
//
// 结果是恰好在最需要 failover 的时刻，Sentinel 被告知不再监控该组，且全程无告警。
// 修复后：是否监控只由拓扑意图（knownSentinelGroups）决定，与 master 健康状态无关。
func TestSyncSentinel_KeepsMonitoringWhenMasterCrashes(t *testing.T) {
	// Agent 存活，但只报告 replica 容器 —— master 容器已经消失
	f := newSentinelRegressionFixture(t,
		[]map[string]interface{}{{"name": "redis-order-replica", "running": true}},
		map[string]string{"order": "127.0.0.1:7000"},
	)
	f.writeOrderGroup(t)

	// 定时容器 reconcile 走的就是这个函数（server.go 的 30s 循环）
	if _, err := f.s.runReconcile(); err != nil {
		t.Fatal(err)
	}
	instances, err := f.s.state.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	if got := instances.Instances["order-master"].Status; got != "failed" {
		t.Fatalf("前置条件不成立：期望 reconcile 把崩溃的 master 标为 failed，实际 %q", got)
	}

	// 模拟后续任意一次无关管理操作触发的 syncSentinel
	removeBefore := f.sentinel.removeCalls.Load()
	if err := f.s.syncSentinelMasters(); err != nil {
		t.Fatalf("syncSentinelMasters failed: %v", err)
	}

	if got := f.sentinel.removeCalls.Load() - removeBefore; got != 0 {
		t.Errorf("master 崩溃后 Sentinel 监控被摘除：发出了 %d 次 SENTINEL REMOVE，期望 0。"+
			"是否监控应当只由拓扑意图决定，与 master 健康状态无关", got)
	}
	if _, ok := f.sentinel.masters.Load("order"); !ok {
		t.Error("order 组已从 Sentinel 上消失，故障转移能力被摘除")
	}
}

// TestSyncSentinel_StillRemovesUnknownGroup 验证修复没有把 REMOVE 能力一起削掉：
// Sentinel 上存在、但 Server 已经完全不认识的组（组被删除、或从 replication 改成
// standalone），仍然必须被清理，否则 Sentinel 会一直监控一个不存在的组并反复告警。
func TestSyncSentinel_StillRemovesUnknownGroup(t *testing.T) {
	f := newSentinelRegressionFixture(t,
		[]map[string]interface{}{{"name": "redis-order-master", "running": true}},
		// Sentinel 上有一个 Server 已经不认识的 stale 组
		map[string]string{"order": "127.0.0.1:7000", "deleted-group": "127.0.0.1:7999"},
	)
	f.writeOrderGroup(t)

	if err := f.s.syncSentinelMasters(); err != nil {
		t.Fatalf("syncSentinelMasters failed: %v", err)
	}

	if _, ok := f.sentinel.masters.Load("deleted-group"); ok {
		t.Error("Server 已不认识的组仍留在 Sentinel 上，未被清理")
	}
	if _, ok := f.sentinel.masters.Load("order"); !ok {
		t.Error("正常组被误删")
	}
}

// TestKnownSentinelGroups 固定 knownSentinelGroups 的语义：只看拓扑意图。
func TestKnownSentinelGroups(t *testing.T) {
	instances := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			// 健康的主从组
			"healthy": {Type: "replication", CurrentMaster: "healthy-master"},
			// master 已崩溃的主从组：仍然必须算作 known，否则监控会被摘除
			"crashed": {Type: "replication", CurrentMaster: "crashed-master"},
			// 单点组：不该由 Sentinel 监控
			"standalone": {Type: "standalone", CurrentMaster: "standalone-1"},
			// 没有 CurrentMaster 的残缺组：无从监控
			"no-master": {Type: "replication", CurrentMaster: ""},
		},
		Instances: map[string]*apitypes.Instance{
			"healthy-master": {Group: "healthy", Role: "master", Status: "running"},
			"crashed-master": {Group: "crashed", Role: "master", Status: "failed"},
			"standalone-1":   {Group: "standalone", Role: "master", Status: "running"},
		},
	}

	known := knownSentinelGroups(instances)
	for _, want := range []string{"healthy", "crashed"} {
		if _, ok := known[want]; !ok {
			t.Errorf("组 %q 应当被视为 Sentinel 监控范围内（拓扑意图），但不在 known 中", want)
		}
	}
	for _, notWant := range []string{"standalone", "no-master"} {
		if _, ok := known[notWant]; ok {
			t.Errorf("组 %q 不应被视为 Sentinel 监控范围内，但出现在 known 中", notWant)
		}
	}
	if len(known) != 2 {
		t.Errorf("expected exactly 2 known groups, got %d: %v", len(known), known)
	}
}

// TestBuildSentinelMasters_IncludesCrashedMaster 验证 desired 配置本身也不再因
// master 不健康而缺项——否则 Sentinel 上该组的 down-after/failover-timeout 等参数
// 会停止被同步。
func TestBuildSentinelMasters_IncludesCrashedMaster(t *testing.T) {
	s := &Server{cfg: &Config{Sentinel: SentinelConfig{
		DownAfterMilliseconds: 6000,
		FailoverTimeout:       40000,
		ParallelSyncs:         2,
	}}}
	node := &apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-a": {Endpoint: "10.0.1.10"},
	}}
	instances := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", CurrentMaster: "order-master"},
		},
		Instances: map[string]*apitypes.Instance{
			// 容器已崩溃，被 reconcile 标记为 failed
			"order-master": {Group: "order", Role: "master", Status: "failed", Server: "srv-a", Port: 6379},
		},
	}

	masters := s.buildSentinelMasters(node, instances)
	if len(masters) != 1 {
		t.Fatalf("崩溃的 master 仍应出现在 desired 配置中，实际 %#v", masters)
	}
	if masters[0].Group != "order" || masters[0].Host != "10.0.1.10" || masters[0].Port != 6379 {
		t.Fatalf("unexpected master: %#v", masters[0])
	}
}
