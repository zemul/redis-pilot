package server

import (
	"bufio"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestSelectSentinelNodes_ConfiguredOnly(t *testing.T) {
	s := newTestServer(t, "")
	s.cfg.Sentinel = SentinelConfig{
		Enabled: true,
		Nodes:   []string{"srv-c", "srv-a", "srv-c", "10.0.1.99"},
		Quorum:  2,
	}
	node := &apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-a": {Endpoint: "10.0.1.1", Status: "healthy", Labels: map[string]string{"zone": "az-1", "role": "production"}},
		"srv-b": {Endpoint: "10.0.1.2", Status: "healthy", Labels: map[string]string{"zone": "az-1", "role": "production"}},
		"srv-c": {Endpoint: "10.0.1.3", Status: "healthy", Labels: map[string]string{"zone": "az-2", "role": "production"}},
		"srv-d": {Endpoint: "10.0.1.4", Status: "healthy", Labels: map[string]string{"zone": "az-3", "role": "standby"}},
	}}
	got := s.selectSentinelNodes(node)
	// sentinel.nodes entries are returned as-is (deduped).
	// "10.0.1.99" is not a registered server but is still included —
	// sentinel nodes can be specified by IP directly without registration.
	want := []string{"srv-c", "srv-a", "10.0.1.99"}
	if len(got) != len(want) {
		t.Fatalf("expected configured sentinel nodes %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected configured sentinel nodes %v, got %v", want, got)
		}
	}
}

func TestBuildSentinelMasters(t *testing.T) {
	s := &Server{cfg: &Config{Sentinel: SentinelConfig{
		DownAfterMilliseconds: 6000,
		FailoverTimeout:       40000,
		ParallelSyncs:         2,
	}}}
	node := &apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-a": {Endpoint: "10.0.1.10"},
		"srv-b": {Endpoint: "10.0.1.11"},
	}}
	instances := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", Engine: "redis", Category: "cache", CurrentMaster: "order-master"},
			"cache": {Type: "standalone", Engine: "redis", Category: "cache", CurrentMaster: "cache-1"},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-a", Port: 6379,
			},
			"order-replica": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-b", Port: 6379,
			},
			"cache-1": {
				Group: "cache", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-a", Port: 6380,
			},
		},
	}
	masters := s.buildSentinelMasters(node, instances)
	if len(masters) != 1 {
		t.Fatalf("expected one monitored master, got %#v", masters)
	}
	m := masters[0]
	// 实例无密码：Sentinel 直连实例不带 auth-pass
	if m.Group != "order" || m.Host != "10.0.1.10" || m.Port != 6379 || m.Password != "" {
		t.Fatalf("unexpected master: %#v", m)
	}
	if m.DownAfterMilliseconds != 6000 || m.FailoverTimeout != 40000 || m.ParallelSyncs != 2 {
		t.Fatalf("sentinel timing config not propagated: %#v", m)
	}
}

func TestReadRESPNestedArrayAndSentinelMasterNames(t *testing.T) {
	resp := "*2\r\n" +
		"*4\r\n$4\r\nname\r\n$5\r\norder\r\n$2\r\nip\r\n$9\r\n10.0.1.10\r\n" +
		"*4\r\n$4\r\nname\r\n$5\r\ncache\r\n$2\r\nip\r\n$9\r\n10.0.1.11\r\n"
	reply, err := readRESP(bufio.NewReader(strings.NewReader(resp)))
	if err != nil {
		t.Fatal(err)
	}
	names := sentinelMasterNames(reply)
	want := []string{"cache", "order"}
	if len(names) != len(want) {
		t.Fatalf("expected names %v, got %v", want, names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("expected names %v, got %v", want, names)
		}
	}
}

func TestParseSwitchMasterMessage(t *testing.T) {
	resp := "*3\r\n" +
		"$7\r\nmessage\r\n" +
		"$14\r\n+switch-master\r\n" +
		"$35\r\norder 10.0.1.10 6379 10.0.1.11 6379\r\n"
	reply, err := readRESP(bufio.NewReader(strings.NewReader(resp)))
	if err != nil {
		t.Fatal(err)
	}
	group, ok := parseSwitchMasterMessage(reply)
	if !ok || group != "order" {
		t.Fatalf("expected order switch-master message, got group=%q ok=%v", group, ok)
	}
}

func TestSentinelEventEndpoint(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-a": {Endpoint: "10.0.1.10"},
		"srv-b": {Endpoint: "10.0.1.11"},
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
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-a", Port: 6379,
			},
			"order-replica": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-b", Port: 6379,
				ReplicaOf: "order-master",
			},
		},
	})
	w := doAuthRequest(s.Router(), "POST", "/sentinel/event", map[string]interface{}{
		"event":      "+switch-master",
		"group":      "order",
		"old_master": "10.0.1.10:6379",
		"new_master": "10.0.1.11:6379",
	})
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleSentinelFailover_UpdatesState(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-a": {Endpoint: "10.0.1.10"},
		"srv-b": {Endpoint: "10.0.1.11"},
		"srv-c": {Endpoint: "10.0.1.12"},
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
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-a", Port: 6379,
			},
			"order-replica": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-b", Port: 6379,
				ReplicaOf: "order-master",
			},
			"order-replica-2": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "running", Server: "srv-c", Port: 6379,
				ReplicaOf: "order-master",
			},
		},
	})
	if err := s.handleSentinelFailover("order", "10.0.1.11:6379", "test", "sentinel"); err != nil {
		t.Fatal(err)
	}
	state, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	oldMaster := state.Instances["order-master"]
	newMaster := state.Instances["order-replica"]
	otherReplica := state.Instances["order-replica-2"]
	// 注意：handleSentinelFailover 不再写 oldMaster 的观测状态——故障转移的原因是未知的
	// （手动 failover 时旧 master 可能完全健康），交给 reconcile 如实观测。
	// 这里只断言拓扑意图。
	if oldMaster.Role != "replica" || oldMaster.ReplicaOf != "order-replica" {
		t.Fatalf("old master not marked failed replica: %#v", oldMaster)
	}
	if newMaster.Role != "master" || newMaster.ReplicaOf != "" {
		t.Fatalf("new master not promoted in state: %#v", newMaster)
	}
	group := state.Groups["order"]
	if group.CurrentMaster != "order-replica" {
		t.Fatalf("group current master not updated: %#v", group)
	}
	if group.Proxy == nil || group.Proxy.AutoPort != 16379 || group.Proxy.MasterPort != 16500 {
		t.Fatalf("group proxy config changed unexpectedly: %#v", group.Proxy)
	}
	if otherReplica.ReplicaOf != "order-replica" {
		t.Fatalf("other replica should point at new master: %#v", otherReplica)
	}
	if oldMaster.Group != "order" || newMaster.Group != "order" || otherReplica.Group != "order" {
		t.Fatalf("group should remain stable after failover")
	}
}

func TestHandleSentinelFailover_NoOpWhenAlreadySynchronized(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteNode(&apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{
		"srv-a": {Endpoint: "10.0.1.10"},
		"srv-b": {Endpoint: "10.0.1.11"},
	}})
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type:          "replication",
				Engine:        "redis",
				Category:      "cache",
				CurrentMaster: "order-replica",
				Proxy:         &apitypes.ProxyConfig{AutoPort: 16379, MasterPort: 16500},
			},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {
				Group: "order", Role: "replica", DesiredState: "running", ObservedStatus: "missing", Server: "srv-a", Port: 6379,
				ReplicaOf: "order-replica",
			},
			"order-replica": {
				Group: "order", Role: "master", DesiredState: "running", ObservedStatus: "running", Server: "srv-b", Port: 6379,
			},
		},
	})
	if err := s.handleSentinelFailover("order", "10.0.1.11:6379", "test", "sentinel"); err != nil {
		t.Fatal(err)
	}
	state, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	if state.Groups["order"].CurrentMaster != "order-replica" {
		t.Fatalf("current master changed unexpectedly: %#v", state.Groups["order"])
	}
	if state.Instances["order-master"].Role != "replica" || state.Instances["order-master"].ReplicaOf != "order-replica" {
		t.Fatalf("old master was mutated unexpectedly: %#v", state.Instances["order-master"])
	}
}

// TestSentinelCurrentAddr 验证从 SENTINEL MASTERS 回复中正确提取指定 group 的当前地址。
func TestSentinelCurrentAddr(t *testing.T) {
	resp := "*2\r\n" +
		"*4\r\n$4\r\nname\r\n$5\r\norder\r\n$2\r\nip\r\n$9\r\n10.0.1.10\r\n" +
		"*4\r\n$4\r\nname\r\n$5\r\ncache\r\n$2\r\nip\r\n$9\r\n10.0.1.11\r\n"
	reply, err := readRESP(bufio.NewReader(strings.NewReader(resp)))
	if err != nil {
		t.Fatal(err)
	}
	// 上面构造的回复没有 port 字段，验证缺 port 时视为"未知"而不是拼出错误地址。
	if _, ok := sentinelCurrentAddr(reply, "order"); ok {
		t.Fatal("expected no addr when port field is missing")
	}
	if _, ok := sentinelCurrentAddr(reply, "does-not-exist"); ok {
		t.Fatal("expected no addr for unknown group")
	}

	resp2 := "*1\r\n" +
		"*6\r\n$4\r\nname\r\n$5\r\norder\r\n$2\r\nip\r\n$9\r\n10.0.1.10\r\n$4\r\nport\r\n$4\r\n6379\r\n"
	reply2, err := readRESP(bufio.NewReader(strings.NewReader(resp2)))
	if err != nil {
		t.Fatal(err)
	}
	addr, ok := sentinelCurrentAddr(reply2, "order")
	if !ok || addr != "10.0.1.10:6379" {
		t.Fatalf("expected addr 10.0.1.10:6379, got %q ok=%v", addr, ok)
	}
}

// TestSyncSentinelNode_PreservesTrackingWhenAddrUnchanged 验证当 Sentinel 当前认知的
// master 地址与期望地址一致时，syncSentinelNode 不会 REMOVE+MONITOR 该组。
//
// 回归背景：此前的实现无条件对每个期望组做 REMOVE+MONITOR。这个操作会清空 Sentinel
// 对该组的全部历史状态——包括 failover 刚完成时 Sentinel 自己"虚拟加入"的旧 master
// 追踪记录（Sentinel 依赖这条记录，在旧 master 重新可达时自动将其降级为 replica，
// 这是它自愈机制的核心）。handleSentinelFailover 结束时会调用 syncSentinel()，如果
// 每次都无条件重置，就会在 Sentinel 刚建立追踪记录时立刻抹掉它，导致旧 master 永远
// 不会被自动纠正为 replica——造成脑裂。
func TestSyncSentinelNode_PreservesTrackingWhenAddrUnchanged(t *testing.T) {
	fs := newFakeSentinelWithMasters(t, map[string]string{"order": "10.0.1.10:6379"})

	desired := []apitypes.SentinelMaster{
		{Group: "order", Host: "10.0.1.10", Port: 6379, DownAfterMilliseconds: 5000, FailoverTimeout: 30000, ParallelSyncs: 1},
	}
	if err := syncSentinelNode(fs.addr(), desired, map[string]struct{}{"order": {}}, 2); err != nil {
		t.Fatal(err)
	}
	if fs.monitorCalls.Load() != 0 {
		t.Errorf("expected no MONITOR call when addr already matches, got %d calls", fs.monitorCalls.Load())
	}
	if fs.removeCalls.Load() != 0 {
		t.Errorf("expected no REMOVE call when addr already matches, got %d calls", fs.removeCalls.Load())
	}
	if fs.setCalls.Load() == 0 {
		t.Error("expected SENTINEL SET calls to still apply timing config")
	}
}

// TestSyncSentinelNode_ReconfiguresWhenAddrChanged 验证当地址确实变化时（例如手动修复
// 配置错误的场景），syncSentinelNode 仍然会正确 REMOVE+MONITOR 到新地址。
func TestSyncSentinelNode_ReconfiguresWhenAddrChanged(t *testing.T) {
	fs := newFakeSentinelWithMasters(t, map[string]string{"order": "10.0.1.10:6379"})

	desired := []apitypes.SentinelMaster{
		{Group: "order", Host: "10.0.1.99", Port: 6380, DownAfterMilliseconds: 5000, FailoverTimeout: 30000, ParallelSyncs: 1},
	}
	if err := syncSentinelNode(fs.addr(), desired, map[string]struct{}{"order": {}}, 2); err != nil {
		t.Fatal(err)
	}
	if fs.monitorCalls.Load() != 1 {
		t.Errorf("expected exactly one MONITOR call when addr changed, got %d", fs.monitorCalls.Load())
	}
}
