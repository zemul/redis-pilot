package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// TestInjectSentinelRuntime 验证 injectSentinelRuntime 把缓存里的 Sentinel 共识 master
// 与运行时实际 master 注入到 replication 组视图，且不碰 standalone 组。
func TestInjectSentinelRuntime(t *testing.T) {
	s := &Server{
		sentinelMaster:   map[string]string{"order": "order-1"},
		runtimeReplicaOf: map[string]string{"order-1": "", "order-2": "10.0.1.12:6379"},
	}

	view := &apitypes.InstancesStateView{
		Groups: map[string]*apitypes.InstanceGroupView{
			"order": {Type: "replication", CurrentMaster: "order-1"},
			"cache": {Type: "standalone", CurrentMaster: "cache-1"},
		},
		Instances: map[string]*apitypes.InstanceView{
			"order-1": {Group: "order", Role: "master"},
			"order-2": {Group: "order", Role: "replica"},
			"cache-1": {Group: "cache", Role: "standalone"},
		},
	}

	s.injectSentinelRuntime(view)

	if got := view.Groups["order"].SentinelMaster; got != "order-1" {
		t.Errorf("order SentinelMaster = %q, want order-1", got)
	}
	// order-1 的 runtimeReplicaOf 为空串（自认 master），应被识别为运行时实际 master。
	if got := view.Groups["order"].RuntimeMaster; got != "order-1" {
		t.Errorf("order RuntimeMaster = %q, want order-1", got)
	}
	// standalone 组不做 master 一致性推导，两个字段都应保持空。
	if got := view.Groups["cache"].SentinelMaster; got != "" {
		t.Errorf("standalone cache SentinelMaster = %q, want empty", got)
	}
	if got := view.Groups["cache"].RuntimeMaster; got != "" {
		t.Errorf("standalone cache RuntimeMaster = %q, want empty", got)
	}
}

// TestInjectSentinelRuntime_Divergence 验证共识/运行时与平台记录不一致时字段如实反映，
// 供前端做"不一致"高亮（inject 只负责填值，不做判定）。
func TestInjectSentinelRuntime_Divergence(t *testing.T) {
	s := &Server{
		// Sentinel 和运行时都认为 master 已切到 order-2，但平台记录仍是 order-1。
		sentinelMaster:   map[string]string{"order": "order-2"},
		runtimeReplicaOf: map[string]string{"order-1": "10.0.1.12:6379", "order-2": ""},
	}
	view := &apitypes.InstancesStateView{
		Groups: map[string]*apitypes.InstanceGroupView{
			"order": {Type: "replication", CurrentMaster: "order-1"},
		},
		Instances: map[string]*apitypes.InstanceView{
			"order-1": {Group: "order"},
			"order-2": {Group: "order"},
		},
	}

	s.injectSentinelRuntime(view)

	if got := view.Groups["order"].SentinelMaster; got != "order-2" {
		t.Errorf("SentinelMaster = %q, want order-2", got)
	}
	if got := view.Groups["order"].RuntimeMaster; got != "order-2" {
		t.Errorf("RuntimeMaster = %q, want order-2", got)
	}
	if view.Groups["order"].CurrentMaster != "order-1" {
		t.Errorf("CurrentMaster should stay order-1 (platform record untouched)")
	}
}
