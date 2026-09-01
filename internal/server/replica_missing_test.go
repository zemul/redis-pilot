package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// resultsFromObserved 把 observed(实例名→actual) 转成 emitReplicaMissing 需要的
// []ReconcileResult 切片，简化用例构造。
func resultsFromObserved(observed map[string]string) []ReconcileResult {
	out := make([]ReconcileResult, 0, len(observed))
	for name, actual := range observed {
		out = append(out, ReconcileResult{Instance: name, Actual: actual})
	}
	return out
}

// 1 主 0 从、master 在服务：连续 3 轮观测后应记 open 的 replica_missing warning。
func TestEmitReplicaMissing_OneMasterZeroReplicaTriggers(t *testing.T) {
	s := newTestServer(t, "")
	if s.incidents == nil {
		t.Fatal("test server must have incidents wired")
	}
	fp := "replica_missing:group:ai"
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"ai": {Type: "replication", CurrentMaster: "ai-m"},
		},
		Instances: map[string]*apitypes.Instance{
			"ai-m": {Group: "ai", Role: "master"},
		},
	}
	observed := map[string]string{"ai-m": apitypes.ObservedRunning}
	results := resultsFromObserved(observed)

	// 去抖：前两轮不落库。
	s.emitReplicaMissing(results, st)
	s.emitReplicaMissing(results, st)
	if got, _ := s.db.GetActiveIncident(fp); got != nil {
		t.Fatal("must not record before 3rd observation")
	}
	// 第三轮：确认。
	s.emitReplicaMissing(results, st)
	got, _ := s.db.GetActiveIncident(fp)
	if got == nil {
		t.Fatal("expected open replica_missing after 3 observations")
	}
	if got.Severity != "warning" || got.GroupName != "ai" {
		t.Fatalf("unexpected incident %+v", got)
	}
}

// master 未在服务时不记 replica_missing：降级根因是 master，交给实例级异常，避免重复。
func TestEmitReplicaMissing_MasterDownSkipped(t *testing.T) {
	s := newTestServer(t, "")
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"ai": {Type: "replication", CurrentMaster: "ai-m"},
		},
		Instances: map[string]*apitypes.Instance{
			"ai-m": {Group: "ai", Role: "master"},
		},
	}
	observed := map[string]string{"ai-m": apitypes.ObservedStopped}
	results := resultsFromObserved(observed)
	for i := 0; i < 5; i++ {
		s.emitReplicaMissing(results, st)
	}
	if got, _ := s.db.GetActiveIncident("replica_missing:group:ai"); got != nil {
		t.Fatal("must not record replica_missing when master is not serving")
	}
}

// standalone 组从不判定 replica_missing。
func TestEmitReplicaMissing_StandaloneSkipped(t *testing.T) {
	s := newTestServer(t, "")
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"cache": {Type: "standalone", CurrentMaster: "cache-1"},
		},
		Instances: map[string]*apitypes.Instance{
			"cache-1": {Group: "cache", Role: "master"},
		},
	}
	observed := map[string]string{"cache-1": apitypes.ObservedRunning}
	results := resultsFromObserved(observed)
	for i := 0; i < 5; i++ {
		s.emitReplicaMissing(results, st)
	}
	if got, _ := s.db.GetActiveIncident("replica_missing:group:cache"); got != nil {
		t.Fatal("standalone group must never trigger replica_missing")
	}
}

// 补上一个在跑的从节点后连续 2 轮正常应恢复（resolved），不与 no_running_replica 并存。
func TestEmitReplicaMissing_RecoversAfterReplicaAdded(t *testing.T) {
	s := newTestServer(t, "")
	fp := "replica_missing:group:ai"
	missingState := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"ai": {Type: "replication", CurrentMaster: "ai-m"},
		},
		Instances: map[string]*apitypes.Instance{
			"ai-m": {Group: "ai", Role: "master"},
		},
	}
	missingObserved := map[string]string{"ai-m": apitypes.ObservedRunning}
	// 触发 open。
	for i := 0; i < 3; i++ {
		s.emitReplicaMissing(resultsFromObserved(missingObserved), missingState)
	}
	if got, _ := s.db.GetActiveIncident(fp); got == nil {
		t.Fatal("expected open incident before recovery test")
	}

	// 补上一个在服务的从节点。
	servingState := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"ai": {Type: "replication", CurrentMaster: "ai-m"},
		},
		Instances: map[string]*apitypes.Instance{
			"ai-m":  {Group: "ai", Role: "master"},
			"ai-r1": {Group: "ai", Role: "replica", DesiredState: apitypes.DesiredRunning},
		},
	}
	servingObserved := map[string]string{"ai-m": apitypes.ObservedRunning, "ai-r1": apitypes.ObservedRunning}
	// 连续 2 轮正常 → resolved。
	s.emitReplicaMissing(resultsFromObserved(servingObserved), servingState)
	s.emitReplicaMissing(resultsFromObserved(servingObserved), servingState)
	if got, _ := s.db.GetActiveIncident(fp); got != nil {
		t.Fatalf("expected incident resolved after replica added, still active: %+v", got)
	}
}
