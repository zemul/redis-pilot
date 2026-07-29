package apitypes

import "testing"

// TestPhase 是 DesiredState × ObservedStatus 的完整推导矩阵。
//
// 拆分之前，Status 单字段同时承担意图与观测，unexpected_stopped 这类本质上是
// 两个字段差值的信息被塞进同一个字段，导致写入者和读取者对同一个值的理解出现分歧。
// 现在派生值只读不存，矩阵在这里被完整固定下来。
func TestPhase(t *testing.T) {
	cases := []struct {
		desired  string
		observed string
		want     string
		why      string
	}{
		// 意图 running
		{DesiredRunning, ObservedUnknown, "creating", "要求它跑，但一次都还没观测到"},
		{DesiredRunning, ObservedRunning, "running", "意图与观测一致"},
		{DesiredRunning, ObservedStopped, "unexpected_stopped", "没人要求它停，它自己停了"},
		{DesiredRunning, ObservedMissing, "failed", "容器不见了"},
		{DesiredRunning, ObservedUnreachable, "unreachable", "观测能力本身失效"},

		// 意图 stopped
		{DesiredStopped, ObservedStopped, "stopped", "意图与观测一致"},
		{DesiredStopped, ObservedMissing, "stopped", "要求它停，容器被清掉也算停"},
		{DesiredStopped, ObservedRunning, "stopping", "已要求停止但还观测到在跑"},
		{DesiredStopped, ObservedUnknown, "stopping", "已要求停止但还没观测到结果"},
	}

	for _, c := range cases {
		inst := &Instance{DesiredState: c.desired, ObservedStatus: c.observed}
		if got := inst.Phase(); got != c.want {
			t.Errorf("Phase(desired=%q, observed=%q) = %q, want %q（%s）",
				c.desired, c.observed, got, c.want, c.why)
		}
	}
}

func TestPhase_NilInstance(t *testing.T) {
	var inst *Instance
	if got := inst.Phase(); got != "unknown" {
		t.Errorf("nil 实例的 Phase = %q, want unknown", got)
	}
}

// TestIsServing 固定"能否承接业务流量"的判据是观测而非意图。
//
// 用意图判断会把流量发给一个"应该在跑但实际已经挂了"的实例。
func TestIsServing(t *testing.T) {
	cases := []struct {
		desired  string
		observed string
		want     bool
	}{
		{DesiredRunning, ObservedRunning, true},
		{DesiredRunning, ObservedStopped, false},
		{DesiredRunning, ObservedMissing, false},
		{DesiredRunning, ObservedUnknown, false},
		{DesiredRunning, ObservedUnreachable, false},
		// 意图是停，但观测到还在跑：正在停止过程中，不该再接新流量
		{DesiredStopped, ObservedRunning, true},
	}
	for _, c := range cases {
		inst := &Instance{DesiredState: c.desired, ObservedStatus: c.observed}
		if got := inst.IsServing(); got != c.want {
			t.Errorf("IsServing(desired=%q, observed=%q) = %v, want %v",
				c.desired, c.observed, got, c.want)
		}
	}

	var nilInst *Instance
	if nilInst.IsServing() {
		t.Error("nil 实例不应被认为可以承接流量")
	}
}

// TestToView_ExposesBothSources 验证对外视图既给派生状态也给两个来源字段，
// 让"为什么是这个状态"可以被直接看出来，而不用猜。
func TestToView_ExposesBothSources(t *testing.T) {
	inst := &Instance{
		Group:          "order",
		Role:           "replica",
		DesiredState:   DesiredRunning,
		ObservedStatus: ObservedStopped,
	}
	v := inst.ToView()
	if v.Status != "unexpected_stopped" {
		t.Errorf("Status = %q, want unexpected_stopped", v.Status)
	}
	if v.DesiredState != DesiredRunning {
		t.Errorf("DesiredState = %q", v.DesiredState)
	}
	if v.ObservedStatus != ObservedStopped {
		t.Errorf("ObservedStatus = %q", v.ObservedStatus)
	}
}
