package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestReplicaAvailabilityStatus(t *testing.T) {
	// 构造一个 replication 组：master=g-m，从节点由各用例给定。
	mk := func(insts map[string]*apitypes.Instance) *apitypes.InstancesState {
		return &apitypes.InstancesState{
			Groups: map[string]*apitypes.InstanceGroupState{
				"g": {Type: "replication", CurrentMaster: "g-m"},
			},
			Instances: insts,
		}
	}

	cases := []struct {
		name     string
		insts    map[string]*apitypes.Instance
		observed map[string]string
		want     string
	}{
		{
			name: "至少一个从在跑 → serving",
			insts: map[string]*apitypes.Instance{
				"g-m": {Group: "g", Role: "master"},
				"g-r1": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredRunning},
				"g-r2": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredRunning},
			},
			observed: map[string]string{"g-m": apitypes.ObservedRunning, "g-r1": apitypes.ObservedStopped, "g-r2": apitypes.ObservedRunning},
			want:     "serving",
		},
		{
			name: "所有从确定性 down → all-down",
			insts: map[string]*apitypes.Instance{
				"g-m": {Group: "g", Role: "master"},
				"g-r1": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredRunning},
				"g-r2": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredRunning},
			},
			observed: map[string]string{"g-m": apitypes.ObservedRunning, "g-r1": apitypes.ObservedStopped, "g-r2": apitypes.ObservedMissing},
			want:     "all-down",
		},
		{
			name: "有从观测不确定(unreachable) → unknown",
			insts: map[string]*apitypes.Instance{
				"g-m": {Group: "g", Role: "master"},
				"g-r1": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredRunning},
				"g-r2": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredRunning},
			},
			observed: map[string]string{"g-r1": apitypes.ObservedStopped, "g-r2": apitypes.ObservedUnreachable},
			want:     "unknown",
		},
		{
			name: "从节点被用户主动 stop 不计入 → no-replicas",
			insts: map[string]*apitypes.Instance{
				"g-m": {Group: "g", Role: "master"},
				"g-r1": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredStopped},
			},
			observed: map[string]string{"g-r1": apitypes.ObservedStopped},
			want:     "no-replicas",
		},
		{
			name: "组内没有从节点 → no-replicas",
			insts: map[string]*apitypes.Instance{
				"g-m": {Group: "g", Role: "master"},
			},
			observed: map[string]string{"g-m": apitypes.ObservedRunning},
			want:     "no-replicas",
		},
		{
			name: "未观测(空串)也算不确定 → unknown",
			insts: map[string]*apitypes.Instance{
				"g-m": {Group: "g", Role: "master"},
				"g-r1": {Group: "g", Role: "replica", DesiredState: apitypes.DesiredRunning},
			},
			observed: map[string]string{},
			want:     "unknown",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := mk(tc.insts)
			got := replicaAvailabilityStatus("g", "g-m", st, tc.observed)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
