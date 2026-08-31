package store

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestInstanceRoundTrip_StateFields 验证拆分后的两个状态列真的各自独立持久化。
//
// schema 从单列 status 改成 desired_state + observed_status 是手工改的 DDL 和 SQL
// （包括 INSERT 的占位符个数、UPSERT 的 excluded 子句、SELECT 的列顺序与扫描目标），
// 任何一处漏改都会表现为"状态莫名其妙串了"或写入丢失。这里逐个组合往返验证。
func TestInstanceRoundTrip_StateFields(t *testing.T) {
	cases := []struct {
		name     string
		desired  string
		observed string
	}{
		{"运行中", apitypes.DesiredRunning, apitypes.ObservedRunning},
		{"意外停止", apitypes.DesiredRunning, apitypes.ObservedStopped},
		{"容器消失", apitypes.DesiredRunning, apitypes.ObservedMissing},
		{"刚创建未观测", apitypes.DesiredRunning, apitypes.ObservedUnknown},
		{"已停止", apitypes.DesiredStopped, apitypes.ObservedStopped},
		{"正在停止", apitypes.DesiredStopped, apitypes.ObservedRunning},
	}

	db := openTestDB(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inst := &apitypes.Instance{
				Group:          "order",
				Role:           "replica",
				Server:         "srv1",
				Port:           6380,
				ReplicaOf:      "order-master",
				DesiredState:   c.desired,
				ObservedStatus: c.observed,
			}
			if err := db.UpsertInstance(c.name, inst); err != nil {
				t.Fatalf("UpsertInstance: %v", err)
			}
			got, err := db.GetInstance(c.name)
			if err != nil {
				t.Fatalf("GetInstance: %v", err)
			}
			if got.DesiredState != c.desired {
				t.Errorf("DesiredState = %q, want %q", got.DesiredState, c.desired)
			}
			if got.ObservedStatus != c.observed {
				t.Errorf("ObservedStatus = %q, want %q", got.ObservedStatus, c.observed)
			}
			// 顺带确认相邻字段没有因为列顺序改动而错位
			if got.ReplicaOf != "order-master" {
				t.Errorf("ReplicaOf = %q，相邻列可能错位了", got.ReplicaOf)
			}
			if got.Role != "replica" || got.Port != 6380 {
				t.Errorf("role=%q port=%d，字段错位", got.Role, got.Port)
			}
		})
	}
}

func TestGroupRoundTrip_Labels(t *testing.T) {
	db := openTestDB(t)
	group := &apitypes.InstanceGroupState{
		Type: "standalone", Engine: "redis", Category: "cache",
		Labels: []string{"交易", "生产"},
	}
	if err := db.UpsertGroup("order", group); err != nil {
		t.Fatalf("UpsertGroup: %v", err)
	}
	groups, err := db.GetAllGroups()
	if err != nil {
		t.Fatalf("GetAllGroups: %v", err)
	}
	got := groups["order"]
	if got == nil || len(got.Labels) != 2 || got.Labels[0] != "交易" || got.Labels[1] != "生产" {
		t.Fatalf("labels did not round trip: %#v", got)
	}

	group.Labels = []string{}
	if err := db.UpsertGroup("order", group); err != nil {
		t.Fatalf("clear labels: %v", err)
	}
	groups, err = db.GetAllGroups()
	if err != nil {
		t.Fatal(err)
	}
	if len(groups["order"].Labels) != 0 {
		t.Fatalf("labels were not cleared: %#v", groups["order"].Labels)
	}
}

// TestInstanceUpsert_UpdatesStateFieldsIndependently 验证 UPSERT 能分别更新两个字段。
// excluded 子句漏掉任意一列，都会表现为"改了没生效"。
func TestInstanceUpsert_UpdatesStateFieldsIndependently(t *testing.T) {
	db := openTestDB(t)

	inst := &apitypes.Instance{
		Group: "cache", Role: "master", Server: "srv1", Port: 6379,
		DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
	}
	if err := db.UpsertInstance("cache-1", inst); err != nil {
		t.Fatal(err)
	}

	// 只改观测（模拟 reconcile）
	inst.ObservedStatus = apitypes.ObservedMissing
	if err := db.UpsertInstance("cache-1", inst); err != nil {
		t.Fatal(err)
	}
	got, _ := db.GetInstance("cache-1")
	if got.ObservedStatus != apitypes.ObservedMissing {
		t.Errorf("观测字段更新未生效：%q", got.ObservedStatus)
	}
	if got.DesiredState != apitypes.DesiredRunning {
		t.Errorf("意图字段被意外改动：%q", got.DesiredState)
	}

	// 只改意图（模拟用户 stop）
	inst.DesiredState = apitypes.DesiredStopped
	if err := db.UpsertInstance("cache-1", inst); err != nil {
		t.Fatal(err)
	}
	got, _ = db.GetInstance("cache-1")
	if got.DesiredState != apitypes.DesiredStopped {
		t.Errorf("意图字段更新未生效：%q", got.DesiredState)
	}
}

// TestWithInstances_PersistsStateFields 覆盖 WithInstances 走的那条独立 INSERT 语句
// （与 UpsertInstance 是两份 SQL，占位符个数要各自对上）。
func TestWithInstances_PersistsStateFields(t *testing.T) {
	db := openTestDB(t)

	err := db.WithInstances(func(st *apitypes.InstancesState) error {
		if st.Groups == nil {
			st.Groups = map[string]*apitypes.InstanceGroupState{}
		}
		if st.Instances == nil {
			st.Instances = map[string]*apitypes.Instance{}
		}
		st.Groups["order"] = &apitypes.InstanceGroupState{Type: "replication", Engine: "redis"}
		st.Instances["order-a"] = &apitypes.Instance{
			Group: "order", Role: "master", Server: "srv1", Port: 6379,
			DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedStopped,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithInstances: %v", err)
	}

	st, err := db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	inst := st.Instances["order-a"]
	if inst == nil {
		t.Fatal("实例未持久化")
	}
	if inst.DesiredState != apitypes.DesiredRunning || inst.ObservedStatus != apitypes.ObservedStopped {
		t.Errorf("desired=%q observed=%q，want running/stopped", inst.DesiredState, inst.ObservedStatus)
	}
	if got := inst.Phase(); got != "unexpected_stopped" {
		t.Errorf("Phase = %q, want unexpected_stopped", got)
	}
}
