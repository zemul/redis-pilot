package store

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// TestGeneration_EveryMutatingMethodBumps 逐个验证所有拓扑写方法都会推进代际。
//
// 代际是 xDS 长轮询的唤醒依据。漏掉任何一个写方法的后果不是性能问题，而是那类写入
// 之后 Proxy 的配置永久停在旧值——一个不报错、只静默错下去的故障。所以这里把每个
// 方法都单独过一遍，而不是抽样。
//
// 新增写方法时，如果忘了走 execWrite 或忘了在 Commit 后自增，这个测试不会自动发现，
// 但会在这里留下"应该补一项"的位置。同时 store 包内已无裸 d.sql.Exec（审计除外），
// 新写入只要沿用 execWrite 就自动正确。
func TestGeneration_EveryMutatingMethodBumps(t *testing.T) {
	db := openTestDB(t)

	inst := &apitypes.Instance{
		Group: "order", Role: "master", Server: "srv1", Port: 6379,
		DesiredState: apitypes.DesiredRunning, ObservedStatus: apitypes.ObservedRunning,
	}
	srv := &apitypes.NodeServer{Endpoint: "10.0.1.10", AgentPort: 8400, Status: "healthy"}

	cases := []struct {
		name string
		call func() error
	}{
		{"UpsertGroup", func() error {
			return db.UpsertGroup("order", &apitypes.InstanceGroupState{Type: "replication", Engine: "redis"})
		}},
		{"UpsertInstance", func() error { return db.UpsertInstance("order-a", inst) }},
		{"UpsertServer", func() error { return db.UpsertServer("srv1", srv) }},
		{"WithServers", func() error {
			return db.WithServers(func(m map[string]*apitypes.NodeServer) error {
				m["srv2"] = srv
				return nil
			})
		}},
		{"WriteNode", func() error {
			return db.WriteNode(&apitypes.NodeState{
				Servers: map[string]*apitypes.NodeServer{"srv1": srv},
			})
		}},
		{"WithInstances", func() error {
			return db.WithInstances(func(st *apitypes.InstancesState) error {
				if st.Instances == nil {
					st.Instances = map[string]*apitypes.Instance{}
				}
				st.Instances["order-b"] = inst
				return nil
			})
		}},
		{"DeleteInstance", func() error { return db.DeleteInstance("order-b") }},
		{"DeleteServer", func() error { return db.DeleteServer("srv1") }},
		{"DeleteGroup", func() error { return db.DeleteGroup("order") }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := db.Generation()
			if err := c.call(); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			if after := db.Generation(); after <= before {
				t.Errorf("%s 没有推进代际（%d → %d）。"+
					"xDS 长轮询靠代际唤醒，漏掉的写入会让 Proxy 配置永久停在旧值",
					c.name, before, after)
			}
		})
	}
}

// TestGeneration_FailedWriteDoesNotBump 验证失败的写入不推进代际。
// 推进了会导致一次无谓的重算，虽不致错但属于噪声。
func TestGeneration_FailedWriteDoesNotBump(t *testing.T) {
	db := openTestDB(t)
	before := db.Generation()

	// 回调返回错误 → 事务回滚，不应推进
	err := db.WithInstances(func(st *apitypes.InstancesState) error {
		st.Instances = map[string]*apitypes.Instance{"x": {Group: "g"}}
		return errTestRollback
	})
	if err == nil {
		t.Fatal("期望 WithInstances 返回错误")
	}
	if after := db.Generation(); after != before {
		t.Errorf("回滚的事务推进了代际（%d → %d）", before, after)
	}
}

// TestGeneration_ReadsDoNotBump 验证只读操作不推进代际，否则长轮询会被自己唤醒。
func TestGeneration_ReadsDoNotBump(t *testing.T) {
	db := openTestDB(t)
	if err := db.UpsertServer("srv1", &apitypes.NodeServer{Endpoint: "10.0.1.10"}); err != nil {
		t.Fatal(err)
	}
	before := db.Generation()

	if _, err := db.ReadInstances(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReadNode(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetAllServers(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetAllInstances(); err != nil {
		t.Fatal(err)
	}

	if after := db.Generation(); after != before {
		t.Errorf("只读操作推进了代际（%d → %d），长轮询会被自己不断唤醒", before, after)
	}
}

// TestGeneration_AuditDoesNotBump 审计日志写入刻意不推进代际：它不影响代理配置，
// 而审计写入非常频繁，推进会让每条日志都唤醒一次长轮询去做无谓的重算。
func TestGeneration_AuditDoesNotBump(t *testing.T) {
	db := openTestDB(t)
	before := db.Generation()
	if err := db.InsertAudit(AuditRecord{
		ID: "1", Timestamp: "2026-01-01T00:00:00Z", Action: "test", Result: "success",
	}); err != nil {
		t.Fatal(err)
	}
	if after := db.Generation(); after != before {
		t.Errorf("审计写入推进了代际（%d → %d）", before, after)
	}
}

var errTestRollback = errTestRollbackType{}

type errTestRollbackType struct{}

func (errTestRollbackType) Error() string { return "test rollback" }
