package store

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 组级 backup 列的读写往返：UpsertGroup 写入 → GetAllGroups 读回。
// 备份是组级概念（instance_groups.backup），实例级 backup 已移除，无一次性提升迁移。
func TestGroupRoundTrip_Backup(t *testing.T) {
	db := openTestDB(t)
	group := &apitypes.InstanceGroupState{
		Type: "standalone", Engine: "redis", Category: "persistent",
		Backup: &apitypes.BackupConfig{Schedule: "37 2 * * *", Retention: 7},
	}
	if err := db.UpsertGroup("order", group); err != nil {
		t.Fatalf("UpsertGroup: %v", err)
	}
	groups, err := db.GetAllGroups()
	if err != nil {
		t.Fatalf("GetAllGroups: %v", err)
	}
	got := groups["order"]
	if got == nil || got.Backup == nil {
		t.Fatalf("backup did not round trip: %#v", got)
	}
	if got.Backup.Schedule != "37 2 * * *" || got.Backup.Retention != 7 {
		t.Fatalf("backup fields mismatch: %#v", got.Backup)
	}

	// 置 nil 应清回 null。
	group.Backup = nil
	if err := db.UpsertGroup("order", group); err != nil {
		t.Fatalf("clear backup: %v", err)
	}
	groups, _ = db.GetAllGroups()
	if groups["order"].Backup != nil {
		t.Fatalf("backup was not cleared: %#v", groups["order"].Backup)
	}
}
