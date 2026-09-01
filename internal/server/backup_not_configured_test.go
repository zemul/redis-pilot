package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 持久化组无备份策略：连续 3 轮观测后应记 open 的 backup_not_configured warning。
func TestEmitBackupNotConfigured_PersistentTriggers(t *testing.T) {
	s := newTestServer(t, "")
	if s.incidents == nil {
		t.Fatal("test server must have incidents wired")
	}
	fp := "backup_not_configured:group:order"
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "standalone", Engine: "redis", Category: "persistent"},
		},
	}
	// 去抖：前两轮不落库。
	s.emitBackupNotConfigured(st)
	s.emitBackupNotConfigured(st)
	if got, _ := s.db.GetActiveIncident(fp); got != nil {
		t.Fatal("must not record before 3rd observation")
	}
	// 第三轮：确认。
	s.emitBackupNotConfigured(st)
	got, _ := s.db.GetActiveIncident(fp)
	if got == nil {
		t.Fatal("expected open backup_not_configured after 3 observations")
	}
	if got.Severity != "warning" || got.GroupName != "order" {
		t.Fatalf("unexpected incident %+v", got)
	}
}

// cache 组不判定、不记录。
func TestEmitBackupNotConfigured_CacheSkipped(t *testing.T) {
	s := newTestServer(t, "")
	st := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"cache": {Type: "standalone", Engine: "redis", Category: "cache"},
		},
	}
	for i := 0; i < 5; i++ {
		s.emitBackupNotConfigured(st)
	}
	if got, _ := s.db.GetActiveIncident("backup_not_configured:group:cache"); got != nil {
		t.Fatal("cache group must never trigger backup_not_configured")
	}
}

// 配上备份后连续 2 轮正常应恢复（resolved）。
func TestEmitBackupNotConfigured_RecoversAfterConfigured(t *testing.T) {
	s := newTestServer(t, "")
	fp := "backup_not_configured:group:order"
	notConfigured := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "standalone", Engine: "redis", Category: "persistent"},
		},
	}
	// 触发 open。
	s.emitBackupNotConfigured(notConfigured)
	s.emitBackupNotConfigured(notConfigured)
	s.emitBackupNotConfigured(notConfigured)
	if got, _ := s.db.GetActiveIncident(fp); got == nil {
		t.Fatal("expected open incident before recovery test")
	}

	configured := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {
				Type: "standalone", Engine: "redis", Category: "persistent",
				Backup: &apitypes.BackupConfig{Schedule: "37 2 * * *", Retention: 7},
			},
		},
	}
	// 连续 2 轮正常 → resolved。
	s.emitBackupNotConfigured(configured)
	s.emitBackupNotConfigured(configured)
	if got, _ := s.db.GetActiveIncident(fp); got != nil {
		t.Fatalf("expected incident resolved after backup configured, still active: %+v", got)
	}
}
