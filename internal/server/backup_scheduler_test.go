package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 组级备份模型：备份策略挂在 group.Backup 上；建 job 的条件是
// group.Backup.Schedule != "" 且组内存在可服务实例（PickBackupExecInstance 非空）。

func TestSyncJobs_AddNewJob(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroupWithBackup("redis-1", "0 2 * * *"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Group: "redis-1", Role: "master", DesiredState: "running", ObservedStatus: "running",
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(bs.jobs))
	}
	if bs.jobs["redis-1"].schedule != "0 2 * * *" {
		t.Fatalf("expected schedule '0 2 * * *', got %q", bs.jobs["redis-1"].schedule)
	}
}

func TestSyncJobs_SkipNoServingInstance(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroupWithBackup("redis-1", "0 2 * * *"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Group: "redis-1", Role: "master", DesiredState: "stopped", ObservedStatus: "stopped",
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs when no serving instance, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_SkipNoSchedule(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroup("redis-1"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Group: "redis-1", Role: "master", DesiredState: "running", ObservedStatus: "running"},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs for group without schedule, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_RemoveDeletedGroup(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroupWithBackup("redis-1", "0 2 * * *"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Group: "redis-1", Role: "master", DesiredState: "running", ObservedStatus: "running",
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(bs.jobs))
	}

	// 删除整个组
	s.db.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{},
	})
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs after group deleted, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_UpdateSchedule(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroupWithBackup("redis-1", "0 2 * * *"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Group: "redis-1", Role: "master", DesiredState: "running", ObservedStatus: "running",
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	oldEntryID := bs.jobs["redis-1"].entryID

	// 更新组级 schedule
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroupWithBackup("redis-1", "0 3 * * *"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Group: "redis-1", Role: "master", DesiredState: "running", ObservedStatus: "running",
			},
		},
	})
	bs.syncJobs()

	if len(bs.jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(bs.jobs))
	}
	if bs.jobs["redis-1"].schedule != "0 3 * * *" {
		t.Fatalf("expected updated schedule, got %q", bs.jobs["redis-1"].schedule)
	}
	if bs.jobs["redis-1"].entryID == oldEntryID {
		t.Fatal("expected new entry ID after schedule change")
	}
}

func TestSyncJobs_InvalidCron(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroupWithBackup("redis-1", "invalid cron"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Group: "redis-1", Role: "master", DesiredState: "running", ObservedStatus: "running",
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs for invalid cron, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_MultipleGroups(t *testing.T) {
	s := newTestServer(t, "")
	s.db.WriteInstances(&apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"redis-1": testGroupWithBackup("redis-1", "0 2 * * *"),
			"redis-2": testGroupWithBackup("redis-2", "0 4 * * *"),
			"redis-3": testGroupWithBackup("redis-3", "0 6 * * *"),
		},
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Group: "redis-1", Role: "master", DesiredState: "running", ObservedStatus: "running",
			},
			"redis-2": {
				Group: "redis-2", Role: "master", DesiredState: "running", ObservedStatus: "running",
			},
			// redis-3 组内实例已停止：无可服务实例，不建 job
			"redis-3": {
				Group: "redis-3", Role: "master", DesiredState: "stopped", ObservedStatus: "stopped",
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 2 {
		t.Fatalf("expected 2 jobs (redis-3 has no serving instance), got %d", len(bs.jobs))
	}
	if _, ok := bs.jobs["redis-1"]; !ok {
		t.Fatal("expected job for group redis-1")
	}
	if _, ok := bs.jobs["redis-2"]; !ok {
		t.Fatal("expected job for group redis-2")
	}
}

func testGroup(master string) *apitypes.InstanceGroupState {
	return &apitypes.InstanceGroupState{
		Type:          "standalone",
		Engine:        "redis",
		Category:      "cache",
		CurrentMaster: master,
	}
}

func testGroupWithBackup(master, schedule string) *apitypes.InstanceGroupState {
	g := testGroup(master)
	g.Backup = &apitypes.BackupConfig{Schedule: schedule, Retention: 7}
	return g
}
