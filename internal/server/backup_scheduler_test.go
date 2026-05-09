package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestSyncJobs_AddNewJob(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine: "redis", Status: "running",
				Backup: &apitypes.BackupConfig{Schedule: "0 2 * * *"},
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

func TestSyncJobs_SkipStoppedInstance(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine: "redis", Status: "stopped",
				Backup: &apitypes.BackupConfig{Schedule: "0 2 * * *"},
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs for stopped instance, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_SkipNoSchedule(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {Engine: "redis", Status: "running"},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs for instance without schedule, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_RemoveDeletedInstance(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine: "redis", Status: "running",
				Backup: &apitypes.BackupConfig{Schedule: "0 2 * * *"},
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(bs.jobs))
	}

	// 删除实例
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{},
	})
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs after instance deleted, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_UpdateSchedule(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine: "redis", Status: "running",
				Backup: &apitypes.BackupConfig{Schedule: "0 2 * * *"},
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	oldEntryID := bs.jobs["redis-1"].entryID

	// 更新 schedule
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine: "redis", Status: "running",
				Backup: &apitypes.BackupConfig{Schedule: "0 3 * * *"},
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
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine: "redis", Status: "running",
				Backup: &apitypes.BackupConfig{Schedule: "invalid cron"},
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 0 {
		t.Fatalf("expected 0 jobs for invalid cron, got %d", len(bs.jobs))
	}
}

func TestSyncJobs_MultipleInstances(t *testing.T) {
	s := newTestServer(t, "")
	s.state.WriteInstances(&apitypes.InstancesState{
		Instances: map[string]*apitypes.Instance{
			"redis-1": {
				Engine: "redis", Status: "running",
				Backup: &apitypes.BackupConfig{Schedule: "0 2 * * *"},
			},
			"redis-2": {
				Engine: "redis", Status: "running",
				Backup: &apitypes.BackupConfig{Schedule: "0 4 * * *"},
			},
			"redis-3": {
				Engine: "redis", Status: "stopped",
				Backup: &apitypes.BackupConfig{Schedule: "0 6 * * *"},
			},
		},
	})

	bs := newBackupScheduler(s)
	bs.syncJobs()

	if len(bs.jobs) != 2 {
		t.Fatalf("expected 2 jobs (redis-3 stopped), got %d", len(bs.jobs))
	}
	if _, ok := bs.jobs["redis-1"]; !ok {
		t.Fatal("expected job for redis-1")
	}
	if _, ok := bs.jobs["redis-2"]; !ok {
		t.Fatal("expected job for redis-2")
	}
}
