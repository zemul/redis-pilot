package server

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

// nopLogger 满足 incidentLogger，测试里不关心日志输出。
type nopLogger struct{}

func (nopLogger) Errorf(string, ...interface{}) {}
func (nopLogger) Infof(string, ...interface{})  {}

func newTestIncidentService(t *testing.T) (*IncidentService, *store.DB) {
	t.Helper()
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return newIncidentService(db, nopLogger{}), db
}

func downObs(active bool) Observation {
	return Observation{
		Type: "instance_unexpected_down", Active: active, Severity: "critical",
		Source: "reconcile", ObjectType: "instance", ObjectID: "order-1", InstanceName: "order-1",
		Summary: "down",
	}
}

func TestObserveConfirmsAfterThreeFailures(t *testing.T) {
	svc, db := newTestIncidentService(t)
	fp := "instance_unexpected_down:instance:order-1"

	// 前两次异常：仍在去抖窗口，不落库。
	svc.Observe(downObs(true))
	svc.Observe(downObs(true))
	if got, _ := db.GetActiveIncident(fp); got != nil {
		t.Fatal("must not create incident before 3rd failure")
	}
	// 第三次：确认，写 open。
	svc.Observe(downObs(true))
	got, _ := db.GetActiveIncident(fp)
	if got == nil {
		t.Fatal("expected open incident after 3 failures")
	}
	if got.Severity != "critical" || got.InstanceName != "order-1" {
		t.Fatalf("unexpected incident %+v", got)
	}
}

func TestObserveShortBlipDoesNotRecordHistory(t *testing.T) {
	svc, db := newTestIncidentService(t)

	// 两次异常(未到 3)，随后恢复 → 不应进入历史。
	svc.Observe(downObs(true))
	svc.Observe(downObs(true))
	svc.Observe(downObs(false))

	list, total, err := db.QueryIncidents(store.IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(list) != 0 {
		t.Fatalf("short blip must not be recorded, got total=%d", total)
	}
}

func TestObserveResolvesAfterTwoRecoveries(t *testing.T) {
	svc, db := newTestIncidentService(t)
	fp := "instance_unexpected_down:instance:order-1"

	// 确认异常。
	svc.Observe(downObs(true))
	svc.Observe(downObs(true))
	svc.Observe(downObs(true))
	if got, _ := db.GetActiveIncident(fp); got == nil {
		t.Fatal("precondition: incident should be open")
	}

	// 一次正常：还没到恢复阈值，仍 open。
	svc.Observe(downObs(false))
	if got, _ := db.GetActiveIncident(fp); got == nil {
		t.Fatal("one recovery is not enough, must stay open")
	}
	// 第二次正常：确认恢复。
	svc.Observe(downObs(false))
	if got, _ := db.GetActiveIncident(fp); got != nil {
		t.Fatal("should be resolved after 2 recoveries")
	}
	// resolved 记录仍在历史里。
	_, total, _ := db.QueryIncidents(store.IncidentFilter{Status: "resolved"})
	if total != 1 {
		t.Fatalf("expected 1 resolved incident, got %d", total)
	}
}

func TestObserveInterruptedRecoveryStaysOpen(t *testing.T) {
	svc, db := newTestIncidentService(t)
	fp := "instance_unexpected_down:instance:order-1"

	svc.Observe(downObs(true))
	svc.Observe(downObs(true))
	svc.Observe(downObs(true)) // open

	svc.Observe(downObs(false)) // 恢复候选 1
	svc.Observe(downObs(true))  // 又异常，恢复计数清零
	svc.Observe(downObs(false)) // 恢复候选 1(重新计数)
	if got, _ := db.GetActiveIncident(fp); got == nil {
		t.Fatal("recovery counter should have reset; incident must stay open")
	}
}

func TestReportAndResolveTaskIncident(t *testing.T) {
	svc, db := newTestIncidentService(t)
	fp := "backup_task_failed:instance:order-1"

	// 任务失败：立即 open，不去抖。
	svc.Report(Observation{
		Type: "backup_task_failed", Severity: "warning", Source: "task",
		ObjectType: "instance", ObjectID: "order-1", InstanceName: "order-1", Summary: "backup failed",
	})
	if got, _ := db.GetActiveIncident(fp); got == nil {
		t.Fatal("Report must create an open incident immediately")
	}

	// 后续成功：结束异常。
	svc.Resolve("backup_task_failed", "instance", "order-1")
	if got, _ := db.GetActiveIncident(fp); got != nil {
		t.Fatal("Resolve must close the open incident")
	}
}
