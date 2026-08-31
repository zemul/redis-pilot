package store

import "testing"

func TestTaskStorePersistsTerminalResultIdempotently(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTask(Task{ID: "task-1", Kind: "backup.restore", Status: "running", Payload: `{"instance":"redis-1"}`}); err != nil {
		t.Fatal(err)
	}
	changed, err := db.CompleteTask("task-1", "success", "2026-08-27T01:02:03", "")
	if err != nil || !changed {
		t.Fatalf("first completion: changed=%v err=%v", changed, err)
	}
	changed, err = db.CompleteTask("task-1", "failed", "", "late poll")
	if err != nil || changed {
		t.Fatalf("second completion must be ignored: changed=%v err=%v", changed, err)
	}
	task, err := db.GetTask("task-1")
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "success" || task.Result != "2026-08-27T01:02:03" {
		t.Fatalf("unexpected task: %+v", task)
	}
}

func TestQueryTasksFiltersAndKeepsOperationContext(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tasks := []Task{
		{ID: "backup-1", Kind: "backup.create", Status: "running", Operator: "alice", Source: "manual", Trigger: "manual", GroupName: "order", InstanceName: "order-1", ServerName: "srv-a"},
		{ID: "restore-1", Kind: "backup.restore", Status: "interrupted", Operator: "bob", GroupName: "cache", InstanceName: "cache-1", ServerName: "srv-b"},
		{ID: "backup-2", Kind: "backup.create", Status: "success", Operator: "alice", GroupName: "order", InstanceName: "order-10", ServerName: "srv-a"},
	}
	for _, task := range tasks {
		if err := db.CreateTask(task); err != nil {
			t.Fatal(err)
		}
	}
	active, total, err := db.QueryTasks(TaskFilter{Status: "active", Limit: 30})
	if err != nil || total != 1 || len(active) != 1 || active[0].ID != "backup-1" {
		t.Fatalf("active query: tasks=%+v total=%d err=%v", active, total, err)
	}
	failed, total, err := db.QueryTasks(TaskFilter{Status: "failed", Query: "srv-b", Limit: 30})
	if err != nil || total != 1 || len(failed) != 1 || failed[0].InstanceName != "cache-1" {
		t.Fatalf("failed query: tasks=%+v total=%d err=%v", failed, total, err)
	}
	// Instance 是精确过滤：order-1 不应把同前缀的 order-10 带出来。
	scoped, total, err := db.QueryTasks(TaskFilter{Instance: "order-1", Limit: 30})
	if err != nil || total != 1 || len(scoped) != 1 || scoped[0].ID != "backup-1" {
		t.Fatalf("instance query: tasks=%+v total=%d err=%v", scoped, total, err)
	}
	if active[0].Operator != "alice" || active[0].GroupName != "order" || active[0].ServerName != "srv-a" {
		t.Fatalf("operation context missing: %+v", active[0])
	}
}

func TestTaskProgressAndCancellationState(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTask(Task{ID: "scan-1", Kind: "diagnostic.bigkeys", Status: "running", Cancelable: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateTaskProgress("scan-1", "scanning", 420, "正在扫描", `{"scanned":420}`, true, false); err != nil {
		t.Fatal(err)
	}
	changed, err := db.MarkTaskCancelRequested("scan-1")
	if err != nil || !changed {
		t.Fatalf("cancel request: changed=%v err=%v", changed, err)
	}
	task, err := db.GetTask("scan-1")
	if err != nil || task.Progress != 420 || !task.CancelRequested || !task.Cancelable {
		t.Fatalf("unexpected task: %+v err=%v", task, err)
	}
	if changed, err := db.CompleteTask("scan-1", "canceled", task.Result, ""); err != nil || !changed {
		t.Fatalf("complete: changed=%v err=%v", changed, err)
	}
	task, _ = db.GetTask("scan-1")
	if task.Status != "canceled" || task.Cancelable || task.FinishedAt == "" {
		t.Fatalf("unexpected terminal task: %+v", task)
	}
}
