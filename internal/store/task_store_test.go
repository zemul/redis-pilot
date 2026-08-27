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
