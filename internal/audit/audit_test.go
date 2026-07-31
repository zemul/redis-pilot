package audit

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

func newTestLogger(t *testing.T) *Logger {
	t.Helper()
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return New(db)
}

func TestQueryReturnsNewestRecordsFirst(t *testing.T) {
	logger := newTestLogger(t)

	// 写入旧记录（日期较早）
	for _, r := range []store.AuditRecord{
		{ID: "old-1", Timestamp: "2026-05-17T10:00:00Z", Action: "instance.start", Level: LevelNormal, Result: "success"},
		{ID: "old-2", Timestamp: "2026-05-17T11:00:00Z", Action: "instance.stop", Level: LevelNormal, Result: "success"},
	} {
		if err := logger.Log(r); err != nil {
			t.Fatal(err)
		}
	}

	// 写入新记录（日期较新）
	for _, r := range []store.AuditRecord{
		{ID: "new-1", Timestamp: "2026-05-18T10:00:00Z", Action: "instance.start", Level: LevelNormal, Result: "success"},
		{ID: "new-2", Timestamp: "2026-05-18T11:00:00Z", Action: "instance.stop", Level: LevelNormal, Result: "success"},
	} {
		if err := logger.Log(r); err != nil {
			t.Fatal(err)
		}
	}

	records, err := logger.Query(store.AuditFilter{From: "20260517", To: "20260518", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// 按时间倒序：new-2, new-1, old-2
	want := []string{"new-2", "new-1", "old-2"}
	for i, id := range want {
		if records[i].ID != id {
			t.Fatalf("record %d: expected %s, got %s", i, id, records[i].ID)
		}
	}
}

func TestQueryFilterByAction(t *testing.T) {
	logger := newTestLogger(t)

	for _, r := range []store.AuditRecord{
		{ID: "r1", Timestamp: "2026-05-18T10:00:00Z", Action: "instance.create", Level: LevelNormal, Result: "success"},
		{ID: "r2", Timestamp: "2026-05-18T11:00:00Z", Action: "instance.delete", Level: LevelCritical, Result: "success"},
	} {
		logger.Log(r) //nolint:errcheck
	}

	records, err := logger.Query(store.AuditFilter{Action: "instance.create", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != "r1" {
		t.Fatalf("expected [r1], got %v", records)
	}
}

func TestQueryFilterByLevel(t *testing.T) {
	logger := newTestLogger(t)

	for _, r := range []store.AuditRecord{
		{ID: "r1", Timestamp: "2026-05-18T10:00:00Z", Action: "a", Level: LevelNormal, Result: "success"},
		{ID: "r2", Timestamp: "2026-05-18T11:00:00Z", Action: "b", Level: LevelCritical, Result: "success"},
	} {
		logger.Log(r) //nolint:errcheck
	}

	records, err := logger.Query(store.AuditFilter{Level: "critical", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != "r2" {
		t.Fatalf("expected [r2], got %v", records)
	}
}

func TestQueryFilterByGroup(t *testing.T) {
	logger := newTestLogger(t)

	for _, r := range []store.AuditRecord{
		{ID: "r1", Timestamp: "2026-05-18T10:00:00Z", Action: "a", Level: LevelNormal, Result: "success",
			Target: map[string]interface{}{"group": "order"}},
		{ID: "r2", Timestamp: "2026-05-18T11:00:00Z", Action: "b", Level: LevelNormal, Result: "success",
			Target: map[string]interface{}{"group": "cache"}},
	} {
		logger.Log(r) //nolint:errcheck
	}

	records, err := logger.Query(store.AuditFilter{Group: "order", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != "r1" {
		t.Fatalf("expected [r1], got %v", records)
	}
}
