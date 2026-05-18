package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestQueryReturnsNewestRecordsFirst(t *testing.T) {
	logger := New(t.TempDir())
	writeAuditFile(t, logger.dir, "20260517", []Record{
		{ID: "old-1", Action: "instance.start", Level: LevelNormal, Result: "success"},
		{ID: "old-2", Action: "instance.stop", Level: LevelNormal, Result: "success"},
	})
	writeAuditFile(t, logger.dir, "20260518", []Record{
		{ID: "new-1", Action: "instance.start", Level: LevelNormal, Result: "success"},
		{ID: "new-2", Action: "instance.stop", Level: LevelNormal, Result: "success"},
	})

	records, err := logger.Query(QueryFilter{From: "20260517", To: "20260518", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	want := []string{"new-2", "new-1", "old-2"}
	for i, id := range want {
		if records[i].ID != id {
			t.Fatalf("record %d: expected %s, got %s", i, id, records[i].ID)
		}
	}
}

func writeAuditFile(t *testing.T, dir, date string, records []Record) {
	t.Helper()
	var data []byte
	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, line...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "audit-"+date+".jsonl"), data, 0644); err != nil {
		t.Fatal(err)
	}
}
