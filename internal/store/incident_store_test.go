package store

import "testing"

func newIncident(id, fp, itype string) Incident {
	return Incident{
		ID: id, Fingerprint: fp, IncidentType: itype, Status: "open",
		Severity: "critical", Source: "reconcile", ObjectType: "instance", ObjectID: "order-1",
		InstanceName: "order-1", Summary: "down", FirstObservedAt: "2026-08-31T09:00:00Z",
	}
}

func TestCreateAndGetActiveIncident(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fp := "instance_unexpected_down:instance:order-1"
	if err := db.CreateIncident(newIncident("inc-1", fp, "instance_unexpected_down")); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetActiveIncident(fp)
	if err != nil || got == nil {
		t.Fatalf("expected active incident, got=%v err=%v", got, err)
	}
	if got.ID != "inc-1" {
		t.Fatalf("unexpected id %q", got.ID)
	}
}

func TestActiveFingerprintUniqueWhileOpen(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fp := "instance_unexpected_down:instance:order-1"
	if err := db.CreateIncident(newIncident("inc-1", fp, "instance_unexpected_down")); err != nil {
		t.Fatal(err)
	}
	// 第二条同 fingerprint 的 open 记录必须被唯一索引拒绝。
	if err := db.CreateIncident(newIncident("inc-2", fp, "instance_unexpected_down")); err == nil {
		t.Fatal("expected unique constraint violation for second open incident")
	}

	// resolved 之后，同 fingerprint 可以再次 open（新一次生命周期）。
	if ok, err := db.ResolveIncident(fp, "2026-08-31T09:05:00Z"); err != nil || !ok {
		t.Fatalf("resolve: ok=%v err=%v", ok, err)
	}
	if err := db.CreateIncident(newIncident("inc-3", fp, "instance_unexpected_down")); err != nil {
		t.Fatalf("re-open after resolve should succeed: %v", err)
	}
}

func TestResolveIncidentSetsDurationOnQuery(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fp := "master_mismatch:group:order"
	inc := newIncident("inc-1", fp, "master_mismatch")
	inc.ObjectType, inc.ObjectID, inc.GroupName, inc.InstanceName = "group", "order", "order", ""
	inc.FirstObservedAt = "2026-08-31T09:00:00Z"
	if err := db.CreateIncident(inc); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.ResolveIncident(fp, "2026-08-31T09:02:30Z"); err != nil || !ok {
		t.Fatalf("resolve: ok=%v err=%v", ok, err)
	}
	got, err := db.GetIncident("inc-1")
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Status != "resolved" {
		t.Fatalf("status=%q want resolved", got.Status)
	}
	// 09:00:00 → 09:02:30 = 150000ms
	if got.DurationMs != 150000 {
		t.Fatalf("duration=%d want 150000", got.DurationMs)
	}
}

func TestQueryIncidentsFilterAndOpenFirst(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 一条 resolved + 一条 open。默认排序 open 应在前。
	r := newIncident("inc-resolved", "t:instance:a", "instance_unexpected_down")
	r.Status, r.ResolvedAt, r.FirstObservedAt = "resolved", "2026-08-31T08:10:00Z", "2026-08-31T08:00:00Z"
	if err := db.CreateIncident(r); err != nil {
		t.Fatal(err)
	}
	o := newIncident("inc-open", "t:instance:b", "instance_unexpected_down")
	o.FirstObservedAt = "2026-08-31T07:00:00Z" // 更早，但 open 优先应排前
	if err := db.CreateIncident(o); err != nil {
		t.Fatal(err)
	}

	list, total, err := db.QueryIncidents(IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(list) != 2 {
		t.Fatalf("total=%d len=%d want 2/2", total, len(list))
	}
	if list[0].ID != "inc-open" {
		t.Fatalf("open should sort first, got %q", list[0].ID)
	}

	// 按 status 过滤。
	onlyResolved, total, err := db.QueryIncidents(IncidentFilter{Status: "resolved"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || onlyResolved[0].ID != "inc-resolved" {
		t.Fatalf("resolved filter: total=%d list=%v", total, onlyResolved)
	}
}
