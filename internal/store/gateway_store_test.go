package store

import (
	"errors"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func gatewaySpec(t *testing.T, d *DB) apitypes.GatewayDraftSpec {
	t.Helper()
	if err := d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertGroup("group", &apitypes.InstanceGroupState{Engine: "redis", Type: "standalone"}); err != nil {
		t.Fatal(err)
	}
	return apitypes.GatewayDraftSpec{Node: "node", Group: "group", DataPort: 17379, AdminPort: 19200}
}

func TestGatewayDraftCAS(t *testing.T) {
	d := openTestDB(t)
	spec := gatewaySpec(t, d)
	generation := d.Generation()
	v, err := d.SaveGatewayDraft("gw", spec, "")
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != "draft" || v.Spec.Mode != "passthrough" || v.Revision == "" {
		t.Fatalf("bad draft: %+v", v)
	}
	if _, err = d.SaveGatewayDraft("gw", spec, ""); !errors.Is(err, ErrGatewayDraftConflict) {
		t.Fatalf("duplicate: %v", err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := d.SaveGatewayDraft("gw", spec, v.Revision); results <- err }()
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrGatewayDraftConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("CAS success=%d conflict=%d", success, conflict)
	}
	updated, err := d.GetGatewayDraft("gw")
	if err != nil {
		t.Fatal(err)
	}
	if updated.CreatedAt != v.CreatedAt || updated.Revision == v.Revision {
		t.Fatal("revision or creation timestamp incorrect")
	}
	if err = d.DeleteGatewayDraft("gw", v.Revision); !errors.Is(err, ErrGatewayDraftConflict) {
		t.Fatalf("stale delete: %v", err)
	}
	if err = d.DeleteGatewayDraft("gw", updated.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err = d.SaveGatewayDraft("gw", spec, ""); err != nil {
		t.Fatal(err)
	}
	if _, err = d.SaveGatewayDraft("gw", spec, updated.Revision); !errors.Is(err, ErrGatewayDraftConflict) {
		t.Fatalf("delete/recreate ABA: %v", err)
	}
	if d.Generation() != generation {
		t.Fatal("unapplied draft changed topology generation")
	}
}

func TestGatewayDraftPersistenceAndDetachedReferences(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	spec := gatewaySpec(t, d)
	v, err := d.SaveGatewayDraft("gw", spec, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	got, err := d.GetGatewayDraft("gw")
	if err != nil || got.Revision != v.Revision {
		t.Fatalf("reopen: %+v %v", got, err)
	}
	// Drafts must not block ordinary topology rewrites/removal.
	if err = d.DeleteGroup("group"); err != nil {
		t.Fatal(err)
	}
	if _, err = d.GetGatewayDraft("gw"); err != nil {
		t.Fatal(err)
	}
	if _, err = d.SaveGatewayDraft("gw", spec, v.Revision); !errors.Is(err, ErrGatewayDraftInvalid) {
		t.Fatalf("missing reference accepted: %v", err)
	}
	if err = d.DeleteGatewayDraft("gw", v.Revision); err != nil {
		t.Fatal(err)
	}
	list, err := d.ListGatewayDrafts()
	if err != nil || list == nil || len(list) != 0 {
		t.Fatalf("empty list: %v %v", list, err)
	}
}

func TestGatewayDraftValidation(t *testing.T) {
	d := openTestDB(t)
	good := gatewaySpec(t, d)
	for _, change := range []func(*apitypes.GatewayDraftSpec){
		func(s *apitypes.GatewayDraftSpec) { s.Mode = "hybrid" },
		func(s *apitypes.GatewayDraftSpec) { s.Node = "missing" },
		func(s *apitypes.GatewayDraftSpec) { s.Group = "missing" },
		func(s *apitypes.GatewayDraftSpec) { s.DataPort = 80 },
		func(s *apitypes.GatewayDraftSpec) { s.AdminPort = s.DataPort },
	} {
		spec := good
		change(&spec)
		if _, err := d.SaveGatewayDraft("gw", spec, ""); !errors.Is(err, ErrGatewayDraftInvalid) {
			t.Fatalf("accepted %+v: %v", spec, err)
		}
	}
	if _, err := d.SaveGatewayDraft("../gw", good, ""); !errors.Is(err, ErrGatewayDraftInvalid) {
		t.Fatal("invalid name accepted")
	}
	if err := d.UpsertGroup("group", &apitypes.InstanceGroupState{Engine: "kvrocks", Type: "standalone"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SaveGatewayDraft("gw", good, ""); !errors.Is(err, ErrGatewayDraftInvalid) {
		t.Fatal("kvrocks accepted")
	}
}
