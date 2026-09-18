package store

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestInstanceProfilePersistence(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertGroup("g", &apitypes.InstanceGroupState{Engine: "redis", Type: "standalone", Category: "cache"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertInstance("i", &apitypes.Instance{Group: "g", Profile: "redis-cache", Role: "master"}); err != nil {
		t.Fatal(err)
	}
	if err := d.WithInstances(func(s *apitypes.InstancesState) error { s.Instances["i"].ObservedStatus = "running"; return nil }); err != nil {
		t.Fatal(err)
	}
	d.Close()
	d, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s, err := d.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	if s.Instances["i"].Profile != "redis-cache" || s.Instances["i"].ToView().Profile != "redis-cache" {
		t.Fatal("lost profile")
	}
	if err := d.WithInstances(func(s *apitypes.InstancesState) error { s.Instances["i"].Profile = ""; return nil }); err == nil {
		t.Fatal("profile changed")
	}
	if err := d.UpsertGroup("g", &apitypes.InstanceGroupState{Engine: "redis", Type: "replication", Category: "cache"}); err == nil {
		t.Fatal("profile changed topology")
	}
	if err := d.UpsertInstance("replica", &apitypes.Instance{Group: "g", Role: "replica"}); err == nil {
		t.Fatal("joined dedicated group")
	}
	if err := d.UpsertGroup("ordinary", &apitypes.InstanceGroupState{Engine: "redis", Type: "standalone", Category: "cache"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertInstance("other", &apitypes.Instance{Group: "ordinary", Role: "master"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertInstance("other", &apitypes.Instance{Group: "g", Role: "master"}); err == nil {
		t.Fatal("existing member joined dedicated group")
	}
	if err := d.UpsertInstance("i", &apitypes.Instance{Group: "ordinary", Profile: "redis-cache", Role: "master"}); err == nil {
		t.Fatal("dedicated instance moved groups")
	}
}
