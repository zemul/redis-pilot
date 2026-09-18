package store

import (
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"testing"
)

func TestRedisClusterEngineBoundaries(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	disabled := false
	spec.Engine, spec.CacheEnabled, spec.HotInstance = "redis", &disabled, ""
	if _, err := d.CreateColdCluster(spec); err == nil {
		t.Fatal("accepted Kvrocks members for Redis cluster")
	}
	for _, name := range []string{"a", "b"} {
		if err := d.UpsertGroup(name, &apitypes.InstanceGroupState{Engine: "redis", Type: "standalone", CurrentMaster: name}); err != nil {
			t.Fatal(err)
		}
	}
	enabled := true
	spec.CacheEnabled = &enabled
	if _, err := d.CreateColdCluster(spec); err == nil {
		t.Fatal("accepted Redis with cache")
	}
	spec.CacheEnabled = &disabled
	c, err := d.CreateColdCluster(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.StorageEngine() != "redis" || c.UsesCache() {
		t.Fatal("engine or cache lost")
	}
	if _, err := d.CreateColdTask(c.ID, apitypes.ColdTaskRequest{ID: "wrong-engine", ExpectedRevision: 1, Shards: append(spec.Shards, apitypes.ColdShard{ID: "c", Instance: "c"})}); err == nil {
		t.Fatal("accepted Kvrocks migration target")
	}
	if _, err := d.CreateColdTask(c.ID, apitypes.ColdTaskRequest{ID: "shrink", ExpectedRevision: 1, Shards: spec.Shards[:1]}); err != nil {
		t.Fatal(err)
	}
}
