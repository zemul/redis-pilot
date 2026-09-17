package store

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestCustomClusterWithoutCache(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	disabled := false
	spec.CacheEnabled, spec.HotInstance = &disabled, ""
	c, err := d.CreateColdCluster(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.UsesCache() || c.HotAddress != "" || c.Engine != "kvrocks" {
		t.Fatalf("unexpected cluster %+v", c)
	}
	loaded, err := d.GetColdCluster(c.ID)
	if err != nil || loaded.UsesCache() {
		t.Fatalf("persisted switch: %+v %v", loaded, err)
	}
	var count int
	if err := d.sql.QueryRow(`SELECT count(*) FROM cold_resources WHERE instance='' OR instance='hot'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("reserved absent hot: %d %v", count, err)
	}
	task, err := d.CreateColdTask(c.ID, apitypes.ColdTaskRequest{ID: "shrink", ExpectedRevision: 1, Shards: spec.Shards[:1]})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ReportColdTask(c.GatewayID, c.ID, apitypes.ColdTaskReport{TaskID: task.ID, ToRevision: 2, Phase: "complete"}); err != nil {
		t.Fatal(err)
	}
	loaded, err = d.GetColdCluster(c.ID)
	if err != nil || loaded.UsesCache() || len(loaded.Shards) != 1 {
		t.Fatalf("migration changed mode: %+v %v", loaded, err)
	}
}

func TestCustomClusterRejectUnsupportedCombinations(t *testing.T) {
	for _, name := range []string{"redis", "disabled-with-hot", "enabled-without-hot"} {
		t.Run(name, func(t *testing.T) {
			d := openTestDB(t)
			spec := coldFixture(t, d)
			disabled := false
			switch name {
			case "redis":
				spec.Engine = "redis"
			case "disabled-with-hot":
				spec.CacheEnabled = &disabled
			case "enabled-without-hot":
				spec.HotInstance = ""
			}
			if _, err := d.CreateColdCluster(spec); err == nil {
				t.Fatal("unsupported combination accepted")
			}
		})
	}
}
