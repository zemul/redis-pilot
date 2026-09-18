package redisconf

import "testing"

func TestDedicatedProfiles(t *testing.T) {
	for _, p := range []struct{ profile, engine, category string }{{RedisCacheProfile, "redis", "cache"}, {KvrocksStorageProfile, "kvrocks", "persistent"}} {
		if err := ValidateProfile(p.profile, p.engine, p.category, "standalone", "", nil); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"save", "appendonly", "maxmemory", "maxmemory-policy", "max-db-size", "rocksdb.write_options.sync"} {
			if err := ValidateProfile(p.profile, p.engine, p.category, "standalone", "", map[string]string{key: "no"}); err == nil {
				t.Fatal("accepted override", key)
			}
		}
		if err := ValidateProfile(p.profile, p.engine, p.category, "replication", "", nil); err == nil {
			t.Fatal("accepted replicas")
		}
	}
	if err := ValidateProfile("other", "redis", "cache", "standalone", "", nil); err == nil {
		t.Fatal("unknown profile")
	}
	if err := ValidateProfile("", "redis", "cache", "standalone", "", map[string]string{"save": "900 1"}); err != nil {
		t.Fatal("ordinary config changed", err)
	}
}
