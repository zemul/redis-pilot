package redisconf

import "testing"

func TestDedicatedProfiles(t *testing.T) {
	for _, p := range []struct{ profile, engine, category string }{{RedisCacheProfile, "redis", "cache"}, {KvrocksStorageProfile, "kvrocks", "persistent"}, {RedisStorageProfile, "redis", "persistent"}} {
		if err := ValidateProfile(p.profile, p.engine, p.category, "standalone", "", nil); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"save", "appendonly", "maxmemory", "maxmemory-policy", "max-db-size", "rocksdb.write_options.sync", "appendfsync", "no-appendfsync-on-rewrite"} {
			if err := ValidateProfile(p.profile, p.engine, p.category, "standalone", "", map[string]string{key: "no"}); err == nil {
				t.Fatal("accepted override", key)
			}
		}
		for _, upstream := range []string{"", "10.0.0.1:6379"} {
			err := ValidateProfile(p.profile, p.engine, p.category, "replication", upstream, nil)
			if (err != nil) != (p.profile == RedisCacheProfile) {
				t.Fatal("incorrect replica profile validation", p.profile, err)
			}
		}
	}
	if err := ValidateProfile("other", "redis", "cache", "standalone", "", nil); err == nil {
		t.Fatal("unknown profile")
	}
	if err := ValidateProfile("", "redis", "cache", "standalone", "", map[string]string{"save": "900 1"}); err != nil {
		t.Fatal("ordinary config changed", err)
	}
}
