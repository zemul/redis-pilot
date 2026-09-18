package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/redisconf"
)

func TestDedicatedConfigSurvivesRewrite(t *testing.T) {
	a := newTestAgent(t)
	for _, profile := range []string{redisconf.RedisCacheProfile, redisconf.KvrocksStorageProfile, redisconf.RedisStorageProfile} {
		t.Run(profile, func(t *testing.T) {
			r := instanceConfigRequest{Name: profile, Profile: profile, Type: "standalone", Engine: "redis", Category: "cache", Memory: "256Mi", Port: 6379, EngineVersion: "7"}
			file := "redis.conf"
			if profile == redisconf.KvrocksStorageProfile {
				r.Engine, r.Category, file = "kvrocks", "persistent", "kvrocks.conf"
			}
			if profile == redisconf.RedisStorageProfile {
				r.Category = "persistent"
			}
			for round := 0; round < 2; round++ {
				if err := a.writeInstanceConfig(&r); err != nil {
					t.Fatal(err)
				}
				b, err := os.ReadFile(filepath.Join(a.cfg.DataDir, r.Name, "conf", file))
				if err != nil {
					t.Fatal(err)
				}
				want := []string{"# redis-pilot profile: " + profile}
				if profile == redisconf.RedisCacheProfile {
					want = append(want, `save ""`, "appendonly no", "maxmemory 201326592", "maxmemory-policy allkeys-lru", `rename-command FLUSHALL ""`)
					if strings.Contains(string(b), "rename-command FLUSHDB") {
						t.Fatal("cache reset disabled")
					}
				} else if profile == redisconf.RedisStorageProfile {
					want = append(want, "appendonly yes", "appendfsync always", "no-appendfsync-on-rewrite no", "maxmemory-policy noeviction", "maxmemory 201326592", `rename-command FLUSHDB ""`)
				} else {
					want = append(want, "max-db-size 0", "rocksdb.write_options.sync yes", "rocksdb.write_options.disable_wal no", "rocksdb.block_cache_size 64", "rocksdb.write_buffer_size 16")
				}
				for _, v := range want {
					if !strings.Contains(string(b), v) {
						t.Fatalf("missing %s", v)
					}
				}
			}
			// A fresh Agent instance still rejects a legacy request losing purpose.
			a2 := &Agent{cfg: a.cfg}
			r.Profile = ""
			if err := a2.writeInstanceConfig(&r); err == nil {
				t.Fatal("profile silently removed")
			}
		})
	}
}

func TestOrdinaryRedisStillDisablesFlush(t *testing.T) {
	a := newTestAgent(t)
	r := instanceConfigRequest{Name: "ordinary", Engine: "redis", Type: "standalone", Category: "cache", Memory: "256Mi", Port: 6379, EngineVersion: "7"}
	if err := a.writeInstanceConfig(&r); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(a.cfg.DataDir, r.Name, "conf", "redis.conf"))
	if !strings.Contains(string(b), `rename-command FLUSHDB ""`) {
		t.Fatal("ordinary instance enabled FLUSHDB")
	}
	r.Profile = redisconf.RedisCacheProfile
	if err := a.writeInstanceConfig(&r); err == nil {
		t.Fatal("converted existing ordinary database into disposable cache")
	}
}
