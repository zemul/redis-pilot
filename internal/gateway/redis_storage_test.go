package gateway

import (
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestRedisStorageAOFRestart(t *testing.T) {
	binary := os.Getenv("GATEWAY_TEST_REDIS_SERVER")
	if binary == "" {
		t.Skip("real Redis required")
	}
	address := freeAddress(t)
	_, port, _ := net.SplitHostPort(address)
	dir := t.TempDir()
	args := []string{"--bind", "127.0.0.1", "--port", port, "--dir", dir, "--save", "", "--appendonly", "yes", "--appendfsync", "always", "--maxmemory-policy", "noeviction", "--loglevel", "warning"}
	var backend *exec.Cmd
	start := func() {
		backend = ownProcess(t, binary, args...)
		deadline := time.Now().Add(5 * time.Second)
		for {
			conn, r, err := authenticated(address, "")
			if err == nil {
				conn.SetDeadline(time.Now().Add(time.Second))
				writeAll(conn, encodeCommand("PING"))
				b, e := r.ReadFrame()
				conn.Close()
				if e == nil && string(b) == "+PONG\r\n" {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("Redis restart timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	start()
	c := shardConfigForTest(t)
	disabled := false
	c.Cache.Enabled, c.Cache.ColdEngine, c.RedisAddress, c.BackendPasswordFile = &disabled, "redis", "", ""
	c.Listen, c.AdminListen = freeAddress(t), freeAddress(t)
	c.FrontendPasswordFile = secretFile(t, "shard-config-front")
	c.Cache.Sharding.Nodes = []ShardNodeConfig{{ID: "a", Address: address}}
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InitializeShardState(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop := startConfiguredShardProcess(t, c)
	conn, r, err := authenticated(c.Listen, "shard-config-front")
	if err != nil {
		t.Fatal(err)
	}
	shardWant(t, conn, r, "+OK\r\n", "SET", "persist:s", "value")
	shardWant(t, conn, r, ":1\r\n", "HSET", "persist:h", "f", "value")
	shardWant(t, conn, r, ":1\r\n", "INCR", "persist:counter")
	conn.Close()
	stop()
	if err := backend.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	backend.Wait()
	start()
	stop = startConfiguredShardProcess(t, c)
	defer stop()
	conn, r, err = authenticated(c.Listen, "shard-config-front")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	shardWant(t, conn, r, "$5\r\nvalue\r\n", "GET", "persist:s")
	shardWant(t, conn, r, "$5\r\nvalue\r\n", "HGET", "persist:h", "f")
	shardWant(t, conn, r, "$1\r\n1\r\n", "GET", "persist:counter")
	shardWant(t, conn, r, ":2\r\n", "INCR", "persist:counter")
}

func TestRedisStoragePreflight(t *testing.T) {
	c := shardConfigForTest(t)
	disabled := false
	c.Cache.Enabled, c.Cache.ColdEngine, c.RedisAddress, c.BackendPasswordFile = &disabled, "redis", "", ""
	addr := isolatedTestRedis(t, true)
	c.Cache.Sharding.Nodes = []ShardNodeConfig{{ID: "a", Address: addr, PasswordFile: secretFile(t, "g4-backend-test")}}
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := s.cache.shardEndpoints()
	if err != nil {
		t.Fatal(err)
	}
	topology, err := c.Cache.Sharding.registry()
	if err != nil {
		t.Fatal(err)
	}
	shards, err := newCacheShards(s.cache, topology, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	check := func(want bool) {
		t.Helper()
		if err := shards.preflightCold(context.Background(), shards.node("a"), true); (err == nil) != want {
			t.Fatalf("preflight want %v: %v", want, err)
		}
	}
	check(true)
	for _, option := range []struct{ key, bad, good string }{
		{"appendfsync", "everysec", "always"},
		{"maxmemory-policy", "allkeys-lru", "noeviction"},
		{"no-appendfsync-on-rewrite", "yes", "no"},
	} {
		configuredBackendCall(t, c, addr, "CONFIG", "SET", option.key, option.bad)
		check(false)
		configuredBackendCall(t, c, addr, "CONFIG", "SET", option.key, option.good)
		check(true)
	}
	configuredBackendCall(t, c, addr, "SET", "foreign", "data")
	check(false)
	configuredBackendCall(t, c, addr, "DEL", "foreign")
	check(true)
	configuredBackendCall(t, c, addr, "CONFIG", "SET", "appendonly", "no")
	check(false)
}
