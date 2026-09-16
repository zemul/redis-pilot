package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
	"gopkg.in/yaml.v3"
)

func TestShardConfiguredProcessHelper(t *testing.T) {
	p := os.Getenv("PILOT_TEST_SHARD_CONFIG")
	if p == "" {
		t.Skip("subprocess helper")
	}
	c, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func startConfiguredShardProcess(t *testing.T, c Config) func() {
	t.Helper()
	b, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestShardConfiguredProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "PILOT_TEST_SHARD_CONFIG="+p)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { cmd.Process.Kill(); cmd.Wait() }) }
	t.Cleanup(stop)
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, reader, e := authenticated(c.Listen, "shard-config-front")
		if e == nil {
			conn.SetDeadline(time.Now().Add(time.Second))
			writeAll(conn, encodeCommand("PING"))
			b, e := reader.ReadFrame()
			conn.Close()
			if e == nil && string(b) == "+PONG\r\n" {
				return stop
			}
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("configured Gateway startup: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func configuredBackendCall(t *testing.T, c Config, address string, args ...string) []byte {
	t.Helper()
	conn, r, err := authenticated(address, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return cacheCall(t, &cacheConn{Conn: conn, reader: r, cfg: c}, args...)
}

func TestShardConfiguredRestartAndMigration(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			ctx := context.Background()
			a := migrationCold(t)
			b, stopB := migrationColdManaged(t)
			if stopB == nil {
				stopB = func() {
					conn, reader, err := authenticated(b, "g4-backend-test")
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					if err := writeAll(conn, encodeCommand("SHUTDOWN", "NOSAVE")); err != nil {
						t.Fatal(err)
					}
					if reply, err := reader.ReadFrame(); err == nil {
						t.Fatalf("shutdown did not close connection: %q", reply)
					}
				}
			}
			hot := g4Redis(t)
			c := shardConfigForTest(t)
			c.RedisAddress, c.Listen, c.AdminListen = hot, freeAddress(t), freeAddress(t)
			c.BackendPasswordFile = secretFile(t, "g4-backend-test")
			c.FrontendPasswordFile = secretFile(t, "shard-config-front")
			c.Cache.WritePolicy = policy
			c.Cache.Sharding.Nodes = []ShardNodeConfig{
				{ID: "a", Address: a, PasswordFile: secretFile(t, "g4-backend-test")},
				{ID: "b", Address: b, PasswordFile: secretFile(t, "g4-backend-test")},
			}
			configuredBackendCall(t, c, hot, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
			s, err := NewService(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.cache.start(ctx); err == nil {
				s.cache.close()
				t.Fatal("uninitialized startup accepted")
			}
			if err := s.InitializeShardState(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.InitializeShardState(ctx); err == nil {
				t.Fatal("second initialization overwrote state")
			}
			from, _ := c.Cache.Sharding.registry()
			stop := startConfiguredShardProcess(t, c)
			conn, r, err := authenticated(c.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			var keys []string
			for i, kind := range []string{"string", "hash", "list", "set", "zset"} {
				key := shardKeyFor(from, "b", "configured-"+kind)
				keys = append(keys, key)
				switch kind {
				case "string":
					shardWant(t, conn, r, "+OK\r\n", "SET", key, "old")
				case "hash":
					shardWant(t, conn, r, ":2\r\n", "HSET", key, "a", "old", "b", "keep")
				case "list":
					shardWant(t, conn, r, ":2\r\n", "RPUSH", key, "old", "keep")
				case "set":
					shardWant(t, conn, r, ":2\r\n", "SADD", key, "old", "keep")
				case "zset":
					shardWant(t, conn, r, ":2\r\n", "ZADD", key, "1", "old", "2", "keep")
				}
				if i == 1 {
					shardWant(t, conn, r, "$3\r\nold\r\n", "HGET", key, "a")
				}
			}
			conn.Close()
			stop() // SIGKILL stable multi-node service, no test-only runtime assembly
			if err := s.InitializeShardState(ctx); err == nil {
				t.Fatal("nonempty backends accepted for initialization")
			}
			// Register an already-created task through the durable internal seam.
			// The public Server task API is deliberately outside this change.
			state, err := openShardDisk(ctx, c, false)
			if err != nil {
				t.Fatal(err)
			}
			target := from.Config()
			target.Revision = 2
			target.Nodes = target.Nodes[:1]
			to, _ := sharding.New(target)
			path, _ := state.taskPath("shrink-1")
			j, err := sharding.CreateJournal(ctx, path, "shrink-1", from, to)
			if err != nil {
				t.Fatal(err)
			}
			j.Close()
			if err := state.bindTask(ctx, "shrink-1", to); err != nil {
				t.Fatal(err)
			}
			state.close()
			u := url.URL{Scheme: "file", Path: filepath.Join(path, "handoffs.db")}
			u.RawQuery = "mode=ro&_pragma=busy_timeout(1000)"
			db, err := sql.Open("sqlite", u.String())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			wait := func(published bool) {
				deadline := time.Now().Add(15 * time.Second)
				for {
					var phase string
					var count int
					err := db.QueryRow(`SELECT phase,(SELECT COUNT(*) FROM handoff WHERE state='cleaned') FROM finalization WHERE id=1`).Scan(&phase, &count)
					if err == nil && ((!published && phase == "active" && count > 0) || (published && phase == "published" && count > 0)) {
						return
					}
					if time.Now().After(deadline) {
						t.Fatalf("migration phase=%s count=%d err=%v", phase, count, err)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			c.Cache.Sharding.Migration.KeysPerSecond = 5
			stop = startConfiguredShardProcess(t, c)
			wait(false)
			stop() // active scan/handoff recovery
			stop = startConfiguredShardProcess(t, c)
			wait(true)
			stop() // published target, marker reclamation still unfinished
			c.Cache.Sharding.Migration.KeysPerSecond = 100000
			stop = startConfiguredShardProcess(t, c)
			deadline := time.Now().Add(10 * time.Second)
			for {
				var phase string
				if err := db.QueryRow(`SELECT phase FROM finalization WHERE id=1`).Scan(&phase); err != nil {
					t.Fatal(err)
				}
				if phase == "complete" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("configured worker did not complete")
				}
				time.Sleep(10 * time.Millisecond)
			}
			stop()
			// YAML still declares the original node registry: it must not put
			// migrated keys back on b. Then remove b entirely and stop its engine.
			stop = startConfiguredShardProcess(t, c)
			conn, r, err = authenticated(c.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "+OK\r\n", "SET", keys[0], "new")
			shardWant(t, conn, r, ":0\r\n", "HSET", keys[1], "a", "new")
			shardWant(t, conn, r, "$3\r\nnew\r\n", "HGET", keys[1], "a")
			shardWant(t, conn, r, "$4\r\nkeep\r\n", "HGET", keys[1], "b")
			conn.Close()
			stop()
			stopB()
			c.Cache.Sharding.Nodes = c.Cache.Sharding.Nodes[:1]
			stop = startConfiguredShardProcess(t, c)
			conn, r, err = authenticated(c.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "$3\r\nnew\r\n", "GET", keys[0])
			shardWant(t, conn, r, "$3\r\nnew\r\n", "HGET", keys[1], "a")
			shardWant(t, conn, r, "*2\r\n$3\r\nold\r\n$4\r\nkeep\r\n", "LRANGE", keys[2], "0", "-1")
			shardWant(t, conn, r, ":1\r\n", "SISMEMBER", keys[3], "keep")
			shardWant(t, conn, r, "$1\r\n2\r\n", "ZSCORE", keys[4], "keep")
			conn.Close()
			stop()
			if got := string(configuredBackendCall(t, c, a, "GET", keys[0])); got != "$3\r\nnew\r\n" {
				t.Fatal("new value not on final owner", got)
			}
			// Changed address / missing member must fail before claiming owners.
			before := string(configuredBackendCall(t, c, a, "GET", cachePrefix+"owner"))
			c.Cache.Sharding.Nodes[0].Address = freeAddress(t)
			bad, err := NewService(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := bad.cache.start(ctx); err == nil {
				bad.cache.close()
				t.Fatal("changed endpoint silently applied")
			}
			if got := string(configuredBackendCall(t, c, a, "GET", cachePrefix+"owner")); got != before {
				t.Fatal("invalid startup touched backend")
			}
		})
	}
}

func TestShardConfiguredRegistryDoesNotReshard(t *testing.T) {
	c := shardConfigForTest(t)
	ctx := context.Background()
	state, err := openShardDisk(ctx, c, true)
	if err != nil {
		t.Fatal(err)
	}
	state.close()
	// Extra endpoint is just a credential/address entry, not an active node.
	c.Cache.Sharding.Nodes = append(c.Cache.Sharding.Nodes, ShardNodeConfig{ID: "extra", Address: "127.0.0.1:17103"})
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.cache.restoreSharding(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.cache.close()
	if got := len(s.cache.shards.topology.Config().Nodes); got != 2 {
		t.Fatalf("config changed routing: %d", got)
	}
	if s.cache.shards.workerConfig == nil || !s.cache.shards.workerConfig.Finalize {
		t.Fatal("recovery worker missing")
	}
	// Configuration and manifest carry paths only, not secret file contents.
	b, _ := yaml.Marshal(c)
	if strings.Contains(string(b), "private-test-password") {
		t.Fatal(fmt.Sprint("secret in config"))
	}
}
