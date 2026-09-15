package gateway

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

type shardRuntimeFixture struct {
	m       *migrationFixture
	s       *Service
	address string
	hot     *cacheConn
}

func newShardRuntimeFixture(t *testing.T, policy string, oneNode bool, wrapB func(string) string) *shardRuntimeFixture {
	t.Helper()
	m := newMigrationFixture(t)
	// Each cold node deliberately has its own backend credential.
	cacheCall(t, m.b, "CONFIG", "SET", "requirepass", "shard-b-password")
	toConfig := m.to.Config()
	if wrapB != nil {
		toConfig.Nodes[1].Address = wrapB(toConfig.Nodes[1].Address)
		var err error
		m.to, err = sharding.New(toConfig)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.j.Close(); err != nil {
			t.Fatal(err)
		}
		m.dir = filepath.Join(t.TempDir(), "wrapped-journal")
		m.j, err = sharding.CreateJournal(context.Background(), m.dir, "move-1", m.from, m.to)
		if err != nil {
			t.Fatal(err)
		}
	}
	hotAddr := g4Redis(t)
	c, reader, err := authenticated(hotAddr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := passConfig(hotAddr)
	cfg.Mode = "cache"
	cfg.RequestTimeout = 3 * time.Second
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	cfg.FrontendPasswordFile = secretFile(t, "shard-front-password")
	private := t.TempDir()
	if err := os.Chmod(private, 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Cache = &CacheConfig{ColdAddress: m.from.Config().Nodes[0].Address, ColdPasswordFile: secretFile(t, "g4-backend-test"), ExclusiveBackends: true, LockFile: filepath.Join(private, "owner.lock"), WritePolicy: policy, ColdOnlyPrefixes: []string{"queue:"}, HotTimeout: 500 * time.Millisecond}
	hot := &cacheConn{Conn: c, reader: reader, cfg: cfg}
	t.Cleanup(func() { hot.Close() })
	cacheCall(t, hot, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	initial := m.to
	if oneNode {
		initial = m.from
	}
	s.cache.shards, err = newCacheShards(s.cache, initial, []cacheShardEndpoint{{m.to.Config().Nodes[0], "g4-backend-test"}, {m.to.Config().Nodes[1], "shard-b-password"}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		data.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, data, admin) }()
	t.Cleanup(func() {
		cancel()
		data.Close()
		admin.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("sharded gateway did not stop")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for !s.serving.Load() {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("sharded gateway startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("sharded gateway did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return &shardRuntimeFixture{m: m, s: s, address: data.Addr().String(), hot: hot}
}

func (f *shardRuntimeFixture) client(t *testing.T) (net.Conn, *Reader) {
	t.Helper()
	c, r, err := authenticated(f.address, "shard-front-password")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, r
}

func (f *shardRuntimeFixture) hotCall(t *testing.T, args ...string) []byte {
	t.Helper()
	// Hot recovery intentionally kills pre-start normal clients. Audit over a
	// fresh connection, never reuse the one that configured Redis before start.
	c, r, err := authenticated(f.s.cfg.RedisAddress, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return cacheCall(t, &cacheConn{Conn: c, reader: r, cfg: f.s.cfg}, args...)
}

func shardAPI(t *testing.T, c net.Conn, r *Reader, args ...string) []byte {
	t.Helper()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if err := writeAll(c, encodeCommand(args...)); err != nil {
		t.Fatal(err)
	}
	b, err := r.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func shardWant(t *testing.T, c net.Conn, r *Reader, want string, args ...string) {
	t.Helper()
	if b := shardAPI(t, c, r, args...); string(b) != want {
		t.Fatalf("%v: got %q want %q", args, b, want)
	}
}

func shardKeyFor(topology *sharding.Topology, id, prefix string) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("%s:%d", prefix, i)
		if topology.Owner([]byte(k)).ID == id {
			return k
		}
	}
}

func TestShardRuntimeTypesAndBatch(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, false, nil)
			c, r := f.client(t)
			a, b := shardKeyFor(f.m.to, "a", "batch"), shardKeyFor(f.m.to, "b", "batch")
			shardWant(t, c, r, "+OK\r\n", "MSET", a, "1", b, "2", a, "3")
			shardWant(t, c, r, "*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n2\r\n", "MGET", b, a, b)
			shardWant(t, c, r, ":3\r\n", "EXISTS", a, b, a)
			shardWant(t, c, r, "+OK\r\n", "MSET", "", "empty-key", "binary\x00\xff", "binary-value")
			shardWant(t, c, r, "*3\r\n"+string(cacheBulk("binary-value"))+string(cacheBulk("empty-key"))+string(cacheBulk("binary-value")), "MGET", "binary\x00\xff", "", "binary\x00\xff")
			if !bytes.HasPrefix(shardAPI(t, c, r, "MSETNX", a, "bad", b, "bad"), []byte("-CROSSNODE")) {
				t.Fatal("cross-node MSETNX accepted")
			}
			if !bytes.HasPrefix(shardAPI(t, c, r, "RENAME", a, b), []byte("-CROSSNODE")) {
				t.Fatal("cross-node RENAME accepted")
			}
			for _, id := range []string{"a", "b"} {
				for _, kind := range []string{"string", "hash", "list", "set", "zset"} {
					key := shardKeyFor(f.m.to, id, "type-"+kind)
					var seed, read, write []string
					var want string
					switch kind {
					case "string":
						seed = []string{"SET", key, "1"}
						read = []string{"GET", key}
						write = []string{"INCR", key}
						want = "$1\r\n2\r\n"
					case "hash":
						seed = []string{"HSET", key, "f", "old", "keep", "untouched"}
						read = []string{"HGET", key, "f"}
						write = []string{"HSET", key, "f", "new"}
						want = "$3\r\nnew\r\n"
					case "list":
						seed = []string{"RPUSH", key, "one", "two"}
						read = []string{"LINDEX", key, "0"}
						write = []string{"LPOP", key}
						want = "$3\r\ntwo\r\n"
					case "set":
						seed = []string{"SADD", key, "old"}
						read = []string{"SCARD", key}
						write = []string{"SADD", key, "new"}
						want = ":2\r\n"
					case "zset":
						seed = []string{"ZADD", key, "1", "f"}
						read = []string{"ZSCORE", key, "f"}
						write = []string{"ZADD", key, "2", "f"}
						want = "$1\r\n2\r\n"
					}
					for _, op := range [][]string{seed, read, write} {
						if out := shardAPI(t, c, r, op...); len(out) == 0 || out[0] == '-' {
							t.Fatalf("%s: %q", op, out)
						}
					}
					if policy == "update_existing" && string(f.hotCall(t, "TYPE", key)) != "+"+kind+"\r\n" {
						t.Fatalf("hot %s was discarded instead of updated", kind)
					}
					shardWant(t, c, r, want, read...)
					owner, other := f.m.a, f.m.b
					if id == "b" {
						owner, other = other, owner
					}
					if string(cacheCall(t, owner, "TYPE", key)) != "+"+kind+"\r\n" || string(cacheCall(t, other, "EXISTS", key)) != ":0\r\n" {
						t.Fatal("request used wrong cold node")
					}
				}
			}
			// Real frontend pipeline: sequential replies, including cross-node work.
			batch := append(encodeCommand("MSET", a, "4", b, "5"), encodeCommand("MGET", b, a)...)
			batch = append(batch, encodeCommand("UNLINK", a, b, a)...)
			if err := writeAll(c, batch); err != nil {
				t.Fatal(err)
			}
			expect(t, r, "+OK\r\n")
			expect(t, r, "*2\r\n$1\r\n5\r\n$1\r\n4\r\n")
			expect(t, r, ":2\r\n")
		})
	}
}

func TestShardRuntimeLuaAndTransactions(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, false, nil)
			c, r := f.client(t)
			x, y := "{same}:x", "{same}:y"
			shardWant(t, c, r, "+OK\r\n", "MSET", x, "old", y, "old")
			shardWant(t, c, r, ":0\r\n", "MSETNX", x, "no", y, "no")
			script := `redis.call('SET',KEYS[1],ARGV[1]);return {#KEYS,#ARGV,redis.call('GET',KEYS[1]),KEYS[2]}`
			want := "*4\r\n:2\r\n:1\r\n" + string(cacheBulk("binary\x00\xff")) + string(cacheBulk(y))
			shardWant(t, c, r, want, "EVAL", script, "2", x, y, "binary\x00\xff")
			sha, ok := frameString(shardAPI(t, c, r, "SCRIPT", "LOAD", script))
			if !ok {
				t.Fatal("SCRIPT LOAD failed")
			}
			shardWant(t, c, r, want, "EVALSHA", sha, "2", x, y, "binary\x00\xff")
			shardWant(t, c, r, ":7\r\n", "EVAL", "return 7", "0")
			shardWant(t, c, r, "+OK\r\n", "MULTI")
			shardWant(t, c, r, "+QUEUED\r\n", "SET", x, "tx")
			shardWant(t, c, r, "+QUEUED\r\n", "GET", x)
			shardWant(t, c, r, "*2\r\n+OK\r\n$2\r\ntx\r\n", "EXEC")
			shardWant(t, c, r, "+OK\r\n", "MULTI")
			shardWant(t, c, r, "+QUEUED\r\n", "EVAL", "return redis.call('SET',KEYS[1],ARGV[1])", "1", x, "lua-tx")
			shardWant(t, c, r, "*1\r\n+OK\r\n", "EXEC")
			shardWant(t, c, r, "+OK\r\n", "WATCH", x)
			shardWant(t, c, r, "+OK\r\n", "MULTI")
			shardWant(t, c, r, "+QUEUED\r\n", "GET", x)
			shardWant(t, c, r, "*1\r\n$6\r\nlua-tx\r\n", "EXEC")
			shardWant(t, c, r, "+OK\r\n", "WATCH", x)
			other, or := f.client(t)
			shardWant(t, other, or, "+OK\r\n", "SET", x, "other")
			shardWant(t, c, r, "+OK\r\n", "MULTI")
			shardWant(t, c, r, "+QUEUED\r\n", "SET", x, "wrong")
			shardWant(t, c, r, "*-1\r\n", "EXEC")
			a, b := shardKeyFor(f.m.to, "a", "cross"), shardKeyFor(f.m.to, "b", "cross")
			shardWant(t, c, r, "+OK\r\n", "MSET", a, "old", b, "old")
			if !bytes.HasPrefix(shardAPI(t, c, r, "EVAL", "return redis.call('MSET',KEYS[1],'bad',KEYS[2],'bad')", "2", a, b), []byte("-CROSSNODE")) {
				t.Fatal("cross-node Lua accepted")
			}
			shardWant(t, c, r, "+OK\r\n", "MULTI")
			shardWant(t, c, r, "+QUEUED\r\n", "MSET", a, "bad", b, "bad")
			if !bytes.HasPrefix(shardAPI(t, c, r, "EXEC"), []byte("-CROSSNODE")) {
				t.Fatal("cross-node transaction accepted")
			}
			shardWant(t, c, r, "*2\r\n$3\r\nold\r\n$3\r\nold\r\n", "MGET", a, b)
			for _, args := range [][]string{{"KEYS", "*"}, {"BLPOP", x, "1"}, {"EVAL", "return redis.call('HSCAN',KEYS[1],0)", "1", x}} {
				if b := shardAPI(t, c, r, args...); len(b) == 0 || b[0] != '-' {
					t.Fatalf("unguarded cursor/blocking path: %v %q", args, b)
				}
			}
		})
	}
}

func TestShardRuntimeMigrationKeepsHotHash(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, true, nil)
			c, r := f.client(t)
			key := shardKeyFor(f.m.to, "b", "big-hash")
			for start := 0; start < 5000; start += 100 {
				args := []string{"HSET", key}
				for i := start; i < start+100; i++ {
					args = append(args, strconv.Itoa(i), "value-"+strconv.Itoa(i))
				}
				shardWant(t, c, r, ":100\r\n", args...)
			}
			shardWant(t, c, r, "$7\r\nvalue-0\r\n", "HGET", key, "0")
			if string(f.hotCall(t, "HLEN", key)) != ":5000\r\n" {
				t.Fatal("hash was not fully cached")
			}
			// Capture a real legacy command with the old admission, but delay
			// sending it until after handoff. The server guard must reject it.
			ctx, release, err := f.s.cache.shards.admit(context.Background(), [][]byte{[]byte(key)}, 0)
			if err != nil {
				t.Fatal(err)
			}
			token, err := f.s.cache.ensureEpoch(ctx)
			if err != nil {
				release()
				t.Fatal(err)
			}
			op := cacheArgs("HSET", key, "0", "late")
			prepared, _ := f.s.cache.prepareCache(op)
			late, err := shardAdmissionFrom(ctx).wrap(f.s.cache.preparedCold(op, prepared, token))
			release()
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, c, r, "+OK\r\n", "WATCH", key)
			if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
				t.Fatal(err)
			}
			if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
				t.Fatal(err)
			}
			if string(f.hotCall(t, "HLEN", key)) != ":5000\r\n" {
				t.Fatal("migration discarded existing hot hash")
			}
			shardWant(t, c, r, "+OK\r\n", "MULTI")
			shardWant(t, c, r, "+QUEUED\r\n", "HSET", key, "0", "wrong")
			shardWant(t, c, r, "*-1\r\n", "EXEC")
			shardWant(t, c, r, ":0\r\n", "HSET", key, "0", "new")
			if policy == "update_existing" && string(f.hotCall(t, "HLEN", key)) != ":5000\r\n" {
				t.Fatal("hot hash lost after migrated update")
			}
			if policy == "invalidate" && string(f.hotCall(t, "EXISTS", key)) != ":0\r\n" {
				t.Fatal("invalidate policy did not clear cache")
			}
			out, err := f.m.a.call(context.Background(), late...)
			if err != nil || len(out) == 0 || out[0] != '-' {
				t.Fatalf("late source write accepted: %q %v", out, err)
			}
			shardWant(t, c, r, "*2\r\n$3\r\nnew\r\n$10\r\nvalue-4999\r\n", "HMGET", key, "0", "4999")
			if string(cacheCall(t, f.m.a, "EXISTS", key)) != ":0\r\n" || string(cacheCall(t, f.m.b, "HLEN", key)) != ":5000\r\n" {
				t.Fatal("wrong cold ownership")
			}
			fresh := shardKeyFor(f.m.to, "b", "new-during-migration")
			shardWant(t, c, r, ":1\r\n", "INCR", fresh)
			if string(cacheCall(t, f.m.a, "EXISTS", fresh)) != ":0\r\n" || string(cacheCall(t, f.m.b, "GET", fresh)) != "$1\r\n1\r\n" {
				t.Fatal("new key not handed to target")
			}
		})
	}
}

func TestShardRuntimePartialWriteFailure(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			var armed atomic.Bool
			f := newShardRuntimeFixture(t, policy, false, func(addr string) string {
				return cacheFaultRelayHooks(t, addr, nil, func(a [][]byte, b []byte) bool {
					if !armed.Load() || len(a) == 0 || string(a[0]) != "EVAL" {
						return false
					}
					for _, arg := range a {
						if string(arg) == "MSET" {
							return armed.CompareAndSwap(true, false)
						}
					}
					return false
				})
			})
			c, r := f.client(t)
			a, b := shardKeyFor(f.m.to, "a", "partial"), shardKeyFor(f.m.to, "b", "partial")
			shardWant(t, c, r, "+OK\r\n", "MSET", a, "old", b, "old")
			shardAPI(t, c, r, "MGET", a, b)
			armed.Store(true)
			out := shardAPI(t, c, r, "MSET", a, "new", b, "new")
			if !strings.Contains(string(out), "earlier groups may have succeeded") || armed.Load() {
				t.Fatalf("did not report partial/unknown write: %q", out)
			}
			shardWant(t, c, r, "*2\r\n$3\r\nnew\r\n$3\r\nnew\r\n", "MGET", a, b)
		})
	}
}

func TestShardRuntimeConcurrentMigration(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, true, nil)
			c, r := f.client(t)
			key := shardKeyFor(f.m.to, "b", "concurrent")
			seed := []string{"HSET", key, "n", "0"}
			for i := 0; i < 500; i++ {
				seed = append(seed, fmt.Sprint(i), "unchanged")
			}
			shardWant(t, c, r, ":501\r\n", seed...)
			shardWant(t, c, r, "$1\r\n0\r\n", "HGET", key, "n")
			writer, wr := f.client(t)
			started := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				call := func(args ...string) ([]byte, error) {
					writer.SetDeadline(time.Now().Add(10 * time.Second))
					if err := writeAll(writer, encodeCommand(args...)); err != nil {
						return nil, err
					}
					return wr.ReadFrame()
				}
				for i := 1; i <= 40; i++ {
					b, err := call("HINCRBY", key, "n", "1")
					if i == 1 {
						close(started)
					}
					if err != nil || string(b) != fmt.Sprintf(":%d\r\n", i) {
						done <- fmt.Errorf("increment %d: %q %v", i, b, err)
						return
					}
					b, err = call("HGET", key, "n")
					if err != nil || string(b) != string(cacheBulk(strconv.Itoa(i))) {
						done <- fmt.Errorf("stale read after write %d: %q %v", i, b, err)
						return
					}
				}
				done <- nil
			}()
			<-started
			if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
				t.Fatal(err)
			}
			if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			shardWant(t, c, r, "$2\r\n40\r\n", "HGET", key, "n")
			if string(cacheCall(t, f.m.b, "HLEN", key)) != ":501\r\n" || string(cacheCall(t, f.m.a, "EXISTS", key)) != ":0\r\n" {
				t.Fatal("incomplete/wrong migration result")
			}
		})
	}
}

func TestShardAdmissionLifetimeAndBounds(t *testing.T) {
	r := &cacheRuntime{guard: cachePrefix + "owner"}
	a := &shardAdmission{runtime: r, keys: []shardKeyGuard{{key: "business"}}}
	a.live.Store(true)
	for _, args := range [][]string{{"GET", "outside"}, {"WATCH", "outside"}, {"SET", "business", "bad"}, {"EVAL", "return 1", "1", "outside"}} {
		if _, err := a.wrap(args); err == nil {
			t.Fatalf("escaped admission: %v", args)
		}
	}
	a.live.Store(false)
	if _, err := a.wrap([]string{"EVAL", "return 1", "1", "business"}); err == nil {
		t.Fatal("expired admission wrapped a command")
	}
	if _, err := a.reenter(r, [][]byte{[]byte("business")}); err == nil {
		t.Fatal("expired admission reacquired keys")
	}
}
