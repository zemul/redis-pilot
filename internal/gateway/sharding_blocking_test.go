package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

type shardBlockResult struct {
	body []byte
	err  error
}

func shardPopAsync(t *testing.T, f *shardRuntimeFixture, args ...string) <-chan shardBlockResult {
	t.Helper()
	c, reader := f.client(t)
	c.SetDeadline(time.Now().Add(12 * time.Second))
	if err := writeAll(c, encodeCommand(args...)); err != nil {
		t.Fatal(err)
	}
	done := make(chan shardBlockResult, 1)
	go func() { b, err := reader.ReadFrame(); done <- shardBlockResult{b, err} }()
	return done
}
func shardPopResult(t *testing.T, done <-chan shardBlockResult) []byte {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.body
	case <-time.After(10 * time.Second):
		t.Fatal("blocking request did not finish")
	}
	return nil
}

func shardWantDrainedPop(t *testing.T, done <-chan shardBlockResult, started time.Time, timeout time.Duration) {
	t.Helper()
	b := shardPopResult(t, done)
	if bytes.Contains(b, []byte("not retried")) {
		return
	}
	// Kvrocks may leave an asynchronously killed blocking connection alive
	// until its native timeout fires. That is a real empty result, not a
	// transport interruption disguised as one. Early empty replies are errors.
	if string(b) == "*-1\r\n" && time.Since(started) >= timeout-50*time.Millisecond {
		return
	}
	t.Fatalf("unexpected drained pop after %s: %q", time.Since(started), b)
}

func shardAssertBlockingDrained(t *testing.T, cold *cacheConn) {
	t.Helper()
	list, _ := frameString(cacheCall(t, cold, "CLIENT", "LIST"))
	if strings.Contains(list, "name=pilot-block-") {
		t.Fatalf("migration returned with a live registered connection: %s", list)
	}
	if b := cacheCall(t, cold, "HLEN", shardBlockingRegistry); string(b) != ":0\r\n" {
		t.Fatalf("migration returned before registration cleanup: %q", b)
	}
}

func shardWaitBlocked(t *testing.T, cold *cacheConn) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var observed string
	for {
		b := cacheCall(t, cold, "CLIENT", "LIST")
		v, _ := frameString(b)
		for _, line := range strings.Split(v, "\n") {
			if strings.Contains(line, "name=pilot-block-") {
				observed = line
			}
			if strings.Contains(line, "name=pilot-block-") && (strings.Contains(line, "cmd=blpop") || strings.Contains(line, "cmd=brpop")) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("native blocking request not observed; named connection: %q; latest list: %q", observed, v)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestShardBlockingNativeAndProducer(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, false, nil)
			c, r := f.client(t)
			key := shardKeyFor(f.m.to, "b", "queue:\x00\xff")
			done := shardPopAsync(t, f, "BLPOP", key, "5")
			shardWaitBlocked(t, f.m.b)
			// A blocking consumer must not hold the producer's key lock.
			shardWant(t, c, r, ":1\r\n", "RPUSH", key, "value\x00\xff")
			want := cacheArray([][]byte{cacheBulk(key), cacheBulk("value\x00\xff")})
			if b := shardPopResult(t, done); !bytes.Equal(b, want) {
				t.Fatalf("pop %q", b)
			}
			shardWant(t, c, r, "*-1\r\n", "BRPOP", key, "0.05")
			shardWant(t, c, r, ":2\r\n", "RPUSH", key, "left", "right")
			shardWant(t, c, r, string(cacheArray([][]byte{cacheBulk(key), cacheBulk("right")})), "BRPOP", key, "1")
			first, second := "queue:{order}:first", "queue:{order}:second"
			shardWant(t, c, r, ":1\r\n", "RPUSH", first, "one")
			shardWant(t, c, r, ":1\r\n", "RPUSH", second, "two")
			shardWant(t, c, r, string(cacheArray([][]byte{cacheBulk(first), cacheBulk("one")})), "BLPOP", first, second, first, "1")
			shardWant(t, c, r, ":0\r\n", "EXISTS", "missing")
			if string(f.hotCall(t, "EXISTS", key)) != ":0\r\n" {
				t.Fatal("blocking queue entered cache")
			}
			if b := cacheCall(t, f.m.b, "HLEN", shardBlockingRegistry); string(b) != ":0\r\n" {
				t.Fatalf("finished registration retained: %q", b)
			}
			x := shardKeyFor(f.m.to, "a", "queue:cross")
			if b := shardAPI(t, c, r, "BLPOP", x, key, "1"); !bytes.HasPrefix(b, []byte("-CROSSNODE")) {
				t.Fatalf("cross-node blocking accepted: %q", b)
			}
			for _, a := range [][]string{{"BLPOP", "plain", "1"}, {"BLPOP", key, "0"}, {"BLPOP", key, "NaN"}, {"BLPOP", key, "+Inf"}} {
				if b := shardAPI(t, c, r, a...); len(b) == 0 || b[0] != '-' {
					t.Fatalf("invalid blocking accepted: %q", b)
				}
			}
		})
	}
}

func TestShardBlockingMigrationDrains(t *testing.T) {
	f := newShardRuntimeFixture(t, "update_existing", true, nil)
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "queue:moving")
	started := time.Now()
	done := shardPopAsync(t, f, "BLPOP", key, "5")
	shardWaitBlocked(t, f.m.a)
	if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
		t.Fatal(err)
	}
	if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
		t.Fatal(err)
	}
	shardAssertBlockingDrained(t, f.m.a)
	shardWantDrainedPop(t, done, started, 5*time.Second)
	shardWant(t, c, r, ":1\r\n", "LPUSH", key, "new")
	shardWant(t, c, r, string(cacheArray([][]byte{cacheBulk(key), cacheBulk("new")})), "BLPOP", key, "1")
	if string(cacheCall(t, f.m.a, "EXISTS", key)) != ":0\r\n" {
		t.Fatal("late source pop recreated key")
	}
}

func shardStartShrink(t *testing.T, f *shardRuntimeFixture) {
	t.Helper()
	to, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 3, Nodes: f.m.to.Config().Nodes[:1]})
	if err != nil {
		t.Fatal(err)
	}
	j, err := sharding.CreateJournal(context.Background(), filepath.Join(t.TempDir(), "shrink"), "shrink-block", f.m.to, to)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	if err := f.s.cache.shards.beginTransition(to, j); err != nil {
		t.Fatal(err)
	}
}

func TestShardBlockingLateSend(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string {
		return cacheFaultRelayHooks(t, address, func(a [][]byte) bool {
			if len(a) > 0 && string(a[0]) == "BLPOP" {
				once.Do(func() { close(entered); <-resume })
			}
			return false
		}, nil)
	})
	var resumed sync.Once
	unpause := func() { resumed.Do(func() { close(resume) }) }
	t.Cleanup(unpause)
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "queue:late")
	shardWant(t, c, r, ":1\r\n", "RPUSH", key, "keep")
	done := shardPopAsync(t, f, "BLPOP", key, "5")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native send not intercepted")
	}
	shardStartShrink(t, f)
	if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
		t.Fatal(err)
	}
	unpause()
	if b := shardPopResult(t, done); !bytes.Contains(b, []byte("not retried")) {
		t.Fatalf("late pop result %q", b)
	}
	shardWant(t, c, r, "*1\r\n$4\r\nkeep\r\n", "LRANGE", key, "0", "-1")
}

// A successful KILL count is deliberately forged while the actual connection
// stays alive, modelling an engine that has only scheduled asynchronous close.
func shardKillAckRelay(t *testing.T, target string, fake *atomic.Bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				up, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer up.Close()
				stop := context.AfterFunc(ctx, func() { client.Close(); up.Close() })
				defer stop()
				r, _ := NewReader(client, DefaultConfig().Limits)
				replies, _ := NewReader(up, DefaultConfig().Limits)
				for {
					raw, a, err := r.ReadCommand()
					if err != nil {
						return
					}
					if len(a) > 1 && string(a[0]) == "CLIENT" && string(a[1]) == "KILL" && fake.Load() {
						if writeAll(client, []byte(":1\r\n")) != nil {
							return
						}
						continue
					}
					if writeAll(up, raw) != nil {
						return
					}
					b, err := replies.ReadFrame()
					if err != nil || writeAll(client, b) != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); cancel(); wg.Wait() })
	return ln.Addr().String()
}

func TestShardBlockingKillAckIsNotDrain(t *testing.T) {
	var fake atomic.Bool
	f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string { return shardKillAckRelay(t, address, &fake) })
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "queue:ack")
	started := time.Now()
	done := shardPopAsync(t, f, "BLPOP", key, "5")
	shardWaitBlocked(t, f.m.b)
	shardStartShrink(t, f)
	fake.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err := f.s.cache.shards.migrate(ctx, []byte(key))
	cancel()
	if err == nil {
		t.Fatal("accepted KILL acknowledgement without disappearance")
	}
	if b := cacheCall(t, f.m.b, "HLEN", shardBlockingRegistry); string(b) != ":1\r\n" {
		t.Fatalf("unconfirmed registration removed: %q", b)
	}
	// An unrelated producer/read remains available while this key cannot move.
	other := shardKeyFor(f.m.to, "a", "unrelated")
	shardWant(t, c, r, "+OK\r\n", "SET", other, "works")
	shardWant(t, c, r, "$5\r\nworks\r\n", "GET", other)
	fake.Store(false)
	if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
		t.Fatal(err)
	}
	shardAssertBlockingDrained(t, f.m.b)
	shardWantDrainedPop(t, done, started, 5*time.Second)
}

func TestShardBlockingRecordValidation(t *testing.T) {
	field := rand.Text()
	r := shardBlockRecord{Version: 1, ClientID: "42", RunID: "run", Name: "pilot-block-" + field, Keys: [][]byte{[]byte("queue:\x00\xff")}}
	b, _ := json.Marshal(r)
	decoded, err := decodeShardBlock(field, string(b))
	if err != nil || !bytes.Equal(decoded.Keys[0], r.Keys[0]) {
		t.Fatalf("binary record roundtrip: %v", err)
	}
	r.Version, r.RunID = 2, ""
	b, _ = json.Marshal(r)
	if _, err := decodeShardBlock(field, string(b)); err != nil || bytes.Contains(b, []byte("run_id")) {
		t.Fatalf("v2 record requires no process identity: %s, %v", b, err)
	}
	for _, invalid := range []shardBlockRecord{
		{Version: 1, ClientID: r.ClientID, Name: r.Name, Keys: r.Keys},
		{Version: 2, ClientID: r.ClientID, RunID: "unexpected", Name: r.Name, Keys: r.Keys},
		{Version: 3, ClientID: r.ClientID, Name: r.Name, Keys: r.Keys},
	} {
		b, _ := json.Marshal(invalid)
		if _, err := decodeShardBlock(field, string(b)); err == nil {
			t.Fatalf("invalid record version accepted: %s", b)
		}
	}
	for _, bad := range []string{"{}", "garbage", strings.Repeat("x", 100*1024+1)} {
		if _, err := decodeShardBlock(field, bad); err == nil {
			t.Fatal("invalid record accepted")
		}
	}
	for _, bad := range [][]byte{[]byte("-ERR unavailable\r\n"), cacheBulk("flags=b name=test\n"), cacheBulk("id=0 name=test\n")} {
		if _, err := shardNamedClients(bad, "test"); err == nil {
			t.Fatal("malformed client list treated as absence")
		}
	}
}

func TestShardBlockingWithoutProcessInfo(t *testing.T) {
	var infoCalls atomic.Int32
	f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string {
		return cacheFaultRelayHooks(t, address, func(a [][]byte) bool {
			if len(a) == 2 && string(a[0]) == "INFO" && strings.EqualFold(string(a[1]), "server") {
				infoCalls.Add(1)
				return true
			}
			return false
		}, func(a [][]byte, b []byte) bool {
			// Connection setup still needs INFO all for backend validation, but
			// a Redis-backed test must not accidentally supply a usable run_id.
			if len(a) > 0 && string(a[0]) == "INFO" {
				copy(b, bytes.ReplaceAll(b, []byte("run_id:"), []byte("unused:")))
			}
			return false
		})
	})
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "queue:no-info")
	shardWant(t, c, r, ":1\r\n", "RPUSH", key, "value")
	shardWant(t, c, r, string(cacheArray([][]byte{cacheBulk(key), cacheBulk("value")})), "BLPOP", key, "1")
	started := time.Now()
	done := shardPopAsync(t, f, "BRPOP", key, "5")
	shardWaitBlocked(t, f.m.b)
	shardStartShrink(t, f)
	if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
		t.Fatal(err)
	}
	shardAssertBlockingDrained(t, f.m.b)
	shardWantDrainedPop(t, done, started, 5*time.Second)
	if infoCalls.Load() != 0 {
		t.Fatalf("blocking path still requested INFO: %d", infoCalls.Load())
	}
}

// Keep a manually registered connection locally active so orphan maintenance
// cannot race the explicit retirement assertions below.
func shardTestBlockRecord(t *testing.T, f *shardRuntimeFixture) (string, shardBlockRecord, *cacheConn) {
	t.Helper()
	c, err := f.s.cache.shards.raw(context.Background(), f.s.cache.shards.nodes["b"])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	field := rand.Text()
	r := shardBlockRecord{Version: 2, Name: "pilot-block-" + field, Keys: [][]byte{[]byte("queue:identity")}}
	cacheCall(t, c, "CLIENT", "SETNAME", r.Name)
	r.ClientID, _ = frameString(cacheCall(t, c, "CLIENT", "ID"))
	f.s.cache.shards.blocking.set("b", field, true)
	return field, r, c
}

func TestShardBlockingRetireIdentity(t *testing.T) {
	for _, scenario := range []string{"legacy-different-run", "reused-id-other-name", "same-name-other-id", "duplicate-name"} {
		t.Run(scenario, func(t *testing.T) {
			f := newShardRuntimeFixture(t, "invalidate", false, nil)
			field, record, live := shardTestBlockRecord(t, f)
			switch scenario {
			case "legacy-different-run":
				record.Version, record.RunID = 1, "different-old-process"
			case "reused-id-other-name":
				cacheCall(t, live, "CLIENT", "SETNAME", "unrelated-connection")
			case "same-name-other-id":
				record.ClientID = "18446744073709551615"
			case "duplicate-name":
				_, _, other := shardTestBlockRecord(t, f)
				cacheCall(t, other, "CLIENT", "SETNAME", record.Name)
			}
			payload, _ := json.Marshal(record)
			cacheCall(t, f.m.b, "HSET", shardBlockingRegistry, field, string(payload))
			err := f.s.cache.shards.retireBlocking(context.Background(), f.s.cache.shards.nodes["b"], field, string(payload), record)
			mismatch := scenario == "same-name-other-id" || scenario == "duplicate-name"
			if (err != nil) != mismatch {
				t.Fatalf("retirement result: %v", err)
			}
			wantRegistry := ":0\r\n"
			if mismatch {
				wantRegistry = ":1\r\n"
			}
			if b := cacheCall(t, f.m.b, "HEXISTS", shardBlockingRegistry, field); string(b) != wantRegistry {
				t.Fatalf("retirement registry: %q", b)
			}
			if scenario == "legacy-different-run" {
				ids, err := shardNamedClients(cacheCall(t, f.m.b, "CLIENT", "LIST"), record.Name)
				if err != nil || len(ids) != 0 {
					t.Fatalf("legacy run ID bypassed live connection: %v, %v", ids, err)
				}
			} else if b := cacheCall(t, live, "PING"); string(b) != "+PONG\r\n" {
				t.Fatalf("unconfirmed/unrelated connection killed: %q", b)
			}
		})
	}
}

func TestShardBlockingRetireDisconnect(t *testing.T) {
	for _, stage := range []string{"before-kill", "after-kill", "confirmation-list"} {
		t.Run(stage, func(t *testing.T) {
			var armed, killed, dropped atomic.Bool
			f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string {
				return cacheFaultRelayHooks(t, address, func(a [][]byte) bool {
					return armed.Load() && stage == "before-kill" && len(a) > 1 && string(a[0]) == "CLIENT" && string(a[1]) == "KILL" && dropped.CompareAndSwap(false, true)
				}, func(a [][]byte, b []byte) bool {
					if !armed.Load() || len(a) < 2 || string(a[0]) != "CLIENT" {
						return false
					}
					if string(a[1]) == "KILL" {
						killed.Store(true)
						return stage == "after-kill" && dropped.CompareAndSwap(false, true)
					}
					return stage == "confirmation-list" && string(a[1]) == "LIST" && killed.Load() && dropped.CompareAndSwap(false, true)
				})
			})
			field, record, _ := shardTestBlockRecord(t, f)
			payload, _ := json.Marshal(record)
			cacheCall(t, f.m.b, "HSET", shardBlockingRegistry, field, string(payload))
			armed.Store(true)
			err := f.s.cache.shards.retireBlocking(context.Background(), f.s.cache.shards.nodes["b"], field, string(payload), record)
			if err == nil || !dropped.Load() {
				t.Fatalf("control disconnect was not rejected: %v, fault=%v", err, dropped.Load())
			}
			if b := cacheCall(t, f.m.b, "HEXISTS", shardBlockingRegistry, field); string(b) != ":1\r\n" {
				t.Fatalf("unconfirmed record removed: %q", b)
			}
			armed.Store(false)
			if err := f.s.cache.shards.retireBlocking(context.Background(), f.s.cache.shards.nodes["b"], field, string(payload), record); err != nil {
				t.Fatal(err)
			}
			if b := cacheCall(t, f.m.b, "HEXISTS", shardBlockingRegistry, field); string(b) != ":0\r\n" {
				t.Fatalf("record not recovered: %q", b)
			}
		})
	}
}

type shardBlockProcessConfig struct {
	Gateway            Config
	Topology           sharding.Config
	Target             *sharding.Config
	Worker             *migrationWorkerOptions
	JournalDir, TaskID string
}

func TestShardBlockingProcessHelper(t *testing.T) {
	raw := os.Getenv("PILOT_TEST_BLOCKING_PROCESS")
	if raw == "" {
		t.Skip("subprocess helper")
	}
	var cfg shardBlockProcessConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(cfg.Gateway)
	if err != nil {
		t.Fatal(err)
	}
	topology, err := sharding.New(cfg.Topology)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints []cacheShardEndpoint
	nodes := append([]sharding.Node(nil), cfg.Topology.Nodes...)
	if cfg.Target != nil {
		for _, target := range cfg.Target.Nodes {
			retained := false
			for _, source := range cfg.Topology.Nodes {
				retained = retained || target.ID == source.ID
			}
			if !retained {
				nodes = append(nodes, target)
			}
		}
	}
	for _, n := range nodes {
		endpoints = append(endpoints, cacheShardEndpoint{n, "g4-backend-test"})
	}
	s.cache.shards, err = newCacheShards(s.cache, topology, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	s.cache.shards.workerConfig = cfg.Worker
	if cfg.Target != nil {
		to, err := sharding.New(*cfg.Target)
		if err != nil {
			t.Fatal(err)
		}
		j, err := sharding.OpenJournal(context.Background(), cfg.JournalDir, cfg.TaskID, topology, to)
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()
		if err := s.cache.shards.beginTransition(to, j); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestShardBlockingProcessRestart(t *testing.T) {
	m := newMigrationFixture(t)
	hotAddress := g4Redis(t)
	hotConn, hotReader, err := authenticated(hotAddress, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := passConfig(hotAddress)
	cfg.Mode = "cache"
	cfg.Listen, cfg.AdminListen = freeAddress(t), freeAddress(t)
	cfg.RequestTimeout = 2 * time.Second
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	cfg.FrontendPasswordFile = secretFile(t, "shard-front-password")
	private := t.TempDir()
	if err := os.Chmod(private, 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Cache = &CacheConfig{ColdAddress: m.from.Config().Nodes[0].Address, ColdPasswordFile: secretFile(t, "g4-backend-test"), ExclusiveBackends: true, LockFile: filepath.Join(private, "owner.lock"), WritePolicy: "invalidate", ColdOnlyPrefixes: []string{"queue:"}}
	cacheCall(t, &cacheConn{Conn: hotConn, reader: hotReader, cfg: cfg}, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
	hotConn.Close()
	payload, err := json.Marshal(shardBlockProcessConfig{Gateway: cfg, Topology: m.from.Config()})
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	start := func() func() {
		p := exec.Command(executable, "-test.run=^TestShardBlockingProcessHelper$", "-test.count=1")
		p.Env = append(os.Environ(), "PILOT_TEST_BLOCKING_PROCESS="+string(payload))
		var output bytes.Buffer
		p.Stdout, p.Stderr = &output, &output
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		kill := func() { once.Do(func() { p.Process.Kill(); p.Wait() }) }
		t.Cleanup(kill)
		deadline := time.Now().Add(8 * time.Second)
		for {
			c, r, err := authenticated(cfg.Listen, "shard-front-password")
			if err == nil {
				c.SetDeadline(time.Now().Add(time.Second))
				err = writeAll(c, encodeCommand("PING"))
				b, e := r.ReadFrame()
				c.Close()
				if err == nil && e == nil && string(b) == "+PONG\r\n" {
					return kill
				}
			}
			if time.Now().After(deadline) {
				kill()
				t.Fatalf("child gateway failed to start: %s", output.String())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	first := start()
	client, _, err := authenticated(cfg.Listen, "shard-front-password")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := writeAll(client, encodeCommand("BLPOP", "queue:restart", "30")); err != nil {
		t.Fatal(err)
	}
	shardWaitBlocked(t, m.a)
	first() // SIGKILL: no Gateway cleanup/defer is allowed to run.
	if b := cacheCall(t, m.a, "HLEN", shardBlockingRegistry); string(b) != ":1\r\n" {
		t.Fatalf("crash registration was not preserved: %q", b)
	}
	second := start()
	defer second()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b := cacheCall(t, m.a, "HLEN", shardBlockingRegistry)
		if string(b) == ":0\r\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("orphan registration did not recover: %q", b)
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, r, err := authenticated(cfg.Listen, "shard-front-password")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	shardWant(t, c, r, ":1\r\n", "RPUSH", "queue:restart", "after")
	shardWant(t, c, r, "*2\r\n$13\r\nqueue:restart\r\n$5\r\nafter\r\n", "BLPOP", "queue:restart", "1")
}

func TestShardBlockingRegistrationReplyLost(t *testing.T) {
	var armed, dropped atomic.Bool
	f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string {
		return cacheFaultRelayHooks(t, address, nil, func(a [][]byte, b []byte) bool {
			return armed.Load() && len(a) > 1 && string(a[0]) == "EVAL" && strings.Contains(string(a[1]), "blocking registry full") && dropped.CompareAndSwap(false, true)
		})
	})
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "queue:registration")
	shardWant(t, c, r, ":1\r\n", "RPUSH", key, "keep")
	armed.Store(true)
	if b := shardAPI(t, c, r, "BLPOP", key, "1"); !bytes.Contains(b, []byte("pop not sent")) {
		t.Fatalf("unconfirmed registration dispatched: %q", b)
	}
	if !dropped.Load() {
		t.Fatal("registration reply fault not injected")
	}
	shardWant(t, c, r, "*1\r\n$4\r\nkeep\r\n", "LRANGE", key, "0", "-1")
}

func TestShardBlockingRegistryCapacity(t *testing.T) {
	f := newShardRuntimeFixture(t, "invalidate", false, nil)
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "queue:full")
	// Active placeholders prevent orphan maintenance from removing the setup.
	args := []string{"HSET", shardBlockingRegistry}
	for i := 0; i < shardBlockingCapacity; i++ {
		field := rand.Text()
		v := shardBlockRecord{Version: 1, ClientID: strconv.Itoa(i + 1), RunID: "old-process", Name: "pilot-block-" + field, Keys: [][]byte{[]byte("queue:other")}}
		payload, _ := json.Marshal(v)
		f.s.cache.shards.blocking.set("b", field, true)
		args = append(args, field, string(payload))
	}
	cacheCall(t, f.m.b, args...)
	shardWant(t, c, r, ":1\r\n", "RPUSH", key, "keep")
	if b := shardAPI(t, c, r, "BLPOP", key, "1"); !bytes.Contains(b, []byte("pop not sent")) {
		t.Fatalf("full registry accepted pop: %q", b)
	}
	shardWant(t, c, r, "*1\r\n$4\r\nkeep\r\n", "LRANGE", key, "0", "-1")
}

func TestShardBlockingPopReplyLost(t *testing.T) {
	var dropped atomic.Bool
	f := newShardRuntimeFixture(t, "update_existing", false, func(address string) string {
		return cacheFaultRelayHooks(t, address, nil, func(a [][]byte, b []byte) bool {
			return len(a) > 0 && string(a[0]) == "BLPOP" && dropped.CompareAndSwap(false, true)
		})
	})
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "queue:lost-pop")
	shardWant(t, c, r, ":2\r\n", "RPUSH", key, "first", "second")
	if b := shardAPI(t, c, r, "BLPOP", key, "1"); !bytes.Contains(b, []byte("not retried")) {
		t.Fatalf("lost pop reply became success: %q", b)
	}
	if !dropped.Load() {
		t.Fatal("pop reply fault not injected")
	}
	shardWant(t, c, r, "*1\r\n$6\r\nsecond\r\n", "LRANGE", key, "0", "-1")
}
