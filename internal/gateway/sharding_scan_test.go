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

type shardScanExchange struct {
	args  []string
	reply []byte // nil closes the backend connection without a reply
}

func shardScanIdentityConn(t *testing.T, steps []shardScanExchange) *cacheConn {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		r, _ := NewReader(server, DefaultConfig().Limits)
		for _, step := range steps {
			raw, _, err := r.ReadCommand()
			if err != nil {
				done <- err
				return
			}
			if !bytes.Equal(raw, encodeCommand(step.args...)) {
				done <- fmt.Errorf("identity sent %q, want %v", raw, step.args)
				return
			}
			if step.reply == nil {
				done <- nil
				return
			}
			if err := writeAll(server, step.reply); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	t.Cleanup(func() {
		client.Close()
		server.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	r, _ := NewReader(client, DefaultConfig().Limits)
	cfg := DefaultConfig()
	cfg.RequestTimeout = time.Second
	return &cacheConn{Conn: client, reader: r, cfg: cfg}
}

func shardScanTestInfo(pid, micros, uptime string) []byte {
	return cacheBulk("# Server\r\nprocess_id:" + pid + "\r\nserver_time_usec:" + micros + "\r\nuptime_in_seconds:" + uptime + "\r\n")
}

func TestShardScanIdentitySampling(t *testing.T) {
	info := func(b []byte) shardScanExchange { return shardScanExchange{[]string{"INFO", "server"}, b} }
	clock := func(sec, usec string) shardScanExchange {
		return shardScanExchange{[]string{"TIME"}, cacheArray([][]byte{cacheBulk(sec), cacheBulk(usec)})}
	}
	base := info(shardScanTestInfo("42", "1005100000", "5"))
	for _, tc := range []struct {
		name  string
		steps []shardScanExchange
		want  string
	}{
		{"redis", []shardScanExchange{info(cacheBulk("# Server\r\nrun_id:abc123\r\n"))}, "run:abc123"},
		{"kvrocks", []shardScanExchange{base, clock("1005", "100100")}, "start:42:1000"},
		{"later-same-process", []shardScanExchange{info(shardScanTestInfo("42", "1015999000", "15")), clock("1015", "999100")}, "start:42:1000"},
		{"cross-second-resample", []shardScanExchange{info(shardScanTestInfo("42", "1005999999", "6")), clock("1006", "10"), info(shardScanTestInfo("42", "1006100000", "6")), clock("1006", "100001")}, "start:42:1000"},
		{"backwards-within-second", []shardScanExchange{base, clock("1005", "99999"), base, clock("1005", "100001")}, "start:42:1000"},
		{"bounded-retries", []shardScanExchange{base, clock("1006", "1"), base, clock("1006", "1"), base, clock("1006", "1")}, ""},
		{"disconnect-info", []shardScanExchange{info(nil)}, ""},
		{"disconnect-time", []shardScanExchange{base, {[]string{"TIME"}, nil}}, ""},
		{"error-info", []shardScanExchange{info([]byte("-ERR unavailable\r\n"))}, ""},
		{"empty-run-id", []shardScanExchange{info(cacheBulk("run_id:\r\n"))}, ""},
		{"duplicate-run-id", []shardScanExchange{info(cacheBulk("run_id:a\r\nrun_id:b\r\n"))}, ""},
		{"oversized-run-id", []shardScanExchange{info(cacheBulk("run_id:" + strings.Repeat("x", 129) + "\r\n"))}, ""},
		{"missing-pid", []shardScanExchange{info(shardScanTestInfo("", "1005100000", "5"))}, ""},
		{"zero-pid", []shardScanExchange{info(shardScanTestInfo("0", "1005100000", "5"))}, ""},
		{"negative-uptime", []shardScanExchange{info(shardScanTestInfo("42", "1005100000", "-1"))}, ""},
		{"overflow-time", []shardScanExchange{info(shardScanTestInfo("42", "9223372036854775808", "5"))}, ""},
		{"uptime-exceeds-time", []shardScanExchange{info(shardScanTestInfo("42", "1005100000", "1006"))}, ""},
		{"invalid-time-micros", []shardScanExchange{base, clock("1005", "1000000")}, ""},
		{"invalid-time-shape", []shardScanExchange{base, {[]string{"TIME"}, cacheBulk("1005")}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := shardScanBackendIdentity(context.Background(), shardScanIdentityConn(t, tc.steps))
			if identity != tc.want || (err == nil) != (tc.want != "") {
				t.Fatalf("identity=%q err=%v, want=%q", identity, err, tc.want)
			}
		})
	}
}

func shardScanPage(t *testing.T, b []byte) (string, []string) {
	t.Helper()
	p, err := frameParts(b)
	if err != nil || len(p) != 2 {
		t.Fatalf("scan response: %q", b)
	}
	cursor, ok := frameString(p[0])
	if !ok {
		t.Fatal("invalid cursor")
	}
	if _, err := strconv.ParseUint(cursor, 10, 64); err != nil {
		t.Fatal(err)
	}
	items, err := frameParts(p[1])
	if err != nil {
		t.Fatal(err)
	}
	values := make([]string, len(items))
	for i, item := range items {
		values[i], ok = frameString(item)
		if !ok {
			t.Fatal("invalid scan item")
		}
	}
	return cursor, values
}

func TestShardScanCursorBounds(t *testing.T) {
	var store shardCursorStore
	now := time.Now()
	v := shardCursor{backend: "opaque\x00cursor", revision: 1}
	id, err := store.put(v, now)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.ParseUint(id, 10, 64)
	if got, ok := store.get(n, now); !ok || got.backend != v.backend {
		t.Fatal("cursor missing")
	}
	if _, ok := store.get(n, now.Add(shardCursorTTL)); ok {
		t.Fatal("expired cursor accepted")
	}
	for i := 1; i <= shardCursorCapacity; i++ {
		if _, err := store.put(v, now.Add(time.Duration(i)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.entries) != shardCursorCapacity {
		t.Fatal("cursor storage not bounded")
	}
	if _, ok := store.get(n, now); ok {
		t.Fatal("oldest cursor not evicted")
	}
	v.backend = strings.Repeat("x", 1025)
	if _, err := store.put(v, now); err == nil {
		t.Fatal("oversized cursor accepted")
	}
	a := shardScanFingerprint(cacheArgs("HSCAN", "a\x00b", "1", "COUNT", "2"), 2)
	b := shardScanFingerprint(cacheArgs("hscan", "a\x00b", "42", "COUNT", "2"), 2)
	if a != b || a == shardScanFingerprint(cacheArgs("HSCAN", "a", "1", "COUNT", "2"), 2) {
		t.Fatal("fingerprint binding")
	}
}

func TestShardScanObjects(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, true, nil)
			c, r := f.client(t)
			key := shardKeyFor(f.m.to, "b", "object-scan")
			for _, kind := range []string{"H", "S", "Z"} {
				shardAPI(t, c, r, "DEL", key)
				command := map[string]string{"H": "HSET", "S": "SADD", "Z": "ZADD"}[kind]
				for start := 0; start < 700; start += 100 {
					a := []string{command, key}
					for i := start; i < start+100; i++ {
						member := fmt.Sprintf("f:%04d\x00", i)
						switch kind {
						case "H":
							a = append(a, member, "v\xff"+member)
						case "S":
							a = append(a, member)
						case "Z":
							a = append(a, strconv.Itoa(i), member)
						}
					}
					shardWant(t, c, r, ":100\r\n", a...)
				}
				cursor, seen := "0", map[string]string{}
				for page := 0; ; page++ {
					if page > 2000 {
						t.Fatal("scan did not finish")
					}
					old := cursor
					var items []string
					cursor, items = shardScanPage(t, shardAPI(t, c, r, kind+"SCAN", key, cursor, "COUNT", "13"))
					step := 2
					if kind == "S" {
						step = 1
					}
					for i := 0; i < len(items); i += step {
						value := ""
						if step == 2 {
							value = items[i+1]
						}
						seen[items[i]] = value
					}
					if page == 1 {
						// Replaying a recent cursor is a read, not advancing a mutable session.
						_, repeated := shardScanPage(t, shardAPI(t, c, r, kind+"SCAN", key, old, "COUNT", "13"))
						if fmt.Sprint(repeated) != fmt.Sprint(items) {
							t.Fatal("cursor replay lost page")
						}
						if b := shardAPI(t, c, r, kind+"SCAN", key+"wrong", old, "COUNT", "13"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
							t.Fatalf("key escape: %q", b)
						}
						if b := shardAPI(t, c, r, kind+"SCAN", key, old, "COUNT", "14"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
							t.Fatalf("options escape: %q", b)
						}
					}
					if cursor == "0" {
						break
					}
				}
				if len(seen) != 700 {
					t.Fatalf("%s missing members: %d", kind, len(seen))
				}
				for i := 0; i < 700; i++ {
					member := fmt.Sprintf("f:%04d\x00", i)
					want := ""
					if kind == "H" {
						want = "v\xff" + member
					}
					if kind == "Z" {
						want = strconv.Itoa(i)
					}
					if got, exists := seen[member]; !exists || got != want {
						t.Fatalf("%s content mismatch", kind)
					}
				}
				if b := f.hotCall(t, "EXISTS", key); string(b) != ":0\r\n" {
					t.Fatalf("%s scan unexpectedly warmed cache: %q", kind, b)
				}
			}
			cursor, _ := shardScanPage(t, shardAPI(t, c, r, "ZSCAN", key, "0", "COUNT", "1"))
			if cursor == "0" {
				t.Fatal("need multiple pages")
			}
			if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
				t.Fatal(err)
			}
			if b := shardAPI(t, c, r, "ZSCAN", key, cursor, "COUNT", "1"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
				t.Fatalf("old topology cursor accepted: %q", b)
			}
			// Starting at 0 performs on-demand handoff; all remaining pages stay target-bound.
			cursor, _ = shardScanPage(t, shardAPI(t, c, r, "ZSCAN", key, "0", "COUNT", "1"))
			if cursor == "0" {
				t.Fatal("need target pages")
			}
			n, _ := strconv.ParseUint(cursor, 10, 64)
			f.s.cache.shards.cursors.mu.Lock()
			v := f.s.cache.shards.cursors.entries[n]
			v.identity = "previous-process"
			f.s.cache.shards.cursors.entries[n] = v
			f.s.cache.shards.cursors.mu.Unlock()
			if b := shardAPI(t, c, r, "ZSCAN", key, cursor, "COUNT", "1"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
				t.Fatalf("old backend cursor accepted: %q", b)
			}
		})
	}
}

func TestShardScanGlobalDuringMigration(t *testing.T) {
	f := newShardRuntimeFixture(t, "update_existing", true, nil)
	c, r := f.client(t)
	keys, moving := map[string]bool{}, []string{}
	for start := 0; start < 300; start += 50 {
		a := []string{"MSET"}
		for i := start; i < start+50; i++ {
			key := fmt.Sprintf("scan:%d\x00", i)
			keys[key] = true
			if f.m.to.Owner([]byte(key)).ID == "b" {
				moving = append(moving, key)
			}
			a = append(a, key, "value")
		}
		shardWant(t, c, r, "+OK\r\n", a...)
	}
	old, _ := shardScanPage(t, shardAPI(t, c, r, "SCAN", "0", "COUNT", "3"))
	if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
		t.Fatal(err)
	}
	if b := shardAPI(t, c, r, "SCAN", old, "COUNT", "3"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
		t.Fatalf("old global cursor accepted: %q", b)
	}
	seen, cursor, moved := map[string]bool{}, "0", 0
	for page := 0; ; page++ {
		if page > 3000 {
			t.Fatal("global scan did not finish")
		}
		var items []string
		cursor, items = shardScanPage(t, shardAPI(t, c, r, "SCAN", cursor, "COUNT", "3"))
		for _, key := range items {
			if !keys[key] {
				t.Fatalf("internal/unknown key exposed: %q", key)
			}
			seen[key] = true
		}
		// Move keys between every page, including while traversing target nodes.
		if moved < len(moving) {
			if err := f.s.cache.shards.migrate(context.Background(), []byte(moving[moved])); err != nil {
				t.Fatal(err)
			}
			moved++
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != len(keys) {
		t.Fatalf("migration scan omitted continuous keys: %d/%d", len(seen), len(keys))
	}
	// A committed-but-not-cleaned source and an uncommitted target must not
	// surface as live keys in global scans.
	key := moving[0]
	shardWant(t, c, r, ":1\r\n", "DEL", key)
	cacheCall(t, f.m.a, "SET", key, "stale-source")
	if ok, err := f.s.cache.shards.scanVisible(context.Background(), f.m.from.Config().Nodes[0], []byte(key)); err != nil || ok {
		t.Fatalf("stale source visible: %v %v", ok, err)
	}
	newKey := shardKeyFor(f.m.to, "b", "uncommitted")
	if _, err := f.m.j.Prepare(context.Background(), []byte(newKey)); err != nil {
		t.Fatal(err)
	}
	cacheCall(t, f.m.b, "SET", newKey, "half-copy")
	if ok, err := f.s.cache.shards.scanVisible(context.Background(), f.m.to.Config().Nodes[1], []byte(newKey)); err != nil || ok {
		t.Fatalf("uncommitted target visible: %v %v", ok, err)
	}
}

func TestShardScanGlobalShrinkAndOptions(t *testing.T) {
	f := newShardRuntimeFixture(t, "invalidate", false, nil)
	c, r := f.client(t)
	keys := map[string]bool{}
	var moving []string
	for start := 0; start < 200; start += 50 {
		a := []string{"MSET"}
		for i := start; i < start+50; i++ {
			key := fmt.Sprintf("shrink:%d", i)
			keys[key] = true
			if f.m.to.Owner([]byte(key)).ID == "a" {
				moving = append(moving, key)
			}
			a = append(a, key, "value")
		}
		shardWant(t, c, r, "+OK\r\n", a...)
	}
	shardWant(t, c, r, ":1\r\n", "HSET", "shrink:hash", "f", "v")
	shardWant(t, c, r, "+OK\r\n", "SET", "outside", "v")
	to, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 3, Nodes: f.m.to.Config().Nodes[1:]})
	if err != nil {
		t.Fatal(err)
	}
	j, err := sharding.CreateJournal(context.Background(), filepath.Join(t.TempDir(), "shrink"), "shrink", f.m.to, to)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err := f.s.cache.shards.beginTransition(to, j); err != nil {
		t.Fatal(err)
	}
	seen, cursor, moved := map[string]bool{}, "0", 0
	for page := 0; ; page++ {
		if page > 3000 {
			t.Fatal("shrink scan did not finish")
		}
		var items []string
		cursor, items = shardScanPage(t, shardAPI(t, c, r, "SCAN", cursor, "MATCH", "shrink:*", "TYPE", "string", "COUNT", "2"))
		for _, key := range items {
			if !keys[key] {
				t.Fatalf("MATCH/TYPE not honored: %q", key)
			}
			seen[key] = true
		}
		if moved < len(moving) {
			if err := f.s.cache.shards.migrate(context.Background(), []byte(moving[moved])); err != nil {
				t.Fatal(err)
			}
			moved++
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != len(keys) {
		t.Fatalf("shrink omitted continuous keys: %d/%d", len(seen), len(keys))
	}
	for _, a := range [][]string{{"SCAN", "not-a-cursor"}, {"SCAN", "0", "COUNT", "0"}, {"SCAN", "0", "BAD", "option"}, {"HSCAN", "shrink:hash", "0", "TYPE", "hash"}} {
		if b := shardAPI(t, c, r, a...); len(b) == 0 || b[0] != '-' {
			t.Fatalf("invalid options accepted: %v %q", a, b)
		}
	}
}

func TestShardScanBackendFailureAndRestart(t *testing.T) {
	var fail atomic.Bool
	f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string {
		return cacheFaultRelayHooks(t, address, func(a [][]byte) bool {
			return len(a) > 1 && string(a[0]) == "EVAL" && strings.Contains(string(a[1]), "redis.call('SCAN'") && fail.Load()
		}, nil)
	})
	c, r := f.client(t)
	// Empty page from A must still carry a continuation for B, not cursor 0.
	cursor, _ := shardScanPage(t, shardAPI(t, c, r, "SCAN", "0", "COUNT", "100"))
	if cursor == "0" {
		t.Fatal("ended before visiting second node")
	}
	fail.Store(true)
	if b := shardAPI(t, c, r, "SCAN", cursor, "COUNT", "100"); len(b) == 0 || b[0] != '-' {
		t.Fatalf("backend failure became successful EOF: %q", b)
	}
	fail.Store(false)
	// Once a backend error is observed, even a recovered backend must not
	// accept this old cursor. Restarting from 0 issues a fresh traversal.
	if b := shardAPI(t, c, r, "SCAN", cursor, "COUNT", "100"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
		t.Fatalf("failed cursor became reusable: %q", b)
	}
	cursor, _ = shardScanPage(t, shardAPI(t, c, r, "SCAN", "0", "COUNT", "100"))
	f.s.cache.shards.cursors.mu.Lock()
	f.s.cache.shards.cursors.entries = nil // model lost ephemeral cursor state
	f.s.cache.shards.cursors.mu.Unlock()
	if b := shardAPI(t, c, r, "SCAN", cursor, "COUNT", "100"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
		t.Fatalf("restart silently resumed stale cursor: %q", b)
	}
}

func TestShardScanStartupFallback(t *testing.T) {
	var pid, started atomic.Int64
	pid.Store(42)
	started.Store(time.Now().Unix() - 600)
	f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string {
		return cacheFaultRelayHooks(t, address, nil, func(a [][]byte, b []byte) bool {
			if len(a) == 2 && string(a[0]) == "INFO" && strings.EqualFold(string(a[1]), "server") {
				original, ok := frameString(b)
				now := time.Now()
				fields := fmt.Sprintf("# Server\r\nprocess_id:%d\r\nserver_time_usec:%d\r\nuptime_in_seconds:%d\r\n", pid.Load(), now.UnixMicro(), now.Unix()-started.Load())
				if !ok || len(fields) > len(original) {
					t.Error("test INFO reply has insufficient padding")
					return true
				}
				// Preserve the frame length required by the fault relay while
				// removing Redis run_id and supplying Kvrocks-style fields.
				copy(b, cacheBulk(fields+strings.Repeat(" ", len(original)-len(fields))))
			}
			return false
		})
	})
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "startup-hash")
	a := []string{"HSET", key}
	for i := 0; i < 700; i++ {
		a = append(a, fmt.Sprint(i), "value")
	}
	shardWant(t, c, r, ":700\r\n", a...)
	for _, change := range []string{"pid", "started", "invalid-info"} {
		cursor, _ := shardScanPage(t, shardAPI(t, c, r, "HSCAN", key, "0", "COUNT", "7"))
		if cursor == "0" {
			t.Fatal("need multiple pages")
		}
		id, _ := strconv.ParseUint(cursor, 10, 64)
		stored, ok := f.s.cache.shards.cursors.get(id, time.Now())
		if !ok || !strings.HasPrefix(stored.identity, "start:") {
			t.Fatalf("not using startup fallback: %+v", stored)
		}
		shardScanPage(t, shardAPI(t, c, r, "HSCAN", key, cursor, "COUNT", "7"))
		switch change {
		case "pid":
			pid.Add(1)
		case "started":
			started.Add(1)
		case "invalid-info":
			pid.Store(0)
		}
		if b := shardAPI(t, c, r, "HSCAN", key, cursor, "COUNT", "7"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
			t.Fatalf("%s did not invalidate scan: %q", change, b)
		}
		if change == "invalid-info" {
			pid.Store(43)
		}
		if _, ok := f.s.cache.shards.cursors.get(id, time.Now()); ok {
			t.Fatal("failed startup-bound cursor was retained")
		}
		shardWant(t, c, r, "$5\r\nvalue\r\n", "HGET", key, "1")
	}
}

func TestShardScanGatewayProcessRestart(t *testing.T) {
	m := newMigrationFixture(t)
	hot := g4Redis(t)
	c, r, err := authenticated(hot, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := passConfig(hot)
	cfg.Mode = "cache"
	cfg.Listen, cfg.AdminListen = freeAddress(t), freeAddress(t)
	cfg.RequestTimeout = 3 * time.Second
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	cfg.FrontendPasswordFile = secretFile(t, "shard-front-password")
	private := t.TempDir()
	if err := os.Chmod(private, 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Cache = &CacheConfig{ColdAddress: m.from.Config().Nodes[0].Address, ColdPasswordFile: secretFile(t, "g4-backend-test"), ExclusiveBackends: true, LockFile: filepath.Join(private, "owner.lock"), WritePolicy: "invalidate"}
	cacheCall(t, &cacheConn{Conn: c, reader: r, cfg: cfg}, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
	c.Close()
	a := []string{"HSET", "restart-hash"}
	for i := 0; i < 700; i++ {
		a = append(a, fmt.Sprint(i), "value")
	}
	cacheCall(t, m.a, a...)
	envelope := shardBlockProcessConfig{Gateway: cfg, Topology: m.from.Config()}
	connect := func() (net.Conn, *Reader) {
		c, r, err := authenticated(cfg.Listen, "shard-front-password")
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(10 * time.Second))
		t.Cleanup(func() { c.Close() })
		return c, r
	}
	stop := startFinalTestProcess(t, envelope)
	c, r = connect()
	cursor, _ := shardScanPage(t, shardAPI(t, c, r, "HSCAN", "restart-hash", "0", "COUNT", "7"))
	if cursor == "0" {
		t.Fatal("need multiple pages")
	}
	stop() // SIGKILL: only bounded, ephemeral cursor state is lost.
	stop = startFinalTestProcess(t, envelope)
	defer stop()
	c, r = connect()
	if b := shardAPI(t, c, r, "HSCAN", "restart-hash", cursor, "COUNT", "7"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
		t.Fatalf("Gateway restart accepted old cursor: %q", b)
	}
	shardScanPage(t, shardAPI(t, c, r, "HSCAN", "restart-hash", "0", "COUNT", "7"))
	shardWant(t, c, r, ":700\r\n", "HLEN", "restart-hash")
}
