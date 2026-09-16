package gateway

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func TestKvrocksIdentityProbe(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_KVROCKS_SERVER") == "" {
		t.Skip("Kvrocks only")
	}
	addr := migrationCold(t)
	c, r, err := authenticated(addr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	x := &cacheConn{Conn: c, reader: r, cfg: passConfig(addr)}
	for _, a := range [][]string{{"INFO", "server"}, {"CLIENT", "ID"}, {"CLIENT", "LIST"}} {
		b, err := x.call(context.Background(), a...)
		t.Logf("%v: %s (%v)", a, b, err)
	}
}

func TestShardBlockingKvrocksRestart(t *testing.T) {
	addr, kill, start := isolatedKvrocks(t)
	// The shard fixture assigns node b its own credential. Reapply that test
	// credential after restarting from the original isolated config file.
	configure := func() {
		c, r, err := authenticated(addr, "g4-backend-test")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		cacheCall(t, &cacheConn{Conn: c, reader: r, cfg: passConfig(addr)}, "CONFIG", "SET", "requirepass", "shard-b-password")
	}
	configure()
	f := newShardRuntimeFixture(t, "invalidate", false, func(string) string { return addr })
	ctx := context.Background()
	connect := func() *cacheConn {
		c, err := f.s.cache.shards.raw(ctx, f.s.cache.shards.nodes["b"])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	cold := connect()
	key := shardKeyFor(f.m.to, "b", "queue:cold-restart")
	done := shardPopAsync(t, f, "BLPOP", key, "30")
	shardWaitBlocked(t, cold)
	if b := cacheCall(t, cold, "HLEN", shardBlockingRegistry); string(b) != ":1\r\n" {
		t.Fatalf("blocking registration missing before cold crash: %q", b)
	}
	kill()
	if b := shardPopResult(t, done); !bytes.Contains(b, []byte("not retried")) {
		t.Fatalf("cold restart replayed or hid unknown pop: %q", b)
	}
	start()
	configure()
	cold = connect()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b := cacheCall(t, cold, "HLEN", shardBlockingRegistry)
		if string(b) == ":0\r\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cold restart registration did not recover: %q", b)
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, r := f.client(t)
	shardWant(t, c, r, ":1\r\n", "RPUSH", key, "after")
	shardWant(t, c, r, string(cacheArray([][]byte{cacheBulk(key), cacheBulk("after")})), "BLPOP", key, "1")
}

func TestShardScanKvrocksRestart(t *testing.T) {
	addr, kill, start := isolatedKvrocks(t)
	configure := func() {
		c, r, err := authenticated(addr, "g4-backend-test")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		cacheCall(t, &cacheConn{Conn: c, reader: r, cfg: passConfig(addr)}, "CONFIG", "SET", "requirepass", "shard-b-password")
	}
	configure()
	f := newShardRuntimeFixture(t, "update_existing", false, func(string) string { return addr })
	cold, err := f.s.cache.shards.raw(context.Background(), f.s.cache.shards.nodes["b"])
	if err != nil {
		t.Fatal(err)
	}
	identity, err := shardScanBackendIdentity(context.Background(), cold)
	cold.Close()
	if err != nil || !strings.HasPrefix(identity, "start:") {
		t.Fatalf("test requires Kvrocks startup identity: %q, %v", identity, err)
	}
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "scan-cold-restart")
	a := []string{"HSET", key}
	for i := 0; i < 700; i++ {
		a = append(a, fmt.Sprint(i), "value")
	}
	shardWant(t, c, r, ":700\r\n", a...)
	before, _ := shardScanPage(t, shardAPI(t, c, r, "HSCAN", key, "0", "COUNT", "7"))
	if before == "0" {
		t.Fatal("need multiple pages")
	}
	kill()
	start() // Same address/data directory, no request during the outage.
	configure()
	if b := shardAPI(t, c, r, "HSCAN", key, before, "COUNT", "7"); !bytes.HasPrefix(b, []byte("-TRYAGAIN")) {
		t.Fatalf("cold restart accepted previous cursor: %q", b)
	}
	shardWant(t, c, r, ":0\r\n", "HSET", key, "0", "new")
	seen, cursor := map[string]string{}, "0"
	for page := 0; ; page++ {
		if page > 1000 {
			t.Fatal("rescan did not complete")
		}
		var items []string
		cursor, items = shardScanPage(t, shardAPI(t, c, r, "HSCAN", key, cursor, "COUNT", "13"))
		for i := 0; i < len(items); i += 2 {
			seen[items[i]] = items[i+1]
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 700 || seen["0"] != "new" {
		t.Fatalf("rescan content: members=%d updated=%q", len(seen), seen["0"])
	}
	for i := 1; i < 700; i++ {
		if seen[fmt.Sprint(i)] != "value" {
			t.Fatal("cold restart lost hash field", i)
		}
	}
}

// Each test owns fresh cold processes/data directories. Never reuse a live
// development database: fault tests change credentials and kill processes.
func migrationCold(t *testing.T) string {
	t.Helper()
	addr, _ := migrationColdManaged(t)
	return addr
}

func migrationColdManaged(t *testing.T) (string, func()) {
	t.Helper()
	binary := os.Getenv("GATEWAY_TEST_KVROCKS_SERVER")
	if binary == "" {
		return g4Redis(t), nil
	}
	addr, stop, _ := isolatedKvrocks(t)
	return addr, stop
}

func isolatedKvrocks(t *testing.T) (string, func(), func()) {
	addr, stop, start, _ := isolatedKvrocksWithConfig(t)
	return addr, stop, start
}

func isolatedKvrocksWithConfig(t *testing.T) (string, func(), func(), string) {
	t.Helper()
	binary := os.Getenv("GATEWAY_TEST_KVROCKS_SERVER")
	if binary == "" {
		t.Skip("set GATEWAY_TEST_KVROCKS_SERVER")
	}
	addr := freeAddress(t)
	_, port, _ := net.SplitHostPort(addr)
	dir := t.TempDir()
	config := filepath.Join(dir, "kvrocks.conf")
	strict := "yes"
	if os.Getenv("GATEWAY_TEST_KVROCKS_STRICT") == "no" {
		strict = "no"
	}
	contents := fmt.Sprintf("bind 127.0.0.1\nport %s\ndir %s\ndaemonize no\nrequirepass g4-backend-test\nworkers 2\nrocksdb.block_cache_size 8\nrocksdb.write_buffer_size 4\nrocksdb.write_options.sync yes\nrocksdb.write_options.disable_wal no\nlua-strict-key-accessing %s\nlog-level warning\n", port, dir, strict)
	if err := os.WriteFile(config, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	var p *exec.Cmd
	start := func() {
		p = ownProcess(t, binary, "-c", config)
		deadline := time.Now().Add(5 * time.Second)
		for {
			c, _, err := authenticated(addr, "g4-backend-test")
			if err == nil {
				c.Close()
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("isolated Kvrocks did not start")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	stop := func() {
		if p != nil {
			p.Process.Kill()
			p.Wait()
			p = nil
		}
	}
	start()
	return addr, stop, start, config
}

func TestMigrationKvrocksColdProcessRecovery(t *testing.T) {
	a, killA, startA := isolatedKvrocks(t)
	b, killB, startB := isolatedKvrocks(t)
	ctx := context.Background()
	from, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 1, Nodes: []sharding.Node{{ID: "a", Address: a}}})
	if err != nil {
		t.Fatal(err)
	}
	to, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 2, Nodes: []sharding.Node{{ID: "b", Address: b}}})
	if err != nil {
		t.Fatal(err)
	}
	j, err := sharding.CreateJournal(ctx, filepath.Join(t.TempDir(), "journal"), "cold-crash", from, to)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	connect := func(addr string) *cacheConn {
		c, r, err := authenticated(addr, "g4-backend-test")
		if err != nil {
			t.Fatal(err)
		}
		x := &cacheConn{Conn: c, reader: r, cfg: passConfig(addr)}
		x.cfg.RequestTimeout = 3 * time.Second
		t.Cleanup(func() { x.Close() })
		return x
	}
	x, y := connect(a), connect(b)
	const owner = "kvrocks-crash-owner"
	for _, c := range []*cacheConn{x, y} {
		cacheCall(t, c, "SET", cachePrefix+"owner", owner)
		for _, option := range []string{"rocksdb.write_options.sync", "rocksdb.write_options.disable_wal", "workers", "lua-strict-key-accessing"} {
			t.Logf("%s %s", option, cacheCall(t, c, "CONFIG", "GET", option))
		}
	}
	key := "durable-hash\x00\xff"
	for i := 0; i < 700; i++ {
		cacheCall(t, x, "HSET", key, fmt.Sprint(i), fmt.Sprintf("v%d\x00\xff", i))
	}
	cacheCall(t, x, "PEXPIRE", key, "60000")
	expiry := string(cacheCall(t, x, "PEXPIRETIME", key))
	h, err := j.Prepare(ctx, []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	fences := func() (*migrationFence, *migrationFence) {
		f, e := newMigrationFence(x, from.Owner([]byte(key)), cachePrefix+"owner", owner, h)
		if e != nil {
			t.Fatal(e)
		}
		g, e := newMigrationFence(y, to.Owner([]byte(key)), cachePrefix+"owner", owner, h)
		if e != nil {
			t.Fatal(e)
		}
		return f, g
	}
	f, g := fences()
	attempt, err := copyMigrationKey(ctx, j, h, f, g)
	if err != nil {
		t.Fatal(err)
	}
	killB()
	startB()
	y = connect(b)
	f, g = fences()
	if phase, _, err := g.state(ctx); err != nil || phase != "ready" {
		t.Fatal("ready not durable", phase, err)
	}
	if err := j.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := g.activate(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := g.run(ctx, migrationBusinessGuard+`return redis.call('HSET',KEYS[3],'0','new')`, "serving"); err != nil {
		t.Fatal(err)
	}
	killA()
	killB()
	startA()
	startB()
	x, y = connect(a), connect(b)
	f, g = fences()
	if err := finishMigrationKey(ctx, j, h, f, g, attempt); err != nil {
		t.Fatal(err)
	}
	killA()
	killB()
	startA()
	startB()
	x, y = connect(a), connect(b)
	if string(cacheCall(t, x, "EXISTS", key)) != ":0\r\n" {
		t.Fatal("source resurrected")
	}
	if string(cacheCall(t, y, "HLEN", key)) != ":700\r\n" {
		t.Fatal("hash incomplete after crash")
	}
	for i := 0; i < 700; i++ {
		want := fmt.Sprintf("v%d\x00\xff", i)
		if i == 0 {
			want = "new"
		}
		if string(cacheCall(t, y, "HGET", key, fmt.Sprint(i))) != string(cacheBulk(want)) {
			t.Fatalf("field %d changed after crash", i)
		}
	}
	if string(cacheCall(t, y, "PEXPIRETIME", key)) != expiry {
		t.Fatal("absolute TTL changed after crash")
	}
	if saved, found, err := j.Lookup(ctx, []byte(key)); err != nil || !found || saved.State != sharding.Cleaned {
		t.Fatal("ownership not cleaned")
	}
}
