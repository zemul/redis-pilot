package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func startFinalTestProcess(t *testing.T, envelope shardBlockProcessConfig) func() {
	t.Helper()
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := exec.Command(executable, "-test.run=^TestShardBlockingProcessHelper$", "-test.count=1")
	p.Env = append(os.Environ(), "PILOT_TEST_BLOCKING_PROCESS="+string(payload))
	var output bytes.Buffer
	p.Stdout, p.Stderr = &output, &output
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { p.Process.Kill(); p.Wait() }) }
	t.Cleanup(stop)
	deadline := time.Now().Add(8 * time.Second)
	for {
		c, r, e := authenticated(envelope.Gateway.Listen, "shard-front-password")
		if e == nil {
			c.SetDeadline(time.Now().Add(time.Second))
			writeAll(c, encodeCommand("PING"))
			b, e := r.ReadFrame()
			c.Close()
			if e == nil && string(b) == "+PONG\r\n" {
				return stop
			}
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("finalization subprocess startup: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMigrationFinalizeProcessRecovery(t *testing.T) {
	m := newMigrationFixture(t)
	ctx := context.Background()
	from := m.to
	to, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 3, Nodes: from.Config().Nodes[1:]})
	if err != nil {
		t.Fatal(err)
	}
	m.j.Close()
	m.from, m.to = from, to
	m.dir = filepath.Join(t.TempDir(), "shrink")
	m.j, err = sharding.CreateJournal(ctx, m.dir, "move-1", from, to)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for i := 0; i < 30; i++ {
		key := shardKeyFor(from, "a", fmt.Sprintf("final-process-%d", i))
		keys = append(keys, key)
		cacheCall(t, m.a, "SET", key, "old")
		h, err := m.j.Prepare(ctx, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		a, _ := newMigrationFence(m.a, h.Placement.Source, cachePrefix+"owner", "migration-test-owner", h)
		b, _ := newMigrationFence(m.b, h.Placement.Target, cachePrefix+"owner", "migration-test-owner", h)
		attempt, err := copyMigrationKey(ctx, m.j, h, a, b)
		if err != nil {
			t.Fatal(err)
		}
		if err := finishMigrationKey(ctx, m.j, h, a, b, attempt); err != nil {
			t.Fatal(err)
		}
	}
	// Isolate finalization crash windows; source scanning is independently
	// covered by worker tests. All keys have actually finished handoff here.
	progress, err := m.j.BeginWorker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	progress.Phase = sharding.WorkerAwaitFinalize
	if err := m.j.SaveWorker(ctx, progress); err != nil {
		t.Fatal(err)
	}
	m.j.Close()
	hot := g4Redis(t)
	c, reader, err := authenticated(hot, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := passConfig(hot)
	cfg.Mode = "cache"
	cfg.RequestTimeout = 3 * time.Second
	cfg.Listen, cfg.AdminListen = freeAddress(t), freeAddress(t)
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	cfg.FrontendPasswordFile = secretFile(t, "shard-front-password")
	private := t.TempDir()
	if err := os.Chmod(private, 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Cache = &CacheConfig{ColdAddress: from.Config().Nodes[0].Address, ColdPasswordFile: secretFile(t, "g4-backend-test"), ExclusiveBackends: true, LockFile: filepath.Join(private, "owner.lock"), WritePolicy: "update_existing"}
	cacheCall(t, &cacheConn{Conn: c, reader: reader, cfg: cfg}, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
	c.Close()
	o := fastWorkerOptions()
	o.Finalize = true
	o.KeysPerSecond = 1
	target := to.Config()
	envelope := shardBlockProcessConfig{Gateway: cfg, Topology: from.Config(), Target: &target, Worker: &o, JournalDir: m.dir, TaskID: "move-1"}
	u := url.URL{Scheme: "file", Path: filepath.Join(m.dir, "handoffs.db")}
	u.RawQuery = "mode=ro&_pragma=busy_timeout(1000)"
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	wait := func(want string, partial bool) {
		deadline := time.Now().Add(12 * time.Second)
		for {
			var phase string
			var n int
			e := db.QueryRow(`SELECT phase,(SELECT COUNT(*) FROM handoff) FROM finalization WHERE id=1`).Scan(&phase, &n)
			if e == nil && phase == want && (!partial || (n > 0 && n < len(keys))) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("phase %s records %d: %v", phase, n, e)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	first := startFinalTestProcess(t, envelope)
	wait("published", true)
	client, r, err := authenticated(cfg.Listen, "shard-front-password")
	if err != nil {
		t.Fatal(err)
	}
	shardWant(t, client, r, "+OK\r\n", "SET", keys[0], "new")
	client.Close()
	first() // real SIGKILL after publication, in the middle of marker GC
	o.KeysPerSecond = 100000
	second := startFinalTestProcess(t, envelope)
	wait("complete", false)
	second()
	// The removed source is now offline. Completed-task recovery must select
	// target-only startup/probes even though the original task snapshot has A.
	if m.stopA != nil {
		m.stopA()
	} else {
		_, _ = m.a.call(ctx, "SHUTDOWN", "NOSAVE")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		probe, e := net.DialTimeout("tcp", from.Config().Nodes[0].Address, 100*time.Millisecond)
		if e != nil {
			break
		}
		probe.Close()
		if time.Now().After(deadline) {
			t.Fatal("removed source did not actually go offline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	third := startFinalTestProcess(t, envelope)
	defer third()
	client, r, err = authenticated(cfg.Listen, "shard-front-password")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	shardWant(t, client, r, "$3\r\nnew\r\n", "GET", keys[0])
	for _, key := range keys[1:] {
		shardWant(t, client, r, "$3\r\nold\r\n", "GET", key)
	}
	shardWant(t, client, r, "+OK\r\n", "SET", "after-restart", "works")
	shardWant(t, client, r, "$5\r\nworks\r\n", "GET", "after-restart")
	third()
	m.j, err = sharding.OpenJournal(ctx, m.dir, "move-1", from, to)
	if err != nil {
		t.Fatal(err)
	}
	awaitFinal(t, m.j, sharding.FinalComplete)
}
