package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func TestMigrationWorkerProcessRestart(t *testing.T) {
	m := newMigrationFixture(t)
	hot := g4Redis(t)
	c, reader, err := authenticated(hot, "g4-backend-test")
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
	cfg.Cache = &CacheConfig{ColdAddress: m.from.Config().Nodes[0].Address, ColdPasswordFile: secretFile(t, "g4-backend-test"), ExclusiveBackends: true, LockFile: filepath.Join(private, "owner.lock"), WritePolicy: "update_existing"}
	cacheCall(t, &cacheConn{Conn: c, reader: reader, cfg: cfg}, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
	c.Close()
	keys := map[string]string{}
	for i := 0; i < 60; i++ {
		key := shardKeyFor(m.to, "b", "process-worker-"+strconv.Itoa(i))
		keys[key] = "old"
		cacheCall(t, m.a, "SET", key, "old")
	}
	m.j.Close() // child exclusively owns the durable task during execution
	to := m.to.Config()
	options := fastWorkerOptions()
	options.KeysPerSecond = 10
	envelope := shardBlockProcessConfig{Gateway: cfg, Topology: m.from.Config(), Target: &to, Worker: &options, JournalDir: m.dir, TaskID: "move-1"}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	start := func() func() {
		payload, err := json.Marshal(envelope)
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
			c, r, e := authenticated(cfg.Listen, "shard-front-password")
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
				t.Fatalf("worker subprocess startup: %s", output.String())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	// Read-only observation is not another journal writer or task owner.
	u := url.URL{Scheme: "file", Path: filepath.Join(m.dir, "handoffs.db")}
	u.RawQuery = "mode=ro&_pragma=busy_timeout(1000)"
	observation, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer observation.Close()
	first := start()
	deadline := time.Now().Add(6 * time.Second)
	var changed []byte
	for {
		err := observation.QueryRow(`SELECT key FROM handoff WHERE state='cleaned' LIMIT 1`).Scan(&changed)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker never committed a key")
		}
		time.Sleep(10 * time.Millisecond)
	}
	first() // real SIGKILL while a background scan/copy cycle is incomplete
	m.j, err = sharding.OpenJournal(context.Background(), m.dir, "move-1", m.from, m.to)
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.j.WorkerProgress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p.Phase == sharding.WorkerAwaitFinalize {
		t.Fatal("did not interrupt an active migration")
	}
	rows, err := m.j.Page(context.Background(), 0, 100)
	if err != nil || len(rows) >= len(keys) {
		t.Fatal("test did not interrupt partial work")
	}
	// Model a later authoritative write to an already committed target. Resume
	// must not recopy the now-absent source or overwrite this newer value.
	cacheCall(t, m.b, "SET", string(changed), "new")
	keys[string(changed)] = "new"
	m.j.Close()
	options.KeysPerSecond = 100000
	second := start()
	defer second()
	deadline = time.Now().Add(12 * time.Second)
	for {
		var phase string
		err := observation.QueryRow(`SELECT phase FROM worker_progress WHERE id=1`).Scan(&phase)
		if err == nil && phase == string(sharding.WorkerAwaitFinalize) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not resume: %s %v", phase, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	second()
	m.j, err = sharding.OpenJournal(context.Background(), m.dir, "move-1", m.from, m.to)
	if err != nil {
		t.Fatal(err)
	}
	q, err := m.j.WorkerProgress(context.Background())
	if err != nil || q.Generation <= p.Generation {
		t.Fatalf("worker restart generation: %+v %v", q, err)
	}
	for key, want := range keys {
		if got, ok := frameString(cacheCall(t, m.b, "GET", key)); !ok || got != want {
			t.Fatalf("resumed target differs for %s", key)
		}
		if string(cacheCall(t, m.a, "EXISTS", key)) != ":0\r\n" {
			t.Fatal("source remains after resume")
		}
	}
}
