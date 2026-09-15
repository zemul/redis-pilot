package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func fastWorkerOptions() migrationWorkerOptions {
	o := defaultMigrationWorkerOptions()
	o.KeysPerSecond = 100000
	o.BytesPerSecond = 1 << 30
	o.RetryMin = 20 * time.Millisecond
	o.RetryMax = 50 * time.Millisecond
	return o
}
func startTestMigrationWorker(t *testing.T, f *shardRuntimeFixture, o migrationWorkerOptions) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.s.cache.shards.runMigrationWorker(ctx, o) }()
	var stopped atomic.Bool
	stop := func() {
		if !stopped.CompareAndSwap(false, true) {
			return
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("worker stop: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("worker did not cancel")
		}
	}
	t.Cleanup(stop)
	return stop
}
func awaitWorker(t *testing.T, j *sharding.Journal, predicate func(sharding.WorkerProgress) bool) sharding.WorkerProgress {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for {
		p, err := j.WorkerProgress(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if predicate(p) {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not reach expected state: %+v", p)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestMigrationWorkerTypesAndRecovery(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, true, nil)
			c, r := f.client(t)
			type object struct {
				key, kind string
				values    []string
			}
			var objects []object
			for _, kind := range []string{"string", "hash", "list", "set", "zset"} {
				key := shardKeyFor(f.m.to, "b", "worker-"+kind)
				if kind == "string" {
					chunk := strings.Repeat("x\x00\xff", 10000)
					shardWant(t, c, r, "+OK\r\n", "SET", key, chunk)
					shardWant(t, c, r, ":60000\r\n", "APPEND", key, chunk)
					shardWant(t, c, r, ":90000\r\n", "APPEND", key, chunk)
				} else {
					for base := 0; base < 700; base += 100 {
						a := []string{map[string]string{"hash": "HSET", "list": "RPUSH", "set": "SADD", "zset": "ZADD"}[kind], key}
						for i := base; i < base+100; i++ {
							v := fmt.Sprintf("%d\x00\xff", i)
							switch kind {
							case "hash":
								a = append(a, v, "v:"+v)
							case "zset":
								a = append(a, fmt.Sprint(i), v)
							default:
								a = append(a, v)
							}
						}
						b := shardAPI(t, c, r, a...)
						if len(b) == 0 || b[0] == '-' {
							t.Fatalf("seed: %q", b)
						}
					}
				}
				objects = append(objects, object{key, kind, migrationContents(t, f.m.a, key, kind)})
				if kind == "hash" {
					shardAPI(t, c, r, "HGET", key, "0\x00\xff")
				}
			}
			// This pending key cannot be discovered from source SCAN: it already
			// expired, but an incomplete target needs recovery and removal.
			ghost := shardKeyFor(f.m.to, "b", "worker-expired")
			if _, err := f.m.j.Prepare(context.Background(), []byte(ghost)); err != nil {
				t.Fatal(err)
			}
			cacheCall(t, f.m.b, "SET", ghost, "half-copy")
			if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
				t.Fatal(err)
			}
			startTestMigrationWorker(t, f, fastWorkerOptions())
			p := awaitWorker(t, f.m.j, func(p sharding.WorkerProgress) bool { return p.Phase == sharding.WorkerAwaitFinalize })
			if p.Pages < 2 || p.Attempts < 6 {
				t.Fatalf("work missing %+v", p)
			}
			for _, o := range objects {
				if !reflect.DeepEqual(o.values, migrationContents(t, f.m.b, o.key, o.kind)) {
					t.Fatalf("%s changed", o.kind)
				}
				if string(cacheCall(t, f.m.a, "EXISTS", o.key)) != ":0\r\n" {
					t.Fatal("source not cleaned")
				}
				if o.kind == "hash" && string(f.hotCall(t, "HLEN", o.key)) != ":700\r\n" {
					t.Fatal("worker discarded hot hash")
				}
			}
			if string(cacheCall(t, f.m.b, "EXISTS", ghost)) != ":0\r\n" {
				t.Fatal("expired target resurrected")
			}
			if pending, err := f.m.j.HasPending(context.Background()); err != nil || pending {
				t.Fatal("pending handoffs remain")
			}
			// Completion is only awaiting publication: markers and journal survive.
			page, err := f.m.j.Page(context.Background(), 0, 100)
			if err != nil || len(page) != 6 {
				t.Fatalf("journal was prematurely reclaimed: %v", err)
			}
		})
	}
}

func TestMigrationWorkerFailureContinuesAndRetries(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	badKey := ""
	// Bind key selection before enabling the worker; callback only reads it.
	f := newShardRuntimeFixture(t, "invalidate", true, func(address string) string {
		return cacheFaultRelayHooks(t, address, func(a [][]byte) bool {
			return fail.Load() && len(a) > 5 && string(a[0]) == "EVAL" && strings.Contains(string(a[1]), "local allowed={SETRANGE") && string(a[5]) == badKey
		}, nil)
	})
	c, r := f.client(t)
	badKey = shardKeyFor(f.m.to, "b", "worker-bad")
	good := shardKeyFor(f.m.to, "b", "worker-good")
	shardWant(t, c, r, "+OK\r\n", "MSET", badKey, "bad-data", good, "good-data")
	if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
		t.Fatal(err)
	}
	stop := startTestMigrationWorker(t, f, fastWorkerOptions())
	awaitWorker(t, f.m.j, func(p sharding.WorkerProgress) bool { return p.Failures > 0 })
	deadline := time.Now().Add(5 * time.Second)
	for {
		h, found, err := f.m.j.Lookup(context.Background(), []byte(good))
		if err != nil {
			t.Fatal(err)
		}
		if found && h.State == sharding.Cleaned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bad key stopped good key")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	p, err := f.m.j.WorkerProgress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p.Phase == sharding.WorkerAwaitFinalize {
		t.Fatal("failure became task success")
	}
	// Cancellation left durable prepared state; a new runner restarts scans.
	fail.Store(false)
	startTestMigrationWorker(t, f, fastWorkerOptions())
	q := awaitWorker(t, f.m.j, func(q sharding.WorkerProgress) bool { return q.Phase == sharding.WorkerAwaitFinalize })
	if q.Generation <= p.Generation {
		t.Fatal("runner generation not advanced")
	}
	shardWant(t, c, r, "$8\r\nbad-data\r\n", "GET", badKey)
}

func TestMigrationWorkerShrinkScanFailure(t *testing.T) {
	var dropped atomic.Bool
	f := newShardRuntimeFixture(t, "invalidate", false, func(address string) string {
		return cacheFaultRelayHooks(t, address, func(a [][]byte) bool {
			return len(a) > 1 && string(a[0]) == "EVAL" && strings.Contains(string(a[1]), "redis.call('SCAN'") && dropped.CompareAndSwap(false, true)
		}, nil)
	})
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "worker-shrink")
	shardWant(t, c, r, "+OK\r\n", "SET", key, "value")
	shardStartShrink(t, f)
	j := f.s.cache.shards.journal
	startTestMigrationWorker(t, f, fastWorkerOptions())
	p := awaitWorker(t, j, func(p sharding.WorkerProgress) bool { return p.Phase == sharding.WorkerAwaitFinalize })
	if !dropped.Load() || p.Failures < 1 || p.Generation < 2 {
		t.Fatalf("scan failure not retried %+v", p)
	}
	if !bytes.Equal(cacheCall(t, f.m.a, "GET", key), cacheBulk("value")) {
		t.Fatal("shrink did not move data")
	}
}

func TestMigrationPacerAndWorkerLimits(t *testing.T) {
	o := fastWorkerOptions()
	o.KeysPerSecond = 0
	if o.validate() == nil {
		t.Fatal("invalid limits accepted")
	}
	p := migrationPacer{perSecond: 10}
	if err := p.wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	reserved := p.next
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pacer ignored limit/cancellation: %v", err)
	}
	if !p.next.Equal(reserved) {
		t.Fatal("cancelled pacing accumulated retry debt")
	}
}

func TestMigrationWorkerLiveWritesAndNewPending(t *testing.T) {
	f := newShardRuntimeFixture(t, "update_existing", true, nil)
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "worker-live-hash")
	shardWant(t, c, r, ":2\r\n", "HSET", key, "counter", "0", "untouched", "retained")
	shardWant(t, c, r, "$1\r\n0\r\n", "HGET", key, "counter")
	if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
		t.Fatal(err)
	}
	startTestMigrationWorker(t, f, fastWorkerOptions())
	for i := 1; i <= 100; i++ {
		shardWant(t, c, r, fmt.Sprintf(":%d\r\n", i), "HINCRBY", key, "counter", "1")
		shardWant(t, c, r, string(cacheBulk(fmt.Sprint(i))), "HGET", key, "counter")
	}
	if string(f.hotCall(t, "HGET", key, "untouched")) != string(cacheBulk("retained")) {
		t.Fatal("increment discarded full hot hash")
	}
	p := awaitWorker(t, f.m.j, func(p sharding.WorkerProgress) bool { return p.Phase == sharding.WorkerAwaitFinalize })
	if err := f.s.cache.shards.runMigrationWorker(context.Background(), fastWorkerOptions()); err == nil {
		t.Fatal("duplicate worker accepted")
	}
	// Awaiting publication is not a terminal task state. A later failed
	// foreground handoff (including an absent source) must wake recovery.
	ghost := shardKeyFor(f.m.to, "b", "worker-late-pending")
	release, err := f.s.cache.acquire(context.Background(), []byte(ghost))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.j.Prepare(context.Background(), []byte(ghost)); err != nil {
		release()
		t.Fatal(err)
	}
	cacheCall(t, f.m.b, "SET", ghost, "partial")
	release()
	awaitWorker(t, f.m.j, func(q sharding.WorkerProgress) bool {
		return q.Generation > p.Generation && q.Phase == sharding.WorkerAwaitFinalize
	})
	if string(cacheCall(t, f.m.b, "EXISTS", ghost)) != ":0\r\n" {
		t.Fatal("late pending key not recovered")
	}
	shardWant(t, c, r, "$3\r\n100\r\n", "HGET", key, "counter")
}

func TestMigrationWorkerVerifyRejectsResidualSource(t *testing.T) {
	f := newShardRuntimeFixture(t, "invalidate", true, nil)
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "worker-residual")
	shardWant(t, c, r, "+OK\r\n", "SET", key, "current")
	if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
		t.Fatal(err)
	}
	if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
		t.Fatal(err)
	}
	// Deliberately violate exclusive-writer assumptions: a cleaned record
	// must not alone prove that the physical source is safe to retire.
	cacheCall(t, f.m.a, "SET", key, "stale")
	stop := startTestMigrationWorker(t, f, fastWorkerOptions())
	awaitWorker(t, f.m.j, func(p sharding.WorkerProgress) bool { return p.Failures > 0 })
	stop()
	p, err := f.m.j.WorkerProgress(context.Background())
	if err != nil || p.Phase == sharding.WorkerAwaitFinalize {
		t.Fatalf("residual source passed verification: %+v %v", p, err)
	}
	shardWant(t, c, r, "$7\r\ncurrent\r\n", "GET", key)
}
