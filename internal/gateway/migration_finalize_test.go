package gateway

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func awaitFinal(t *testing.T, j *sharding.Journal, want sharding.FinalPhase) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for {
		p, err := j.FinalPhase(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if p == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("finalization stuck at %s, want %s", p, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMigrationFinalizeRepeatedTopology(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			f := newShardRuntimeFixture(t, policy, true, nil)
			c, r := f.client(t)
			key := shardKeyFor(f.m.to, "b", "final-hash")
			shardWant(t, c, r, ":2\r\n", "HSET", key, "n", "0", "keep", "yes")
			shardWant(t, c, r, "$1\r\n0\r\n", "HGET", key, "n")
			if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
				t.Fatal(err)
			}
			o := fastWorkerOptions()
			o.Finalize = true
			stop := startTestMigrationWorker(t, f, o)
			defer stop()
			awaitFinal(t, f.m.j, sharding.FinalComplete)
			if string(f.hotCall(t, "HLEN", key)) != ":2\r\n" {
				t.Fatal("publication flushed hot hash")
			}
			from := f.m.to
			for i, nodes := range [][]sharding.Node{f.m.to.Config().Nodes[:1], f.m.to.Config().Nodes} {
				to, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: uint64(3 + i), Nodes: nodes})
				if err != nil {
					t.Fatal(err)
				}
				j, err := sharding.CreateJournal(context.Background(), filepath.Join(t.TempDir(), "task"), fmt.Sprintf("next-%d", i), from, to)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { j.Close() })
				if err := f.s.cache.shards.beginTransition(to, j); err != nil {
					t.Fatal(err)
				}
				for n := 1; n <= 30; n++ {
					want := i*30 + n
					shardWant(t, c, r, fmt.Sprintf(":%d\r\n", want), "HINCRBY", key, "n", "1")
					shardWant(t, c, r, string(cacheBulk(fmt.Sprint(want))), "HGET", key, "n")
				}
				awaitFinal(t, j, sharding.FinalComplete)
				if page, err := j.Page(context.Background(), 0, 100); err != nil || len(page) != 0 {
					t.Fatal("records retained")
				}
				if len(f.s.cache.shards.requiredNodes()) != len(nodes) {
					t.Fatal("retired nodes still required")
				}
				from = to
			}
			shardWant(t, c, r, "$3\r\nyes\r\n", "HGET", key, "keep")
		})
	}
}

func TestMigrationFinalizeCleanupFailureAndLateScripts(t *testing.T) {
	var fail atomic.Bool
	var lostReply atomic.Bool
	fail.Store(true)
	f := newShardRuntimeFixture(t, "update_existing", true, func(address string) string {
		return cacheFaultRelayHooks(t, address, func(a [][]byte) bool {
			return fail.Load() && len(a) > 1 && string(a[0]) == "EVAL" && string(a[1]) == migrationRetireScript
		}, func(a [][]byte, b []byte) bool {
			return !fail.Load() && len(a) > 1 && string(a[1]) == migrationRetireScript && lostReply.CompareAndSwap(false, true)
		})
	})
	c, r := f.client(t)
	key := shardKeyFor(f.m.to, "b", "retirement-failure")
	shardWant(t, c, r, "+OK\r\n", "SET", key, "old")
	if err := f.s.cache.shards.beginTransition(f.m.to, f.m.j); err != nil {
		t.Fatal(err)
	}
	if err := f.s.cache.shards.migrate(context.Background(), []byte(key)); err != nil {
		t.Fatal(err)
	}
	h, _, err := f.m.j.Lookup(context.Background(), []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	s := f.s.cache.shards
	oldToken, err := s.ensure(context.Background(), s.nodes["a"])
	if err != nil {
		t.Fatal(err)
	}
	old, _ := newMigrationFence(f.m.a, h.Placement.Source, s.r.guard, oldToken, h)
	o := fastWorkerOptions()
	o.Finalize = true
	stop := startTestMigrationWorker(t, f, o)
	defer stop()
	awaitFinal(t, f.m.j, sharding.FinalPublished)
	deadline := time.Now().Add(5 * time.Second)
	for string(cacheCall(t, f.m.a, "EXISTS", old.marker)) != ":0\r\n" {
		if time.Now().After(deadline) {
			t.Fatal("source marker not retired")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Source is already clean, target marker cleanup is unavailable. Both
	// new/missing keys and existing cached keys must use only the new topology.
	shardWant(t, c, r, "+OK\r\n", "SET", key, "new")
	shardWant(t, c, r, "$3\r\nnew\r\n", "GET", key)
	newKey := shardKeyFor(f.m.to, "b", "after-publication")
	shardWant(t, c, r, "+OK\r\n", "SET", newKey, "fresh")
	if err := old.freeze(context.Background()); err == nil {
		t.Fatal("late old freeze recreated a marker")
	}
	if string(cacheCall(t, f.m.a, "EXISTS", old.marker)) != ":0\r\n" {
		t.Fatal("late source marker recreated")
	}
	// Capture a cleanup packet from this epoch; final completion must revoke it.
	cleanupToken, err := s.ensure(context.Background(), s.nodes["b"])
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(false)
	awaitFinal(t, f.m.j, sharding.FinalComplete)
	if !lostReply.Load() {
		t.Fatal("cleanup reply loss was not injected")
	}
	b := cacheCall(t, f.m.b, "EVAL", migrationRetireScript, "3", s.r.guard, old.marker, key, cleanupToken, old.binding, "serving")
	if !strings.HasPrefix(string(b), "-TRYAGAIN") {
		t.Fatalf("late cleanup epoch accepted: %q", b)
	}
	shardWant(t, c, r, "$3\r\nnew\r\n", "GET", key)
	shardWant(t, c, r, "$5\r\nfresh\r\n", "GET", newKey)
}

func TestMigrationFinalizeUnknownPublication(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			f := newShardRuntimeFixture(t, "update_existing", true, nil)
			c, r := f.client(t)
			key := shardKeyFor(f.m.to, "b", "unknown-publication")
			shardWant(t, c, r, "+OK\r\n", "SET", key, "value")
			s := f.s.cache.shards
			if err := s.beginTransition(f.m.to, f.m.j); err != nil {
				t.Fatal(err)
			}
			if err := s.migrate(context.Background(), []byte(key)); err != nil {
				t.Fatal(err)
			}
			p, err := f.m.j.BeginWorker(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			p.Phase = sharding.WorkerAwaitFinalize
			if err := f.m.j.SaveWorker(context.Background(), p); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			err = s.rotateMigrationOwnersLocked(context.Background(), s.change)
			if err == nil && committed {
				err = f.m.j.Publish(context.Background())
			}
			s.publicationPending = true // commit result was not delivered to runtime
			s.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if b := shardAPI(t, c, r, "GET", key); !strings.HasPrefix(string(b), "-TRYAGAIN") {
				t.Fatalf("unknown publication served unchecked routing: %q", b)
			}
			// Published recovery cannot depend on enabling new publications.
			o := fastWorkerOptions()
			o.Finalize = !committed
			stop := startTestMigrationWorker(t, f, o)
			defer stop()
			awaitFinal(t, f.m.j, sharding.FinalComplete)
			shardWant(t, c, r, "$5\r\nvalue\r\n", "GET", key)
		})
	}
}
