package gateway

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheMultiCommands(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			oracle := cacheOracle(t, s)
			a, b, h := "multi-a", "multi-b", "multi-h"
			defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b, h))
			ops := [][]string{
				{"DEL", a, b, h}, {"MGET", a, b, a, h},
				{"MSET", a, "one", b, "two"}, {"MGET", b, a, b}, {"MGET", b, a, b},
				{"EXISTS", a, b, a, h}, {"PEXPIRE", a, "60000"},
				{"MSET", a, "new", b, "\x00\xff\r\n"}, {"PTTL", a}, {"MGET", a, b},
				{"HSET", h, "f", "v"}, {"HGET", h, "f"}, {"MGET", a, h, b}, {"MGET", a, h, b},
				{"MSETNX", a, "ignored", h, "ignored"}, {"MGET", a, h, b},
				{"MSET", h, "now-string", a, "first", a, "last"}, {"MGET", a, h, b, a},
				{"UNLINK", a, b, a}, {"EXISTS", a, b, h, h}, {"DEL", h, h, a},
				{"MSETNX", a, "first", b, "two", a, "last"}, {"MGET", a, b, a},
				{"MSETNX", a, "ignored", "multi-missing", "ignored"}, {"MGET", "multi-missing", a},
				{"DEL", a, b, h, "multi-missing"},
				{"MSET", "", "empty-key", "multi-\xff", "binary-key"},
				{"MGET", "multi-\xff", "", "multi-\xff"},
				{"MGET", ""},
				{"DEL", "", "multi-\xff"},
			}
			for _, op := range ops {
				got := s.cache.execute(context.Background(), cacheArgs(op...))
				want := cacheCall(t, oracle, op...)
				if string(got) != string(want) {
					t.Fatalf("%v got %q want %q", op, got, want)
				}
			}
		})
	}
}

func TestCacheMultiHotPolicies(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			a, b := "multi-hot-a", "multi-hot-b"
			defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b, "queue:multi"))
			cacheExec(t, s, "+OK\r\n", "MSET", a, "old", b, "old")
			old := "*2\r\n$3\r\nold\r\n$3\r\nold\r\n"
			cacheExec(t, s, old, "MGET", a, b)
			hits := metricValue(s.cache.events.WithLabelValues("hit"))
			cacheExec(t, s, old, "MGET", a, b)
			if metricValue(s.cache.events.WithLabelValues("hit")) != hits+1 {
				t.Fatal("all-hot MGET missed")
			}
			hot, e := s.cache.connect(context.Background(), true)
			if e != nil {
				t.Fatal(e)
			}
			defer hot.Close()
			cacheCall(t, hot, "DEL", b)
			hits = metricValue(s.cache.events.WithLabelValues("hit"))
			cacheExec(t, s, old, "MGET", a, b)
			if metricValue(s.cache.events.WithLabelValues("hit")) != hits {
				t.Fatal("mixed MGET assembled hot/cold results")
			}
			// MSETNX no-op must retain both the old cache and business TTL.
			cacheExec(t, s, ":1\r\n", "PEXPIRE", a, "60000")
			cacheExec(t, s, old, "MGET", a, b)
			cacheExec(t, s, ":0\r\n", "MSETNX", a, "ignored", b, "ignored")
			if v := cacheCall(t, hot, "MGET", a, b); string(v) != old {
				t.Fatalf("no-op changed hot: %q", v)
			}
			cacheExec(t, s, "+OK\r\n", "MSET", a, "new", b, "new")
			want := "*2\r\n$3\r\nnew\r\n$3\r\nnew\r\n"
			if policy == "update_existing" {
				if v := cacheCall(t, hot, "MGET", a, b); string(v) != want {
					t.Fatalf("batch hot update %q", v)
				}
			} else if v := cacheCall(t, hot, "EXISTS", a, b); string(v) != ":0\r\n" {
				t.Fatalf("batch invalidation %q", v)
			}
			cacheExec(t, s, ":-1\r\n", "PTTL", a)
			cacheExec(t, s, want, "MGET", a, b)
			cacheExec(t, s, "+OK\r\n", "MSET", a, "normal", "queue:multi", "cold-only")
			if v := cacheCall(t, hot, "EXISTS", "queue:multi"); string(v) != ":0\r\n" {
				t.Fatal("cold-only prefix cached")
			}
			hits = metricValue(s.cache.events.WithLabelValues("hit"))
			cacheExec(t, s, "*2\r\n$6\r\nnormal\r\n$9\r\ncold-only\r\n", "MGET", a, "queue:multi")
			if metricValue(s.cache.events.WithLabelValues("hit")) != hits {
				t.Fatal("cold-only batch hit hot")
			}
		})
	}
}

func TestCacheMultiValidation(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	key := "multi-validation"
	cacheExec(t, s, "+OK\r\n", "SET", key, "old")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
	for _, op := range [][]string{
		{"MSET", key, "new", "unpaired"}, {"MSET"}, {"MSETNX", key}, {"MGET"},
		{"MGET", key, cachePrefix + "owner"}, {"MSET", key, "new", cachePrefix + "owner", "bad"},
		{"MSETNX", key, "new", strings.Repeat("x", 4097), "bad"},
		{"DEL", key, cachePrefix + "owner"}, {"UNLINK", key, cachePrefix + "owner"}, {"EXISTS", key, cachePrefix + "owner"},
	} {
		if b := s.cache.execute(context.Background(), cacheArgs(op...)); len(b) == 0 || b[0] != '-' {
			t.Fatalf("invalid batch accepted: %q", b)
		}
		cacheExec(t, s, "$3\r\nold\r\n", "GET", key)
	}
	// Values, unlike key positions, may contain the reserved prefix.
	cacheExec(t, s, "+OK\r\n", "MSET", key, cachePrefix+"value")
}

func TestCacheMultiLockOrderAndCancellation(t *testing.T) {
	r := &cacheRuntime{}
	for i := range r.keys {
		r.keys[i] = make(chan struct{}, 1)
	}
	var low, high, collision []byte
	for i := 0; high == nil || collision == nil; i++ {
		k := []byte(fmt.Sprintf("lock-%d", i))
		h := sha256.Sum256(k)
		if h[0] == 0 {
			if low == nil {
				low = k
			} else {
				collision = k
			}
		}
		if h[0] == 255 {
			high = k
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, e := r.acquireKeys(ctx, [][]byte{high, low, low, collision})
	if e != nil {
		t.Fatal(e)
	}
	release()
	// Cancellation after acquiring bucket 0 must release it, even while 255 is busy.
	r.keys[255] <- struct{}{}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, e := r.acquireKeys(short, [][]byte{high, low}); e == nil {
		t.Fatal("blocked lock accepted")
	}
	if len(r.keys[0]) != 0 {
		t.Fatal("partial lock set leaked")
	}
	<-r.keys[255]
	var wg sync.WaitGroup
	for _, keys := range [][][]byte{{low, high}, {high, low}} {
		wg.Add(1)
		go func(keys [][]byte) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				release, e := r.acquireKeys(ctx, keys)
				if e != nil {
					t.Error(e)
					return
				}
				release()
			}
		}(keys)
	}
	wg.Wait()
}

func TestCacheMultiAtomicConcurrency(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			s.cfg.RequestTimeout = 5 * time.Second
			a, b := "multi-atomic-a", "multi-atomic-b"
			cacheExec(t, s, "+OK\r\n", "MSET", a, "initial", b, "initial")
			defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
			var wg sync.WaitGroup
			for worker := 0; worker < 4; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					for i := 0; i < 25; i++ {
						if worker < 2 {
							v := fmt.Sprintf("%d-%d", worker, i)
							args := []string{"MSET", a, v, b, v}
							if worker == 1 {
								args = []string{"MSET", b, v, a, v}
							}
							if out := s.cache.execute(context.Background(), cacheArgs(args...)); string(out) != "+OK\r\n" {
								t.Errorf("write %q", out)
								return
							}
						} else {
							out := s.cache.execute(context.Background(), cacheArgs("MGET", a, b, a))
							p, e := frameParts(out)
							if e != nil || len(p) != 3 || string(p[0]) != string(p[1]) || string(p[0]) != string(p[2]) {
								t.Errorf("torn MGET: %q", out)
								return
							}
						}
					}
				}(worker)
			}
			wg.Wait()
		})
	}
}

func TestCacheMultiHoldsSingleKeyReaders(t *testing.T) {
	s, addr, _ := cacheTestSetup(t, "update_existing")
	s.cfg.RequestTimeout = 3 * time.Second
	a, b := "multi-held-a", "multi-held-b"
	cacheExec(t, s, "+OK\r\n", "MSET", a, "old", b, "old")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	s.cfg.RedisAddress = cacheFaultRelay(t, addr, func(args [][]byte) bool {
		if len(args) > 3 && string(args[1]) == cacheUpdateScript && string(args[3]) == b && once.CompareAndSwap(false, true) {
			close(entered)
			<-proceed
		}
		return false
	})
	done := make(chan []byte, 1)
	go func() { done <- s.cache.execute(context.Background(), cacheArgs("MSET", a, "new", b, "new")) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(proceed)
		t.Fatal("did not reach second hot update")
	}
	read := make(chan []byte, 1)
	go func() { read <- s.cache.execute(context.Background(), cacheArgs("GET", b)) }()
	select {
	case v := <-read:
		close(proceed)
		t.Fatalf("single-key read escaped batch lock: %q", v)
	case <-time.After(20 * time.Millisecond):
	}
	close(proceed)
	if v := <-done; string(v) != "+OK\r\n" {
		t.Fatal(string(v))
	}
	if v := <-read; string(v) != "$3\r\nnew\r\n" {
		t.Fatal(string(v))
	}
}

func TestCacheMultiPartialHotFailure(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, addr, _ := cacheTestSetup(t, policy)
			a, b := "multi-fail-a", "multi-fail-b"
			cacheExec(t, s, "+OK\r\n", "MSET", a, "old", b, "old")
			s.cache.execute(context.Background(), cacheArgs("MGET", a, b))
			defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
			var dropped atomic.Bool
			s.cfg.RedisAddress = cacheFaultRelay(t, addr, func(args [][]byte) bool {
				match := len(args) > 3 && string(args[1]) == cacheUpdateScript && string(args[3]) == b
				if policy == "invalidate" {
					match = len(args) == 3 && string(args[0]) == "DEL" && string(args[1]) == a
				}
				return match && dropped.CompareAndSwap(false, true)
			})
			cacheExec(t, s, "+OK\r\n", "MSET", a, "new", b, "new")
			if !dropped.Load() || s.cache.hotReady.Load() {
				t.Fatal("partial hot failure not isolated")
			}
			cacheExec(t, s, "*2\r\n$3\r\nnew\r\n$3\r\nnew\r\n", "MGET", a, b)
		})
	}
}

func TestCacheMultiLostColdReply(t *testing.T) {
	s, _, addr := cacheTestSetup(t, "update_existing")
	a, b := "multi-lost-a", "multi-lost-b"
	cacheExec(t, s, "+OK\r\n", "MSET", a, "old", b, "old")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
	var dropped atomic.Bool
	s.cache.cfg.ColdAddress = cacheFaultRelay(t, addr, func(args [][]byte) bool {
		if len(args) > 1 && string(args[1]) == cacheMultiColdScript && dropped.CompareAndSwap(false, true) {
			c, reader, e := authenticated(addr, s.cache.coldPassword)
			if e != nil {
				t.Error(e)
				return true
			}
			defer c.Close()
			values := make([]string, len(args))
			for i := range args {
				values[i] = string(args[i])
			}
			if e := writeAll(c, encodeCommand(values...)); e != nil {
				t.Error(e)
				return true
			}
			reply, e := reader.ReadFrame()
			v, ok := frameString(reply)
			if e != nil || !ok || v != "OK" {
				t.Errorf("injected reply %q %v", reply, e)
			}
			return true
		}
		return false
	})
	out := s.cache.execute(context.Background(), cacheArgs("MSET", a, "new", b, "new"))
	if !strings.Contains(string(out), "result unknown") {
		t.Fatalf("lost batch reply %q", out)
	}
	cacheExec(t, s, "*2\r\n$3\r\nnew\r\n$3\r\nnew\r\n", "MGET", a, b)
}

func TestCacheMultiFrontendPipeline(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			s.cache.close()
			_, addr, _ := startTestGateway(t, s.cfg)
			c, reader, e := authenticated(addr, "")
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			for _, op := range [][]string{{"MSET", "multi-pipe-a", "1", "multi-pipe-b", "2"}, {"MGET", "multi-pipe-b", "multi-pipe-a"}, {"MSETNX", "multi-pipe-a", "x", "multi-pipe-b", "y"}, {"DEL", "multi-pipe-a", "multi-pipe-b"}} {
				if e := writeAll(c, encodeCommand(op...)); e != nil {
					t.Fatal(e)
				}
			}
			for _, want := range []string{"+OK\r\n", "*2\r\n$1\r\n2\r\n$1\r\n1\r\n", ":0\r\n", ":2\r\n"} {
				expect(t, reader, want)
			}
		})
	}
}

func TestCacheMultiNXContended(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	a, b := "multi-nx-race-a", "multi-nx-race-b"
	s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
	defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := fmt.Sprint(i)
			out := s.cache.execute(context.Background(), cacheArgs("MSETNX", a, v, b, v))
			if string(out) == ":1\r\n" {
				successes.Add(1)
			} else if string(out) != ":0\r\n" {
				t.Errorf("MSETNX %q", out)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful contenders: %d", successes.Load())
	}
	out := s.cache.execute(context.Background(), cacheArgs("MGET", a, b))
	parts, e := frameParts(out)
	if e != nil || len(parts) != 2 || string(parts[0]) != string(parts[1]) {
		t.Fatalf("partial NX batch: %q", out)
	}
}

func TestCacheMultiOwnerFence(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	a, b := "multi-fence-a", "multi-fence-b"
	cacheExec(t, s, "+OK\r\n", "MSET", a, "current", b, "current")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
	old := s.cache.token
	s.cache.uncertainEpoch(old)
	if _, e := s.cache.ensureEpoch(context.Background()); e != nil {
		t.Fatal(e)
	}
	cold, e := s.cache.connect(context.Background(), false)
	if e != nil {
		t.Fatal(e)
	}
	defer cold.Close()
	out := cacheCall(t, cold, "EVAL", cacheMultiColdScript, "3", s.cache.guard, a, b, old, "MSET", a, "late", b, "late")
	if !strings.HasPrefix(string(out), "-TRYAGAIN") {
		t.Fatalf("old batch not fenced: %q", out)
	}
	cacheExec(t, s, "*2\r\n$7\r\ncurrent\r\n$7\r\ncurrent\r\n", "MGET", a, b)
}

func TestCacheMultiColdOutage(t *testing.T) {
	s, _, addr := cacheTestSetup(t, "update_existing")
	a, b := "multi-outage-a", "multi-outage-b"
	cacheExec(t, s, "+OK\r\n", "MSET", a, "a", b, "b")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", a, b))
	var down atomic.Bool
	s.cache.cfg.ColdAddress = cacheFaultRelay(t, addr, func(_ [][]byte) bool { return down.Load() })
	defer down.Store(false)
	down.Store(true)
	want := "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	cacheExec(t, s, want, "MGET", a, b) // trusted all-hot batch remains available
	hot, e := s.cache.connect(context.Background(), true)
	if e != nil {
		t.Fatal(e)
	}
	cacheCall(t, hot, "DEL", b)
	hot.Close()
	out := s.cache.execute(context.Background(), cacheArgs("MGET", a, b))
	if !strings.HasPrefix(string(out), "-TRYAGAIN") {
		t.Fatalf("partial batch returned during outage: %q", out)
	}
	down.Store(false)
	cacheExec(t, s, want, "MGET", a, b)
}
