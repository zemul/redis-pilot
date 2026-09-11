package gateway

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestCacheConfigAndCommands(t *testing.T) {
	base := "mode: cache\nredis_address: 127.0.0.1:16390\nallow_unauthenticated: true\nbuffer_budget_bytes: 2147483648\ncache:\n  cold_address: 127.0.0.1:16666\n  exclusive_backends: true\n  lock_file: /private/cache/owner.lock\n"
	for _, policy := range []string{"invalidate", "update_existing"} {
		if _, err := DecodeConfig(strings.NewReader(base + "  write_policy: " + policy + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	for _, extra := range []string{"  write_policy: async\n", "  max_object_bytes: -1\n", "  max_loads: -1\n", "  ttl: 1ms\n"} {
		if _, err := DecodeConfig(strings.NewReader(base + extra)); err == nil {
			t.Fatal("invalid cache config accepted", extra)
		}
	}
	if _, err := DecodeConfig(strings.NewReader(base + "  types:\n    hash:\n      max_bytes: 32768\n      max_elements: 128\n")); err == nil {
		t.Fatal("removed per-object settings accepted")
	}
	if _, err := DecodeConfig(strings.NewReader(base + "  types:\n    unknown:\n      max_bytes: 32768\n")); err == nil {
		t.Fatal("unknown type accepted")
	}
	for _, args := range [][]string{{"MSET", "a", "1", "b", "2"}, {"SET", "a", "1", "NX", "XX"}, {"SET", "a", "1", "PX", "2", "KEEPTTL"}, {"SET", "a", "1", "EX"}, {"SET", "a", "1", "GET", "GET"}, {"BLPOP", "a", "0"}, {"EVAL", "return 1", "0"}, {"GET", cachePrefix + "owner"}, {"DEL", "a", "b"}} {
		if _, ok := classifyCacheCommand(cacheArgs(args...)); ok {
			t.Fatal("unsupported command accepted", args)
		}
	}
}

func TestCacheColdUnavailableAndRecovery(t *testing.T) {
	s, _, cold := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "outage", "old")
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "outage")
	var down atomic.Bool
	relay := cacheFaultRelay(t, cold, func(a [][]byte) bool { return down.Load() })
	s.cache.cfg.ColdAddress = relay
	down.Store(true)
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "outage")
	for _, args := range [][]string{{"SET", "outage", "new"}, {"GET", "not-cached"}} {
		b := s.cache.execute(context.Background(), cacheArgs(args...))
		if !strings.HasPrefix(string(b), "-TRYAGAIN") {
			t.Fatal(string(b))
		}
	}
	down.Store(false)
	cacheExec(t, s, "+OK\r\n", "SET", "outage", "new")
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "outage")
	cacheExec(t, s, ":1\r\n", "DEL", "outage")
}

func cacheArgs(args ...string) [][]byte {
	a := make([][]byte, len(args))
	for i, s := range args {
		a[i] = []byte(s)
	}
	return a
}

func cacheTestSetup(t *testing.T, policy string) (*Service, string, string) {
	t.Helper()
	hot := g4Redis(t)
	cold := os.Getenv("GATEWAY_TEST_CACHE_COLD_ADDRESS")
	if cold == "" {
		cold = g4Redis(t)
	}
	c, r, err := authenticated(hot, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	writeAll(c, encodeCommand("CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru"))
	expect(t, r, "+OK\r\n")
	c.Close()
	cfg := passConfig(hot)
	cfg.RequestTimeout = time.Second
	cfg.Mode = "cache"
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	password := "g4-backend-test"
	if os.Getenv("GATEWAY_TEST_CACHE_COLD_ADDRESS") != "" {
		password = os.Getenv("GATEWAY_TEST_CACHE_COLD_PASSWORD")
	}
	secret := ""
	if password != "" {
		secret = secretFile(t, password)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Cache = &CacheConfig{ColdAddress: cold, ColdPasswordFile: secret, ExclusiveBackends: true, LockFile: filepath.Join(dir, "owner.lock"), WritePolicy: policy, ColdOnlyPrefixes: []string{"queue:"}}
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.cache.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.cache.close)
	return s, hot, cold
}

func cacheExec(t *testing.T, s *Service, want string, args ...string) {
	t.Helper()
	got := s.cache.execute(context.Background(), cacheArgs(args...))
	if string(got) != want {
		t.Fatalf("%s got %q want %q", args[0], got, want)
	}
}

func TestCacheFiveTypes(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			for _, tc := range []struct {
				write, read, modify []string
				first, second       string
			}{
				{[]string{"SET", "s", "a"}, []string{"GET", "s"}, []string{"SET", "s", "b"}, "$1\r\na\r\n", "$1\r\nb\r\n"},
				{[]string{"HSET", "h", "a", "1", "b", "2"}, []string{"HGET", "h", "a"}, []string{"HSET", "h", "a", "3"}, "$1\r\n1\r\n", "$1\r\n3\r\n"},
				{[]string{"RPUSH", "l", "a", "b"}, []string{"LINDEX", "l", "0"}, []string{"LPOP", "l"}, "$1\r\na\r\n", "$1\r\nb\r\n"},
				{[]string{"SADD", "set", "a", "b"}, []string{"SISMEMBER", "set", "a"}, []string{"SREM", "set", "a"}, ":1\r\n", ":0\r\n"},
				{[]string{"ZADD", "z", "1", "a", "2", "b"}, []string{"ZSCORE", "z", "a"}, []string{"ZADD", "z", "3", "a"}, "$1\r\n1\r\n", "$1\r\n3\r\n"},
			} {
				key := tc.write[1]
				cacheExec(t, s, ":0\r\n", "DEL", key)
				out := s.cache.execute(context.Background(), cacheArgs(tc.write...))
				if out[0] == '-' {
					t.Fatal(string(out))
				}
				cacheExec(t, s, tc.first, tc.read...)
				hits := metricValue(s.cache.events.WithLabelValues("hit"))
				cacheExec(t, s, tc.first, tc.read...)
				if metricValue(s.cache.events.WithLabelValues("hit")) != hits+1 {
					t.Fatal("second read not cached", key)
				}
				out = s.cache.execute(context.Background(), cacheArgs(tc.modify...))
				if out[0] == '-' {
					t.Fatal(string(out))
				}
				cacheExec(t, s, tc.second, tc.read...)
				cacheExec(t, s, ":1\r\n", "DEL", key)
				cacheExec(t, s, "$-1\r\n", "GET", key)
			}
			cacheExec(t, s, "+OK\r\n", "SET", "precise", "9007199254740992")
			cacheExec(t, s, ":9007199254740993\r\n", "INCR", "precise")
			cacheExec(t, s, "$16\r\n9007199254740993\r\n", "GET", "precise")
			cacheExec(t, s, ":1\r\n", "DEL", "precise")
		})
	}
}

func TestCacheBoundsRestartAndFencing(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "invalidate")
	large := strings.Repeat("a", 2048)
	cacheExec(t, s, "+OK\r\n", "SET", "large", large)
	want := fmt.Sprintf("$%d\r\n%s\r\n", len(large), large)
	hits := metricValue(s.cache.events.WithLabelValues("hit"))
	cacheExec(t, s, want, "GET", "large")
	cacheExec(t, s, want, "GET", "large")
	if metricValue(s.cache.events.WithLabelValues("hit")) != hits+1 {
		t.Fatal("large object was not cached")
	}
	cacheExec(t, s, "+OK\r\n", "SET", "restart", "old")
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "restart")
	old := s.cache.token
	other, err := NewService(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = other.cache.start(context.Background()); err == nil {
		other.cache.close()
		t.Fatal("duplicate owner allowed")
	}
	s.cache.close()
	if err = other.cache.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer other.cache.close()
	cacheExec(t, other, "+OK\r\n", "SET", "restart", "new")
	cacheExec(t, other, "$3\r\nnew\r\n", "GET", "restart")
	c, err := other.cache.connect(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := c.call(context.Background(), "EVAL", cacheColdScript, "2", other.cache.guard, "restart", old, "SET", "normal", "late")
	if err != nil || !strings.HasPrefix(string(b), "-TRYAGAIN") {
		t.Fatal("old cold write not fenced", string(b), err)
	}
	b, err = c.call(context.Background(), "EVAL", cacheClaimScript, "1", other.cache.guard, old, "late-token")
	if err != nil || string(b) != ":0\r\n" {
		t.Fatal("old recovery CAS replaced owner", string(b), err)
	}
	cacheExec(t, other, "$3\r\nnew\r\n", "GET", "restart")
	cacheExec(t, other, ":1\r\n", "DEL", "restart")
	cacheExec(t, other, ":1\r\n", "DEL", "large")
}

func TestCacheConcurrentIncrements(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	cacheExec(t, s, "+OK\r\n", "SET", "counter", "0")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				b := s.cache.execute(context.Background(), cacheArgs("INCR", "counter"))
				if len(b) == 0 || b[0] != ':' {
					t.Error(string(b))
				}
			}
		}()
	}
	wg.Wait()
	cacheExec(t, s, "$2\r\n80\r\n", "GET", "counter")
	cacheExec(t, s, ":1\r\n", "DEL", "counter")
}

func TestCacheBlockingColdOnly(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	cacheExec(t, s, ":0\r\n", "DEL", "queue:test")
	result := make(chan []byte, 1)
	go func() { result <- s.cache.execute(context.Background(), cacheArgs("BLPOP", "queue:test", "2")) }()
	deadline := time.Now().Add(time.Second)
	for len(s.cache.blocking) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("blocking lane not entered")
		}
		time.Sleep(time.Millisecond)
	}
	cacheExec(t, s, ":1\r\n", "LPUSH", "queue:test", "value")
	select {
	case b := <-result:
		if string(b) != "*2\r\n$10\r\nqueue:test\r\n$5\r\nvalue\r\n" {
			t.Fatal(string(b))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer blocked behind consumer")
	}
	cacheExec(t, s, ":0\r\n", "LLEN", "queue:test")
	if metricValue(s.cache.events.WithLabelValues("filled")) != 0 {
		t.Fatal("cold-only queue was cached")
	}
	cacheExec(t, s, "*-1\r\n", "BRPOP", "queue:test", "0.05")
	for _, args := range [][]string{{"BLPOP", "normal", "1"}, {"BLPOP", "queue:test", "0"}, {"BRPOP", "queue:test", "NaN"}} {
		b := s.cache.execute(context.Background(), cacheArgs(args...))
		if b[0] != '-' {
			t.Fatal("invalid blocking accepted")
		}
	}
}

// A transparent RESP fault relay. Returning true drops the command before
// dispatch; a hook may delay dispatch to exercise late-command fencing.
func cacheFaultRelay(t *testing.T, target string, hook func([][]byte) bool) string {
	return cacheFaultRelayHooks(t, target, hook, nil)
}

func cacheFaultRelayHooks(t *testing.T, target string, hook func([][]byte) bool, after func([][]byte, []byte) bool) string {
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
			client, e := ln.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				up, e := net.DialTimeout("tcp", target, time.Second)
				if e != nil {
					return
				}
				defer up.Close()
				stop := context.AfterFunc(ctx, func() { client.Close(); up.Close() })
				defer stop()
				reader, _ := NewReader(client, DefaultConfig().Limits)
				replies, _ := NewReader(up, DefaultConfig().Limits)
				for {
					raw, args, e := reader.ReadCommand()
					if e != nil {
						return
					}
					if hook != nil && hook(args) {
						return
					}
					if writeAll(up, raw) != nil {
						return
					}
					b, e := replies.ReadFrame()
					if e == nil && after != nil && after(args, b) {
						return
					}
					if e != nil || writeAll(client, b) != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); cancel(); wg.Wait() })
	return ln.Addr().String()
}

func TestCacheHotInvalidationFailureRecovers(t *testing.T) {
	s, hot, _ := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "fault", "old")
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "fault")
	var dropped atomic.Bool
	relay := cacheFaultRelay(t, hot, func(a [][]byte) bool {
		return len(a) > 0 && string(a[0]) == "DEL" && dropped.CompareAndSwap(false, true)
	})
	s.cfg.RedisAddress = relay
	cacheExec(t, s, "+OK\r\n", "SET", "fault", "new")
	if !dropped.Load() || s.cache.hotReady.Load() {
		t.Fatal("invalidation fault not isolated")
	}
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "fault")
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "fault")
	cacheExec(t, s, ":1\r\n", "DEL", "fault")
}

func TestCacheLateColdWriteFenced(t *testing.T) {
	s, _, cold := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "late", "old")
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "late")
	var once atomic.Bool
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	entered := make(chan struct{})
	relay := cacheFaultRelay(t, cold, func(a [][]byte) bool {
		if len(a) > 7 && string(a[0]) == "EVAL" && string(a[4]) == "late" && string(a[6]) == "SET" && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return false
	})
	s.cache.cfg.ColdAddress = relay
	s.cfg.RequestTimeout = 200 * time.Millisecond
	done := make(chan []byte, 1)
	go func() { done <- s.cache.execute(context.Background(), cacheArgs("SET", "late", "stale")) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("write did not reach relay")
	}
	select {
	case b := <-done:
		if !strings.Contains(string(b), "result unknown") {
			close(release)
			t.Fatal(string(b))
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("write did not time out")
	}
	// Install a new fencing token before allowing the delayed old command.
	cacheExec(t, s, "+OK\r\n", "SET", "late", "new")
	close(release)
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "late")
	cacheExec(t, s, ":1\r\n", "DEL", "late")
}

func TestCacheFrontendPipeline(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "invalidate")
	s.cache.close()
	_, address, _ := startTestGateway(t, s.cfg)
	c, r, err := authenticated(address, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, a := range [][]string{{"SET", "pipeline", "1"}, {"INCR", "pipeline"}, {"GET", "pipeline"}, {"DEL", "pipeline"}} {
		if err := writeAll(c, encodeCommand(a...)); err != nil {
			t.Fatal(err)
		}
	}
	expect(t, r, "+OK\r\n")
	expect(t, r, ":2\r\n")
	expect(t, r, "$1\r\n2\r\n")
	expect(t, r, ":1\r\n")
}

func TestCacheExecutedWriteLostReply(t *testing.T) {
	s, _, cold := cacheTestSetup(t, "update_existing")
	cacheExec(t, s, "+OK\r\n", "SET", "lost-ack", "0")
	var once atomic.Bool
	relay := cacheFaultRelay(t, cold, func(a [][]byte) bool {
		if len(a) > 7 && string(a[0]) == "EVAL" && string(a[4]) == "lost-ack" && string(a[6]) == "INCR" && once.CompareAndSwap(false, true) {
			c, rd, e := authenticated(cold, s.cache.coldPassword)
			if e != nil {
				t.Error(e)
				return true
			}
			defer c.Close()
			args := make([]string, len(a))
			for i, v := range a {
				args[i] = string(v)
			}
			if e := writeAll(c, encodeCommand(args...)); e != nil {
				t.Error(e)
				return true
			}
			b, e := rd.ReadFrame()
			if e != nil || string(b) != "$1\r\n1\r\n" {
				t.Errorf("injected write: %q %v", b, e)
			}
			return true // execution completed, but original Gateway gets no reply
		}
		return false
	})
	s.cache.cfg.ColdAddress = relay
	b := s.cache.execute(context.Background(), cacheArgs("INCR", "lost-ack"))
	if !strings.Contains(string(b), "result unknown") {
		t.Fatal(string(b))
	}
	cacheExec(t, s, "$1\r\n1\r\n", "GET", "lost-ack")
	cacheExec(t, s, ":1\r\n", "DEL", "lost-ack")
}

func TestCacheComplexBoundsAndImmutableQueuePolicy(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	big := strings.Repeat("x", 2048)
	cacheExec(t, s, ":1\r\n", "HSET", "wide-hash", "f", big)
	fills := metricValue(s.cache.events.WithLabelValues("filled"))
	cacheExec(t, s, ":1\r\n", "HLEN", "wide-hash")
	if metricValue(s.cache.events.WithLabelValues("filled")) != fills+1 {
		t.Fatal("large hash was not cached")
	}
	cacheExec(t, s, ":1\r\n", "DEL", "wide-hash")
	// Already-expiring cold objects are never published with a longer cache TTL.
	c, err := s.cache.connect(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := c.call(context.Background(), "SET", "expiring", "x", "PX", "2000")
	if err != nil || string(b) != "+OK\r\n" {
		t.Fatal(string(b), err)
	}
	cacheExec(t, s, "$1\r\nx\r\n", "GET", "expiring")
	if metricValue(s.cache.events.WithLabelValues("filled")) != fills+2 {
		t.Fatal("expiring small object not cached")
	}
	cacheExec(t, s, ":1\r\n", "DEL", "expiring")
	s.cache.close()
	cfg := s.cfg
	copy := *cfg.Cache
	copy.ColdOnlyPrefixes = []string{"other:"}
	cfg.Cache = &copy
	next, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.cache.start(context.Background()); err == nil {
		next.cache.close()
		t.Fatal("cold-only policy changed in place")
	}
}

func TestCacheProcessRestart(t *testing.T) {
	binary := os.Getenv("GATEWAY_TEST_CACHE_BINARY")
	if binary == "" {
		t.Skip("set GATEWAY_TEST_CACHE_BINARY for process restart coverage")
	}
	s, _, _ := cacheTestSetup(t, "update_existing")
	s.cache.close()
	cfg := s.cfg
	cfg.Listen = freeAddress(t)
	cfg.AdminListen = freeAddress(t)
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	start := func() *exec.Cmd {
		p := exec.Command(binary, "-config", file)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.Process.Kill(); p.Wait() })
		deadline := time.Now().Add(5 * time.Second)
		for {
			c, r, e := authenticated(cfg.Listen, "")
			if e == nil {
				c.SetDeadline(time.Now().Add(300 * time.Millisecond))
				writeAll(c, encodeCommand("PING"))
				b, e := r.ReadFrame()
				c.Close()
				if e == nil && string(b) == "+PONG\r\n" {
					return p
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("cache Gateway process did not start")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	request := func(want string, args ...string) {
		t.Helper()
		c, r, e := authenticated(cfg.Listen, "")
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(2 * time.Second))
		if err := writeAll(c, encodeCommand(args...)); err != nil {
			t.Fatal(err)
		}
		expect(t, r, want)
	}
	first := start()
	request("+OK\r\n", "SET", "process-restart", "old")
	request("$3\r\nold\r\n", "GET", "process-restart")
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	first.Wait()
	// While the old Gateway is stopped, change cold state to make any reuse
	// of its surviving hot namespace visibly wrong.
	c, err := s.cache.connect(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.call(context.Background(), "SET", "process-restart", "new")
	c.Close()
	if err != nil || string(b) != "+OK\r\n" {
		t.Fatal(string(b), err)
	}
	second := start()
	request("$3\r\nnew\r\n", "GET", "process-restart")
	request(":1\r\n", "DEL", "process-restart")
	second.Process.Kill()
	second.Wait()
}
