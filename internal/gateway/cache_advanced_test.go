package gateway

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func cacheSortedArray(t *testing.T, b []byte) []byte {
	t.Helper()
	p, e := frameParts(b)
	if e != nil {
		t.Fatal(string(b))
	}
	sort.Slice(p, func(i, j int) bool { return string(p[i]) < string(p[j]) })
	return cacheArray(p)
}

func TestCacheCrossCompatibility(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			oracle := cacheOracle(t, s)
			defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:a", "adv:b", "adv:c"))
			ops := [][]string{
				{"DEL", "adv:a", "adv:b", "adv:c"}, {"HSET", "adv:a", "field", "old", "keep", "yes"}, {"HGETALL", "adv:a"},
				{"RENAME", "adv:a", "adv:b"}, {"HGETALL", "adv:b"}, {"HSET", "adv:b", "field", "new"}, {"HGET", "adv:b", "field"},
				{"SET", "adv:a", "exists"}, {"RENAMENX", "adv:b", "adv:a"}, {"GET", "adv:a"}, {"RENAMENX", "adv:b", "adv:c"}, {"HGETALL", "adv:c"},
				{"DEL", "adv:a", "adv:b", "adv:c"}, {"RPUSH", "adv:a", "a", "b", "c"}, {"RPUSH", "adv:b", "x"}, {"LRANGE", "adv:a", "0", "-1"}, {"LRANGE", "adv:b", "0", "-1"},
				{"LMOVE", "adv:a", "adv:b", "LEFT", "RIGHT"}, {"LRANGE", "adv:a", "0", "-1"}, {"LRANGE", "adv:b", "0", "-1"},
				{"RPOPLPUSH", "adv:a", "adv:b"}, {"LRANGE", "adv:b", "0", "-1"}, {"LMOVE", "adv:b", "adv:b", "RIGHT", "LEFT"}, {"LRANGE", "adv:b", "0", "-1"},
				{"DEL", "adv:a", "adv:b", "adv:c"}, {"SADD", "adv:a", "a"}, {"SADD", "adv:b", "b"}, {"SMEMBERS", "adv:a"}, {"SMEMBERS", "adv:b"},
				{"SMOVE", "adv:a", "adv:b", "a"}, {"SCARD", "adv:a"}, {"SCARD", "adv:b"}, {"SMOVE", "adv:b", "adv:b", "a"}, {"SINTER", "adv:b", "adv:b"},
				{"SINTERCARD", "2", "adv:b", "adv:b", "LIMIT", "1"},
				{"SUNIONSTORE", "adv:c", "adv:a", "adv:b"}, {"SCARD", "adv:c"}, {"SDIFFSTORE", "adv:c", "adv:b", "adv:b"}, {"SCARD", "adv:c"},
				{"DEL", "adv:a", "adv:b", "adv:c"}, {"ZADD", "adv:a", "1", "a", "2", "b"}, {"ZADD", "adv:b", "3", "a"},
				{"ZUNION", "2", "adv:a", "adv:b", "WITHSCORES"}, {"ZINTER", "2", "adv:a", "adv:b", "WEIGHTS", "2", "3", "WITHSCORES"},
				{"ZINTERCARD", "2", "adv:a", "adv:b"},
				{"ZDIFFSTORE", "adv:c", "2", "adv:a", "adv:b"}, {"ZRANGE", "adv:c", "0", "-1", "WITHSCORES"},
				{"ZRANGESTORE", "adv:c", "adv:a", "0", "0"}, {"ZRANGE", "adv:c", "0", "-1", "WITHSCORES"},
			}
			for _, a := range ops {
				got := s.cache.execute(context.Background(), cacheArgs(a...))
				want := cacheCall(t, oracle, a...)
				if a[0] == "SINTER" {
					got = cacheSortedArray(t, got)
					want = cacheSortedArray(t, want)
				}
				if string(got) != string(want) {
					t.Fatalf("%v got %q want %q", a, got, want)
				}
			}
		})
	}
}

func TestCacheUserScript(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, hotAddr, _ := cacheTestSetup(t, policy)
			defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:script", "adv:escape"))
			cacheExec(t, s, ":2\r\n", "HSET", "adv:script", "f", "old", "keep", "yes")
			cacheExec(t, s, "$3\r\nold\r\n", "HGET", "adv:script", "f")
			source := `redis.call('HSET',KEYS[1],'f',ARGV[1]);return redis.call('HGET',KEYS[1],'f')`
			cacheExec(t, s, "$3\r\nnew\r\n", "EVAL", source, "1", "adv:script", "new")
			if policy == "update_existing" {
				c, r, e := authenticated(hotAddr, "g4-backend-test")
				if e != nil {
					t.Fatal(e)
				}
				writeAll(c, encodeCommand("HGET", "adv:script", "keep"))
				expect(t, r, "$3\r\nyes\r\n")
				c.Close()
			}
			cacheExec(t, s, "$3\r\nnew\r\n", "HGET", "adv:script", "f")
			cacheExec(t, s, "$4\r\nnext\r\n", "EVALSHA", cacheScriptSHA(source), "1", "adv:script", "next")
			cacheExec(t, s, ":1\r\n", "EVAL_RO", `return redis.call('HEXISTS',KEYS[1],'keep')`, "1", "adv:script")
			for _, script := range []string{
				`return redis.call('SET','adv:escape','bad')`,
				`return _G.redis.call('GET','__pilot_cache_v1__:owner')`,
				`return loadstring("return redis.call('GET','__pilot_cache_v1__:owner')")()`,
				`return getfenv(0).redis.call('GET','__pilot_cache_v1__:owner')`,
				`getmetatable('').__index.upper=function()return 'GET' end;return 1`,
				`return redis.call('FLUSHDB')`,
			} {
				b := s.cache.execute(context.Background(), cacheArgs("EVAL", script, "1", "adv:script"))
				if len(b) == 0 || b[0] != '-' {
					t.Fatalf("escape accepted %s: %q", script, b)
				}
			}
			cacheExec(t, s, "$-1\r\n", "GET", "adv:escape")
			b := s.cache.execute(context.Background(), cacheArgs("EVAL", `redis.call('HSET',KEYS[1],'f','after-error');error('intentional')`, "1", "adv:script"))
			if !strings.HasPrefix(string(b), "-ERR") {
				t.Fatal(string(b))
			}
			cacheExec(t, s, "$11\r\nafter-error\r\n", "HGET", "adv:script", "f")
			cacheExec(t, s, "+OK\r\n", "SCRIPT", "FLUSH")
			b = s.cache.execute(context.Background(), cacheArgs("EVALSHA", cacheScriptSHA(source), "1", "adv:script", "bad"))
			if !strings.HasPrefix(string(b), "-NOSCRIPT") {
				t.Fatal(string(b))
			}
			cacheExec(t, s, string(cacheBulk(cacheScriptSHA(source))), "SCRIPT", "LOAD", source)
			cacheExec(t, s, "*1\r\n:1\r\n", "SCRIPT", "EXISTS", cacheScriptSHA(source))
		})
	}
}

func TestCacheTransactions(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			client := &cacheSession{r: s.cache}
			defer client.close()
			defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:tx", "adv:hash"))
			run := func(want string, a ...string) {
				t.Helper()
				got := client.execute(context.Background(), cacheArgs(a...))
				if string(got) != want {
					t.Fatalf("%v got %q want %q", a, got, want)
				}
			}
			cacheExec(t, s, "+OK\r\n", "SET", "adv:tx", "old")
			cacheExec(t, s, "$3\r\nold\r\n", "GET", "adv:tx")
			run("+OK\r\n", "MULTI")
			run("+QUEUED\r\n", "SET", "adv:tx", "new")
			run("+QUEUED\r\n", "GET", "adv:tx")
			run("*2\r\n+OK\r\n$3\r\nnew\r\n", "EXEC")
			cacheExec(t, s, "$3\r\nnew\r\n", "GET", "adv:tx")
			run("+OK\r\n", "WATCH", "adv:tx")
			cacheExec(t, s, "+OK\r\n", "SET", "adv:tx", "changed")
			run("+OK\r\n", "MULTI")
			run("+QUEUED\r\n", "SET", "adv:tx", "wrong")
			run("*-1\r\n", "EXEC")
			cacheExec(t, s, "$7\r\nchanged\r\n", "GET", "adv:tx")
			run("+OK\r\n", "WATCH", "adv:tx")
			run("+OK\r\n", "MULTI")
			run("+QUEUED\r\n", "SET", "adv:tx", "7")
			run("+QUEUED\r\n", "INCR", "adv:tx")
			run("*2\r\n+OK\r\n:8\r\n", "EXEC")
			cacheExec(t, s, "$1\r\n8\r\n", "GET", "adv:tx")
			run("+OK\r\n", "MULTI")
			bad := client.execute(context.Background(), cacheArgs("SET", "adv:tx"))
			if len(bad) == 0 || bad[0] != '-' {
				t.Fatal(string(bad))
			}
			run("-EXECABORT Transaction discarded because of previous errors.\r\n", "EXEC")
			cacheExec(t, s, ":2\r\n", "HSET", "adv:hash", "f", "0", "keep", "yes")
			cacheExec(t, s, "$1\r\n0\r\n", "HGET", "adv:hash", "f")
			run("+OK\r\n", "MULTI")
			run("+QUEUED\r\n", "HINCRBY", "adv:hash", "f", "1")
			run("+QUEUED\r\n", "HINCRBY", "adv:hash", "f", "1")
			run("*2\r\n:1\r\n:2\r\n", "EXEC")
			cacheExec(t, s, "$1\r\n2\r\n", "HGET", "adv:hash", "f")
			run("+OK\r\n", "MULTI")
			run("+QUEUED\r\n", "EVAL", `return redis.call('HINCRBY',KEYS[1],'f',1)`, "1", "adv:hash")
			run("*1\r\n:3\r\n", "EXEC")
			cacheExec(t, s, "$1\r\n3\r\n", "HGET", "adv:hash", "f")
		})
	}
}

func TestCacheKeyspaceScan(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:scan"))
	cacheExec(t, s, "+OK\r\n", "SET", "adv:scan", "value")
	seen := false
	cursor := "0"
	for i := 0; i < 1000; i++ {
		b := s.cache.execute(context.Background(), cacheArgs("SCAN", cursor, "COUNT", "3"))
		p, e := frameParts(b)
		if e != nil || len(p) != 2 {
			t.Fatal(string(b))
		}
		cursor, _ = frameString(p[0])
		keys, e := frameParts(p[1])
		if e != nil {
			t.Fatal(e)
		}
		for _, raw := range keys {
			k, _ := frameString(raw)
			if strings.HasPrefix(k, cachePrefix) {
				t.Fatal("internal key exposed", k)
			}
			if k == "adv:scan" {
				seen = true
			}
		}
		if cursor == "0" {
			break
		}
		if i == 999 {
			t.Fatal("scan did not terminate")
		}
	}
	if !seen {
		t.Fatal("business key not scanned")
	}
	b := s.cache.execute(context.Background(), cacheArgs("KEYS", cachePrefix+"*"))
	if string(b) != "*0\r\n" {
		t.Fatal(fmt.Sprintf("internal keys: %q", b))
	}
}

func TestCacheScriptTranscriptIsPrivate(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:transcript"))
	cacheExec(t, s, ":3\r\n", "SADD", "adv:transcript", "a", "b", "c")
	s.cache.execute(context.Background(), cacheArgs("SMEMBERS", "adv:transcript"))
	cacheExec(t, s, ":1\r\n", "EVAL", `local x=redis.call('SPOP',KEYS[1],1);x[1]='forged';return 1`, "1", "adv:transcript")
	cold, e := s.cache.connect(context.Background(), false)
	if e != nil {
		t.Fatal(e)
	}
	defer cold.Close()
	want := cacheSortedArray(t, cacheCall(t, cold, "SMEMBERS", "adv:transcript"))
	got := cacheSortedArray(t, s.cache.execute(context.Background(), cacheArgs("SMEMBERS", "adv:transcript")))
	if string(got) != string(want) {
		t.Fatalf("script changed private mutation transcript: %q != %q", got, want)
	}
}

func TestCacheAdvancedHotFailure(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		for _, kind := range []string{"transaction", "script", "rename"} {
			t.Run(policy+"/"+kind, func(t *testing.T) {
				s, hot, _ := cacheTestSetup(t, policy)
				defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:fault", "adv:renamed"))
				cacheExec(t, s, "+OK\r\n", "SET", "adv:fault", "old")
				cacheExec(t, s, "$3\r\nold\r\n", "GET", "adv:fault")
				var dropped atomic.Bool
				s.cfg.RedisAddress = cacheFaultRelay(t, hot, func(a [][]byte) bool {
					return (string(a[0]) == "DEL" || (len(a) > 1 && (string(a[1]) == cacheUpdateScript || string(a[1]) == cacheMoveHotScript))) && dropped.CompareAndSwap(false, true)
				})
				key, want := "adv:fault", "$3\r\nnew\r\n"
				switch kind {
				case "transaction":
					tx := &cacheSession{r: s.cache}
					defer tx.close()
					for _, a := range [][]string{{"MULTI"}, {"SET", key, "new"}, {"EXEC"}} {
						b := tx.execute(context.Background(), cacheArgs(a...))
						if len(b) == 0 || b[0] == '-' {
							t.Fatal(string(b))
						}
					}
				case "script":
					cacheExec(t, s, "+OK\r\n", "EVAL", `return redis.call('SET',KEYS[1],'new')`, "1", key)
				case "rename":
					cacheExec(t, s, "+OK\r\n", "RENAME", key, "adv:renamed")
					key = "adv:renamed"
					want = "$3\r\nold\r\n"
				}
				if !dropped.Load() || s.cache.hotReady.Load() {
					t.Fatal("failed hot update not isolated")
				}
				cacheExec(t, s, want, "GET", key)
				cacheExec(t, s, want, "GET", key)
			})
		}
	}
}

func TestCacheTransactionOwnerFenceAndExpiryWatch(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:watch"))
	cacheExec(t, s, "+OK\r\n", "SET", "adv:watch", "old")
	cold, e := s.cache.connect(context.Background(), false)
	if e != nil {
		t.Fatal(e)
	}
	defer cold.Close()
	old := s.cache.token
	cacheCall(t, cold, "WATCH", s.cache.guard)
	cacheCall(t, cold, "MULTI")
	cacheCall(t, cold, "EVAL", cacheColdScript, "2", s.cache.guard, "adv:watch", old, "SET", "normal", "stale")
	s.cache.uncertainEpoch(old)
	if _, e = s.cache.ensureEpoch(context.Background()); e != nil {
		t.Fatal(e)
	}
	b := cacheCall(t, cold, "EXEC")
	if string(b) != "*-1\r\n" && string(b) != "$-1\r\n" {
		t.Fatalf("late EXEC not fenced: %q", b)
	}
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "adv:watch")
	cacheExec(t, s, "+OK\r\n", "SET", "adv:watch", "expires", "PX", "150")
	tx := &cacheSession{r: s.cache}
	defer tx.close()
	tx.execute(context.Background(), cacheArgs("WATCH", "adv:watch"))
	time.Sleep(180 * time.Millisecond)
	tx.execute(context.Background(), cacheArgs("MULTI"))
	tx.execute(context.Background(), cacheArgs("SET", "adv:watch", "wrong"))
	b = tx.execute(context.Background(), cacheArgs("EXEC"))
	if string(b) != "*-1\r\n" {
		t.Fatalf("expiry WATCH not aborted: %q", b)
	}
}

func TestCacheAdvancedConcurrentAtomicity(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:atomic-a", "adv:atomic-b"))
	cacheExec(t, s, "+OK\r\n", "MSET", "adv:atomic-a", "0", "adv:atomic-b", "0")
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			tx := &cacheSession{r: s.cache}
			defer tx.close()
			for i := 0; i < 12; i++ {
				value := fmt.Sprintf("%d:%d", worker, i)
				var b []byte
				if worker == 0 {
					b = s.cache.execute(context.Background(), cacheArgs("EVAL", `redis.call('SET',KEYS[1],ARGV[1]);return redis.call('SET',KEYS[2],ARGV[1])`, "2", "adv:atomic-a", "adv:atomic-b", value))
				} else if worker == 1 {
					tx.execute(context.Background(), cacheArgs("MULTI"))
					tx.execute(context.Background(), cacheArgs("SET", "adv:atomic-a", value))
					tx.execute(context.Background(), cacheArgs("SET", "adv:atomic-b", value))
					b = tx.execute(context.Background(), cacheArgs("EXEC"))
				} else {
					b = s.cache.execute(context.Background(), cacheArgs("MGET", "adv:atomic-a", "adv:atomic-b"))
					p, e := frameParts(b)
					if e != nil || len(p) != 2 || string(p[0]) != string(p[1]) {
						t.Errorf("torn batch: %q", b)
					}
				}
				if len(b) == 0 || b[0] == '-' {
					t.Errorf("operation failed: %q", b)
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestCacheAdvancedLostAcknowledgement(t *testing.T) {
	for _, kind := range []string{"transaction", "script"} {
		t.Run(kind, func(t *testing.T) {
			s, _, cold := cacheTestSetup(t, "update_existing")
			defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:lost"))
			cacheExec(t, s, "+OK\r\n", "SET", "adv:lost", "0")
			cacheExec(t, s, "$1\r\n0\r\n", "GET", "adv:lost")
			var dropped atomic.Bool
			s.cache.cfg.ColdAddress = cacheFaultRelayHooks(t, cold, nil, func(a [][]byte, b []byte) bool {
				match := kind == "transaction" && string(a[0]) == "EXEC" || kind == "script" && len(a) > 1 && string(a[1]) == cacheUserScript
				return match && dropped.CompareAndSwap(false, true)
			})
			var b []byte
			if kind == "transaction" {
				tx := &cacheSession{r: s.cache}
				defer tx.close()
				tx.execute(context.Background(), cacheArgs("MULTI"))
				tx.execute(context.Background(), cacheArgs("INCR", "adv:lost"))
				b = tx.execute(context.Background(), cacheArgs("EXEC"))
			} else {
				b = s.cache.execute(context.Background(), cacheArgs("EVAL", `return redis.call('INCR',KEYS[1])`, "1", "adv:lost"))
			}
			if !dropped.Load() || !strings.Contains(string(b), "result unknown") {
				t.Fatalf("missing unknown result %q", b)
			}
			cacheExec(t, s, "$1\r\n1\r\n", "GET", "adv:lost")
			cacheExec(t, s, "$1\r\n1\r\n", "GET", "adv:lost")
		})
	}
}

func TestCacheTransactionExecutionErrors(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	tx := &cacheSession{r: s.cache}
	defer tx.close()
	defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:tx-error"))
	tx.execute(context.Background(), cacheArgs("MULTI"))
	for _, a := range [][]string{{"SET", "adv:tx-error", "text"}, {"INCR", "adv:tx-error"}, {"EVALSHA", strings.Repeat("0", 40), "0"}, {"SET", "adv:tx-error", "after"}} {
		b := tx.execute(context.Background(), cacheArgs(a...))
		if string(b) != "+QUEUED\r\n" {
			t.Fatalf("queue %v: %q", a, b)
		}
	}
	b := tx.execute(context.Background(), cacheArgs("EXEC"))
	p, e := frameParts(b)
	if e != nil || len(p) != 4 || len(p[1]) == 0 || p[1][0] != '-' || !strings.HasPrefix(string(p[2]), "-NOSCRIPT") || string(p[3]) != "+OK\r\n" {
		t.Fatalf("execution errors lost/aborted transaction %q", b)
	}
	cacheExec(t, s, "$5\r\nafter\r\n", "GET", "adv:tx-error")
}

func TestCacheRenameMissingHotSource(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	defer s.cache.execute(context.Background(), cacheArgs("DEL", "adv:rename-source", "adv:rename-target"))
	cacheExec(t, s, "+OK\r\n", "SET", "adv:rename-target", "stale")
	cacheExec(t, s, ":2\r\n", "HSET", "adv:rename-source", "a", "new", "keep", "complete")
	cacheExec(t, s, "+OK\r\n", "RENAME", "adv:rename-source", "adv:rename-target")
	cacheExec(t, s, "*2\r\n$3\r\nnew\r\n$8\r\ncomplete\r\n", "HMGET", "adv:rename-target", "a", "keep")
}

func TestCacheScriptReplyCompatibility(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "invalidate")
	oracle := cacheOracle(t, s)
	for _, source := range []string{`return -1.9`, `return -0.1`, `return 0.1`, `return 1.9`, `return {"bulk",redis.status_reply("OK"),false,-1.9,{true,3.5}}`, `return nil`, `return redis.error_reply('ERR explicit')`} {
		a := []string{"EVAL", source, "0"}
		want := cacheCall(t, oracle, a...)
		got := s.cache.execute(context.Background(), cacheArgs(a...))
		if string(got) != string(want) {
			t.Fatalf("%s got %q want %q", source, got, want)
		}
	}
	for _, source := range []string{`return math.huge`, `return 0/0`} {
		b := s.cache.execute(context.Background(), cacheArgs("EVAL", source, "0"))
		if !strings.Contains(string(b), "Lua number exceeds RESP integer range") {
			t.Fatalf("unsafe number accepted: %q", b)
		}
	}
}
