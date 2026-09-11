package gateway

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The oracle is native Redis, including when the tested cold backend is
// Kvrocks. Comparing only to Kvrocks would hide scripting RESP differences.
func cacheOracle(t *testing.T, s *Service) *cacheConn {
	t.Helper()
	c, reader, err := authenticated(g4Redis(t), "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &cacheConn{Conn: c, reader: reader, cfg: s.cfg}
}

func cacheCall(t *testing.T, c *cacheConn, args ...string) []byte {
	t.Helper()
	b, e := c.call(context.Background(), args...)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestCacheSingleKeyCompatibility(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			oracle := cacheOracle(t, s)
			// Cases use an explicit key placeholder to keep every run isolated.
			const key = "compat-single"
			defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
			str := []string{"SET", key, "10"}
			hash := []string{"HSET", key, "f", "10", "keep", "untouched"}
			list := []string{"RPUSH", key, "a", "b", "a", "c", "\xff\x00"}
			set := []string{"SADD", key, "a"}
			zset := []string{"ZADD", key, "1", "a", "2", "b", "3", "c"}
			get := []string{"GET", key}
			hget := []string{"HMGET", key, "f", "keep", "new"}
			lget := []string{"LRANGE", key, "0", "-1"}
			zget := []string{"ZRANGE", key, "0", "-1", "WITHSCORES"}
			now := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
			nowms := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
			cases := []struct {
				name           string
				seed, op, read []string
			}{
				{"set-get", str, []string{"SET", key, "new", "GET"}, get},
				{"set-get-ok-string", []string{"SET", key, "OK"}, []string{"SET", key, "new", "GET"}, get},
				{"set-get-missing", nil, []string{"SET", key, "new", "GET"}, get},
				{"set-nx-get-noop", str, []string{"SET", key, "new", "NX", "GET"}, get},
				{"set-xx-get-missing", nil, []string{"SET", key, "new", "XX", "GET"}, get},
				{"set-exat", str, []string{"SET", key, "new", "EXAT", now}, get},
				{"set-pxat", str, []string{"SET", key, "new", "PXAT", nowms}, get},
				{"setnx-noop", str, []string{"SETNX", key, "new"}, get},
				{"setnx-create", nil, []string{"SETNX", key, "new"}, get},
				{"setex", str, []string{"SETEX", key, "60", "new"}, get},
				{"psetex", str, []string{"PSETEX", key, "60000", "new"}, get},
				{"getset", str, []string{"GETSET", key, "new"}, get},
				{"getdel", str, []string{"GETDEL", key}, get},
				{"getex", str, []string{"GETEX", key, "EX", "60"}, get},
				{"getex-px", str, []string{"GETEX", key, "PX", "60000"}, get},
				{"getex-exat", str, []string{"GETEX", key, "EXAT", now}, get},
				{"getex-pxat", str, []string{"GETEX", key, "PXAT", nowms}, get},
				{"getex-persist", str, []string{"GETEX", key, "PERSIST"}, get},
				{"setrange", str, []string{"SETRANGE", key, "3", "\x00\xff"}, get},
				{"incrbyfloat", str, []string{"INCRBYFLOAT", key, "0.5"}, get},
				{"hmset", hash, []string{"HMSET", key, "f", "11", "new", "value"}, hget},
				{"hsetnx-noop", hash, []string{"HSETNX", key, "f", "11"}, hget},
				{"hsetnx-new", hash, []string{"HSETNX", key, "new", "11"}, hget},
				{"hincrbyfloat", hash, []string{"HINCRBYFLOAT", key, "f", "0.5"}, hget},
				{"linsert-before", list, []string{"LINSERT", key, "BEFORE", "b", "new"}, lget},
				{"linsert-after", list, []string{"LINSERT", key, "AFTER", "b", "new"}, lget},
				{"linsert-noop", list, []string{"LINSERT", key, "AFTER", "missing", "new"}, lget},
				{"lpop-count", list, []string{"LPOP", key, "2"}, lget},
				{"rpop-count-binary", list, []string{"RPOP", key, "2"}, lget},
				{"lpop-zero", list, []string{"LPOP", key, "0"}, lget},
				{"lpop-all", list, []string{"LPOP", key, "99"}, lget},
				{"lpop-missing", nil, []string{"LPOP", key, "2"}, lget},
				{"rpop-missing", nil, []string{"RPOP", key, "2"}, lget},
				{"spop-missing", nil, []string{"SPOP", key, "2"}, []string{"SMEMBERS", key}},
				{"zpop-missing", nil, []string{"ZPOPMAX", key, "2"}, zget},
				{"spop-count", set, []string{"SPOP", key, "2"}, []string{"SMEMBERS", key}},
				{"spop-zero", set, []string{"SPOP", key, "0"}, []string{"SMEMBERS", key}},
				{"zadd-nx", zset, []string{"ZADD", key, "NX", "8", "a", "9", "new"}, zget},
				{"zadd-xx", zset, []string{"ZADD", key, "XX", "8", "a", "9", "new"}, zget},
				{"zadd-gt-ch", zset, []string{"ZADD", key, "GT", "CH", "0", "a", "5", "b"}, zget},
				{"zadd-lt", zset, []string{"ZADD", key, "LT", "0", "a", "5", "b"}, zget},
				{"zadd-incr", zset, []string{"ZADD", key, "XX", "INCR", "0.5", "a"}, zget},
				{"zadd-incr-noop", zset, []string{"ZADD", key, "NX", "INCR", "0.5", "a"}, zget},
				{"zpopmin", zset, []string{"ZPOPMIN", key}, zget},
				{"zpopmax-count", zset, []string{"ZPOPMAX", key, "2"}, zget},
				{"zrem-rank", zset, []string{"ZREMRANGEBYRANK", key, "0", "1"}, zget},
				{"zrem-score", zset, []string{"ZREMRANGEBYSCORE", key, "(1", "3"}, zget},
				{"zrem-lex", []string{"ZADD", key, "1", "a", "1", "b", "1", "c"}, []string{"ZREMRANGEBYLEX", key, "[a", "(c"}, zget},
				{"expire-nx", str, []string{"EXPIRE", key, "60", "NX"}, get},
				{"expire-xx-noop", str, []string{"EXPIRE", key, "60", "XX"}, get},
				{"expire-lt", str, []string{"PEXPIRE", key, "60000", "LT"}, get},
				{"expire-gt-noop", str, []string{"PEXPIRE", key, "60000", "GT"}, get},
				{"expireat", str, []string{"EXPIREAT", key, now}, get},
				{"pexpireat", str, []string{"PEXPIREAT", key, nowms}, get},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					for _, warm := range []bool{false, true} {
						s.cache.execute(context.Background(), cacheArgs("DEL", key))
						cacheCall(t, oracle, "DEL", key)
						if tc.seed != nil {
							b := s.cache.execute(context.Background(), cacheArgs(tc.seed...))
							if len(b) == 0 || b[0] == '-' {
								t.Fatalf("seed %q", b)
							}
							cacheCall(t, oracle, tc.seed...)
						}
						// Ensure this variant genuinely starts with/without a cache.
						s.cache.recoverHot(context.Background())
						hot, e := s.cache.connect(context.Background(), true)
						if e != nil {
							t.Fatal(e)
						}
						if warm {
							s.cache.execute(context.Background(), cacheArgs(tc.read...))
						} else {
							cacheCall(t, hot, "DEL", key)
						}
						got := s.cache.execute(context.Background(), cacheArgs(tc.op...))
						want := cacheCall(t, oracle, tc.op...)
						if string(got) != string(want) {
							hot.Close()
							t.Fatalf("warm=%v %v: got %q want %q", warm, tc.op, got, want)
						}
						want = cacheCall(t, oracle, tc.read...)
						if !warm && (tc.read[0] == "HMGET" || tc.read[0] == "LRANGE" || tc.read[0] == "ZRANGE" || tc.read[0] == "SMEMBERS") {
							if b := cacheCall(t, hot, "EXISTS", key); string(b) != ":0\r\n" {
								hot.Close()
								t.Fatalf("partial collection created: %v", tc.op)
							}
						}
						if warm && policy == "update_existing" {
							h := cacheCall(t, hot, tc.read...)
							if string(h) != string(want) {
								hot.Close()
								t.Fatalf("incremental hot %q want %q after %v", h, want, tc.op)
							}
						}
						hot.Close()
						for i := 0; i < 2; i++ {
							got = s.cache.execute(context.Background(), cacheArgs(tc.read...))
							if string(got) != string(want) {
								t.Fatalf("read %q want %q after %v", got, want, tc.op)
							}
						}
					}
				})
			}
		})
	}
}

func TestCacheAdditionalReads(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			oracle := cacheOracle(t, s)
			key := "compat-reads"
			defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
			cases := []struct {
				seed  []string
				reads [][]string
			}{
				{[]string{"SET", key, "hello"}, [][]string{{"GETRANGE", key, "1", "3"}, {"SUBSTR", key, "-3", "-1"}, {"EXPIRETIME", key}, {"PEXPIRETIME", key}}},
				{[]string{"HSET", key, "f", "hello"}, [][]string{{"HSTRLEN", key, "f"}, {"HRANDFIELD", key}, {"HRANDFIELD", key, "-2", "WITHVALUES"}}},
				{[]string{"RPUSH", key, "a", "b", "a", "c"}, [][]string{{"LPOS", key, "a"}, {"LPOS", key, "a", "RANK", "-1"}, {"LPOS", key, "a", "COUNT", "0", "MAXLEN", "3"}}},
				{[]string{"SADD", key, "a"}, [][]string{{"SMISMEMBER", key, "a", "b"}, {"SRANDMEMBER", key}, {"SRANDMEMBER", key, "-2"}}},
				{[]string{"ZADD", key, "1", "a", "2", "b", "3", "c"}, [][]string{
					{"ZMSCORE", key, "a", "missing", "b"}, {"ZRANK", key, "b", "WITHSCORE"}, {"ZREVRANK", key, "b", "WITHSCORE"},
					{"ZRANGEBYSCORE", key, "(1", "+inf", "WITHSCORES", "LIMIT", "0", "1"},
					{"ZREVRANGEBYSCORE", key, "+inf", "-inf", "LIMIT", "1", "2", "WITHSCORES"},
					{"ZRANGE", key, "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "0", "2", "WITHSCORES"}}},
				{[]string{"ZADD", key, "1", "a", "1", "b", "1", "c"}, [][]string{
					{"ZLEXCOUNT", key, "[a", "(c"}, {"ZRANGEBYLEX", key, "-", "+", "LIMIT", "1", "1"},
					{"ZREVRANGEBYLEX", key, "+", "-", "LIMIT", "0", "2"}, {"ZRANGE", key, "[a", "[c", "BYLEX", "LIMIT", "1", "1"}}},
				{[]string{"ZADD", key, "1", "a"}, [][]string{{"ZRANDMEMBER", key}, {"ZRANDMEMBER", key, "-2", "WITHSCORES"}}},
			}
			for _, tc := range cases {
				cacheCall(t, oracle, "DEL", key)
				s.cache.execute(context.Background(), cacheArgs("DEL", key))
				cacheCall(t, oracle, tc.seed...)
				s.cache.execute(context.Background(), cacheArgs(tc.seed...))
				for _, a := range tc.reads {
					s.cache.recoverHot(context.Background())
					hot, e := s.cache.connect(context.Background(), true)
					if e != nil {
						t.Fatal(e)
					}
					cacheCall(t, hot, "DEL", key)
					hot.Close()
					for i := 0; i < 2; i++ {
						want := cacheCall(t, oracle, a...)
						got := s.cache.execute(context.Background(), cacheArgs(a...))
						if string(got) != string(want) {
							t.Fatalf("%v: got %q want %q", a, got, want)
						}
					}
				}
			}
		})
	}
}

func TestCacheScanStaysCold(t *testing.T) {
	for _, kind := range []string{"H", "S", "Z"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, "update_existing")
			key := "compat-scan-" + kind
			s.cache.execute(context.Background(), cacheArgs("DEL", key))
			defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
			// Batch fixture writes: this tests cursor continuity, not TCP churn.
			for start := 0; start < 300; start += 100 {
				name := "SADD"
				if kind == "H" {
					name = "HSET"
				}
				if kind == "Z" {
					name = "ZADD"
				}
				a := []string{name, key}
				for i := start; i < start+100; i++ {
					member := fmt.Sprintf("f%03d", i)
					if kind == "Z" {
						a = append(a, "1")
					}
					a = append(a, member)
					if kind == "H" {
						a = append(a, strings.Repeat("v", 80))
					} // force multiple Redis scan pages
				}
				b := s.cache.execute(context.Background(), cacheArgs(a...))
				if b[0] == '-' {
					t.Fatal(string(b))
				}
			}
			seen := map[string]bool{}
			cursor := "0"
			for page := 0; page < 1000; page++ {
				hits := metricValue(s.cache.events.WithLabelValues("hit"))
				b := s.cache.execute(context.Background(), cacheArgs(kind+"SCAN", key, cursor, "MATCH", "f*", "COUNT", "17"))
				if metricValue(s.cache.events.WithLabelValues("hit")) != hits {
					t.Fatal("cursor routed to cache")
				}
				p, e := frameParts(b)
				if e != nil || len(p) != 2 {
					t.Fatalf("scan %q", b)
				}
				cursor, _ = frameString(p[0])
				values, e := cacheValues(p[1])
				if e != nil {
					t.Fatal(e)
				}
				step := 1
				if kind != "S" {
					step = 2
				}
				for i := 0; i < len(values); i += step {
					seen[values[i]] = true
				}
				if cursor == "0" {
					break
				}
				// Fill/evict between pages: an encoded Redis cursor must never be
				// sent to Kvrocks or vice versa.
				s.cache.execute(context.Background(), cacheArgs("TYPE", key))
				hot, e := s.cache.connect(context.Background(), true)
				if e != nil {
					t.Fatal(e)
				}
				cacheCall(t, hot, "DEL", key)
				hot.Close()
			}
			if len(seen) != 300 || cursor != "0" {
				t.Fatalf("scan incomplete: %d cursor=%s", len(seen), cursor)
			}
		})
	}
}

func TestCacheExtendedRejectsUnsafeGrammar(t *testing.T) {
	for _, a := range [][]string{
		{"HSCAN", "k", "0", "TYPE", "hash"}, {"ZRANGE", "k", "0", "1", "STORE", "elsewhere"},
		{"ZADD", "k", "NX", "1"}, {"GETEX", "k", "PX"}, {"SET", "k", "v", "EXAT"},
		{"LMOVE", "a", "b", "LEFT", "RIGHT"}, {"BITOP", "NOT", "a", "b"},
		{"HSETEX", "k", "EX", "10", "FIELDS", "1", "f", "v"},
	} {
		if _, ok := classifyCacheCommand(cacheArgs(a...)); ok {
			t.Fatal("unsafe/unsupported grammar accepted", a)
		}
	}
	for _, a := range [][]string{{"ZADD", "k", "NX", "XX", "1", "a"}, {"EXPIRE", "k", "5", "NX", "XX"}} {
		// Known options are sent to the authority for native validation.
		if _, ok := classifyCacheCommand(cacheArgs(a...)); !ok {
			t.Fatal("known syntax rejected", a)
		}
	}
}

func TestCacheExtendedErrorsAndNilReplies(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			oracle := cacheOracle(t, s)
			key := "compat-errors"
			defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
			for _, a := range [][]string{
				{"GETEX", key}, {"GETDEL", key}, {"LPOP", key, "1"}, {"RPOP", key, "1"},
				{"LPOS", key, "a"}, {"LPOS", key, "a", "COUNT", "2"},
				{"ZRANK", key, "a", "WITHSCORE"}, {"ZREVRANK", key, "a", "WITHSCORE"},
				{"HRANDFIELD", key}, {"HRANDFIELD", key, "2", "WITHVALUES"},
				{"SRANDMEMBER", key}, {"ZRANDMEMBER", key},
				{"SRANDMEMBER", key, "2"}, {"ZRANDMEMBER", key, "2", "WITHSCORES"},
				{"ZMSCORE", key, "a", "b"}, {"SMISMEMBER", key, "a", "b"},
			} {
				s.cache.execute(context.Background(), cacheArgs("DEL", key))
				cacheCall(t, oracle, "DEL", key)
				got := s.cache.execute(context.Background(), cacheArgs(a...))
				want := cacheCall(t, oracle, a...)
				if string(got) != string(want) {
					t.Fatalf("nil %v got %q want %q", a, got, want)
				}
			}
			for _, a := range [][]string{
				{"GETEX", key, "PX", "bad"}, {"SETRANGE", key, "-1", "value"},
				{"INCRBYFLOAT", key, "nan"}, {"SET", key, "new", "GET", "PX", "bad"},
				{"HINCRBYFLOAT", key, "f", "1"}, {"HSETNX", key, "f", "1"},
				{"LPOP", key, "-1"}, {"LINSERT", key, "BEFORE", "a", "b"},
				{"SPOP", key, "2"}, {"ZPOPMIN", key, "2"},
				{"ZADD", key, "NX", "XX", "1", "a"}, {"EXPIRE", key, "60", "NX", "XX"},
				{"EXPIRE", key, "not-an-int", "XX"},
			} {
				cacheExec(t, s, "+OK\r\n", "SET", key, "10")
				cacheExec(t, s, "$2\r\n10\r\n", "GET", key)
				b := s.cache.execute(context.Background(), cacheArgs(a...))
				if !strings.HasPrefix(string(b), "-") {
					t.Fatalf("expected error for %v got %q", a, b)
				}
				cacheExec(t, s, "$2\r\n10\r\n", "GET", key)
			}
		})
	}
}

func TestCacheExtendedExpiry(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			key := "compat-expiry"
			defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
			base := time.Now().Add(time.Hour).UnixMilli()
			for _, tc := range []struct {
				op    []string
				want  string
				delta int64
			}{
				{[]string{"PEXPIREAT", key, strconv.FormatInt(base+1000, 10), "GT"}, ":1\r\n", 1000},
				{[]string{"PEXPIREAT", key, strconv.FormatInt(base, 10), "GT"}, ":0\r\n", 0},
				{[]string{"PEXPIREAT", key, strconv.FormatInt(base-1000, 10), "LT", "XX"}, ":1\r\n", -1000},
				{[]string{"PEXPIREAT", key, strconv.FormatInt(base+1000, 10), "NX"}, ":0\r\n", 0},
			} {
				cacheExec(t, s, "+OK\r\n", "SET", key, "value", "PXAT", strconv.FormatInt(base, 10))
				cacheExec(t, s, "$5\r\nvalue\r\n", "GET", key)
				cacheExec(t, s, tc.want, tc.op...)
				cacheExec(t, s, fmt.Sprintf(":%d\r\n", base+tc.delta), "PEXPIRETIME", key)
				cacheExec(t, s, "$5\r\nvalue\r\n", "GET", key)
			}
			cacheExec(t, s, "$5\r\nvalue\r\n", "GETEX", key, "PERSIST")
			cacheExec(t, s, ":-1\r\n", "PTTL", key)
			cacheExec(t, s, ":1\r\n", "PEXPIRE", key, "0", "LT")
			cacheExec(t, s, "$-1\r\n", "GET", key)
		})
	}
}

func TestCacheExtendedHotFailure(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		for _, kind := range []string{"string", "hash", "list", "set", "zset"} {
			t.Run(policy+"/"+kind, func(t *testing.T) {
				s, addr, _ := cacheTestSetup(t, policy)
				key := "compat-hot-failure"
				s.cache.execute(context.Background(), cacheArgs("DEL", key))
				defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
				seed, op, read := []string{"SET", key, "old"}, []string{"SET", key, "new", "GET"}, []string{"GET", key}
				switch kind {
				case "hash":
					seed = []string{"HSET", key, "f", "1", "keep", "untouched"}
					op = []string{"HINCRBYFLOAT", key, "f", ".5"}
					read = []string{"HMGET", key, "f", "keep"}
				case "list":
					seed = []string{"RPUSH", key, "a", "b", "c"}
					op = []string{"LPOP", key, "2"}
					read = []string{"LRANGE", key, "0", "-1"}
				case "set":
					seed = []string{"SADD", key, "a", "b", "c"}
					op = []string{"SPOP", key, "2"}
					read = []string{"SMEMBERS", key}
				case "zset":
					seed = []string{"ZADD", key, "1", "a", "2", "b"}
					op = []string{"ZADD", key, "INCR", ".5", "a"}
					read = []string{"ZRANGE", key, "0", "-1", "WITHSCORES"}
				}
				b := s.cache.execute(context.Background(), cacheArgs(seed...))
				if b[0] == '-' {
					t.Fatal(string(b))
				}
				s.cache.execute(context.Background(), cacheArgs(read...))
				var dropped atomic.Bool
				s.cfg.RedisAddress = cacheFaultRelay(t, addr, func(a [][]byte) bool {
					match := len(a) > 1 && string(a[0]) == "EVAL" && string(a[1]) == cacheUpdateScript
					if policy == "invalidate" {
						match = len(a) == 2 && string(a[0]) == "DEL" && string(a[1]) == key
					}
					return match && dropped.CompareAndSwap(false, true)
				})
				b = s.cache.execute(context.Background(), cacheArgs(op...))
				if b[0] == '-' || !dropped.Load() || s.cache.hotReady.Load() {
					t.Fatalf("failed hot operation not isolated: %q", b)
				}
				cold, e := s.cache.connect(context.Background(), false)
				if e != nil {
					t.Fatal(e)
				}
				want := cacheCall(t, cold, read...)
				cold.Close()
				got := s.cache.execute(context.Background(), cacheArgs(read...))
				if kind == "set" {
					wv, _ := cacheValues(want)
					gv, _ := cacheValues(got)
					sort.Strings(wv)
					sort.Strings(gv)
					if fmt.Sprint(wv) != fmt.Sprint(gv) {
						t.Fatalf("stale Set: %q vs %q", got, want)
					}
				} else if string(got) != string(want) {
					t.Fatalf("stale read: %q vs %q", got, want)
				}
			})
		}
	}
}

func TestCacheSetGetExecutedLostReply(t *testing.T) {
	s, _, coldAddr := cacheTestSetup(t, "update_existing")
	key := "compat-set-lost"
	cacheExec(t, s, "+OK\r\n", "SET", key, "old")
	cacheExec(t, s, "$3\r\nold\r\n", "GET", key)
	defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
	var dropped atomic.Bool
	s.cache.cfg.ColdAddress = cacheFaultRelay(t, coldAddr, func(a [][]byte) bool {
		if len(a) > 7 && string(a[0]) == "EVAL" && string(a[4]) == key && string(a[6]) == "SET" && dropped.CompareAndSwap(false, true) {
			c, reader, e := authenticated(coldAddr, s.cache.coldPassword)
			if e != nil {
				t.Error(e)
				return true
			}
			defer c.Close()
			args := make([]string, len(a))
			for i := range a {
				args[i] = string(a[i])
			}
			if e := writeAll(c, encodeCommand(args...)); e != nil {
				t.Error(e)
				return true
			}
			b, e := reader.ReadFrame()
			if e != nil || string(b) != "*2\r\n$3\r\nold\r\n:1\r\n" {
				t.Errorf("write envelope %q %v", b, e)
			}
			return true
		}
		return false
	})
	b := s.cache.execute(context.Background(), cacheArgs("SET", key, "new", "GET"))
	if !strings.Contains(string(b), "result unknown") || !dropped.Load() {
		t.Fatalf("lost reply: %q", b)
	}
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", key)
}
