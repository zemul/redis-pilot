package gateway

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheLargeHashIncrementalNotRebuilt(t *testing.T) {
	s, hotAddr, coldAddr := cacheTestSetup(t, "update_existing")
	s.cfg.RequestTimeout = 5 * time.Second
	cold, err := s.cache.connect(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer cold.Close()
	const key = "large-incremental"
	for start := 0; start < 5000; start += 100 {
		a := []string{"HSET", key}
		for i := start; i < start+100; i++ {
			a = append(a, fmt.Sprintf("f%d", i), strings.Repeat("v", 100))
		}
		b, e := cold.call(context.Background(), a...)
		if e != nil || len(b) == 0 || b[0] == '-' {
			t.Fatalf("seed %q %v", b, e)
		}
	}
	defer s.cache.execute(context.Background(), cacheArgs("DEL", key))
	cacheExec(t, s, ":5000\r\n", "HLEN", key)
	var scans, deletes atomic.Int64
	s.cache.cfg.ColdAddress = cacheFaultRelay(t, coldAddr, func(a [][]byte) bool {
		if string(a[0]) == "HSCAN" || string(a[0]) == "HGETALL" {
			scans.Add(1)
		}
		return false
	})
	s.cfg.RedisAddress = cacheFaultRelay(t, hotAddr, func(a [][]byte) bool {
		if string(a[0]) == "DEL" || (len(a) > 1 && string(a[1]) == cachePublishScript) {
			deletes.Add(1)
		}
		return false
	})
	fills := metricValue(s.cache.events.WithLabelValues("filled"))
	cacheExec(t, s, ":0\r\n", "HSET", key, "f0", "new")
	cacheExec(t, s, ":1\r\n", "HDEL", key, "f1")
	cacheExec(t, s, ":9007199254740993\r\n", "HINCRBY", key, "counter", "9007199254740993")
	if scans.Load() != 0 || deletes.Load() != 0 || metricValue(s.cache.events.WithLabelValues("filled")) != fills {
		t.Fatal("incremental update scanned/deleted/rebuilt entire Hash")
	}
	hot, e := s.cache.connect(context.Background(), true)
	if e != nil {
		t.Fatal(e)
	}
	defer hot.Close()
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"HLEN", key}, ":5000\r\n"},
		{[]string{"HGET", key, "f0"}, "$3\r\nnew\r\n"},
		{[]string{"HGET", key, "counter"}, "$16\r\n9007199254740993\r\n"},
		{[]string{"HGET", key, "f4999"}, "$100\r\n" + strings.Repeat("v", 100) + "\r\n"},
	} {
		b, e := hot.call(context.Background(), tc.args...)
		if e != nil || string(b) != tc.want {
			t.Fatalf("%v: %q %v", tc.args, b, e)
		}
	}
}

func TestCacheEvictionBeforeIncrementalUpdate(t *testing.T) {
	s, hotAddr, _ := cacheTestSetup(t, "update_existing")
	cacheExec(t, s, ":2\r\n", "HSET", "evict-update", "a", "1", "b", "2")
	hot, e := s.cache.connect(context.Background(), true)
	if e != nil {
		t.Fatal(e)
	}
	defer hot.Close()
	b, e := hot.call(context.Background(), "EXISTS", "evict-update")
	if e != nil || string(b) != ":0\r\n" {
		t.Fatal("absent Hash created by partial write")
	}
	cacheExec(t, s, ":2\r\n", "HLEN", "evict-update")
	var evicted atomic.Bool
	s.cfg.RedisAddress = cacheFaultRelay(t, hotAddr, func(a [][]byte) bool {
		if len(a) > 1 && string(a[1]) == cacheUpdateScript && evicted.CompareAndSwap(false, true) {
			hot.call(context.Background(), "DEL", "evict-update")
		}
		return false
	})
	cacheExec(t, s, ":0\r\n", "HSET", "evict-update", "a", "new")
	b, e = hot.call(context.Background(), "EXISTS", "evict-update")
	if e != nil || string(b) != ":0\r\n" {
		t.Fatal("partial Hash recreated after eviction")
	}
	cacheExec(t, s, ":2\r\n", "HLEN", "evict-update")
	cacheExec(t, s, "$1\r\n2\r\n", "HGET", "evict-update", "b")
	cacheExec(t, s, ":1\r\n", "DEL", "evict-update")
}

func TestCacheIncrementalAllTypes(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	s.cache.recoverHot(context.Background())
	cold, e := s.cache.connect(context.Background(), false)
	if e != nil {
		t.Fatal(e)
	}
	defer cold.Close()
	hot, e := s.cache.connect(context.Background(), true)
	if e != nil {
		t.Fatal(e)
	}
	defer hot.Close()
	cases := []struct {
		seed, read []string
		writes     [][]string
		unordered  bool
	}{
		{[]string{"SET", "upd-str", "10"}, []string{"GET", "upd-str"}, [][]string{{"INCR", "upd-str"}, {"DECR", "upd-str"}, {"INCRBY", "upd-str", "8"}, {"DECRBY", "upd-str", "4"}, {"APPEND", "upd-str", "x"}}, false},
		{[]string{"RPUSH", "upd-list", "a", "b", "c"}, []string{"LRANGE", "upd-list", "0", "-1"}, [][]string{{"LPUSH", "upd-list", "x"}, {"RPUSHX", "upd-list", "y"}, {"LPUSHX", "upd-list", "q"}, {"LSET", "upd-list", "1", "z"}, {"LREM", "upd-list", "1", "b"}, {"LPOP", "upd-list"}, {"RPOP", "upd-list"}, {"LTRIM", "upd-list", "0", "1"}}, false},
		{[]string{"SADD", "upd-set", "a", "b", "c"}, []string{"SMEMBERS", "upd-set"}, [][]string{{"SADD", "upd-set", "d"}, {"SREM", "upd-set", "a"}, {"SPOP", "upd-set"}}, true},
		{[]string{"ZADD", "upd-z", "1", "a", "2", "b"}, []string{"ZRANGE", "upd-z", "0", "-1", "WITHSCORES"}, [][]string{{"ZADD", "upd-z", "3", "c"}, {"ZINCRBY", "upd-z", "0.5", "a"}, {"ZREM", "upd-z", "b"}}, false},
	}
	for _, tc := range cases {
		s.cache.execute(context.Background(), cacheArgs(tc.seed...))
		s.cache.execute(context.Background(), cacheArgs(tc.read...))
		for _, a := range tc.writes {
			b := s.cache.execute(context.Background(), cacheArgs(a...))
			if len(b) == 0 || b[0] == '-' {
				t.Fatalf("%v: %q", a, b)
			}
			c, e1 := cold.call(context.Background(), tc.read...)
			h, e2 := hot.call(context.Background(), tc.read...)
			if e1 != nil || e2 != nil {
				t.Fatal(e1, e2)
			}
			if tc.unordered {
				cv, _ := cacheValues(c)
				hv, _ := cacheValues(h)
				sort.Strings(cv)
				sort.Strings(hv)
				if strings.Join(cv, ",") != strings.Join(hv, ",") {
					t.Fatalf("%v differs", a)
				}
			} else if string(c) != string(h) {
				t.Fatalf("%v: hot %q cold %q", a, h, c)
			}
		}
		cacheExec(t, s, ":1\r\n", "DEL", tc.seed[1])
	}
}

func TestCacheConcurrentHashReadAfterWrite(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	cacheExec(t, s, ":1\r\n", "HSET", "hash-race", "n", "0")
	cacheExec(t, s, "$1\r\n0\r\n", "HGET", "hash-race", "n")
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				b := s.cache.execute(context.Background(), cacheArgs("HINCRBY", "hash-race", "n", "1"))
				v, ok := frameString(b)
				n, e := strconv.Atoi(v)
				if !ok || e != nil {
					t.Errorf("write %q", b)
					return
				}
				b = s.cache.execute(context.Background(), cacheArgs("HGET", "hash-race", "n"))
				v, ok = frameString(b)
				got, e := strconv.Atoi(v)
				if !ok || e != nil || got < n {
					t.Errorf("stale read %q after %d", b, n)
					return
				}
			}
		}()
	}
	wg.Wait()
	cacheExec(t, s, "$2\r\n80\r\n", "HGET", "hash-race", "n")
	cacheExec(t, s, ":1\r\n", "DEL", "hash-race")
}

func TestCacheIncrementalFailureCannotReadOldHash(t *testing.T) {
	s, hotAddr, _ := cacheTestSetup(t, "update_existing")
	cacheExec(t, s, ":2\r\n", "HSET", "update-fault", "a", "old", "b", "keep")
	cacheExec(t, s, ":2\r\n", "HLEN", "update-fault")
	var dropped atomic.Bool
	s.cfg.RedisAddress = cacheFaultRelay(t, hotAddr, func(a [][]byte) bool {
		return len(a) > 1 && string(a[1]) == cacheUpdateScript && dropped.CompareAndSwap(false, true)
	})
	cacheExec(t, s, ":0\r\n", "HSET", "update-fault", "a", "new")
	if !dropped.Load() || s.cache.hotReady.Load() {
		t.Fatal("failed update was not isolated")
	}
	cacheExec(t, s, "$3\r\nnew\r\n", "HGET", "update-fault", "a")
	cacheExec(t, s, "$4\r\nkeep\r\n", "HGET", "update-fault", "b")
	cacheExec(t, s, ":1\r\n", "DEL", "update-fault")
}

func TestCachePartialStreamingLoadRemoved(t *testing.T) {
	s, _, coldAddr := cacheTestSetup(t, "invalidate")
	cold, e := s.cache.connect(context.Background(), false)
	if e != nil {
		t.Fatal(e)
	}
	defer cold.Close()
	a := []string{"RPUSH", "stream-fault"}
	for i := 0; i < 300; i++ {
		a = append(a, strconv.Itoa(i))
	}
	if b, e := cold.call(context.Background(), a...); e != nil || string(b) != ":300\r\n" {
		t.Fatal(string(b), e)
	}
	var reads atomic.Int64
	s.cache.cfg.ColdAddress = cacheFaultRelay(t, coldAddr, func(a [][]byte) bool { return string(a[0]) == "LRANGE" && reads.Add(1) == 2 })
	cacheExec(t, s, ":300\r\n", "LLEN", "stream-fault")
	hot, e := s.cache.connect(context.Background(), true)
	if e != nil {
		t.Fatal(e)
	}
	defer hot.Close()
	b, e := hot.call(context.Background(), "EXISTS", "stream-fault")
	if e != nil || string(b) != ":0\r\n" {
		t.Fatal("partial list survived", string(b), e)
	}
	cacheExec(t, s, ":300\r\n", "LLEN", "stream-fault")
	b, e = hot.call(context.Background(), "LLEN", "stream-fault")
	if e != nil || string(b) != ":300\r\n" {
		t.Fatal("list not fully rewarmed", string(b), e)
	}
	cacheExec(t, s, ":1\r\n", "DEL", "stream-fault")
}
