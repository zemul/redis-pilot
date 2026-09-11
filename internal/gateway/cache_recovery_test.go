package gateway

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheOriginalKeysOnly(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "update_existing")
	for _, a := range [][]string{{"SET", "plain", "value"}, {"HSET", "user:123", "name", "alice"}} {
		s.cache.execute(context.Background(), cacheArgs(a...))
	}
	cacheExec(t, s, "$5\r\nalice\r\n", "HGET", "user:123", "name")
	hot, err := s.cache.connect(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	for _, tc := range []struct {
		a    []string
		want string
	}{
		{[]string{"DBSIZE"}, ":2\r\n"},
		{[]string{"TYPE", "plain"}, "+string\r\n"},
		{[]string{"TYPE", "user:123"}, "+hash\r\n"},
		{[]string{"HGET", "user:123", "name"}, "$5\r\nalice\r\n"},
		{[]string{"KEYS", cachePrefix + "*"}, "*0\r\n"},
	} {
		b, e := hot.call(context.Background(), tc.a...)
		if e != nil || string(b) != tc.want {
			t.Fatalf("%v: %q %v", tc.a, b, e)
		}
	}
	cacheExec(t, s, ":1\r\n", "DEL", "plain")
	cacheExec(t, s, ":1\r\n", "DEL", "user:123")
}

func TestCacheFailedFlushKeepsColdAvailable(t *testing.T) {
	s, hot, _ := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "flush-fault", "old")
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "flush-fault")
	var fail atomic.Bool
	fail.Store(true)
	s.cfg.RedisAddress = cacheFaultRelay(t, hot, func(a [][]byte) bool { return string(a[0]) == "FLUSHDB" && fail.Load() })
	s.cache.hotFailed()
	cacheExec(t, s, "+OK\r\n", "SET", "flush-fault", "new")
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "flush-fault")
	if s.cache.hotReady.Load() {
		t.Fatal("enabled before successful flush")
	}
	fail.Store(false)
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "flush-fault")
	if !s.cache.hotReady.Load() {
		t.Fatal("did not automatically recover")
	}
	cacheExec(t, s, ":1\r\n", "DEL", "flush-fault")
}

func TestCacheLateHotPublishCannotSurviveFlush(t *testing.T) {
	s, hot, _ := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "late-hot", "old")
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	s.cfg.RedisAddress = cacheFaultRelay(t, hot, func(a [][]byte) bool {
		if len(a) > 1 && string(a[1]) == cachePublishScript && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return false
	})
	// Release before relay cleanup even if an assertion fails.
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	done := make(chan []byte, 1)
	go func() { done <- s.cache.execute(context.Background(), cacheArgs("GET", "late-hot")) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("publish not reached")
	}
	select {
	case b := <-done:
		if string(b) != "$3\r\nold\r\n" {
			t.Fatal(string(b))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hot timeout not bounded")
	}
	cacheExec(t, s, "+OK\r\n", "SET", "late-hot", "new")
	close(release)
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "late-hot")
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "late-hot")
	cacheExec(t, s, ":1\r\n", "DEL", "late-hot")
}

func TestCacheRecoveryDoesNotBlockColdWrites(t *testing.T) {
	s, hot, _ := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "during-flush", "old")
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	s.cfg.RedisAddress = cacheFaultRelay(t, hot, func(a [][]byte) bool {
		if string(a[0]) == "FLUSHDB" && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return false
	})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.cache.hotFailed()
	done := make(chan struct{})
	go func() { s.cache.recoverHot(context.Background()); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("flush not reached")
	}
	cacheExec(t, s, "+OK\r\n", "SET", "during-flush", "new")
	close(release)
	<-done
	if s.cache.hotReady.Load() {
		t.Fatal("write overlapping recovery was not accounted for")
	}
	cacheExec(t, s, "$3\r\nnew\r\n", "GET", "during-flush")
	cacheExec(t, s, ":1\r\n", "DEL", "during-flush")
}

func TestCachePublishErrorDeletesPartialObject(t *testing.T) {
	s, _, _ := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "partial", "old")
	hot, e := s.cache.connect(context.Background(), true)
	if e != nil {
		t.Fatal(e)
	}
	defer hot.Close()
	b, e := hot.call(context.Background(), "EVAL", cachePublishScript, "1", "partial", "HSET", "9999999999999", "field")
	if e != nil || !strings.HasPrefix(string(b), "-") {
		t.Fatalf("%q %v", b, e)
	}
	b, e = hot.call(context.Background(), "EXISTS", "partial")
	if e != nil || string(b) != ":0\r\n" {
		t.Fatalf("partial cache survived: %q %v", b, e)
	}
	cacheExec(t, s, "$3\r\nold\r\n", "GET", "partial")
	cacheExec(t, s, ":1\r\n", "DEL", "partial")
}
