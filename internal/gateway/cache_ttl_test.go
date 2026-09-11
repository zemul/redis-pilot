package gateway

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCacheTTLAndConditions(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			cacheExec(t, s, ":0\r\n", "DEL", "ttl-key")
			cacheExec(t, s, "$-1\r\n", "SET", "ttl-key", "bad", "XX")
			cacheExec(t, s, "+OK\r\n", "SET", "ttl-key", "old", "NX", "EX", "10")
			cacheExec(t, s, "$3\r\nold\r\n", "GET", "ttl-key")
			cacheExec(t, s, "$-1\r\n", "SET", "ttl-key", "bad", "NX")
			cacheExec(t, s, "$3\r\nold\r\n", "GET", "ttl-key")
			cacheExec(t, s, "+OK\r\n", "SET", "ttl-key", "new", "XX", "KEEPTTL")
			if b := string(s.cache.execute(context.Background(), cacheArgs("PTTL", "ttl-key"))); !strings.HasPrefix(b, ":") || b == ":-1\r\n" {
				t.Fatal(b)
			}
			cacheExec(t, s, "$3\r\nnew\r\n", "GET", "ttl-key")
			cacheExec(t, s, ":1\r\n", "PERSIST", "ttl-key")
			cacheExec(t, s, ":-1\r\n", "PTTL", "ttl-key")
			cacheExec(t, s, ":0\r\n", "PERSIST", "ttl-key")
			cacheExec(t, s, ":1\r\n", "EXPIRE", "ttl-key", "10")
			cacheExec(t, s, "+OK\r\n", "SET", "ttl-key", "plain")
			cacheExec(t, s, ":-1\r\n", "TTL", "ttl-key")
			cacheExec(t, s, ":1\r\n", "PEXPIRE", "ttl-key", "100")
			cacheExec(t, s, "$5\r\nplain\r\n", "GET", "ttl-key")
			time.Sleep(130 * time.Millisecond)
			cacheExec(t, s, "$-1\r\n", "GET", "ttl-key")
			cacheExec(t, s, ":-2\r\n", "PTTL", "ttl-key")
			cacheExec(t, s, ":0\r\n", "EXPIRE", "ttl-key", "10")
			cacheExec(t, s, "+OK\r\n", "SET", "ttl-key", "again", "PX", "1000")
			cacheExec(t, s, ":1\r\n", "PEXPIRE", "ttl-key", "0")
			cacheExec(t, s, "$-1\r\n", "GET", "ttl-key")
		})
	}
}

func TestCacheTTLDelayedPublish(t *testing.T) {
	s, hot, _ := cacheTestSetup(t, "invalidate")
	cacheExec(t, s, "+OK\r\n", "SET", "ttl-delay", "x", "PX", "150")
	relay := cacheFaultRelay(t, hot, func(a [][]byte) bool {
		if len(a) > 1 && string(a[0]) == "EVAL" && string(a[1]) == cachePublishScript {
			time.Sleep(200 * time.Millisecond)
		}
		return false
	})
	s.cfg.RedisAddress = relay
	// Permit the deliberately delayed publication to finish, so the test
	// exercises the absolute expiry, not merely the hot timeout fallback.
	s.cache.cfg.HotTimeout = 500 * time.Millisecond
	cacheExec(t, s, "$1\r\nx\r\n", "GET", "ttl-delay")
	cacheExec(t, s, "$-1\r\n", "GET", "ttl-delay")
}

func TestCacheTTLCollectionsAndRenewal(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			s, _, _ := cacheTestSetup(t, policy)
			for _, cmd := range [][]string{{"HSET", "ttl-h", "f", "v"}, {"RPUSH", "ttl-l", "v"}, {"SADD", "ttl-s", "v"}, {"ZADD", "ttl-z", "1", "v"}} {
				cacheExec(t, s, ":1\r\n", cmd...)
				cacheExec(t, s, ":1\r\n", "PEXPIRE", cmd[1], "500")
				cacheExec(t, s, ":1\r\n", "EXISTS", cmd[1])
			}
			time.Sleep(550 * time.Millisecond)
			for _, key := range []string{"ttl-h", "ttl-l", "ttl-s", "ttl-z"} {
				cacheExec(t, s, ":0\r\n", "EXISTS", key)
			}
			cacheExec(t, s, "+OK\r\n", "SET", "ttl-renew", "v", "PX", "100")
			cacheExec(t, s, "$1\r\nv\r\n", "GET", "ttl-renew")
			cacheExec(t, s, ":1\r\n", "PEXPIRE", "ttl-renew", "10000")
			time.Sleep(130 * time.Millisecond)
			cacheExec(t, s, "$1\r\nv\r\n", "GET", "ttl-renew")
			bad := s.cache.execute(context.Background(), cacheArgs("SET", "ttl-renew", "bad", "PX", "0"))
			if len(bad) == 0 || bad[0] != '-' {
				t.Fatal(string(bad))
			}
			cacheExec(t, s, "$1\r\nv\r\n", "GET", "ttl-renew")
			cacheExec(t, s, ":1\r\n", "DEL", "ttl-renew")
		})
	}
}
