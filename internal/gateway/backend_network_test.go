package gateway

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type clockReply string

func (r clockReply) call(context.Context, ...string) ([]byte, error) { return []byte(r), nil }

func TestMigrationBackendClock(t *testing.T) {
	now := time.Now()
	good := clockReply(fmt.Sprintf("*2\r\n:%d\r\n:%d\r\n", now.Unix(), now.Nanosecond()/1000))
	if err := checkBackendClock(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []clockReply{"+OK\r\n", "*2\r\n:0\r\n:0\r\n", "*2\r\n:1\r\n:1000000\r\n", "-ERR TIME unavailable\r\n"} {
		if checkBackendClock(context.Background(), raw) == nil {
			t.Fatal("accepted invalid/skewed clock", raw)
		}
	}
}

func TestCacheBackendNetworks(t *testing.T) {
	c := shardConfigForTest(t)
	c.RedisAddress = "10.70.100.63:17000"
	c.Cache.Sharding.Nodes[0].Address = "10.70.100.68:17001"
	if c.Validate() == nil {
		t.Fatal("remote backends admitted implicitly")
	}
	c.Cache.BackendNetworks = []string{"10.70.100.63/32", "10.70.100.68/32"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"10.70.100.67:17001", "example.com:6379", "0.0.0.0:6379", "[fe80::1%eth0]:6379", "8.8.8.8:6379", "10.70.100.63:0"} {
		if c.Cache.allowsBackend(address) {
			t.Fatal("unsafe address", address)
		}
	}
	for _, cidr := range []string{"0.0.0.0/0", "10.0.0.0/7", "172.16.0.0/11", "192.168.0.0/15", "10.0.0.1/24", "::/0", "fc00::/6", "[::ffff:10.0.0.1]/128"} {
		c.Cache.BackendNetworks = []string{cidr}
		if c.Validate() == nil {
			t.Fatal("unsafe CIDR", cidr)
		}
	}
}

func TestMigrationClockInterval(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		elapsed, offset time.Duration
		ok              bool
	}{
		{time.Millisecond, 0, true}, {100 * time.Millisecond, 50 * time.Millisecond, true},
		{time.Millisecond, time.Second, false}, {time.Millisecond, -time.Second, false},
		{time.Second, 500 * time.Millisecond, false},
	} {
		if got := clockIntervalSafe(now, now.Add(tc.elapsed), now.Add(tc.offset)); got != tc.ok {
			t.Fatalf("%+v: %v", tc, got)
		}
	}
}
