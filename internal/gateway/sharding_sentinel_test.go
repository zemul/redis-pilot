package gateway

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A real isolated Sentinel election, not REPLICAOF NO ONE scripted by the test.
// Server generation dispatch is covered separately by the Server handler tests.
func TestShardPrimaryNativeSentinelElection(t *testing.T) {
	s, assignment, replica := primaryTestRuntime(t)
	ctx := context.Background()
	old := assignment.Primaries[0].Address
	attachNativeReplica(t, s.cfg, old, replica)
	assignment.Primaries[0].Replicas, assignment.Primaries[0].Generation = 1, 2
	if err := s.reconcilePrimaries(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	key := primaryKey(t, s, "a")
	if b := s.cache.shards.execute(ctx, cacheArgs("SET", key, "before")); string(b) != "+OK\r\n" {
		t.Fatal(string(b))
	}
	// Confirm test data arrived before injecting primary failure; business writes
	// themselves remain asynchronous.
	for deadline := time.Now().Add(5 * time.Second); ; {
		if string(configuredBackendCall(t, s.cfg, replica, "GET", key)) == "$6\r\nbefore\r\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("test data not replicated")
		}
		time.Sleep(20 * time.Millisecond)
	}
	address := freeAddress(t)
	_, port, _ := net.SplitHostPort(address)
	host, primaryPort, _ := net.SplitHostPort(old)
	dir := t.TempDir()
	path := filepath.Join(dir, "sentinel.conf")
	config := fmt.Sprintf("bind 127.0.0.1\nport %s\ndir %s\nprotected-mode no\nsentinel monitor isolated %s %s 1\nsentinel auth-pass isolated g4-backend-test\nsentinel down-after-milliseconds isolated 500\nsentinel failover-timeout isolated 3000\n", port, dir, host, primaryPort)
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	ownProcess(t, os.Getenv("GATEWAY_TEST_REDIS_SERVER"), path, "--sentinel")
	call := func(args ...string) ([]byte, error) {
		conn, err := net.DialTimeout("tcp", address, 200*time.Millisecond)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		reader, _ := NewReader(conn, s.cfg.Limits)
		return (&cacheConn{Conn: conn, reader: reader, cfg: s.cfg}).call(ctx, args...)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		b, err := call("SENTINEL", "REPLICAS", "isolated")
		if err == nil {
			if parts, e := frameParts(b); e == nil && len(parts) == 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("Sentinel did not discover replica")
		}
		time.Sleep(50 * time.Millisecond)
	}
	conn, reader, err := authenticated(old, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = (&cacheConn{Conn: conn, reader: reader, cfg: s.cfg}).call(ctx, "SHUTDOWN", "NOSAVE")
	conn.Close()
	for deadline := time.Now().Add(20 * time.Second); ; {
		b, err := call("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "isolated")
		if err == nil {
			if values, e := cacheValues(b); e == nil && len(values) == 2 && net.JoinHostPort(values[0], values[1]) == replica {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("Sentinel election did not complete")
		}
		time.Sleep(50 * time.Millisecond)
	}
	assignment.Primaries[0].Address, assignment.Primaries[0].Instance, assignment.Primaries[0].Generation = replica, "sentinel-promoted", 3
	if err := s.reconcilePrimaries(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	if b := s.cache.shards.execute(ctx, cacheArgs("GET", key)); string(b) != "$6\r\nbefore\r\n" {
		t.Fatal(string(b))
	}
	if b := s.cache.shards.execute(ctx, cacheArgs("SET", key, "after")); string(b) != "+OK\r\n" {
		t.Fatal("degraded primary cannot accept writes", string(b))
	}
}
