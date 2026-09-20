package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestShardPrimaryInitializationAfterElection(t *testing.T) {
	live := isolatedTestRedis(t, true)
	c := shardConfigForTest(t)
	disabled := false
	c.Cache.Enabled, c.Cache.ColdEngine, c.RedisAddress, c.BackendPasswordFile = &disabled, "redis", "", ""
	configuredBackendCall(t, c, live, "CONFIG", "SET", "requirepass", "")
	dead := freeAddress(t)
	c.Cache.Sharding.Nodes = []ShardNodeConfig{{ID: "a", Address: dead}}
	assignment := apitypes.ColdCluster{Revision: 1, ColdClusterSpec: apitypes.ColdClusterSpec{ID: c.ServiceID, GatewayID: c.GatewayID, NodeID: "node", Engine: "redis", CacheEnabled: &disabled, Shards: []apitypes.ColdShard{{ID: "a", Instance: "original", Address: dead}}}, Primaries: []apitypes.ShardPrimary{{ShardID: "a", Group: "g", Instance: "promoted", Address: live, Generation: 2, Replicas: 1}}}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer initialization-test-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		json.NewEncoder(w).Encode(apitypes.APIResponse{Success: true, Data: assignment})
	}))
	defer api.Close()
	c.ControlPlane = ControlPlaneConfig{ManageShards: true, ServerEndpoint: api.URL, TokenFile: secretFile(t, "initialization-test-token"), NodeID: "node", ClusterID: c.ServiceID, ShardIDs: []string{"entry"}, HeartbeatInterval: time.Second, RequestTimeout: time.Second}
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.InitializeShardState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.cache.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.cache.close()
	if b := s.cache.shards.execute(context.Background(), cacheArgs("SET", "key", "value")); string(b) != "+OK\r\n" {
		t.Fatal(string(b))
	}
	if s.cache.shards.node("a").backendAddress() != live {
		t.Fatal("stale initial endpoint")
	}
}

func primaryTestRuntime(t *testing.T, configure ...func(*Config)) (*Service, apitypes.ColdCluster, string) {
	return primaryEngineTestRuntime(t, "redis", configure...)
}

func primaryTestBackend(t *testing.T, engine string) string {
	t.Helper()
	if engine == "kvrocks" {
		a, _, _ := isolatedKvrocks(t)
		return a
	}
	return isolatedTestRedis(t, true)
}

func primaryEngineTestRuntime(t *testing.T, engine string, configure ...func(*Config)) (*Service, apitypes.ColdCluster, string) {
	t.Helper()
	a, b, replica := primaryTestBackend(t, engine), primaryTestBackend(t, engine), primaryTestBackend(t, engine)
	c := shardConfigForTest(t)
	disabled := false
	c.Cache.Enabled, c.Cache.ColdEngine, c.RedisAddress, c.BackendPasswordFile = &disabled, engine, "", ""
	password := secretFile(t, "g4-backend-test")
	c.Cache.Sharding.Nodes = []ShardNodeConfig{{ID: "a", Address: a, PasswordFile: password}, {ID: "b", Address: b, PasswordFile: password}}
	for _, f := range configure {
		f(&c)
	}
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.InitializeShardState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.cache.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.cache.close)
	assignment := apitypes.ColdCluster{Primaries: []apitypes.ShardPrimary{
		{ShardID: "a", Group: "ga", Instance: "a-master", Address: a, Generation: 1},
		{ShardID: "b", Group: "gb", Instance: "b-master", Address: b, Generation: 1},
	}}
	if err = s.reconcilePrimaries(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	return s, assignment, replica
}

func TestShardPrimaryInvalidatesHotCache(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			ctx := context.Background()
			s, assignment, replacement := primaryTestRuntime(t)
			// Exercise the shared cache runtime with native Redis as a cold test
			// double; public Redis+cache configurations remain unsupported.
			hot := isolatedTestRedis(t, false)
			configuredBackendCall(t, s.cfg, hot, "CONFIG", "SET", "maxmemory", "64mb", "maxmemory-policy", "allkeys-lru")
			enabled := true
			s.cache.cfg.Enabled = &enabled
			s.cache.cfg.WritePolicy = policy
			s.cfg.RedisAddress, s.backendPassword = hot, "g4-backend-test"
			key := primaryKey(t, s, "a")
			s.cache.recoverHot(ctx)
			if !s.cache.hotReady.Load() {
				t.Fatal("hot cache unavailable")
			}
			if b := s.cache.shards.execute(ctx, cacheArgs("SET", key, "old")); string(b) != "+OK\r\n" {
				t.Fatal(string(b))
			}
			if b := s.cache.shards.execute(ctx, cacheArgs("GET", key)); string(b) != "$3\r\nold\r\n" {
				t.Fatal(string(b))
			}
			if b := configuredBackendCall(t, s.cfg, hot, "GET", key); string(b) != "$3\r\nold\r\n" {
				t.Fatal("cache not populated", string(b))
			}
			configuredBackendCall(t, s.cfg, replacement, "SET", key, "new")
			assignment.Primaries[0].Address, assignment.Primaries[0].Instance, assignment.Primaries[0].Generation = replacement, "replacement", 2
			if err := s.reconcilePrimaries(ctx, assignment); err != nil {
				t.Fatal(err)
			}
			if s.cache.hotReady.Load() {
				t.Fatal("old hot layer remained readable")
			}
			if b := s.cache.shards.execute(ctx, cacheArgs("GET", key)); string(b) != "$3\r\nnew\r\n" {
				t.Fatal("stale hot value", string(b))
			}
			s.cache.recoverHot(ctx)
			if b := s.cache.shards.execute(ctx, cacheArgs("GET", key)); string(b) != "$3\r\nnew\r\n" {
				t.Fatal(string(b))
			}
			if b := configuredBackendCall(t, s.cfg, hot, "GET", key); string(b) != "$3\r\nnew\r\n" {
				t.Fatal("cache re-enabled without reset", string(b))
			}
		})
	}
}

func TestShardPrimaryInterruptsBlockingConnection(t *testing.T) {
	ctx := context.Background()
	s, assignment, replacement := primaryTestRuntime(t, func(c *Config) { c.Cache.ColdOnlyPrefixes = []string{"queue:"} })
	key := ""
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("queue:%d", i)
		if s.cache.shards.topology.Owner([]byte(k)).ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no queue key")
	}
	result := make(chan []byte, 1)
	go func() { result <- s.cache.shards.execute(ctx, cacheArgs("BLPOP", key, "30")) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		b := &s.cache.shards.blocking
		b.mu.Lock()
		ready := len(b.connections) > 0
		b.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("blocking connection not registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assignment.Primaries[0].Address, assignment.Primaries[0].Instance, assignment.Primaries[0].Generation = replacement, "replacement", 2
	if err := s.reconcilePrimaries(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-result:
		if !strings.HasPrefix(string(b), "-ERR blocking result unknown") {
			t.Fatal(string(b))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("old blocking connection left running")
	}
}

func primaryKey(t *testing.T, s *Service, id string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("primary:%d", i)
		if s.cache.shards.topology.Owner([]byte(k)).ID == id {
			return k
		}
	}
	t.Fatal("no key for shard")
	return ""
}

func TestShardPrimaryNativeReplicaSwitchAndRestart(t *testing.T) {
	ctx := context.Background()
	s, assignment, replica := primaryTestRuntime(t)
	key := primaryKey(t, s, "a")
	if b := s.cache.shards.execute(ctx, cacheArgs("SET", key, "before")); string(b) != "+OK\r\n" {
		t.Fatal(string(b))
	}
	configuredBackendCall(t, s.cfg, replica, "CONFIG", "SET", "masterauth", "g4-backend-test")
	host, port, _ := net.SplitHostPort(assignment.Primaries[0].Address)
	if b := configuredBackendCall(t, s.cfg, replica, "REPLICAOF", host, port); string(b) != "+OK\r\n" {
		t.Fatal(string(b))
	}
	deadline := time.Now().Add(10 * time.Second)
	for string(configuredBackendCall(t, s.cfg, replica, "GET", key)) != "$6\r\nbefore\r\n" {
		if time.Now().After(deadline) {
			t.Fatal("replica did not catch up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	configuredBackendCall(t, s.cfg, replica, "REPLICAOF", "NO", "ONE")
	watch := &cacheSession{r: s.cache}
	defer watch.close()
	if b := watch.execute(ctx, cacheArgs("WATCH", key)); string(b) != "+OK\r\n" {
		t.Fatal(string(b))
	}
	old := assignment.Primaries[0]
	assignment.Primaries[0].Address, assignment.Primaries[0].Instance, assignment.Primaries[0].Generation = replica, "a-replica", 2
	if err := s.reconcilePrimaries(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	watch.execute(ctx, cacheArgs("MULTI"))
	watch.execute(ctx, cacheArgs("SET", key, "must-not-run"))
	if b := watch.execute(ctx, cacheArgs("EXEC")); string(b) != "*-1\r\n" {
		t.Fatal("WATCH survived primary switch", string(b))
	}
	if b := s.cache.shards.execute(ctx, cacheArgs("GET", key)); string(b) != "$6\r\nbefore\r\n" {
		t.Fatal(string(b))
	}
	if b := s.cache.shards.execute(ctx, cacheArgs("SET", key, "after")); string(b) != "+OK\r\n" {
		t.Fatal(string(b))
	}
	if b := configuredBackendCall(t, s.cfg, replica, "GET", key); string(b) != "$5\r\nafter\r\n" {
		t.Fatal(string(b))
	}
	if b := configuredBackendCall(t, s.cfg, old.Address, "GET", key); string(b) != "$6\r\nbefore\r\n" {
		t.Fatal("old primary still received writes", string(b))
	}
	stale := assignment
	stale.Primaries = append([]apitypes.ShardPrimary(nil), assignment.Primaries...)
	stale.Primaries[0] = old
	if err := s.reconcilePrimaries(ctx, stale); err == nil {
		t.Fatal("stale control assignment accepted")
	}
	s.cache.close()
	restarted, err := NewService(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.cache.start(ctx); err != nil {
		t.Fatal(err)
	}
	defer restarted.cache.close()
	if b := restarted.cache.shards.execute(ctx, cacheArgs("GET", key)); string(b) != "$5\r\nafter\r\n" {
		t.Fatal("restart used obsolete YAML endpoint", string(b))
	}
}

func TestShardPrimaryPendingRecoveryAndRetarget(t *testing.T) {
	ctx := context.Background()
	s, assignment, replacement := primaryTestRuntime(t)
	a, b := primaryKey(t, s, "a"), primaryKey(t, s, "b")
	assignment.Primaries[0].Address, assignment.Primaries[0].Instance, assignment.Primaries[0].Generation = freeAddress(t), "failed-candidate", 2
	if err := s.reconcilePrimaries(ctx, assignment); err == nil {
		t.Fatal("unreachable primary activated")
	}
	if reply := s.cache.shards.execute(ctx, cacheArgs("GET", a)); !strings.HasPrefix(string(reply), "-TRYAGAIN") {
		t.Fatal("pending key admitted", string(reply))
	}
	if reply := s.cache.shards.execute(ctx, cacheArgs("SET", b, "live")); string(reply) != "+OK\r\n" {
		t.Fatal("unrelated shard blocked", string(reply))
	}
	s.cache.close()
	r, err := NewService(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.cache.start(ctx); err != nil {
		t.Fatal(err)
	}
	defer r.cache.close()
	if r.backendReady.Load() {
		t.Fatal("pending recovery falsely ready")
	}
	if reply := r.cache.shards.execute(ctx, cacheArgs("GET", a)); !strings.HasPrefix(string(reply), "-TRYAGAIN") {
		t.Fatal("restart forgot pending barrier", string(reply))
	}
	assignment.Primaries[0].Address, assignment.Primaries[0].Instance, assignment.Primaries[0].Generation = replacement, "new-candidate", 3
	if err = r.reconcilePrimaries(ctx, assignment); err != nil {
		t.Fatal("failed intermediate primary required", err)
	}
	if reply := r.cache.shards.execute(ctx, cacheArgs("SET", a, "new")); string(reply) != "+OK\r\n" {
		t.Fatal(string(reply))
	}
}
