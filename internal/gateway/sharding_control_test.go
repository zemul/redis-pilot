package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestColdControlConfiguration(t *testing.T) {
	c := shardConfigForTest(t)
	c.ControlPlane.ManageShards = true
	if c.Validate() == nil {
		t.Fatal("enabled without control endpoint")
	}
	c.ControlPlane.ServerEndpoint = "http://127.0.0.1:8080"
	c.ControlPlane.TokenFile = "/private/token"
	c.ControlPlane.NodeID = "node"
	c.ControlPlane.ClusterID = c.ServiceID
	c.ControlPlane.ShardIDs = []string{"entry"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.BackendPasswordFile = "/private/backend"
	if c.Validate() == nil {
		t.Fatal("managed backend password accepted")
	}
	c.BackendPasswordFile = ""
	c.Cache.Sharding.Nodes[0].PasswordFile = "/private/cold"
	if c.Validate() == nil {
		t.Fatal("managed cold password accepted")
	}
}

func anonymousColdCall(t *testing.T, c Config, address string, args ...string) []byte {
	t.Helper()
	conn, r, err := authenticated(address, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return cacheCall(t, &cacheConn{Conn: conn, reader: r, cfg: c}, args...)
}

// Uses actual Kvrocks durability settings and real Gateway subprocesses. The
// HTTP fixture delegates persistence/idempotency to the real Server store;
// HTTP authorization/handlers are independently covered in internal/server.
func TestColdControlKvrocksRecovery(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_KVROCKS_SERVER") == "" {
		t.Skip("real Kvrocks required for task preflight")
	}
	for _, policy := range []string{"invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			ctx := context.Background()
			a, hot := migrationCold(t), g4Redis(t)
			b, stopB, startB, backendConfig := isolatedKvrocksWithConfig(t)
			cfg := shardConfigForTest(t)
			cfg.RedisAddress = hot
			cfg.GatewayID = "gw"
			cfg.ServiceID = "cluster"
			cfg.Listen = freeAddress(t)
			cfg.AdminListen = freeAddress(t)
			cfg.Cache.WritePolicy = policy
			cfg.FrontendPasswordFile = secretFile(t, "shard-config-front")
			configuredBackendCall(t, cfg, hot, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
			for _, address := range []string{a, b, hot} {
				configuredBackendCall(t, cfg, address, "CONFIG", "SET", "requirepass", "")
			}
			cfg.Cache.Sharding.Nodes = []ShardNodeConfig{{ID: "a", Address: a}}
			service, err := NewService(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.InitializeShardState(ctx); err != nil {
				t.Fatal(err)
			}
			d, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if err := d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "127.0.0.1"}); err != nil {
				t.Fatal(err)
			}
			for name, address := range map[string]string{"hot": hot, "a": a, "b": b} {
				engine := "kvrocks"
				if name == "hot" {
					engine = "redis"
				}
				_, port, _ := net.SplitHostPort(address)
				p, _ := strconv.Atoi(port)
				if err := d.UpsertGroup(name, &apitypes.InstanceGroupState{Engine: engine, Type: "standalone"}); err != nil {
					t.Fatal(err)
				}
				if err := d.UpsertInstance(name, &apitypes.Instance{Group: name, Server: "node", Role: "master", Port: p, DesiredState: "running"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := d.CreateColdCluster(apitypes.ColdClusterSpec{ID: "cluster", GatewayID: "gw", NodeID: "node", HotInstance: "hot", Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}); err != nil {
				t.Fatal(err)
			}
			var unavailable, loseAck atomic.Bool
			const token = "cold-control-machine-credential"
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+token {
					w.WriteHeader(401)
					return
				}
				if unavailable.Load() {
					w.WriteHeader(503)
					return
				}
				var data any
				var err error
				if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/report") {
					var report apitypes.ColdTaskReport
					err = json.NewDecoder(r.Body).Decode(&report)
					if err == nil {
						err = d.ReportColdTask("gw", "cluster", report)
					}
					if loseAck.Swap(false) {
						w.WriteHeader(503)
						return
					}
				} else if r.Method == "POST" {
					data = map[string]any{"accepted_sequence": 1}
				} else {
					data, err = d.GetColdCluster("cluster")
				}
				if err != nil {
					w.WriteHeader(409)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
			}))
			defer httpServer.Close()
			cfg.ControlPlane = ControlPlaneConfig{ManageShards: true, ServerEndpoint: httpServer.URL, TokenFile: secretFile(t, token), NodeID: "node", ClusterID: "cluster", ShardIDs: []string{"entry"}, HeartbeatInterval: time.Second, RequestTimeout: time.Second}
			cfg.Cache.Sharding.Migration.KeysPerSecond = 1
			stop := startConfiguredShardProcess(t, cfg)
			targetNodes := []apitypes.ColdShard{{ID: "a", Instance: "a", Address: a}, {ID: "b", Instance: "b", Address: b}}
			to, _ := coldTaskTopology(2, targetNodes)
			conn, r, err := authenticated(cfg.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			var keys []string
			for i := 0; i < 5; i++ {
				key := shardKeyFor(to, "b", "control-"+strconv.Itoa(i))
				keys = append(keys, key)
				shardWant(t, conn, r, "+OK\r\n", "SET", key, "old")
			}
			conn.Close()
			task, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "expand", ExpectedRevision: 1, Shards: targetNodes})
			if err != nil {
				t.Fatal(err)
			}
			waitReport := func(predicate func(*apitypes.ColdTask) bool) {
				t.Helper()
				deadline := time.Now().Add(20 * time.Second)
				for {
					c, err := d.GetColdCluster("cluster")
					if err != nil {
						t.Fatal(err)
					}
					if c.Task != nil && predicate(c.Task) {
						return
					}
					if time.Now().After(deadline) {
						t.Fatalf("report timeout: %+v", c.Task)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			waitReport(func(t *apitypes.ColdTask) bool { return t.ErrorCode == "waiting_configuration" })
			conn, r, err = authenticated(cfg.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "+OK\r\n", "SET", "unaffected", "available")
			conn.Close()
			stop()
			cfg.Cache.Sharding.Nodes = append(cfg.Cache.Sharding.Nodes, ShardNodeConfig{ID: "b", Address: b})
			setSync := func(old, value string) {
				stopB()
				contents, err := os.ReadFile(backendConfig)
				if err != nil {
					t.Fatal(err)
				}
				contents = bytes.ReplaceAll(contents, []byte("rocksdb.write_options.sync "+old+"\n"), []byte("rocksdb.write_options.sync "+value+"\n"))
				if err := os.WriteFile(backendConfig, contents, 0600); err != nil {
					t.Fatal(err)
				}
				startB()
				configuredBackendCall(t, cfg, b, "CONFIG", "SET", "requirepass", "")
			}
			setSync("yes", "no")
			stop = startConfiguredShardProcess(t, cfg)
			waitReport(func(t *apitypes.ColdTask) bool { return t.ErrorCode == "preflight_failed" })
			conn, r, err = authenticated(cfg.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "$9\r\navailable\r\n", "GET", "unaffected")
			conn.Close()
			setSync("no", "yes")
			loseAck.Store(true) // Server commits acceptance but response is lost.
			waitReport(func(t *apitypes.ColdTask) bool { return t.Phase == "active" })
			unavailable.Store(true)
			stop()
			cfg.Cache.Sharding.Migration.KeysPerSecond = 100000
			stop = startConfiguredShardProcess(t, cfg) // Server still down; restores locally.
			u := url.URL{Scheme: "file", Path: filepath.Join(cfg.Cache.Sharding.StateDir, "tasks", task.ID, "handoffs.db")}
			u.RawQuery = "mode=ro&_pragma=busy_timeout(1000)"
			journal, err := sql.Open("sqlite", u.String())
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			deadline := time.Now().Add(15 * time.Second)
			for {
				var phase string
				if err := journal.QueryRow(`SELECT phase FROM finalization WHERE id=1`).Scan(&phase); err != nil {
					t.Fatal(err)
				}
				if phase == "complete" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("local worker waited for Server")
				}
				time.Sleep(20 * time.Millisecond)
			}
			conn, r, err = authenticated(cfg.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				shardWant(t, conn, r, "$3\r\nold\r\n", "GET", key)
				shardWant(t, conn, r, "+OK\r\n", "SET", key, "new")
				shardWant(t, conn, r, "$3\r\nnew\r\n", "GET", key)
			}
			conn.Close()
			unavailable.Store(false)
			waitReport(func(t *apitypes.ColdTask) bool { return t.Phase == "complete" })
			c, err := d.GetColdCluster("cluster")
			if err != nil || c.Revision != 2 {
				t.Fatal("Server did not commit completion", err)
			}
			stop()
			stop = startConfiguredShardProcess(t, cfg)
			conn, r, err = authenticated(cfg.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "$3\r\nnew\r\n", "GET", keys[0])
			conn.Close()
			if _, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "shrink", ExpectedRevision: 2, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}); err != nil {
				t.Fatal(err)
			}
			if err := d.DeleteInstance("b"); err == nil {
				t.Fatal("source released before Gateway cleanup")
			}
			waitReport(func(t *apitypes.ColdTask) bool { return t.ID == "shrink" && t.Phase == "complete" })
			if err := d.DeleteInstance("b"); err != nil {
				t.Fatal("completed source not released", err)
			}
			stop()
			stopB()
			cfg.Cache.Sharding.Nodes = cfg.Cache.Sharding.Nodes[:1]
			stop = startConfiguredShardProcess(t, cfg)
			conn, r, err = authenticated(cfg.Listen, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				shardWant(t, conn, r, "$3\r\nnew\r\n", "GET", key)
			}
			conn.Close()
			stop()
		})
	}
}
