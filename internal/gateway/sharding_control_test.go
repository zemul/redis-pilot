package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/proxysvc"
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

func TestColdProxyProcessHelper(t *testing.T) {
	endpoint := os.Getenv("PILOT_TEST_COLD_PROXY")
	if endpoint == "" {
		t.Skip("subprocess helper")
	}
	p := proxysvc.New(proxysvc.Config{ServerEndpoint: endpoint, ServerToken: "cold-control-machine-credential", NodeID: "node", PollInterval: 20 * time.Millisecond, PollWait: time.Millisecond, PoolSize: 4, ConnectTimeout: time.Second})
	if err := p.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
}

func startColdProxyProcess(t *testing.T, endpoint, address string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestColdProxyProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "PILOT_TEST_COLD_PROXY="+endpoint)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { cmd.Process.Kill(); cmd.Wait() }) }
	t.Cleanup(stop)
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, r, err := authenticated(address, "shard-config-front")
		if err == nil {
			c.SetDeadline(time.Now().Add(time.Second))
			writeAll(c, encodeCommand("PING"))
			b, e := r.ReadFrame()
			c.Close()
			if e == nil && string(b) == "+PONG\r\n" {
				return
			}
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("Proxy startup: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Uses actual Kvrocks durability settings and real Gateway subprocesses. The
// HTTP fixture delegates persistence/idempotency to the real Server store;
// HTTP authorization/handlers are independently covered in internal/server.
func startColdLiveWriter(t *testing.T, address, key string) func() string {
	t.Helper()
	c, r, err := authenticated(address, "shard-config-front")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeAll(c, encodeCommand("HSET", key, "other", "keep")); err != nil {
		t.Fatal(err)
	}
	b, err := r.ReadFrame()
	if err != nil || (string(b) != ":0\r\n" && string(b) != ":1\r\n") {
		t.Fatal(string(b), err)
	}
	shardWant(t, c, r, "$4\r\nkeep\r\n", "HGET", key, "other")
	stop, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var last string
	var failure error
	go func() {
		defer close(done)
		defer c.Close()
		for i := 1; ; i++ {
			value := strconv.Itoa(i)
			c.SetDeadline(time.Now().Add(5 * time.Second))
			for _, args := range [][]string{{"HSET", key, "counter", value}, {"HGET", key, "counter"}, {"HGET", key, "other"}} {
				if e := writeAll(c, encodeCommand(args...)); e != nil {
					failure = e
					return
				}
				b, e := r.ReadFrame()
				if e != nil {
					failure = e
					return
				}
				if args[0] == "HSET" {
					if string(b) != ":0\r\n" && string(b) != ":1\r\n" {
						failure = fmt.Errorf("write failed: %q", b)
						return
					}
				} else {
					want := value
					if args[2] == "other" {
						want = "keep"
					}
					if string(b) != fmt.Sprintf("$%d\r\n%s\r\n", len(want), want) {
						failure = fmt.Errorf("successful write followed by wrong read: %q want %q", b, want)
						return
					}
				}
			}
			last = value
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	finish := func() string {
		once.Do(func() { close(stop) })
		<-done
		if failure != nil {
			t.Fatal(failure)
		}
		return last
	}
	t.Cleanup(func() { finish() })
	return finish
}

func TestColdControlStorageRecovery(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing", "redis"} {
		t.Run(policy, func(t *testing.T) {
			redisStorage := policy == "redis"
			if !redisStorage && os.Getenv("GATEWAY_TEST_KVROCKS_SERVER") == "" {
				t.Skip("real Kvrocks required for task preflight")
			}
			ctx := context.Background()
			var a, b, hot, backendConfig string
			stopB, startB := func() {}, func() {}
			if redisStorage {
				a, b = isolatedTestRedis(t, true), isolatedTestRedis(t, true)
			} else {
				a, hot = migrationCold(t), g4Redis(t)
				b, stopB, startB, backendConfig = isolatedKvrocksWithConfig(t)
			}
			cfg := shardConfigForTest(t)
			cfg.RedisAddress = hot
			cfg.GatewayID = "gw"
			cfg.ServiceID = "cluster"
			cfg.Listen = freeAddress(t)
			cfg.AdminListen = freeAddress(t)
			cfg.Cache.WritePolicy = policy
			if redisStorage {
				disabled := false
				cfg.Cache.Enabled, cfg.Cache.ColdEngine, cfg.Cache.WritePolicy = &disabled, "redis", "invalidate"
			}
			cfg.FrontendPasswordFile = ""
			cfg.AllowUnauthenticated = true
			if hot != "" {
				configuredBackendCall(t, cfg, hot, "CONFIG", "SET", "maxmemory", "33554432", "maxmemory-policy", "allkeys-lru")
			}
			for _, address := range []string{a, b, hot} {
				if address == "" {
					continue
				}
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
				if address == "" {
					continue
				}
				engine := "kvrocks"
				if name == "hot" || redisStorage {
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
			spec := apitypes.ColdClusterSpec{ID: "cluster", GatewayID: "gw", NodeID: "node", HotInstance: "hot", Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}
			if redisStorage {
				disabled := false
				spec.Engine, spec.CacheEnabled, spec.HotInstance = "redis", &disabled, ""
			}
			if _, err := d.CreateColdCluster(spec); err != nil {
				t.Fatal(err)
			}
			proxyAddress := freeAddress(t)
			_, proxyPort, _ := net.SplitHostPort(proxyAddress)
			port, _ := strconv.Atoi(proxyPort)
			if _, err := d.CreateColdIngress("cluster", apitypes.ColdIngressRequest{GatewayAddress: cfg.Listen, Password: "shard-config-front"}, port, port); err != nil {
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
				if r.URL.Path == "/api/v1/proxy/snapshot" {
					snap := apitypes.ProxySnapshot{Version: "empty"}
					if r.URL.Query().Get("node_id") == "node" {
						cluster, e := d.GetColdCluster("cluster")
						if e != nil {
							t.Error(e)
							w.WriteHeader(500)
							return
						}
						if cluster.Ingress.Ready {
							host, p, _ := net.SplitHostPort(cfg.Listen)
							pn, _ := strconv.Atoi(p)
							snap.Version = "ready"
							snap.Listeners = []apitypes.ProxyListener{{Name: "cold:cluster", Group: "cold:cluster", Mode: "master", Bind: "127.0.0.1", Port: port, Cluster: "cold:cluster", Password: "shard-config-front", AccessMode: "standard"}}
							snap.Clusters = []apitypes.ProxyCluster{{Name: "cold:cluster", Endpoints: []apitypes.ProxyEndpoint{{Address: host, Port: pn}}}}
						}
					}
					data = snap
				} else if strings.HasSuffix(r.URL.Path, "/ingress-ready") {
					err = d.ConfirmColdIngress("gw", "cluster", cfg.Listen)
				} else if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/report") {
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
					data, err = d.ColdAssignment("gw", "cluster")
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
			startColdProxyProcess(t, httpServer.URL, proxyAddress)
			targetNodes := []apitypes.ColdShard{{ID: "a", Instance: "a", Address: a}, {ID: "b", Instance: "b", Address: b}}
			to, _ := coldTaskTopology(2, targetNodes)
			conn, r, err := authenticated(proxyAddress, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			var keys []string
			for i := 0; i < 5; i++ {
				key := shardKeyFor(to, "b", "control-"+strconv.Itoa(i))
				keys = append(keys, key)
				shardWant(t, conn, r, "+OK\r\n", "SET", key, "old")
			}
			// All five types share a tag that moves to b, then returns on shrink.
			tag := "{" + shardKeyFor(to, "b", "five-types") + "}:"
			shardWant(t, conn, r, "+OK\r\n", "SET", tag+"s", "value", "PX", "300000")
			shardWant(t, conn, r, ":1\r\n", "HSET", tag+"h", "field", "value")
			shardWant(t, conn, r, ":2\r\n", "RPUSH", tag+"l", "a", "b")
			shardWant(t, conn, r, ":1\r\n", "SADD", tag+"set", "member")
			shardWant(t, conn, r, ":1\r\n", "ZADD", tag+"z", "3", "member")
			expires := time.Now().Add(300 * time.Second).UnixMilli()
			conn.Close()
			liveKey := shardKeyFor(to, "b", "live-hash")
			checkLive := startColdLiveWriter(t, proxyAddress, liveKey)
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
			conn, r, err = authenticated(proxyAddress, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "+OK\r\n", "SET", "unaffected", "available")
			conn.Close()
			setSync := func(old, value string) {
				if redisStorage {
					mode := "always"
					if value == "no" {
						mode = "everysec"
					}
					anonymousColdCall(t, cfg, b, "CONFIG", "SET", "appendfsync", mode)
					return
				}
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
			task, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "expand", ExpectedRevision: 1, Shards: targetNodes})
			if err != nil {
				t.Fatal(err)
			}
			waitReport(func(t *apitypes.ColdTask) bool { return t.ErrorCode == "preflight_failed" })
			conn, r, err = authenticated(proxyAddress, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "$9\r\navailable\r\n", "GET", "unaffected")
			conn.Close()
			setSync("no", "yes")
			loseAck.Store(true) // Server commits acceptance but response is lost.
			waitReport(func(t *apitypes.ColdTask) bool { return t.Phase == "active" })
			liveValue := checkLive()
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
			conn, r, err = authenticated(proxyAddress, "shard-config-front")
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
			conn, r, err = authenticated(proxyAddress, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, fmt.Sprintf("$%d\r\n%s\r\n", len(liveValue), liveValue), "HGET", liveKey, "counter")
			shardWant(t, conn, r, "$4\r\nkeep\r\n", "HGET", liveKey, "other")
			conn.Close()
			waitReport(func(t *apitypes.ColdTask) bool { return t.Phase == "complete" })
			c, err := d.GetColdCluster("cluster")
			if err != nil || c.Revision != 2 {
				t.Fatal("Server did not commit completion", err)
			}
			stop()
			stop = startConfiguredShardProcess(t, cfg)
			conn, r, err = authenticated(proxyAddress, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "$3\r\nnew\r\n", "GET", keys[0])
			conn.Close()
			checkLive = startColdLiveWriter(t, proxyAddress, liveKey)
			if _, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "shrink", ExpectedRevision: 2, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}); err != nil {
				t.Fatal(err)
			}
			if err := d.DeleteInstance("b"); err == nil {
				t.Fatal("source released before Gateway cleanup")
			}
			waitReport(func(t *apitypes.ColdTask) bool { return t.ID == "shrink" && t.Phase == "complete" })
			liveValue = checkLive()
			if err := d.DeleteInstance("b"); err != nil {
				t.Fatal("completed source not released", err)
			}
			// The platform may allocate a released port to a new shard identity.
			// Reusing it must not require restarting the Gateway registry.
			_, bp, _ := net.SplitHostPort(b)
			bport, _ := strconv.Atoi(bp)
			if err := d.UpsertInstance("b", &apitypes.Instance{Group: "b", Server: "node", Role: "master", Port: bport, DesiredState: "running"}); err != nil {
				t.Fatal(err)
			}
			if _, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "reuse-expand", ExpectedRevision: 3, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}, {ID: "replacement", Instance: "b"}}}); err != nil {
				t.Fatal(err)
			}
			waitReport(func(t *apitypes.ColdTask) bool { return t.ID == "reuse-expand" && t.Phase == "complete" })
			if _, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "reuse-shrink", ExpectedRevision: 4, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}); err != nil {
				t.Fatal(err)
			}
			waitReport(func(t *apitypes.ColdTask) bool { return t.ID == "reuse-shrink" && t.Phase == "complete" })
			if err := d.DeleteInstance("b"); err != nil {
				t.Fatal(err)
			}
			stop()
			stopB()
			cfg.Cache.Sharding.Nodes = cfg.Cache.Sharding.Nodes[:1]
			stop = startConfiguredShardProcess(t, cfg)
			conn, r, err = authenticated(proxyAddress, "shard-config-front")
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				shardWant(t, conn, r, "$3\r\nnew\r\n", "GET", key)
			}
			shardWant(t, conn, r, fmt.Sprintf("$%d\r\n%s\r\n", len(liveValue), liveValue), "HGET", liveKey, "counter")
			conn.Close()
			stop()
			conn, r, err = authenticated(a, "")
			if err != nil {
				t.Fatal(err)
			}
			shardWant(t, conn, r, "$5\r\nvalue\r\n", "GET", tag+"s")
			shardWant(t, conn, r, "$5\r\nvalue\r\n", "HGET", tag+"h", "field")
			shardWant(t, conn, r, "*2\r\n$1\r\na\r\n$1\r\nb\r\n", "LRANGE", tag+"l", "0", "-1")
			shardWant(t, conn, r, ":1\r\n", "SISMEMBER", tag+"set", "member")
			shardWant(t, conn, r, "$1\r\n3\r\n", "ZSCORE", tag+"z", "member")
			if err := writeAll(conn, encodeCommand("PTTL", tag+"s")); err != nil {
				t.Fatal(err)
			}
			ttlFrame, err := r.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			ttl, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(string(ttlFrame), ":"), "\r\n"), 10, 64)
			if err != nil || ttl <= 0 || time.Now().UnixMilli()+ttl > expires+1000 {
				t.Fatalf("TTL reset or lost: %s", ttlFrame)
			}
			conn.Close()
		})
	}
}
