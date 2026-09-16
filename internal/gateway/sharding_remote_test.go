package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Explicit opt-in helper: exposes only a dedicated loopback test engine and
// accepts only one test runner IP. No production backend is opened to the LAN.
func TestRemoteEngineRelay(t *testing.T) {
	address := os.Getenv("GATEWAY_TEST_RELAY_LISTEN")
	if address == "" {
		t.Skip("remote relay helper")
	}
	allowed := net.ParseIP(os.Getenv("GATEWAY_TEST_RELAY_SOURCE"))
	backend := os.Getenv("GATEWAY_TEST_RELAY_BACKEND")
	h, _, err := net.SplitHostPort(backend)
	if err != nil || net.ParseIP(h) == nil || !net.ParseIP(h).IsLoopback() || allowed == nil {
		t.Fatal("explicit source and loopback backend required")
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for {
		c, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer c.Close()
			h, _, _ := net.SplitHostPort(c.RemoteAddr().String())
			if !net.ParseIP(h).Equal(allowed) {
				return
			}
			b, err := net.DialTimeout("tcp", backend, time.Second)
			if err != nil {
				return
			}
			defer b.Close()
			go func() { io.Copy(b, c); b.Close() }()
			io.Copy(c, b)
		}()
	}
}

// Uses NEW EMPTY dedicated engines only. No FLUSH or changes to engine config.
// HTTP control is a fixture with the production store, not a production Server.
func TestColdRemoteAcceptance(t *testing.T) {
	hot, a, b := os.Getenv("GATEWAY_TEST_REMOTE_HOT"), os.Getenv("GATEWAY_TEST_REMOTE_A"), os.Getenv("GATEWAY_TEST_REMOTE_B")
	if hot == "" || a == "" || b == "" {
		t.Skip("three dedicated remote engines required")
	}
	policy := os.Getenv("GATEWAY_TEST_REMOTE_POLICY")
	if policy != "invalidate" && policy != "update_existing" {
		t.Fatal("explicit write policy required")
	}
	cfg := shardConfigForTest(t)
	cfg.RedisAddress = hot
	cfg.BackendPasswordFile = ""
	cfg.GatewayID = "gw"
	cfg.ServiceID = "cluster"
	cfg.Listen = freeAddress(t)
	cfg.AdminListen = freeAddress(t)
	cfg.FrontendPasswordFile = secretFile(t, "shard-config-front")
	cfg.Cache.WritePolicy = policy
	cfg.Cache.HotTimeout = 500 * time.Millisecond
	cfg.Cache.Sharding.Nodes = []ShardNodeConfig{{ID: "a", Address: a}}
	for _, addr := range []string{hot, a, b} {
		h, _, _ := net.SplitHostPort(addr)
		cfg.Cache.BackendNetworks = append(cfg.Cache.BackendNetworks, h+"/32")
	}
	d, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.UpsertServer("entry", &apitypes.NodeServer{Endpoint: "10.70.100.67"}); err != nil {
		t.Fatal(err)
	}
	for name, addr := range map[string]string{"hot": hot, "a": a, "b": b} {
		h, p, _ := net.SplitHostPort(addr)
		pn, _ := strconv.Atoi(p)
		if err = d.UpsertServer(name, &apitypes.NodeServer{Endpoint: h}); err != nil {
			t.Fatal(err)
		}
		engine := "kvrocks"
		if name == "hot" {
			engine = "redis"
		}
		if err = d.UpsertGroup(name, &apitypes.InstanceGroupState{Engine: engine, Type: "standalone"}); err != nil {
			t.Fatal(err)
		}
		if err = d.UpsertInstance(name, &apitypes.Instance{Group: name, Server: name, Port: pn, Role: "master", DesiredState: "running"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = d.CreateColdCluster(apitypes.ColdClusterSpec{ID: "cluster", GatewayID: "gw", NodeID: "entry", HotInstance: "hot", Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer remote-test-token" {
			w.WriteHeader(401)
			return
		}
		var data any
		var e error
		if r.Method == "GET" {
			data, e = d.GetColdCluster("cluster")
		} else if strings.HasSuffix(r.URL.Path, "/report") {
			var report apitypes.ColdTaskReport
			e = json.NewDecoder(r.Body).Decode(&report)
			if e == nil {
				e = d.ReportColdTask("gw", "cluster", report)
			}
		} else {
			data = map[string]any{"accepted_sequence": 1}
		}
		if e != nil {
			w.WriteHeader(409)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
	}))
	defer httpServer.Close()
	cfg.ControlPlane = ControlPlaneConfig{ManageShards: true, ServerEndpoint: httpServer.URL, TokenFile: secretFile(t, "remote-test-token"), NodeID: "entry", ClusterID: "cluster", ShardIDs: []string{"entry"}, HeartbeatInterval: time.Second, RequestTimeout: time.Second}
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.InitializeShardState(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop := startConfiguredShardProcess(t, cfg)
	conn, r, err := authenticated(cfg.Listen, "shard-config-front")
	if err != nil {
		t.Fatal(err)
	}
	target := []apitypes.ColdShard{{ID: "a", Instance: "a"}, {ID: "b", Instance: "b"}}
	to, _ := coldTaskTopology(2, []apitypes.ColdShard{{ID: "a", Address: a}, {ID: "b", Address: b}})
	var keys []string
	var expiries []string
	for i := 0; i < 40; i++ {
		key := shardKeyFor(to, "b", fmt.Sprintf("remote-%d", i))
		keys = append(keys, key)
		shardWant(t, conn, r, "+OK\r\n", "SET", key, "old", "PX", "120000")
		if err := writeAll(conn, encodeCommand("PEXPIRETIME", key)); err != nil {
			t.Fatal(err)
		}
		frame, err := r.ReadFrame()
		if err != nil || len(frame) == 0 || frame[0] != ':' {
			t.Fatalf("expiry: %q %v", frame, err)
		}
		expiries = append(expiries, string(frame))
	}
	expiring := shardKeyFor(to, "b", "expires-during-migration")
	shardWant(t, conn, r, "+OK\r\n", "SET", expiring, "gone", "PX", "100")
	conn.Close()
	checkLive := startColdLiveWriter(t, cfg.Listen, shardKeyFor(to, "b", "live-hash"))
	wait := func(revision uint64) {
		t.Helper()
		deadline := time.Now().Add(45 * time.Second)
		for {
			c, e := d.GetColdCluster("cluster")
			if e != nil {
				t.Fatal(e)
			}
			if c.Revision == revision {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("migration timeout: %+v", c.Task)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if _, err = d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "expand", ExpectedRevision: 1, Shards: target}); err != nil {
		t.Fatal(err)
	}
	wait(2)
	if _, err = d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "shrink", ExpectedRevision: 2, Shards: target[1:]}); err != nil {
		t.Fatal(err)
	}
	wait(3)
	last := checkLive()
	stop()
	stop = startConfiguredShardProcess(t, cfg) // YAML still only contains retired a.
	defer stop()
	conn, r, err = authenticated(cfg.Listen, "shard-config-front")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	shardWant(t, conn, r, "$-1\r\n", "GET", expiring)
	for i, key := range keys {
		shardWant(t, conn, r, "$3\r\nold\r\n", "GET", key)
		shardWant(t, conn, r, expiries[i], "PEXPIRETIME", key)
		shardWant(t, conn, r, "+OK\r\n", "SET", key, "new")
		shardWant(t, conn, r, "$3\r\nnew\r\n", "GET", key)
	}
	shardWant(t, conn, r, fmt.Sprintf("$%d\r\n%s\r\n", len(last), last), "HGET", shardKeyFor(to, "b", "live-hash"), "counter")
	t.Log("remote hot/cold, dynamic expansion, shrink, concurrent Hash writes and durable Gateway restart passed", policy)
}
