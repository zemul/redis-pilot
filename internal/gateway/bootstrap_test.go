package gateway

import (
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func bootstrapFixture() (apitypes.ColdCluster, apitypes.GatewayBootstrapRequest) {
	return apitypes.ColdCluster{Revision: 1, HotAddress: "10.0.0.2:17000", ColdClusterSpec: apitypes.ColdClusterSpec{ID: "orders", GatewayID: "gw", NodeID: "node", Engine: "kvrocks", WritePolicy: "update_existing", Shards: []apitypes.ColdShard{{ID: "a", Address: "10.0.0.2:17001"}, {ID: "b", Address: "[fd00::3]:17002"}}}}, apitypes.GatewayBootstrapRequest{ServerEndpoint: "http://127.0.0.1:8080", Listen: "127.0.0.1:17579", AdminListen: "127.0.0.1:19500", StateDir: "/var/lib/redis-gateway/orders", TokenFile: "/etc/redis-gateway/orders.secret"}
}

func TestInitialConfigRoundTrip(t *testing.T) {
	for _, policy := range []string{"", "invalidate", "update_existing"} {
		t.Run(policy, func(t *testing.T) {
			cluster, req := bootstrapFixture()
			enabled := policy != ""
			cluster.CacheEnabled, cluster.WritePolicy = &enabled, policy
			if !enabled {
				cluster.HotAddress = ""
			}
			raw, err := InitialConfig(cluster, req)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := DecodeConfig(strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Cache.enabled() != enabled || cfg.RedisAddress != cluster.HotAddress || cfg.Cache.Sharding.StateDir != req.StateDir+"/sharding" || !cfg.ControlPlane.ManageShards || len(cfg.Cache.Sharding.Nodes) != 2 {
				t.Fatalf("lost settings: %+v", cfg)
			}
			if enabled && cfg.Cache.WritePolicy != policy {
				t.Fatal("lost policy")
			}
			if strings.Join(cfg.Cache.BackendNetworks, ",") != "10.0.0.2/32,fd00::3/128" {
				t.Fatal(cfg.Cache.BackendNetworks)
			}
			if cfg.FrontendPasswordFile != "" || cfg.BackendPasswordFile != "" || !cfg.AllowUnauthenticated {
				t.Fatal("unexpected auth model")
			}
			again, err := InitialConfig(cluster, req)
			if err != nil || again != raw {
				t.Fatal("unstable output", err)
			}
		})
	}
}

func TestInitialConfigRejectsUnsafeBootstrap(t *testing.T) {
	for name, change := range map[string]func(*apitypes.ColdCluster, *apitypes.GatewayBootstrapRequest){
		"migrated":          func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) { c.Revision = 2 },
		"migration-pending": func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) { c.Task = &apitypes.ColdTask{} },
		"unknown-policy":    func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) { c.WritePolicy = "" },
		"root-state":        func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) { r.StateDir = "/" },
		"token-in-state": func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) {
			r.TokenFile = r.StateDir + "/secret"
		},
		"public-backend":  func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) { c.HotAddress = "8.8.8.8:6379" },
		"public-listener": func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) { r.Listen = "0.0.0.0:6379" },
		"zero-port":       func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) { r.AdminListen = "127.0.0.1:0" },
		"remote-http": func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) {
			r.ServerEndpoint = "http://10.0.0.1:8080"
		},
		"ingress-mismatch": func(c *apitypes.ColdCluster, r *apitypes.GatewayBootstrapRequest) {
			c.Ingress = &apitypes.ColdIngress{GatewayAddress: "127.0.0.1:9999"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, r := bootstrapFixture()
			change(&c, &r)
			if _, err := InitialConfig(c, r); err == nil {
				t.Fatal("unsafe initial config accepted")
			}
		})
	}
}
