package gateway

import (
	"fmt"
	"net/netip"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"gopkg.in/yaml.v3"
)

// InitialConfig renders a fresh installation only. It must never reconstruct
// lost ownership state after migration or read credential contents.
func InitialConfig(cluster apitypes.ColdCluster, req apitypes.GatewayBootstrapRequest) (string, error) {
	if cluster.Revision != 1 || cluster.Task != nil {
		return "", fmt.Errorf("initial config requires revision 1 with no migration task; preserve existing Gateway state")
	}
	if !cluster.ValidEngine() {
		return "", fmt.Errorf("requires Redis without cache, or Kvrocks with optional cache")
	}
	if !cluster.ValidWritePolicy() || (cluster.UsesCache() && cluster.WritePolicy == "") {
		return "", fmt.Errorf("cluster must declare its cache write policy")
	}
	for _, p := range []string{req.StateDir, req.TokenFile} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || p == "/" || strings.ContainsAny(p, "\r\n\x00") {
			return "", fmt.Errorf("state_dir and token_file must be clean absolute private paths")
		}
	}
	if req.TokenFile == req.StateDir || strings.HasPrefix(req.TokenFile, req.StateDir+"/") {
		return "", fmt.Errorf("token_file must be outside the runtime state directory")
	}
	if cluster.Ingress != nil && cluster.Ingress.GatewayAddress != req.Listen {
		return "", fmt.Errorf("listen must match the registered ingress")
	}
	c := DefaultConfig()
	c.GatewayID, c.ServiceID, c.ShardID = cluster.GatewayID, cluster.ID, cluster.ID+"-entry"
	c.Mode, c.Listen, c.AdminListen = "cache", req.Listen, req.AdminListen
	c.AllowUnauthenticated = true // only loopback, business AUTH remains at Proxy
	c.RedisAddress = cluster.HotAddress
	c.ConnectTimeout, c.RequestTimeout = 200*time.Millisecond, 3*time.Second
	c.Limits = Limits{MaxBytes: 262144, MaxElements: 4096, MaxDepth: 16}
	c.MaxBatchBytes, c.BufferBudgetBytes = 262144, 134217728
	enabled := cluster.UsesCache()
	c.Cache = &CacheConfig{Enabled: &enabled, ExclusiveBackends: true, LockFile: filepath.Join(req.StateDir, "owner.lock"), WritePolicy: cluster.WritePolicy, Sharding: &ShardConfig{StateDir: filepath.Join(req.StateDir, "sharding")}}
	c.Cache.ColdEngine = cluster.StorageEngine()
	resolved := c.Cache.resolved()
	c.Cache = &resolved
	networks := map[string]bool{}
	addAddress := func(address string) error {
		a, err := netip.ParseAddrPort(address)
		if err != nil || a.Port() == 0 || a.Addr().Zone() != "" {
			return fmt.Errorf("invalid backend address")
		}
		ip := a.Addr().Unmap()
		if !ip.IsLoopback() {
			if !ip.IsPrivate() {
				return fmt.Errorf("backend must use a loopback or private IP")
			}
			networks[netip.PrefixFrom(ip, ip.BitLen()).String()] = true
		}
		return nil
	}
	if enabled {
		if err := addAddress(c.RedisAddress); err != nil {
			return "", err
		}
	}
	for _, shard := range cluster.Shards {
		if err := addAddress(shard.Address); err != nil {
			return "", err
		}
		c.Cache.Sharding.Nodes = append(c.Cache.Sharding.Nodes, ShardNodeConfig{ID: shard.ID, Address: shard.Address})
	}
	for network := range networks {
		c.Cache.BackendNetworks = append(c.Cache.BackendNetworks, network)
	}
	sort.Strings(c.Cache.BackendNetworks)
	c.ControlPlane = ControlPlaneConfig{ManageShards: true, ServerEndpoint: req.ServerEndpoint, TokenFile: req.TokenFile, NodeID: cluster.NodeID, ClusterID: cluster.ID, ShardIDs: []string{c.ShardID}, HeartbeatInterval: 5 * time.Second, RequestTimeout: 2 * time.Second}
	if err := c.Validate(); err != nil {
		return "", err
	}
	// Port zero is useful in tests but not a deployable, stable Proxy destination.
	for _, addr := range []string{c.Listen, c.AdminListen} {
		a, err := netip.ParseAddrPort(addr)
		if err != nil || a.Port() == 0 {
			return "", fmt.Errorf("listeners require fixed nonzero ports")
		}
	}
	b, err := yaml.Marshal(c)
	if err != nil {
		return "", err
	}
	if _, err := DecodeConfig(strings.NewReader(string(b))); err != nil {
		return "", err
	}
	return "# Initial configuration only; preserve existing runtime state on restart.\n# Provision the machine token separately (owner-only file); business AUTH is at Proxy.\n" + string(b), nil
}
