package gateway

import (
	"errors"
	"net/netip"
	"path/filepath"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

// Nodes is an endpoint/credential registry. Editing it never changes ownership.
// On explicit initialization only, all listed nodes form revision 1.
type ShardConfig struct {
	StateDir  string               `yaml:"state_dir"`
	Nodes     []ShardNodeConfig    `yaml:"nodes"`
	Migration ShardMigrationConfig `yaml:"migration"`
}

type ShardNodeConfig struct {
	ID           string `yaml:"id"`
	Address      string `yaml:"address"`
	PasswordFile string `yaml:"password_file"`
}

type ShardMigrationConfig struct {
	ScanCount      int           `yaml:"scan_count"`
	KeysPerSecond  int64         `yaml:"keys_per_second"`
	BytesPerSecond int64         `yaml:"bytes_per_second"`
	StepTimeout    time.Duration `yaml:"step_timeout"`
}

func (c ShardMigrationConfig) options() migrationWorkerOptions {
	o := defaultMigrationWorkerOptions()
	if c.ScanCount != 0 {
		o.ScanCount = c.ScanCount
	}
	if c.KeysPerSecond != 0 {
		o.KeysPerSecond = c.KeysPerSecond
	}
	if c.BytesPerSecond != 0 {
		o.BytesPerSecond = c.BytesPerSecond
	}
	if c.StepTimeout != 0 {
		o.StepTimeout = c.StepTimeout
	}
	o.Finalize = true
	return o
}

func (c ShardConfig) registry() (*sharding.Topology, error) {
	v := sharding.Config{Algorithm: sharding.Algorithm, Revision: 1}
	for _, n := range c.Nodes {
		v.Nodes = append(v.Nodes, sharding.Node{ID: n.ID, Address: n.Address})
	}
	return sharding.New(v)
}

func (c Config) validateSharding() error {
	o := c.Cache.Sharding
	if c.Cache.ColdAddress != "" || c.Cache.ColdPasswordFile != "" {
		return errors.New("cache.sharding and cold_address/cold_password_file are mutually exclusive")
	}
	if !filepath.IsAbs(o.StateDir) || filepath.Clean(o.StateDir) != o.StateDir || o.StateDir == "/" {
		return errors.New("sharding requires an absolute clean private state_dir")
	}
	t, err := o.registry()
	if err != nil {
		return err
	}
	hot, err := netip.ParseAddrPort(c.RedisAddress)
	if err != nil {
		return errors.New("invalid hot backend address")
	}
	hot = netip.AddrPortFrom(hot.Addr().Unmap(), hot.Port())
	for _, n := range t.Config().Nodes {
		a, _ := netip.ParseAddrPort(n.Address)
		if !c.Cache.allowsBackend(n.Address) {
			return errors.New("cold backend is outside backend_networks")
		}
		if a == hot {
			return errors.New("cold node cannot reuse hot backend")
		}
	}
	for _, n := range o.Nodes {
		if n.PasswordFile != "" && !filepath.IsAbs(n.PasswordFile) {
			return errors.New("cold node password_file must be absolute")
		}
	}
	return o.Migration.options().validate()
}

func (r *cacheRuntime) shardEndpoints() ([]cacheShardEndpoint, error) {
	o := r.cfg.Sharding
	t, err := o.registry()
	if err != nil {
		return nil, err
	}
	files := make(map[string]string, len(o.Nodes))
	for _, n := range o.Nodes {
		files[n.ID] = n.PasswordFile
	}
	var out []cacheShardEndpoint
	for _, n := range t.Config().Nodes {
		p, err := readPassword(files[n.ID])
		if err != nil {
			return nil, errors.New("cannot read cold shard credential for " + n.ID)
		}
		out = append(out, cacheShardEndpoint{node: n, password: p})
	}
	return out, nil
}
