package gateway

import (
	"errors"
	"path/filepath"
	"time"
)

// CacheConfig is deliberately separate from the retired hot_cold protocol.
// RedisAddress/BackendPasswordFile configure the disposable hot Redis.
type CacheConfig struct {
	Enabled           *bool         `yaml:"enabled"`
	BackendNetworks   []string      `yaml:"backend_networks"`
	Sharding          *ShardConfig  `yaml:"sharding"`
	HotTimeout        time.Duration `yaml:"hot_timeout"`
	ColdOnlyPrefixes  []string      `yaml:"cold_only_prefixes"`
	MaxBlocking       int           `yaml:"max_blocking"`
	ColdAddress       string        `yaml:"cold_address"`
	ColdPasswordFile  string        `yaml:"cold_password_file"`
	ExclusiveBackends bool          `yaml:"exclusive_backends"`
	LockFile          string        `yaml:"lock_file"`
	WritePolicy       string        `yaml:"write_policy"`
	MaxLoads          int           `yaml:"max_loads"`
	TTL               time.Duration `yaml:"ttl"`
}

// Omission preserves existing deployments; disabled never attempts hot recovery.
func (c CacheConfig) enabled() bool { return c.Enabled == nil || *c.Enabled }

func (c CacheConfig) resolved() CacheConfig {
	if c.HotTimeout == 0 {
		c.HotTimeout = 100 * time.Millisecond
	}
	if c.MaxBlocking == 0 {
		c.MaxBlocking = 4
	}
	if c.WritePolicy == "" {
		c.WritePolicy = "invalidate"
	}
	if c.MaxLoads == 0 {
		c.MaxLoads = 4
	}
	if c.TTL == 0 {
		c.TTL = 5 * time.Minute
	}
	return c
}

func (c Config) validateCache() error {
	if c.Mode != "cache" {
		if c.Cache != nil {
			return errors.New("cache configuration requires mode: cache")
		}
		return nil
	}
	if c.Cache == nil {
		return errors.New("cache mode requires cache configuration")
	}
	o := c.Cache.resolved()
	if err := o.validateBackendNetworks(); err != nil {
		return err
	}
	if o.HotTimeout < time.Millisecond || o.HotTimeout > c.RequestTimeout/2 {
		return errors.New("hot_timeout must be at least 1ms and at most half request_timeout")
	}
	if len(o.ColdOnlyPrefixes) > 16 || o.MaxBlocking < 1 || o.MaxBlocking >= c.MaxConnections {
		return errors.New("invalid cold-only prefixes or blocking connection limit")
	}
	for _, prefix := range o.ColdOnlyPrefixes {
		if len(prefix) < 1 || len(prefix) > 128 {
			return errors.New("cold-only prefixes must be nonempty and bounded")
		}
	}
	if !o.ExclusiveBackends || !filepath.IsAbs(o.LockFile) || o.LockFile == "/" {
		return errors.New("cache mode requires exclusive_backends and an absolute private lock_file")
	}
	var addresses []string
	if o.enabled() {
		addresses = append(addresses, c.RedisAddress)
	} else if c.RedisAddress != "" || c.BackendPasswordFile != "" {
		return errors.New("disabled cache must not configure a hot address or credential")
	}
	if o.Sharding == nil {
		addresses = append(addresses, o.ColdAddress)
	} else if err := c.validateSharding(); err != nil {
		return err
	}
	for _, addr := range addresses {
		if !o.allowsBackend(addr) {
			return errors.New("cache backend must be loopback or in backend_networks")
		}
	}
	if o.enabled() && c.RedisAddress == o.ColdAddress {
		return errors.New("hot and cold backends must differ")
	}
	if o.WritePolicy != "invalidate" && o.WritePolicy != "update_existing" {
		return errors.New("cache write_policy must be invalidate or update_existing")
	}
	if o.MaxLoads < 1 || o.MaxLoads > 32 || o.TTL < time.Second || o.TTL > time.Hour {
		return errors.New("invalid cache load/TTL limits")
	}
	workspaces := int64(2) // recovery plus at least one ordinary client
	if len(o.ColdOnlyPrefixes) > 0 {
		workspaces += int64(o.MaxBlocking)
	}
	if c.BufferBudgetBytes < workspaces*c.workspaceBytes()+c.probeWorkspaceBytes() {
		return errors.New("cache budget must accommodate recovery, blocking clients and an ordinary client")
	}
	return nil
}
