package gateway

import (
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"time"
)

// CacheConfig is deliberately separate from the retired hot_cold protocol.
// RedisAddress/BackendPasswordFile configure the disposable hot Redis.
type CacheConfig struct {
	Types             map[string]CacheObjectLimits `yaml:"types"`
	HotTimeout        time.Duration                `yaml:"hot_timeout"`
	ColdOnlyPrefixes  []string                     `yaml:"cold_only_prefixes"`
	MaxBlocking       int                          `yaml:"max_blocking"`
	ColdAddress       string                       `yaml:"cold_address"`
	ColdPasswordFile  string                       `yaml:"cold_password_file"`
	ExclusiveBackends bool                         `yaml:"exclusive_backends"`
	LockFile          string                       `yaml:"lock_file"`
	WritePolicy       string                       `yaml:"write_policy"`
	MaxObjectBytes    int                          `yaml:"max_object_bytes"`
	MaxObjectElements int                          `yaml:"max_object_elements"`
	MaxLoads          int                          `yaml:"max_loads"`
	TTL               time.Duration                `yaml:"ttl"`
}

type CacheObjectLimits struct {
	MaxBytes    int `yaml:"max_bytes"`
	MaxElements int `yaml:"max_elements"`
}

func (c CacheConfig) objectLimits(kind string) CacheObjectLimits {
	o := c.Types[kind]
	if o.MaxBytes == 0 {
		o.MaxBytes = c.MaxObjectBytes
	}
	if o.MaxElements == 0 {
		o.MaxElements = c.MaxObjectElements
	}
	return o
}

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
	if c.MaxObjectBytes == 0 {
		c.MaxObjectBytes = 64 << 10
	}
	if c.MaxObjectElements == 0 {
		c.MaxObjectElements = 256
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
	for kind := range o.Types {
		if kind != "string" && kind != "hash" && kind != "list" && kind != "set" && kind != "zset" {
			return errors.New("unknown cache object type")
		}
		v := o.objectLimits(kind)
		if v.MaxBytes < 64 || v.MaxBytes > 1<<20 || v.MaxElements < 1 || v.MaxElements > 1024 || c.Limits.MaxBytes < v.MaxBytes*2+16384 || c.Limits.MaxElements < v.MaxElements*2+32 {
			return errors.New("invalid per-type cache object limits")
		}
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
	for _, addr := range []string{c.RedisAddress, o.ColdAddress} {
		h, p, err := net.SplitHostPort(addr)
		n, e := strconv.Atoi(p)
		ip := net.ParseIP(h)
		if err != nil || e != nil || n < 1 || n > 65535 || ip == nil || !ip.IsLoopback() {
			return errors.New("cache backends require literal loopback addresses")
		}
	}
	if c.RedisAddress == o.ColdAddress {
		return errors.New("hot and cold backends must differ")
	}
	if o.WritePolicy != "invalidate" && o.WritePolicy != "update_existing" {
		return errors.New("cache write_policy must be invalidate or update_existing")
	}
	if o.MaxObjectBytes < 64 || o.MaxObjectBytes > 1<<20 || o.MaxObjectElements < 1 || o.MaxObjectElements > 1024 || o.MaxLoads < 1 || o.MaxLoads > 32 || o.TTL < time.Second || o.TTL > time.Hour {
		return errors.New("invalid cache size/load/TTL limits")
	}
	if c.Limits.MaxBytes < o.MaxObjectBytes*2+16384 || c.Limits.MaxElements < o.MaxObjectElements*2+32 {
		return errors.New("RESP limits must accommodate a bounded cache object and envelope")
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
