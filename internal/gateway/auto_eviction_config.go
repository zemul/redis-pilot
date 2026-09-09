package gateway

import (
	"errors"
	"math"
	"time"
)

// Nil/disabled leaves manual residency unchanged. Defaults are resolved without
// modifying the caller's configuration, including for programmatic construction.
type AutoEvictionConfig struct {
	Enabled         bool          `yaml:"enabled"`
	HighWatermark   float64       `yaml:"high_watermark"`
	LowWatermark    float64       `yaml:"low_watermark"`
	Interval        time.Duration `yaml:"interval"`
	BatchTimeout    time.Duration `yaml:"batch_timeout"`
	SampleCount     int           `yaml:"sample_count"`
	MaxKeysPerBatch int           `yaml:"max_keys_per_batch"`
	MinIdle         time.Duration `yaml:"min_idle"`
	MinMemoryBytes  int           `yaml:"min_memory_bytes"`
	MaxSyncLagBytes int64         `yaml:"max_sync_lag_bytes"`
}

func (c *AutoEvictionConfig) resolved() AutoEvictionConfig {
	if c == nil {
		return AutoEvictionConfig{}
	}
	r := *c
	if r.HighWatermark == 0 {
		r.HighWatermark = .8
	}
	if r.LowWatermark == 0 {
		r.LowWatermark = .7
	}
	if r.Interval == 0 {
		r.Interval = 5 * time.Second
	}
	if r.BatchTimeout == 0 {
		r.BatchTimeout = 3 * time.Second
	}
	if r.SampleCount == 0 {
		r.SampleCount = 64
	}
	if r.MaxKeysPerBatch == 0 {
		r.MaxKeysPerBatch = 4
	}
	if r.MinIdle == 0 {
		r.MinIdle = time.Minute
	}
	if r.MinMemoryBytes == 0 {
		r.MinMemoryBytes = 8192
	}
	if r.MaxSyncLagBytes == 0 {
		r.MaxSyncLagBytes = 1 << 20
	}
	return r
}

func (c *AutoEvictionConfig) validate() error {
	if c == nil {
		return nil
	}
	r := c.resolved()
	if math.IsNaN(r.HighWatermark) || math.IsNaN(r.LowWatermark) || r.LowWatermark <= 0 || r.HighWatermark >= 1 || r.LowWatermark >= r.HighWatermark ||
		r.Interval < time.Second || r.Interval > time.Hour || r.BatchTimeout < 100*time.Millisecond || r.BatchTimeout > 10*time.Second ||
		r.SampleCount < 1 || r.SampleCount > 256 || r.MaxKeysPerBatch < 1 || r.MaxKeysPerBatch > r.SampleCount ||
		r.MinIdle < time.Second || r.MinIdle > 24*time.Hour || r.MinMemoryBytes < 8192 || r.MinMemoryBytes > syncPendingValueLimit ||
		r.MaxSyncLagBytes < 1 || r.MaxSyncLagBytes > 64<<20 {
		return errors.New("invalid auto_eviction watermarks, timing, sample, value or lag bounds")
	}
	return nil
}
