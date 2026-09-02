// Package config 提供 proxy 的 YAML 配置文件支持。
//
// 配置优先级：CLI flag > YAML > 代码默认值。
package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration 包装 time.Duration，支持从 YAML 字符串解析（如 "5m"、"500ms"）。
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// FileConfig 是 YAML 配置文件的 schema。
type FileConfig struct {
	// Discovery 模式（M4）
	ProxyID           string   `yaml:"proxy_id"`
	ServerEndpoint    string   `yaml:"server_endpoint"`
	ServerToken       string   `yaml:"server_token"`
	HeartbeatInterval Duration `yaml:"heartbeat_interval"`
	PollInterval      Duration `yaml:"poll_interval"`
	PollTimeout       Duration `yaml:"poll_timeout"`
	PollWait          Duration `yaml:"poll_wait"`

	// Static 模式（M3 兼容）
	Listen   string   `yaml:"listen"`
	Master   string   `yaml:"master"`
	Replicas []string `yaml:"replicas"`
	Password string   `yaml:"password"`

	// 伪 Sentinel 服务发现（proxy-design §19）
	// AnnounceIP 完全由 Server 通过 /api/v1/proxy/snapshot 下发（server.yaml
	// 的 proxy_public_endpoint），不支持本地配置覆盖。
	SentinelListen   string `yaml:"sentinel_listen"`
	SentinelPassword string `yaml:"sentinel_password"`

	// 日志
	Log LogConfig `yaml:"log"`

	// 通用
	MasterReadWeight int      `yaml:"master_read_weight"`
	PoolSize         int      `yaml:"pool_size"`
	IdleTimeout      Duration `yaml:"idle_timeout"`
	MetricsListen    string   `yaml:"metrics_listen"`
	MaxRequestBytes  int      `yaml:"max_request_bytes"`
}

// LogConfig 日志配置。
type LogConfig struct {
	Dir           string `yaml:"dir"`
	Stdout        bool   `yaml:"stdout"`
	RetentionDays int    `yaml:"retention_days"`
	Level         string `yaml:"level"` // debug/info/warn/error（默认 info）
}

// Load 读取并解析 YAML 配置文件。
func Load(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg FileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
