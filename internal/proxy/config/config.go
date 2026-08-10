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
	ServerEndpoint string   `yaml:"server_endpoint"`
	ServerToken    string   `yaml:"server_token"`
	PollInterval   Duration `yaml:"poll_interval"`
	PollTimeout    Duration `yaml:"poll_timeout"`
	PollWait       Duration `yaml:"poll_wait"`

	// Static 模式（M3 兼容）
	Listen   string   `yaml:"listen"`
	Master   string   `yaml:"master"`
	Replicas []string `yaml:"replicas"`
	Password string   `yaml:"password"`

	// 通用
	MasterReadWeight  int      `yaml:"master_read_weight"`
	PoolSize          int      `yaml:"pool_size"`
	IdleTimeout       Duration `yaml:"idle_timeout"`
	MetricsListen     string   `yaml:"metrics_listen"`
	MaxRequestBytes   int      `yaml:"max_request_bytes"`
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
