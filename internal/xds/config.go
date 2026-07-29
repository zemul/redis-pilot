package xds

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen    string       `yaml:"listen"`
	AuthToken string       `yaml:"auth_token"`
	Server    ServerConfig `yaml:"server"`
	Poll      PollConfig   `yaml:"poll"`
	Envoy     EnvoyConfig  `yaml:"envoy"`
}

type ServerConfig struct {
	Endpoint string `yaml:"endpoint"`
	Token    string `yaml:"token"`
}

type PollConfig struct {
	// Interval 是两次请求之间的最小间隔。长轮询模式下它只作为下限（防止 Server 不认识
	// 长轮询参数时变成忙循环），短轮询模式下它就是轮询周期。
	Interval time.Duration `yaml:"interval"`
	// Timeout 是网络往返的余量，不包含服务端挂起的时长。
	Timeout time.Duration `yaml:"timeout"`
	// Wait 是长轮询时允许 Server 挂起请求的最长时间，0 表示关闭长轮询、退回定期轮询。
	// Server 侧上限 30s，配大了会被截断。
	Wait time.Duration `yaml:"wait"`
}

type EnvoyConfig struct {
	NodeIDs []string `yaml:"node_ids"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		Listen: ":18000",
		Server: ServerConfig{
			Endpoint: "http://127.0.0.1:8080",
		},
		Poll: PollConfig{
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Wait:     20 * time.Second,
		},
		Envoy: EnvoyConfig{
			NodeIDs: []string{"redis-pilot-envoy"},
		},
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	if cfg.Listen == "" {
		cfg.Listen = ":18000"
	}
	if cfg.Server.Endpoint == "" {
		cfg.Server.Endpoint = "http://127.0.0.1:8080"
	}
	if cfg.Poll.Interval <= 0 {
		cfg.Poll.Interval = 2 * time.Second
	}
	if cfg.Poll.Timeout <= 0 {
		cfg.Poll.Timeout = 5 * time.Second
	}
	// Wait 允许显式配 0 关闭长轮询，所以只在为负时纠正
	if cfg.Poll.Wait < 0 {
		cfg.Poll.Wait = 0
	}
	if len(cfg.Envoy.NodeIDs) == 0 {
		cfg.Envoy.NodeIDs = []string{"redis-pilot-envoy"}
	}
	return cfg, nil
}
