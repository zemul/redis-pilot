package xds

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen string       `yaml:"listen"`
	Server ServerConfig `yaml:"server"`
	Poll   PollConfig   `yaml:"poll"`
	Envoy  EnvoyConfig  `yaml:"envoy"`
}

type ServerConfig struct {
	Endpoint string `yaml:"endpoint"`
	Token    string `yaml:"token"`
}

type PollConfig struct {
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
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
			Timeout:  1 * time.Second,
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
		cfg.Poll.Timeout = 1 * time.Second
	}
	if len(cfg.Envoy.NodeIDs) == 0 {
		cfg.Envoy.NodeIDs = []string{"redis-pilot-envoy"}
	}
	return cfg, nil
}
