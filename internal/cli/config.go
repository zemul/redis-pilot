package cli

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server string `yaml:"server"`
	Token  string `yaml:"token"`
}

func LoadConfig() *Config {
	cfg := &Config{Server: "127.0.0.1:8080"}
	home, _ := os.UserHomeDir()
	data, err := os.ReadFile(filepath.Join(home, ".redis-tool", "config.yaml"))
	if err == nil {
		yaml.Unmarshal(data, cfg)
	}
	if v := os.Getenv("REDIS_SERVER_TOKEN"); v != "" {
		cfg.Token = v
	}
	return cfg
}
