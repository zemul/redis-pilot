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
	data, err := os.ReadFile(filepath.Join(home, ".redis-pilot-cli", "config.yaml"))
	if err == nil {
		yaml.Unmarshal(data, cfg)
	}
	if v := os.Getenv("REDIS_SERVER_TOKEN"); v != "" {
		cfg.Token = v
	}
	return cfg
}

// ConfigPath 返回 CLI 配置文件路径 ~/.redis-pilot-cli/config.yaml。
func ConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".redis-pilot-cli", "config.yaml"), nil
}

// SaveConfig 把配置写入 ~/.redis-pilot-cli/config.yaml。
// 文件含 token，权限设为 0600（仅本人可读写）；目录不存在则创建。
func SaveConfig(cfg *Config) error {
	path, err := ConfigPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
