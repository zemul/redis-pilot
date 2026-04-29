package server

import (
	"os"

	"gopkg.in/yaml.v3"
)

type LogConfig struct {
	Dir    string `yaml:"dir"`    // 日志目录，为空则不写文件
	Stdout bool   `yaml:"stdout"` // 是否同时输出到 stdout
}

type Config struct {
	Port    int       `yaml:"port"`
	Token   string    `yaml:"token"`
	DataDir string    `yaml:"data_dir"`
	Log     LogConfig `yaml:"log"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		Port:    8080,
		DataDir: "/opt/redis-server/state",
		Log: LogConfig{
			Stdout: true,
		},
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	return cfg, yaml.Unmarshal(data, cfg)
}
