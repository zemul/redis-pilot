package server

import (
	"os"

	"gopkg.in/yaml.v3"
)

type LogConfig struct {
	Dir    string `yaml:"dir"`    // 日志目录，为空则不写文件
	Stdout bool   `yaml:"stdout"` // 是否同时输出到 stdout
}

// PortRange 端口范围配置
type PortRange struct {
	Start int `yaml:"start"`
	End   int `yaml:"end"`
}

// PortConfig 端口分配策略配置
type PortConfig struct {
	Redis        PortRange `yaml:"redis"`          // Redis 实例端口范围
	EnvoyRW      PortRange `yaml:"envoy_readwrite"` // Envoy 读写端口范围
	EnvoyWO      PortRange `yaml:"envoy_writeonly"` // Envoy 仅写端口范围
	EnvoyMgmt    PortRange `yaml:"envoy_mgmt"`      // Envoy 管理端口范围
}

type Config struct {
	Port     int        `yaml:"port"`
	Token    string     `yaml:"token"`
	DataDir  string     `yaml:"data_dir"`
	EnvoyDir string     `yaml:"envoy_dir"` // Envoy 配置输出目录，为空则不生成
	Ports    PortConfig `yaml:"ports"`
	Log      LogConfig  `yaml:"log"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		Port:    8080,
		DataDir: "/opt/redis-server/state",
		Ports: PortConfig{
			Redis:     PortRange{Start: 6379, End: 6499},
			EnvoyRW:   PortRange{Start: 16379, End: 16499},
			EnvoyWO:   PortRange{Start: 16500, End: 16619},
			EnvoyMgmt: PortRange{Start: 26379, End: 26499},
		},
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
