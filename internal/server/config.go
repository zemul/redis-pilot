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
	Redis   PortRange `yaml:"redis"`           // Redis 实例端口范围
	EnvoyRW PortRange `yaml:"envoy_readwrite"` // Envoy 读写端口范围
	EnvoyWO PortRange `yaml:"envoy_writeonly"` // Envoy 只读端口范围（yaml key 保持兼容）
}

type SentinelConfig struct {
	Enabled               bool `yaml:"enabled"`
	Replicas              int  `yaml:"replicas"` // 3 or 5
	Port                  int  `yaml:"port"`
	Quorum                int  `yaml:"quorum"`
	DownAfterMilliseconds int  `yaml:"down_after_milliseconds"`
	FailoverTimeout       int  `yaml:"failover_timeout"`
	ParallelSyncs         int  `yaml:"parallel_syncs"`
}

type Config struct {
	Port           int            `yaml:"port"`
	Token          string         `yaml:"token"`
	DataDir        string         `yaml:"data_dir"`
	EnvoyDir       string         `yaml:"envoy_dir"`        // Envoy 配置输出目录，为空则不生成
	EnvoyReloadCmd string         `yaml:"envoy_reload_cmd"` // 写完配置后执行的重载命令，为空则跳过
	Ports          PortConfig     `yaml:"ports"`
	Sentinel       SentinelConfig `yaml:"sentinel"`
	Log            LogConfig      `yaml:"log"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		Port:    8080,
		DataDir: "/opt/redis-server/state",
		Ports: PortConfig{
			Redis:   PortRange{Start: 6379, End: 6499},
			EnvoyRW: PortRange{Start: 16379, End: 16499},
			EnvoyWO: PortRange{Start: 16500, End: 16619},
		},
		Sentinel: SentinelConfig{
			Enabled:               true,
			Replicas:              3,
			Port:                  26379,
			Quorum:                2,
			DownAfterMilliseconds: 5000,
			FailoverTimeout:       30000,
			ParallelSyncs:         1,
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
