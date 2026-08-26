package server

import (
	"os"

	"gopkg.in/yaml.v3"
)

type LogConfig struct {
	Dir           string `yaml:"dir"`            // 日志目录，为空则不写文件
	Stdout        bool   `yaml:"stdout"`         // 是否同时输出到 stdout
	RetentionDays int    `yaml:"retention_days"` // 日志保留天数，0 = 不清理
	Level         string `yaml:"level"`          // debug/info/warn/error（默认 info）
}

// PortRange 端口范围配置
type PortRange struct {
	Start int `yaml:"start"`
	End   int `yaml:"end"`
}

// PortConfig 端口分配策略配置
type PortConfig struct {
	Redis       PortRange `yaml:"redis"`        // Redis 实例端口范围
	ProxyAuto   PortRange `yaml:"proxy_auto"`   // Proxy 自动读写分离端口范围
	ProxyMaster PortRange `yaml:"proxy_master"` // Proxy 强一致主库端口范围
}

type SentinelConfig struct {
	Enabled               bool     `yaml:"enabled"`
	Nodes                 []string `yaml:"nodes"`
	Replicas              int      `yaml:"replicas"` // 3 or 5
	Port                  int      `yaml:"port"`
	Quorum                int      `yaml:"quorum"`
	DownAfterMilliseconds int      `yaml:"down_after_milliseconds"`
	FailoverTimeout       int      `yaml:"failover_timeout"`
	ParallelSyncs         int      `yaml:"parallel_syncs"`
}

type EngineImageConfig struct {
	Default  string            `yaml:"default"`
	Versions map[string]string `yaml:"versions"`
}

type AdminConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Config struct {
	Port                int                          `yaml:"port"`
	DataDir             string                       `yaml:"data_dir"`
	JWTSecret           string                       `yaml:"jwt_secret"`
	InternalToken       string                       `yaml:"internal_token"`
	Admin               AdminConfig                  `yaml:"admin"`
	Ports               PortConfig                   `yaml:"ports"`
	Images              map[string]EngineImageConfig `yaml:"images"`
	Sentinel            SentinelConfig               `yaml:"sentinel"`
	Log                 LogConfig                    `yaml:"log"`
	// ProxyPublicEndpoint 是 Proxy 对外暴露的域名/IP（不含端口）。由 Server 统一
	// 管理并通过 /api/v1/proxy/snapshot 快照下发给 Proxy（用作伪 Sentinel 应答
	// host）和 Dashboard（拼接展示完整连接地址 <ProxyPublicEndpoint>:<port>）。
	// Proxy 侧不提供本地配置项，完全依赖此值，不做兜底或覆盖。
	ProxyPublicEndpoint string `yaml:"proxy_public_endpoint"`
	// ProxySentinelPort 是伪 Sentinel 对外暴露的端口号（如 26379）。
	// 仅用于 Dashboard 展示完整 Sentinel 接入地址，不影响 Proxy 的实际监听行为。
	ProxySentinelPort int `yaml:"proxy_sentinel_port"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		Port:    8080,
		DataDir: "/opt/redis-server/state",
		Ports: PortConfig{
			Redis:       PortRange{Start: 6379, End: 6499},
			ProxyAuto:   PortRange{Start: 16379, End: 16499},
			ProxyMaster: PortRange{Start: 16500, End: 16619},
		},
		Images: defaultImageConfig(),
		Sentinel: SentinelConfig{
			Enabled:               true,
			Nodes:                 nil,
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
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	normalizeImageConfig(cfg)
	if cfg.Admin.Username == "" {
		cfg.Admin.Username = "admin"
	}
	if cfg.Admin.Password == "" {
		cfg.Admin.Password = "admin"
	}
	return cfg, nil
}

func defaultImageConfig() map[string]EngineImageConfig {
	return map[string]EngineImageConfig{
		"redis": {
			Default: "7",
			Versions: map[string]string{
				"5":   "docker.io/redis:5",
				"6.2": "docker.io/redis:6.2",
				"7":   "docker.io/redis:7",
			},
		},
		"kvrocks": {
			Default: "2.15.0",
			Versions: map[string]string{
				"2.15.0": "docker.io/apache/kvrocks:2.15.0",
			},
		},
	}
}

func normalizeImageConfig(cfg *Config) {
	defaults := defaultImageConfig()
	if cfg.Images == nil {
		cfg.Images = defaults
		return
	}
	for engine, def := range defaults {
		current := cfg.Images[engine]
		if current.Default == "" {
			current.Default = def.Default
		}
		if current.Versions == nil {
			current.Versions = map[string]string{}
		}
		for version, image := range def.Versions {
			if current.Versions[version] == "" {
				current.Versions[version] = image
			}
		}
		cfg.Images[engine] = current
	}
}
