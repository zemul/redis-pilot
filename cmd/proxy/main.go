// Command redis-proxy 是自研 Redis 读写分离代理。
//
// M5：共享连接池（Pool + FIFO 解复用）、Prometheus /metrics。
// 支持 discovery 模式（从 Server 快照驱动）和 static 模式（YAML/CLI 配置）。
//
// 用法（配置文件）：
//
//	redis-proxy -config proxy.yaml
//
// 用法（discovery 模式，CLI）：
//
//	redis-proxy -server-endpoint http://127.0.0.1:8080 -metrics-listen :19100
//
// 用法（static 模式，CLI）：
//
//	redis-proxy -listen :16379 -master 127.0.0.1:6379 -metrics-listen :19100
//
// 配置优先级：CLI flag > YAML > 代码默认值。
package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/config"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/proxysvc"
)

func main() {
	// 临时 slog，加载配置后会被 logger.Setup 覆盖
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	configPath := flag.String("config", "", "config file path (YAML)")

	// Discovery 模式
	proxyID := flag.String("proxy-id", "", "stable proxy identity (empty = hostname)")
	serverEndpoint := flag.String("server-endpoint", "", "Server HTTP endpoint for snapshot (empty = static mode)")
	serverToken := flag.String("server-token", "", "Server bearer token")
	pollInterval := flag.Duration("poll-interval", 2*time.Second, "minimum interval between snapshot polls")
	pollTimeout := flag.Duration("poll-timeout", 5*time.Second, "network round-trip timeout for snapshot fetch")
	pollWait := flag.Duration("poll-wait", 20*time.Second, "long-poll wait (0 = disable long-poll)")
	heartbeatInterval := flag.Duration("heartbeat-interval", 10*time.Second, "proxy status heartbeat interval")

	// Static 模式
	listen := flag.String("listen", ":16379", "proxy listen address (static mode only)")
	master := flag.String("master", "127.0.0.1:6379", "backend master address (static mode only)")
	replicaStr := flag.String("replica", "", "comma-separated replica addresses (static mode only)")
	password := flag.String("password", "", "group password (empty = no auth, static mode only)")

	// 通用
	masterReadWeight := flag.Int("master-read-weight", 0, "master read weight in read pool (0 = master not in read pool)")
	poolSize := flag.Int("pool-size", 128, "max shared connections per backend (§13)")
	idleTimeout := flag.Duration("idle-timeout", 5*time.Minute, "idle upstream connection timeout (§13)")
	maxRequestBytes := flag.Int("max-request-bytes", 16*1024*1024, "max bytes per command (D13)")
	metricsListen := flag.String("metrics-listen", ":19100", "Prometheus /metrics HTTP listen address (empty = disabled)")

	// 伪 Sentinel（proxy-design §19）
	// AnnounceIP 完全由 Server 通过 /api/v1/proxy/snapshot 下发，无本地 flag/配置。
	sentinelListen := flag.String("sentinel-listen", "", "pseudo-sentinel listen address (empty = disabled)")
	sentinelPassword := flag.String("sentinel-password", "", "pseudo-sentinel auth password (empty = no auth)")

	flag.Parse()

	// --- 加载配置文件（YAML） ---
	var fc *config.FileConfig
	if *configPath != "" {
		var err error
		fc, err = config.Load(*configPath)
		if err != nil {
			slog.Error("failed to load config file", "path", *configPath, "err", err)
			os.Exit(1)
		}
		slog.Info("config loaded", "path", *configPath)
	}

	// --- 初始化统一日志 ---
	{
		logCfg := logger.Config{Stdout: true, Component: "proxy"} // 默认输出到 stdout
		if fc != nil {
			if fc.Log.Dir != "" || fc.Log.RetentionDays > 0 {
				logCfg.Dir = fc.Log.Dir
				logCfg.Stdout = fc.Log.Stdout
				logCfg.RetentionDays = fc.Log.RetentionDays
			}
			logCfg.Level = fc.Log.Level
		}
		logger.Setup(logCfg)
	}

	// --- 检测哪些 flag 被显式设置 ---
	setFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})

	// --- 构建配置：默认值 -> YAML 覆盖 -> CLI flag 覆盖 ---
	cfg := proxysvc.Config{
		// 代码默认值
		ConnectTimeout:    3 * time.Second,
		ShutdownTimeout:   10 * time.Second,
		PoolSize:          128,
		IdleTimeout:       5 * time.Minute,
		MaxRequestBytes:   16 * 1024 * 1024,
		Listen:            ":16379",
		MasterAddr:        "127.0.0.1:6379",
		MetricsListen:     ":19100",
		PollInterval:      2 * time.Second,
		PollTimeout:       5 * time.Second,
		PollWait:          20 * time.Second,
		HeartbeatInterval: 10 * time.Second,
	}

	// YAML 覆盖
	if fc != nil {
		if fc.ProxyID != "" {
			cfg.ProxyID = fc.ProxyID
		}
		if fc.ServerEndpoint != "" {
			cfg.ServerEndpoint = fc.ServerEndpoint
		}
		if fc.ServerToken != "" {
			cfg.ServerToken = fc.ServerToken
		}
		if fc.PollInterval != 0 {
			cfg.PollInterval = time.Duration(fc.PollInterval)
		}
		if fc.PollTimeout != 0 {
			cfg.PollTimeout = time.Duration(fc.PollTimeout)
		}
		if fc.PollWait != 0 {
			cfg.PollWait = time.Duration(fc.PollWait)
		}
		if fc.HeartbeatInterval != 0 {
			cfg.HeartbeatInterval = time.Duration(fc.HeartbeatInterval)
		}
		if fc.Listen != "" {
			cfg.Listen = fc.Listen
		}
		if fc.Master != "" {
			cfg.MasterAddr = fc.Master
		}
		if len(fc.Replicas) > 0 {
			cfg.ReplicaAddrs = fc.Replicas
		}
		if fc.Password != "" {
			cfg.Password = fc.Password
		}
		if fc.MasterReadWeight != 0 {
			cfg.MasterReadWeight = fc.MasterReadWeight
		}
		if fc.PoolSize != 0 {
			cfg.PoolSize = fc.PoolSize
		}
		if fc.IdleTimeout != 0 {
			cfg.IdleTimeout = time.Duration(fc.IdleTimeout)
		}
		if fc.MetricsListen != "" {
			cfg.MetricsListen = fc.MetricsListen
		}
		if fc.MaxRequestBytes != 0 {
			cfg.MaxRequestBytes = fc.MaxRequestBytes
		}
		if fc.SentinelListen != "" {
			cfg.SentinelListen = fc.SentinelListen
		}
		if fc.SentinelPassword != "" {
			cfg.SentinelPassword = fc.SentinelPassword
		}
	}

	// CLI flag 覆盖（仅显式设置的 flag）
	if setFlags["proxy-id"] {
		cfg.ProxyID = *proxyID
	}
	if setFlags["server-endpoint"] {
		cfg.ServerEndpoint = *serverEndpoint
	}
	if setFlags["server-token"] {
		cfg.ServerToken = *serverToken
	}
	if setFlags["poll-interval"] {
		cfg.PollInterval = *pollInterval
	}
	if setFlags["poll-timeout"] {
		cfg.PollTimeout = *pollTimeout
	}
	if setFlags["poll-wait"] {
		cfg.PollWait = *pollWait
	}
	if setFlags["heartbeat-interval"] {
		cfg.HeartbeatInterval = *heartbeatInterval
	}
	if setFlags["listen"] {
		cfg.Listen = *listen
	}
	if setFlags["master"] {
		cfg.MasterAddr = *master
	}
	if setFlags["replica"] {
		cfg.ReplicaAddrs = nil
		if *replicaStr != "" {
			for _, r := range strings.Split(*replicaStr, ",") {
				r = strings.TrimSpace(r)
				if r != "" {
					cfg.ReplicaAddrs = append(cfg.ReplicaAddrs, r)
				}
			}
		}
	}
	if setFlags["password"] {
		cfg.Password = *password
	}
	if setFlags["master-read-weight"] {
		cfg.MasterReadWeight = *masterReadWeight
	}
	if setFlags["pool-size"] {
		cfg.PoolSize = *poolSize
	}
	if setFlags["idle-timeout"] {
		cfg.IdleTimeout = *idleTimeout
	}
	if setFlags["metrics-listen"] {
		cfg.MetricsListen = *metricsListen
	}
	if setFlags["max-request-bytes"] {
		cfg.MaxRequestBytes = *maxRequestBytes
	}
	if setFlags["sentinel-listen"] {
		cfg.SentinelListen = *sentinelListen
	}
	if setFlags["sentinel-password"] {
		cfg.SentinelPassword = *sentinelPassword
	}

	p := proxysvc.New(cfg)
	if err := p.ListenAndServe(); err != nil {
		slog.Error("proxy fatal", "err", err)
		os.Exit(1)
	}
}
