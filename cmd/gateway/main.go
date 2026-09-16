// redis-gateway starts an isolated gateway, including cold-authoritative cache.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
)

var version = "dev"

func main() {
	file := flag.String("config", "", "gateway YAML configuration")
	check := flag.Bool("check-config", false, "validate configuration and exit")
	initSharding := flag.Bool("init-sharding-state", false, "initialize local sharding state for empty cold backends and exit")
	flag.Parse()
	if *check && *initSharding {
		slog.Error("check-config and init-sharding-state cannot be combined")
		os.Exit(1)
	}
	c := gateway.DefaultConfig()
	if *file != "" {
		var err error
		c, err = gateway.LoadConfig(*file)
		if err != nil {
			slog.Error("invalid gateway configuration", "error", err)
			os.Exit(1)
		}
	}
	c.BuildVersion = version
	s, err := gateway.NewService(c)
	if err != nil {
		slog.Error("invalid gateway configuration", "error", err)
		os.Exit(1)
	}
	if *check {
		slog.Info("gateway configuration valid", "mode", c.Mode)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *initSharding {
		if err := s.InitializeShardState(ctx); err != nil {
			slog.Error("cannot initialize sharding state", "error", err)
			os.Exit(1)
		}
		slog.Info("sharding state initialized; no service started")
		return
	}
	slog.Info("starting development gateway", "mode", c.Mode, "listen", c.Listen, "admin_listen", c.AdminListen)
	if err = s.Run(ctx); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}
