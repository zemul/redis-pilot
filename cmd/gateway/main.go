// redis-gateway starts an isolated skeleton or G2 single-backend passthrough.
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
	file := flag.String("config", "", "gateway skeleton YAML configuration")
	check := flag.Bool("check-config", false, "validate configuration and exit")
	initState := flag.Bool("init-state", false, "initialize hot_cold state once in an empty private directory")
	flag.Parse()
	if *check && *initState {
		slog.Error("check-config and init-state are mutually exclusive")
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
	if *initState {
		if err := s.InitializeHotCold(); err != nil {
			slog.Error("hot_cold initialization failed", "error", err)
			os.Exit(1)
		}
		slog.Info("hot_cold state initialized; normal startup will resume it")
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	slog.Info("starting development gateway", "mode", c.Mode, "listen", c.Listen, "admin_listen", c.AdminListen)
	if err = s.Run(ctx); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}
