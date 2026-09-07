// redis-gateway currently starts only the isolated G1 skeleton, not forwarding.
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

func main() {
	file := flag.String("config", "", "gateway skeleton YAML configuration")
	check := flag.Bool("check-config", false, "validate configuration and exit")
	flag.Parse()
	c := gateway.DefaultConfig()
	if *file != "" {
		var err error
		c, err = gateway.LoadConfig(*file)
		if err != nil {
			slog.Error("invalid gateway configuration", "error", err)
			os.Exit(1)
		}
	}
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
	slog.Warn("starting G1 gateway skeleton; business forwarding unavailable", "listen", c.Listen, "admin_listen", c.AdminListen)
	if err = s.Run(ctx); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}
