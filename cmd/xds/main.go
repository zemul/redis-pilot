package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/xds"
)

var version = "dev"

func main() {
	showVer := flag.Bool("version", false, "print version and exit")
	cfgPath := flag.String("config", "/opt/redis-pilot-xds/xds.yaml", "config file path")
	flag.Parse()

	if *showVer {
		fmt.Println("redis-pilot-xds", version)
		return
	}

	cfg, err := xds.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := xds.NewService(cfg).Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("xds service error: %v", err)
	}
}
