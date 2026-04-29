package main

import (
	"flag"
	"fmt"
	"log"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/server"
)

var version = "dev"

func main() {
	showVer := flag.Bool("version", false, "print version and exit")
	cfgPath := flag.String("config", "/opt/redis-server/server.yaml", "config file path")
	flag.Parse()

	if *showVer {
		fmt.Println("redis-server", version)
		return
	}

	cfg, err := server.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	s := server.New(cfg)
	s.StartReconcileLoop()
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("redis-pilot server listening on %s", addr)
	if err := s.Router().Run(addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
