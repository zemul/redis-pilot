package main

import (
	"flag"
	"fmt"
	"log"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/agent"
)

var version = "dev"

func main() {
	showVer := flag.Bool("version", false, "print version and exit")
	cfgPath := flag.String("config", "/opt/redis-agent/agent.yaml", "config file path")
	flag.Parse()

	if *showVer {
		fmt.Println("redis-agent", version)
		return
	}

	cfg, err := agent.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	agent.New(cfg).Start()
}
