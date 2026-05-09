package main

import (
	"flag"
	"fmt"
	"os"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
)

func inventory(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("inventory", flag.ExitOnError)
	port := fs.String("port", "", "按 Envoy 端口查询")
	server := fs.String("server", "", "按服务器名查询")
	engine := fs.String("engine", "", "按引擎过滤 (redis|kvrocks)")
	view := fs.String("view", "summary", "视图: summary | port | server")
	fs.Parse(args)

	query := fmt.Sprintf("/inventory?view=%s", *view)
	if *port != "" {
		query += "&port=" + *port
	}
	if *server != "" {
		query += "&server=" + *server
	}
	if *engine != "" {
		query += "&engine=" + *engine
	}

	return checkResp(c.Get(query))
}

func inventoryUsage() {
	fmt.Fprintf(os.Stderr, `Usage: redis-tool inventory [flags]

Flags:
  --port     按 Envoy 端口查询
  --server   按服务器名查询
  --engine   按引擎过滤 (redis|kvrocks)
  --view     视图: summary(默认) | port | server
`)
}
