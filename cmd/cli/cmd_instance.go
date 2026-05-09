package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
)

func instanceList(c *cli.Client) error {
	return checkResp(c.Get("/instance/list"))
}

func instanceStatus(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("instance-status", flag.ExitOnError)
	name := fs.String("name", "", "实例名称 (required)")
	fs.Parse(args)
	if *name == "" {
		fs.Usage()
		os.Exit(1)
	}
	return checkResp(c.Get("/instance/status?name=" + *name))
}

func instanceCreate(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("instance-create", flag.ExitOnError)
	name := fs.String("name", "", "实例名称 (required)")
	category := fs.String("category", "cache", "类型: cache | persistent")
	engine := fs.String("engine", "redis", "引擎: redis | kvrocks")
	typ := fs.String("type", "standalone", "拓扑: standalone | replication")
	server := fs.String("server", "", "目标服务器 (required)")
	port := fs.Int("port", 0, "端口 (0=自动分配)")
	memory := fs.String("memory", "1Gi", "内存")
	cpus := fs.Int("cpus", 1, "CPU")
	password := fs.String("password", "", "密码")
	replicaOf := fs.String("replica-of", "", "主库实例名或地址 (name 或 ip:port)")
	overrides := fs.String("config", "", "配置覆盖 (k=v,k=v)")
	fs.Parse(args)

	if *name == "" || *server == "" {
		fs.Usage()
		os.Exit(1)
	}

	req := map[string]interface{}{
		"name":     *name,
		"category": *category,
		"engine":   *engine,
		"type":     *typ,
		"server":   *server,
		"port":     *port,
		"memory":   *memory,
		"cpus":     *cpus,
		"password": *password,
	}
	if *replicaOf != "" {
		req["replica_of"] = *replicaOf
	}
	if *overrides != "" {
		req["config_overrides"] = parseKV(*overrides)
	}
	return checkResp(c.Post("/instance/create", req))
}

func instanceDelete(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("instance-delete", flag.ExitOnError)
	name := fs.String("name", "", "实例名称 (required)")
	cleanData := fs.Bool("clean-data", false, "同时清理数据目录")
	fs.Parse(args)
	if *name == "" {
		fs.Usage()
		os.Exit(1)
	}
	return checkResp(c.Post("/instance/delete", map[string]interface{}{
		"name":       *name,
		"clean_data": *cleanData,
	}))
}

func instanceStart(c *cli.Client, args []string) error {
	name := requireName("instance-start", args)
	return checkResp(c.Post("/instance/start", map[string]string{"name": name}))
}

func instanceStop(c *cli.Client, args []string) error {
	name := requireName("instance-stop", args)
	return checkResp(c.Post("/instance/stop", map[string]string{"name": name}))
}

func instanceConfig(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("instance-config", flag.ExitOnError)
	name := fs.String("name", "", "实例名称 (required)")
	overrides := fs.String("set", "", "配置项 (k=v,k=v) (required)")
	restart := fs.Bool("restart", false, "是否重启生效")
	fs.Parse(args)
	if *name == "" || *overrides == "" {
		fs.Usage()
		os.Exit(1)
	}
	return checkResp(c.Post("/instance/config", map[string]interface{}{
		"name":             *name,
		"config_overrides": parseKV(*overrides),
		"restart":          *restart,
	}))
}

func instancePromote(c *cli.Client, args []string) error {
	name := requireName("instance-promote", args)
	return checkResp(c.Post("/instance/promote", map[string]string{"name": name}))
}

func instanceReplicate(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("instance-replicate", flag.ExitOnError)
	name := fs.String("name", "", "实例名称 (required)")
	replicaOf := fs.String("replica-of", "", "主库实例名或地址 (name 或 ip:port) (required)")
	fs.Parse(args)
	if *name == "" || *replicaOf == "" {
		fs.Usage()
		os.Exit(1)
	}
	return checkResp(c.Post("/instance/replicate", map[string]string{
		"name":       *name,
		"replica_of": *replicaOf,
	}))
}

// --- helpers ---

func requireName(cmd string, args []string) string {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	name := fs.String("name", "", "实例名称 (required)")
	fs.Parse(args)
	if *name == "" {
		fs.Usage()
		os.Exit(1)
	}
	return *name
}

func parseKV(s string) map[string]string {
	m := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok {
			m[k] = v
		} else {
			fmt.Fprintf(os.Stderr, "warning: invalid config pair: %s\n", pair)
		}
	}
	return m
}
