package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func poolQuery(c *cli.Client) error {
	return checkResp(c.Get("/pool/query"))
}

func poolAdd(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("pool-add", flag.ExitOnError)
	name := fs.String("name", "", "服务器名称 (required)")
	endpoint := fs.String("endpoint", "", "服务器 IP (required)")
	agentPort := fs.Int("agent-port", 8400, "Agent 端口")
	agentToken := fs.String("agent-token", "", "Agent Token")
	cpuCores := fs.Int("cpu", 0, "CPU 核数 (required)")
	memory := fs.String("memory", "", "内存 (e.g. 64Gi) (required)")
	disk := fs.String("disk", "", "磁盘 (e.g. 500Gi)")
	zone := fs.String("zone", "", "可用区标签")
	role := fs.String("role", "production", "角色标签")
	fs.Parse(args)

	if *name == "" || *endpoint == "" || *cpuCores == 0 || *memory == "" {
		fs.Usage()
		os.Exit(1)
	}

	labels := map[string]string{}
	if *zone != "" {
		labels["zone"] = *zone
	}
	if *role != "" {
		labels["role"] = *role
	}

	return checkResp(c.Post("/pool/add", map[string]interface{}{
		"name": *name,
		"server": &apitypes.PoolServer{
			Endpoint:   *endpoint,
			AgentPort:  *agentPort,
			AgentToken: *agentToken,
			Labels:     labels,
			Capacity: apitypes.ResourceSpec{
				CPUCores: *cpuCores,
				Memory:   *memory,
				Disk:     *disk,
			},
			Allocated: apitypes.ResourceSpec{Memory: "0Gi", Disk: "0Gi"},
			Status:    "healthy",
		},
	}))
}

func poolRemove(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("pool-remove", flag.ExitOnError)
	name := fs.String("name", "", "服务器名称 (required)")
	fs.Parse(args)
	if *name == "" {
		fs.Usage()
		os.Exit(1)
	}
	return checkResp(c.Post("/pool/remove", map[string]string{"name": *name}))
}

func poolUpdate(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("pool-update", flag.ExitOnError)
	name := fs.String("name", "", "服务器名称 (required)")
	jsonFile := fs.String("json", "", "服务器 JSON 文件路径 (required)")
	fs.Parse(args)
	if *name == "" || *jsonFile == "" {
		fmt.Fprintln(os.Stderr, "Usage: redis-tool pool-update --name <name> --json <file>")
		os.Exit(1)
	}

	data, err := os.ReadFile(*jsonFile)
	if err != nil {
		return fmt.Errorf("read json file: %w", err)
	}
	var srv apitypes.PoolServer
	if err := json.Unmarshal(data, &srv); err != nil {
		return fmt.Errorf("parse json: %w", err)
	}
	return checkResp(c.Post("/pool/update", map[string]interface{}{
		"name":   *name,
		"server": &srv,
	}))
}
