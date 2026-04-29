package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var version = "dev"

var usage = `redis-tool — Redis 多实例管理 CLI

Usage: redis-tool [global flags] <command> [command flags]

Global Flags:
  --server   Server 地址 (default: config file or 127.0.0.1:8080)
  --token    鉴权 Token (priority: flag > env REDIS_SERVER_TOKEN > config)

Commands:
  pool-query                查询资源池
  pool-add                  注册服务器
  pool-remove               移除服务器
  pool-update               更新服务器信息

  instance-list             列出所有实例
  instance-status           查看实例状态
  instance-create           创建实例
  instance-delete           删除实例
  instance-start            启动实例
  instance-stop             停止实例
  instance-config           更新实例配置
  instance-promote          从库提升为主库
  instance-replicate        设置复制目标

  backup-exec               执行备份
  backup-restore            从备份恢复
  backup-list               列出可用备份

  version                   显示版本号
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(1)
	}

	// 提取 global flags（--server, --token）在 subcommand 之前
	var serverFlag, tokenFlag string
	globalFlags := flag.NewFlagSet("global", flag.ContinueOnError)
	globalFlags.StringVar(&serverFlag, "server", "", "Server 地址")
	globalFlags.StringVar(&tokenFlag, "token", "", "鉴权 Token")

	// 找到第一个非 flag 参数作为 subcommand
	args := os.Args[1:]
	var cmdIdx int
	for i, a := range args {
		if a[0] != '-' {
			cmdIdx = i
			break
		}
	}
	globalFlags.Parse(args[:cmdIdx])
	cmd := args[cmdIdx]
	cmdArgs := args[cmdIdx+1:]

	cfg := cli.LoadConfig()
	if serverFlag != "" {
		cfg.Server = serverFlag
	}
	if tokenFlag != "" {
		cfg.Token = tokenFlag
	}

	client := cli.NewClient(cfg)

	var err error
	switch cmd {
	case "pool-query":
		err = poolQuery(client)
	case "pool-add":
		err = poolAdd(client, cmdArgs)
	case "pool-remove":
		err = poolRemove(client, cmdArgs)
	case "pool-update":
		err = poolUpdate(client, cmdArgs)
	case "instance-list":
		err = instanceList(client)
	case "instance-status":
		err = instanceStatus(client, cmdArgs)
	case "instance-create":
		err = instanceCreate(client, cmdArgs)
	case "instance-delete":
		err = instanceDelete(client, cmdArgs)
	case "instance-start":
		err = instanceStart(client, cmdArgs)
	case "instance-stop":
		err = instanceStop(client, cmdArgs)
	case "instance-config":
		err = instanceConfig(client, cmdArgs)
	case "instance-promote":
		err = instancePromote(client, cmdArgs)
	case "instance-replicate":
		err = instanceReplicate(client, cmdArgs)
	case "backup-exec":
		err = backupExec(client, cmdArgs)
	case "backup-restore":
		err = backupRestore(client, cmdArgs)
	case "backup-list":
		err = backupList(client, cmdArgs)
	case "version", "-v", "--version":
		fmt.Println("redis-tool", version)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		fmt.Print(usage)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// printJSON 格式化输出 API 响应
func printJSON(v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

// checkResp 检查 API 响应，成功则打印 data，失败则返回 error
func checkResp(r *apitypes.APIResponse, err error) error {
	if err != nil {
		return err
	}
	if !r.Success {
		return fmt.Errorf("%s", r.Error)
	}
	if r.Data != nil {
		printJSON(r.Data)
	} else {
		fmt.Println("ok")
	}
	return nil
}
