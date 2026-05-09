package main

import (
	"strings"

	"github.com/spf13/cobra"
)

var instanceCmd = &cobra.Command{
	Use:   "instance",
	Short: "实例管理",
}

var instanceListCmd = &cobra.Command{
	Use:   "list",
	Short: "列出所有实例",
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/instance/list"))
	},
}

var instanceStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "查看实例状态",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Get("/instance/status?name=" + name))
	},
}

var instanceCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "创建实例",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		category, _ := cmd.Flags().GetString("category")
		engine, _ := cmd.Flags().GetString("engine")
		typ, _ := cmd.Flags().GetString("type")
		server, _ := cmd.Flags().GetString("server")
		port, _ := cmd.Flags().GetInt("port")
		memory, _ := cmd.Flags().GetString("memory")
		cpus, _ := cmd.Flags().GetInt("cpus")
		password, _ := cmd.Flags().GetString("password")
		replicaOf, _ := cmd.Flags().GetString("replica-of")
		overrides, _ := cmd.Flags().GetString("config")

		req := map[string]interface{}{
			"name":     name,
			"category": category,
			"engine":   engine,
			"type":     typ,
			"server":   server,
			"port":     port,
			"memory":   memory,
			"cpus":     cpus,
			"password": password,
		}
		if replicaOf != "" {
			req["replica_of"] = replicaOf
		}
		if overrides != "" {
			req["config_overrides"] = parseKV(overrides)
		}
		return checkResp(client.Post("/instance/create", req))
	},
}

var instanceDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "删除实例",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		cleanData, _ := cmd.Flags().GetBool("clean-data")
		return checkResp(client.Post("/instance/delete", map[string]interface{}{
			"name":       name,
			"clean_data": cleanData,
		}))
	},
}

var instanceStartCmd = &cobra.Command{
	Use:   "start",
	Short: "启动实例",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/instance/start", map[string]string{"name": name}))
	},
}

var instanceStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "停止实例",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/instance/stop", map[string]string{"name": name}))
	},
}

var instanceConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "更新实例配置",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		overrides, _ := cmd.Flags().GetString("set")
		restart, _ := cmd.Flags().GetBool("restart")
		return checkResp(client.Post("/instance/config", map[string]interface{}{
			"name":             name,
			"config_overrides": parseKV(overrides),
			"restart":          restart,
		}))
	},
}

var instancePromoteCmd = &cobra.Command{
	Use:   "promote",
	Short: "从库提升为主库",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/instance/promote", map[string]string{"name": name}))
	},
}

var instanceReplicateCmd = &cobra.Command{
	Use:   "replicate",
	Short: "设置复制目标",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		replicaOf, _ := cmd.Flags().GetString("replica-of")
		return checkResp(client.Post("/instance/replicate", map[string]string{
			"name":       name,
			"replica_of": replicaOf,
		}))
	},
}

func init() {
	instanceCmd.AddCommand(
		instanceListCmd, instanceStatusCmd, instanceCreateCmd,
		instanceDeleteCmd, instanceStartCmd, instanceStopCmd,
		instanceConfigCmd, instancePromoteCmd, instanceReplicateCmd,
	)

	instanceStatusCmd.Flags().String("name", "", "实例名称")
	instanceStatusCmd.MarkFlagRequired("name")

	instanceCreateCmd.Flags().String("name", "", "实例名称")
	instanceCreateCmd.Flags().String("category", "cache", "类型: cache | persistent")
	instanceCreateCmd.Flags().String("engine", "redis", "引擎: redis | kvrocks")
	instanceCreateCmd.Flags().String("type", "standalone", "拓扑: standalone | replication")
	instanceCreateCmd.Flags().String("server", "", "目标服务器")
	instanceCreateCmd.Flags().Int("port", 0, "端口 (0=自动分配)")
	instanceCreateCmd.Flags().String("memory", "1Gi", "内存")
	instanceCreateCmd.Flags().Int("cpus", 1, "CPU")
	instanceCreateCmd.Flags().String("password", "", "密码")
	instanceCreateCmd.Flags().String("replica-of", "", "主库实例名或地址")
	instanceCreateCmd.Flags().String("config", "", "配置覆盖 (k=v,k=v)")
	instanceCreateCmd.MarkFlagRequired("name")
	instanceCreateCmd.MarkFlagRequired("server")

	instanceDeleteCmd.Flags().String("name", "", "实例名称")
	instanceDeleteCmd.Flags().Bool("clean-data", false, "同时清理数据目录")
	instanceDeleteCmd.MarkFlagRequired("name")

	instanceStartCmd.Flags().String("name", "", "实例名称")
	instanceStartCmd.MarkFlagRequired("name")

	instanceStopCmd.Flags().String("name", "", "实例名称")
	instanceStopCmd.MarkFlagRequired("name")

	instanceConfigCmd.Flags().String("name", "", "实例名称")
	instanceConfigCmd.Flags().String("set", "", "配置项 (k=v,k=v)")
	instanceConfigCmd.Flags().Bool("restart", false, "是否重启生效")
	instanceConfigCmd.MarkFlagRequired("name")
	instanceConfigCmd.MarkFlagRequired("set")

	instancePromoteCmd.Flags().String("name", "", "实例名称")
	instancePromoteCmd.MarkFlagRequired("name")

	instanceReplicateCmd.Flags().String("name", "", "实例名称")
	instanceReplicateCmd.Flags().String("replica-of", "", "主库实例名或地址")
	instanceReplicateCmd.MarkFlagRequired("name")
	instanceReplicateCmd.MarkFlagRequired("replica-of")
}

func parseKV(s string) map[string]string {
	m := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok {
			m[k] = v
		}
	}
	return m
}
