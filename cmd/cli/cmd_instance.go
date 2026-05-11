package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var instanceCmd = &cobra.Command{
	Use:   "instance",
	Short: "Instance management",
}

var instanceListCmd = &cobra.Command{
	Use:     "list",
	Short:   "List all instances",
	Long:    "List all instances with their status, engine, role, and server.",
	Example: `  redis-pilot-cli instance list`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/instance/list"))
	},
}

var instanceStatusCmd = &cobra.Command{
	Use:     "status <name>",
	Short:   "Show instance status",
	Long:    "Show detailed status of an instance, including replication lag, memory usage, and health.",
	Example: `  redis-pilot-cli instance status order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/instance/status?name=" + args[0]))
	},
}

var instanceCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create an instance",
	Long: `Create a new Redis or Kvrocks instance on the specified server.

--category controls the eviction and persistence defaults:
  cache       maxmemory-policy=allkeys-lru, AOF disabled
              Memory-full behavior: evict old keys automatically.
              Use for session cache, rate limiting, etc.
  persistent  maxmemory-policy=noeviction, AOF enabled
              Memory-full behavior: reject writes with an error (no data loss).
              Use for queues, primary data stores, etc.

Choosing the wrong category has real consequences:
  - cache on a persistent store → data silently evicted when memory is full
  - persistent on a cache → writes fail under memory pressure + AOF overhead`,
	Example: `  # Cache instance (auto-schedule node)
  redis-pilot-cli instance create session-cache --group session --memory 4Gi

  # Persistent instance on specific node
  redis-pilot-cli instance create order-master --node redis01 --group order --category persistent --memory 8Gi

  # Kvrocks persistent instance
  redis-pilot-cli instance create order-master --group order --engine kvrocks --category persistent --memory 8Gi

  # With custom config overrides
  redis-pilot-cli instance create order-master --group order --category persistent --memory 4Gi --config "hz=20,tcp-keepalive=60"`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		category, _ := cmd.Flags().GetString("category")
		engine, _ := cmd.Flags().GetString("engine")
		typ, _ := cmd.Flags().GetString("type")
		node, _ := cmd.Flags().GetString("node")
		memory, _ := cmd.Flags().GetString("memory")
		cpus, _ := cmd.Flags().GetInt("cpus")
		password, _ := cmd.Flags().GetString("password")
		group, _ := cmd.Flags().GetString("group")
		replicaOf, _ := cmd.Flags().GetString("replica-of")
		overrides, _ := cmd.Flags().GetString("config")
		if replicaOf == "" && strings.TrimSpace(group) == "" {
			return fmt.Errorf("--group is required when creating a master or standalone instance")
		}

		req := map[string]interface{}{
			"name":     args[0],
			"category": category,
			"group":    group,
			"engine":   engine,
			"type":     typ,
			"server":   node,
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
	Use:   "delete <name>",
	Short: "Delete an instance",
	Long: `Stop and remove an instance. By default, data directories are preserved.
Use --clean-data to also delete all data on disk (irreversible).`,
	Example: `  # Delete instance, keep data
  redis-pilot-cli instance delete order-master

  # Delete instance and wipe data
  redis-pilot-cli instance delete order-master --clean-data`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cleanData, _ := cmd.Flags().GetBool("clean-data")
		return checkResp(client.Post("/instance/delete", map[string]interface{}{
			"name":       args[0],
			"clean_data": cleanData,
		}))
	},
}

var instanceStartCmd = &cobra.Command{
	Use:     "start <name>",
	Short:   "Start an instance",
	Long:    "Start a stopped instance.",
	Example: `  redis-pilot-cli instance start order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/instance/start", map[string]string{"name": args[0]}))
	},
}

var instanceStopCmd = &cobra.Command{
	Use:     "stop <name>",
	Short:   "Stop an instance",
	Long:    "Gracefully stop a running instance. Data is preserved.",
	Example: `  redis-pilot-cli instance stop order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/instance/stop", map[string]string{"name": args[0]}))
	},
}

var instanceConfigCmd = &cobra.Command{
	Use:   "config <name>",
	Short: "Update instance config",
	Long: `Update Redis/Kvrocks runtime configuration. Changes are applied via CONFIG SET where possible.
Use --restart to restart the instance when the parameter requires it (e.g. bind, port).`,
	Example: `  # Hot-reload config (no restart)
  redis-pilot-cli instance config order-master --set "maxmemory=2Gi,hz=20"

  # Apply config that requires restart
  redis-pilot-cli instance config order-master --set "appendonly=yes" --restart`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		overrides, _ := cmd.Flags().GetString("set")
		restart, _ := cmd.Flags().GetBool("restart")
		return checkResp(client.Post("/instance/config", map[string]interface{}{
			"name":             args[0],
			"config_overrides": parseKV(overrides),
			"restart":          restart,
		}))
	},
}

var instancePromoteCmd = &cobra.Command{
	Use:   "promote <name>",
	Short: "Promote replica to master",
	Long: `Promote a replica instance to master. The replica will stop replicating and become writable.
Typically used during failover or planned switchover.`,
	Example: `  redis-pilot-cli instance promote order-replica`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/instance/promote", map[string]string{"name": args[0]}))
	},
}

var instanceReplicateCmd = &cobra.Command{
	Use:     "replicate <name>",
	Short:   "Set replication target",
	Long:    "Make an instance replicate from a master. The instance will sync data from the specified master.",
	Example: `  redis-pilot-cli instance replicate order-replica --replica-of order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		replicaOf, _ := cmd.Flags().GetString("replica-of")
		return checkResp(client.Post("/instance/replicate", map[string]string{
			"name":       args[0],
			"replica_of": replicaOf,
		}))
	},
}

func init() {
	instanceCmd.AddCommand(
		instanceListCmd, instanceStatusCmd, instanceCreateCmd,
		instanceStartCmd, instanceStopCmd, instanceConfigCmd,
		instanceReplicateCmd, instancePromoteCmd, instanceDeleteCmd,
	)

	instanceCreateCmd.Flags().String("category", "cache", "Category: cache | persistent")
	instanceCreateCmd.Flags().String("engine", "redis", "Engine: redis | kvrocks")
	instanceCreateCmd.Flags().String("type", "standalone", "Topology: standalone | replication")
	instanceCreateCmd.Flags().String("node", "", "Target node name (from pool)")
	instanceCreateCmd.Flags().String("memory", "1Gi", "Memory")
	instanceCreateCmd.Flags().Int("cpus", 1, "CPU cores")
	instanceCreateCmd.Flags().String("password", "", "Password")
	instanceCreateCmd.Flags().String("group", "", "Stable logical group name for master/standalone")
	instanceCreateCmd.Flags().String("replica-of", "", "Master instance name or address")
	instanceCreateCmd.Flags().String("config", "", "Config overrides (k=v,k=v)")


	instanceDeleteCmd.Flags().Bool("clean-data", false, "Also remove data directory")

	instanceConfigCmd.Flags().String("set", "", "Config entries (k=v,k=v)")
	instanceConfigCmd.Flags().Bool("restart", false, "Restart to apply")
	instanceConfigCmd.MarkFlagRequired("set")

	instanceReplicateCmd.Flags().String("replica-of", "", "Master instance name or address")
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
