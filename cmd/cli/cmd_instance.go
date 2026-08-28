package main

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
)

var instanceCmd = &cobra.Command{
	Use:   "instance",
	Short: "Instance management",
}

var instanceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all instances",
	Long:  "List all instances with their status, engine, role, and server.",
	Example: `  redis-pilot-cli instance list
  redis-pilot-cli instance list --group order
  redis-pilot-cli instance list --node redis01 --role replica --status running`,
	RunE: func(cmd *cobra.Command, args []string) error {
		path := "/instance/list"
		q := url.Values{}
		for _, flag := range []string{"group", "engine", "role", "status", "category", "engine-version"} {
			if value, _ := cmd.Flags().GetString(flag); value != "" {
				q.Set(strings.ReplaceAll(flag, "-", "_"), value)
			}
		}
		if node, _ := cmd.Flags().GetString("node"); node != "" {
			q.Set("server", node)
		}
		if encoded := q.Encode(); encoded != "" {
			path += "?" + encoded
		}
		return checkResp(client.Get(path))
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

var instanceMetricsCmd = &cobra.Command{
	Use:     "metrics <name>",
	Short:   "Show instance metrics",
	Long:    "Show real-time Redis INFO output collected from the instance.",
	Example: `  redis-pilot-cli instance metrics order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/instance/metrics?name=" + url.QueryEscape(args[0])))
	},
}

var instanceDiagnoseCmd = &cobra.Command{
	Use:     "diagnose <name>",
	Short:   "Diagnose instance (INFO + SLOWLOG + MEMORY DOCTOR)",
	Long:    "Collect diagnostic data from the instance including INFO, SLOWLOG, and MEMORY DOCTOR.",
	Example: `  redis-pilot-cli instance diagnose galaxy-1`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/instance/diagnose?name=" + url.QueryEscape(args[0])))
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
  - cache on a persistent store -> data silently evicted when memory is full
  - persistent on a cache -> writes fail under memory pressure + AOF overhead

Naming: do NOT encode role or node info in instance names (e.g. master, replica,
node1, redis01). Roles swap during failover, instances may migrate to other nodes.
Use ordinal names like order-1, order-2, cache-a, cache-b.`,
		Example: `  # Cache instance (auto-schedule node)
  redis-pilot-cli instance create session-cache --group session --memory 4Gi

  # Persistent instance on specific node
  redis-pilot-cli instance create order-1 --node redis01 --group order --category persistent --memory 8Gi

  # Kvrocks with custom RocksDB tuning
  redis-pilot-cli instance create order-1 --group order --engine kvrocks --category persistent --memory 8Gi --config "rocksdb.compression=lz4,rocksdb.write_buffer_size=256"

  # Redis with custom config overrides
  redis-pilot-cli instance create order-1 --group order --category persistent --memory 4Gi --engine-version 6.2 --config "hz=20,tcp-keepalive=60"`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		category, _ := cmd.Flags().GetString("category")
		engine, _ := cmd.Flags().GetString("engine")
		engineVersion, _ := cmd.Flags().GetString("engine-version")
		typ, _ := cmd.Flags().GetString("type")
		node, _ := cmd.Flags().GetString("node")
		memory, _ := cmd.Flags().GetString("memory")
		disk, _ := cmd.Flags().GetString("disk")
		if disk == "" {
			if engine == "kvrocks" {
				disk = "20Gi"
			} else {
				disk = "10Gi"
			}
		}
		cpus, _ := cmd.Flags().GetFloat64("cpus")
		password, _ := cmd.Flags().GetString("password")
		group, _ := cmd.Flags().GetString("group")
		replicaOf, _ := cmd.Flags().GetString("replica-of")
		overrides, _ := cmd.Flags().GetString("config")
		accessMode, _ := cmd.Flags().GetString("access-mode")
		if replicaOf == "" && strings.TrimSpace(group) == "" {
			group = args[0]
		}

		req := map[string]interface{}{
			"name":           args[0],
			"category":       category,
			"group":          group,
			"engine":         engine,
			"engine_version": engineVersion,
			"type":           typ,
			"server":         node,
			"memory":         memory,
			"disk":           disk,
			"cpus":           cpus,
			"password":       password,
		}
		if replicaOf != "" {
			req["replica_of"] = replicaOf
		}
		if overrides != "" {
			req["config_overrides"] = parseKV(overrides)
		}
		if accessMode != "" {
			req["access_mode"] = accessMode
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
	Use:   "stop <name>",
	Short: "Stop an instance",
	Long: `Gracefully stop a running instance. Data is preserved.

Stopping the current master of a replication group will remove Sentinel monitoring.
Use "instance failover <replica>" first for a safe switchover, or pass --force to override.`,
	Example: `  redis-pilot-cli instance stop order-master
  redis-pilot-cli instance stop order-master --force`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		force, _ := cmd.Flags().GetBool("force")
		return checkResp(client.Post("/instance/stop", map[string]interface{}{
			"name":  args[0],
			"force": force,
		}))
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

var instanceResizeCmd = &cobra.Command{
	Use:   "resize <name>",
	Short: "Resize instance memory / CPU / disk",
	Long: `Change an instance's resource spec in place. Specify any subset of
--memory, --cpus, --disk; unspecified dimensions are left unchanged.

  memory  Adjusts container cgroup (podman update --memory) AND the engine's
          maxmemory (Redis) / max-db-size (Kvrocks). Applied online without
          restart. The maxmemory change is persisted to the config file; the
          cgroup change is re-applied on the next managed container rebuild.
  cpus    Adjusts container cgroup (podman update --cpus). Online, no restart.
  disk    Capacity-planning metadata only (used for scheduling accounting);
          not enforced on the container. Updates the control-plane record only.

The Server rejects an upsize if the node lacks free capacity (excluding this
instance's own current allocation), and rejects a memory downsize that would
drop below the instance's live used_memory (+10% headroom) to avoid OOM.

Note: podman cgroup changes revert to the create-time value if the container
is restarted by an unmanaged event (crash-restart policy, host reboot) before
the next managed rebuild.`,
	Example: `  # Grow memory online (cgroup + maxmemory)
  redis-pilot-cli instance resize order-1 --memory 8Gi

  # Bump CPU only
  redis-pilot-cli instance resize order-1 --cpus 4

  # Adjust disk planning quota only (no container change)
  redis-pilot-cli instance resize order-1 --disk 100Gi

  # Change memory and CPU together
  redis-pilot-cli instance resize order-1 --memory 8Gi --cpus 4`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		req := map[string]interface{}{"name": args[0]}
		// 用 Changed 区分"未指定"与"显式传了默认值",只发送用户实际改动的维度。
		if cmd.Flags().Changed("memory") {
			v, _ := cmd.Flags().GetString("memory")
			req["memory"] = v
		}
		if cmd.Flags().Changed("cpus") {
			v, _ := cmd.Flags().GetFloat64("cpus")
			req["cpus"] = v
		}
		if cmd.Flags().Changed("disk") {
			v, _ := cmd.Flags().GetString("disk")
			req["disk"] = v
		}
		if len(req) == 1 {
			return fmt.Errorf("specify at least one of --memory, --cpus, --disk")
		}
		return checkResp(client.Post("/instance/resize", req))
	},
}

var instanceSetAccessModeCmd = &cobra.Command{
	Use:   "set-access-mode <name> <mode>",
	Short: "Set group access mode (rw-split | standard)",
	Long: `Change the access mode for the group that <name> belongs to.
This affects which port the pseudo-sentinel returns to clients:
  - standard: returns master port (all traffic goes to master)
  - rw-split: returns auto port (read/write splitting enabled)

Takes effect on next proxy snapshot poll (within seconds).`,
	Example: `  # Enable read/write splitting
  redis-pilot-cli instance set-access-mode order-master rw-split

  # Switch back to standard (all traffic to master)
  redis-pilot-cli instance set-access-mode order-master standard`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		mode := args[1]
		if mode != "rw-split" && mode != "standard" {
			return fmt.Errorf("access mode must be 'rw-split' or 'standard', got: %s", mode)
		}
		return checkResp(client.Post("/instance/access-mode", map[string]interface{}{
			"name":        args[0],
			"access_mode": mode,
		}))
	},
}

var instanceFailoverCmd = &cobra.Command{
	Use:   "failover <name>",
	Short: "Trigger failover for the replication group",
	Long: `Trigger a failover for the replication group that <name> belongs to.

<name> is only used to locate the group; it does NOT guarantee that this
specific instance becomes the new master. If the group is managed by Sentinel
(sentinel.enabled=true), the actual promotion decision is made by Sentinel's
own election (based on replication progress, replica-priority, etc.) via
SENTINEL FAILOVER. The response reports which instance actually became the
new master — check "new_master" vs "requested" to see if they match.

If the group is not managed by Sentinel, this falls back to directly
promoting <name> (which must currently be a replica) and demoting the old
master, guaranteeing <name> becomes the new master.`,
	Example: `  redis-pilot-cli instance failover order-replica`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/instance/failover", map[string]string{"name": args[0]}))
	},
}

func init() {
	instanceCmd.AddCommand(
		instanceListCmd,
		instanceStatusCmd,
		instanceMetricsCmd,
		instanceDiagnoseCmd,
		instanceCreateCmd,
		instanceConfigCmd,
		instanceResizeCmd,
		instanceSetAccessModeCmd,
		instanceStartCmd,
		instanceStopCmd,
		instanceFailoverCmd,
		instanceDeleteCmd,
	)

	instanceListCmd.Flags().String("group", "", "Filter by instance group")
	instanceListCmd.Flags().String("node", "", "Filter by node/server name")
	instanceListCmd.Flags().String("engine", "", "Filter by engine: redis | kvrocks")
	instanceListCmd.Flags().String("engine-version", "", "Filter by engine version")
	instanceListCmd.Flags().String("role", "", "Filter by role: master | replica")
	instanceListCmd.Flags().String("status", "", "Filter by instance status")
	instanceListCmd.Flags().String("category", "", "Filter by category: cache | persistent")

	instanceCreateCmd.Flags().String("category", "cache", "Category: cache | persistent")
	instanceCreateCmd.Flags().String("engine", "redis", "Engine: redis | kvrocks")
	instanceCreateCmd.Flags().String("engine-version", "", "Engine version (redis: 5, 6.2, 7; kvrocks: 2.15.0)")
	instanceCreateCmd.Flags().String("type", "standalone", "Topology: standalone | replication")
	instanceCreateCmd.Flags().String("node", "", "Target node name")
	instanceCreateCmd.Flags().String("memory", "1Gi", "Memory")
	instanceCreateCmd.Flags().String("disk", "", "Disk quota for capacity planning (e.g. 20Gi)")
	instanceCreateCmd.Flags().Float64("cpus", 1, "CPU cores")
	instanceCreateCmd.Flags().String("password", "", "Password")
	instanceCreateCmd.Flags().String("group", "", "Stable logical group name for master/standalone")
	instanceCreateCmd.Flags().String("replica-of", "", "Master instance name or address")
	instanceCreateCmd.Flags().String("config", "", "Config overrides (k=v,k=v)")
	instanceCreateCmd.Flags().String("access-mode", "", "Access mode: rw-split | standard (default: rw-split for replication, standard for standalone)")

	instanceDeleteCmd.Flags().Bool("clean-data", false, "Also remove data directory")

	instanceStopCmd.Flags().Bool("force", false, "Force stop even if instance is the current master")

	instanceConfigCmd.Flags().String("set", "", "Config entries (k=v,k=v)")
	instanceConfigCmd.Flags().Bool("restart", false, "Restart to apply")
	instanceConfigCmd.MarkFlagRequired("set")

	instanceResizeCmd.Flags().String("memory", "", "New memory (e.g. 8Gi); adjusts cgroup + maxmemory online")
	instanceResizeCmd.Flags().Float64("cpus", 0, "New CPU cores; adjusts cgroup online")
	instanceResizeCmd.Flags().String("disk", "", "New disk planning quota (e.g. 100Gi); metadata only")
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
