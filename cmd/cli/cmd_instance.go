package main

import (
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
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/instance/list"))
	},
}

var instanceStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show instance status",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Get("/instance/status?name=" + name))
	},
}

var instanceCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create an instance",
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
	Short: "Delete an instance",
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
	Short: "Start an instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/instance/start", map[string]string{"name": name}))
	},
}

var instanceStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop an instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/instance/stop", map[string]string{"name": name}))
	},
}

var instanceConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Update instance config",
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
	Short: "Promote replica to master",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/instance/promote", map[string]string{"name": name}))
	},
}

var instanceReplicateCmd = &cobra.Command{
	Use:   "replicate",
	Short: "Set replication target",
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

	instanceStatusCmd.Flags().String("name", "", "Instance name")
	instanceStatusCmd.MarkFlagRequired("name")

	instanceCreateCmd.Flags().String("name", "", "Instance name")
	instanceCreateCmd.Flags().String("category", "cache", "Category: cache | persistent")
	instanceCreateCmd.Flags().String("engine", "redis", "Engine: redis | kvrocks")
	instanceCreateCmd.Flags().String("type", "standalone", "Topology: standalone | replication")
	instanceCreateCmd.Flags().String("server", "", "Target server")
	instanceCreateCmd.Flags().Int("port", 0, "Port (0=auto)")
	instanceCreateCmd.Flags().String("memory", "1Gi", "Memory")
	instanceCreateCmd.Flags().Int("cpus", 1, "CPU cores")
	instanceCreateCmd.Flags().String("password", "", "Password")
	instanceCreateCmd.Flags().String("replica-of", "", "Master instance name or address")
	instanceCreateCmd.Flags().String("config", "", "Config overrides (k=v,k=v)")
	instanceCreateCmd.MarkFlagRequired("name")
	instanceCreateCmd.MarkFlagRequired("server")

	instanceDeleteCmd.Flags().String("name", "", "Instance name")
	instanceDeleteCmd.Flags().Bool("clean-data", false, "Also remove data directory")
	instanceDeleteCmd.MarkFlagRequired("name")

	instanceStartCmd.Flags().String("name", "", "Instance name")
	instanceStartCmd.MarkFlagRequired("name")

	instanceStopCmd.Flags().String("name", "", "Instance name")
	instanceStopCmd.MarkFlagRequired("name")

	instanceConfigCmd.Flags().String("name", "", "Instance name")
	instanceConfigCmd.Flags().String("set", "", "Config entries (k=v,k=v)")
	instanceConfigCmd.Flags().Bool("restart", false, "Restart to apply")
	instanceConfigCmd.MarkFlagRequired("name")
	instanceConfigCmd.MarkFlagRequired("set")

	instancePromoteCmd.Flags().String("name", "", "Instance name")
	instancePromoteCmd.MarkFlagRequired("name")

	instanceReplicateCmd.Flags().String("name", "", "Instance name")
	instanceReplicateCmd.Flags().String("replica-of", "", "Master instance name or address")
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
