package main

import (
	"encoding/json"
	"os"

	"github.com/spf13/cobra"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var poolCmd = &cobra.Command{
	Use:   "pool",
	Short: "Server pool management",
}

var poolQueryCmd = &cobra.Command{
	Use:     "query",
	Short:   "Query server pool",
	Long:    "List all servers in the pool with their capacity, allocated resources, and status.",
	Example: `  redis-pilot-cli pool query`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/pool/query"))
	},
}

var poolAddCmd = &cobra.Command{
	Use:   "add <name>",
	Short: "Register a server",
	Long:  "Add a new server to the pool. The server must have the Agent running and reachable.",
	Example: `  redis-pilot-cli pool add redis01 --endpoint 10.0.0.1 --cpu 16 --memory 64Gi --disk 500Gi

  # With agent token and zone label
  redis-pilot-cli pool add redis01 --endpoint 10.0.0.1 --cpu 16 --memory 64Gi --disk 500Gi \
    --agent-token secret123 --zone cn-north-1`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoint, _ := cmd.Flags().GetString("endpoint")
		agentPort, _ := cmd.Flags().GetInt("agent-port")
		agentToken, _ := cmd.Flags().GetString("agent-token")
		cpuCores, _ := cmd.Flags().GetInt("cpu")
		memory, _ := cmd.Flags().GetString("memory")
		disk, _ := cmd.Flags().GetString("disk")
		zone, _ := cmd.Flags().GetString("zone")
		role, _ := cmd.Flags().GetString("role")

		labels := map[string]string{}
		if zone != "" {
			labels["zone"] = zone
		}
		if role != "" {
			labels["role"] = role
		}

		return checkResp(client.Post("/pool/add", map[string]interface{}{
			"name": args[0],
			"server": &apitypes.PoolServer{
				Endpoint:   endpoint,
				AgentPort:  agentPort,
				AgentToken: agentToken,
				Labels:     labels,
				Capacity: apitypes.ResourceSpec{
					CPUCores: cpuCores,
					Memory:   memory,
					Disk:     disk,
				},
				Allocated: apitypes.ResourceSpec{Memory: "0Gi", Disk: "0Gi"},
				Status:    "healthy",
			},
		}))
	},
}

var poolRemoveCmd = &cobra.Command{
	Use:     "remove <name>",
	Short:   "Remove a server",
	Long:    "Remove a server from the pool. The server must have no running instances.",
	Example: `  redis-pilot-cli pool remove redis01`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/pool/remove", map[string]string{"name": args[0]}))
	},
}

var poolUpdateCmd = &cobra.Command{
	Use:     "update <name>",
	Short:   "Update server info",
	Long:    "Update server metadata (capacity, labels, agent token) from a JSON file.",
	Example: `  redis-pilot-cli pool update redis01 --json ./redis01.json`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		jsonFile, _ := cmd.Flags().GetString("json")

		data, err := os.ReadFile(jsonFile)
		if err != nil {
			return err
		}
		var srv apitypes.PoolServer
		if err := json.Unmarshal(data, &srv); err != nil {
			return err
		}
		return checkResp(client.Post("/pool/update", map[string]interface{}{
			"name":   args[0],
			"server": &srv,
		}))
	},
}

func init() {
	poolCmd.AddCommand(poolQueryCmd, poolAddCmd, poolRemoveCmd, poolUpdateCmd)

	poolAddCmd.Flags().String("endpoint", "", "Server IP")
	poolAddCmd.Flags().Int("agent-port", 8400, "Agent port")
	poolAddCmd.Flags().String("agent-token", "", "Agent token")
	poolAddCmd.Flags().Int("cpu", 0, "CPU cores")
	poolAddCmd.Flags().String("memory", "", "Memory (e.g. 64Gi)")
	poolAddCmd.Flags().String("disk", "", "Disk (e.g. 500Gi)")
	poolAddCmd.Flags().String("zone", "", "Zone label")
	poolAddCmd.Flags().String("role", "production", "Role label")
	poolAddCmd.MarkFlagRequired("endpoint")
	poolAddCmd.MarkFlagRequired("cpu")
	poolAddCmd.MarkFlagRequired("memory")

	poolUpdateCmd.Flags().String("json", "", "Server JSON file path")
	poolUpdateCmd.MarkFlagRequired("json")
}
