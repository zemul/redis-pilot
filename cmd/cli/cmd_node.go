package main

import (
	"encoding/json"
	"os"

	"github.com/spf13/cobra"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Node management",
}

var nodeQueryCmd = &cobra.Command{
	Use:     "list",
	Short:   "List nodes",
	Long:    "List all registered nodes with their capacity, allocated resources, and status.",
	Example: `  redis-pilot-cli node list`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/node/list"))
	},
}

var nodeAddCmd = &cobra.Command{
	Use:   "add <name>",
	Short: "Register a node",
	Long:  "Add a new node. The node must have the Agent running and reachable.",
	Example: `  redis-pilot-cli node add redis01 --endpoint 10.0.0.1 --cpu 16 --memory 64Gi --disk 500Gi

  # With agent token and zone label
  redis-pilot-cli node add redis01 --endpoint 10.0.0.1 --cpu 16 --memory 64Gi --disk 500Gi \
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

		return checkResp(client.Post("/node/add", map[string]interface{}{
			"name": args[0],
			"server": &apitypes.NodeServer{
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

var nodeRemoveCmd = &cobra.Command{
	Use:     "remove <name>",
	Short:   "Remove a node",
	Long:    "Remove a node. The node must have no running instances.",
	Example: `  redis-pilot-cli node remove redis01`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/node/remove", map[string]string{"name": args[0]}))
	},
}

var nodeUpdateCmd = &cobra.Command{
	Use:     "update <name>",
	Short:   "Update node info",
	Long:    "Update node metadata (capacity, labels, agent token) from a JSON file.",
	Example: `  redis-pilot-cli node update redis01 --json ./redis01.json`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		jsonFile, _ := cmd.Flags().GetString("json")

		data, err := os.ReadFile(jsonFile)
		if err != nil {
			return err
		}
		var srv apitypes.NodeServer
		if err := json.Unmarshal(data, &srv); err != nil {
			return err
		}
		return checkResp(client.Post("/node/update", map[string]interface{}{
			"name":   args[0],
			"server": &srv,
		}))
	},
}

func init() {
	nodeCmd.AddCommand(nodeQueryCmd, nodeAddCmd, nodeRemoveCmd, nodeUpdateCmd)

	nodeAddCmd.Flags().String("endpoint", "", "Node IP")
	nodeAddCmd.Flags().Int("agent-port", 8400, "Agent port")
	nodeAddCmd.Flags().String("agent-token", "", "Agent token")
	nodeAddCmd.Flags().Int("cpu", 0, "CPU cores")
	nodeAddCmd.Flags().String("memory", "", "Memory (e.g. 64Gi)")
	nodeAddCmd.Flags().String("disk", "", "Disk (e.g. 500Gi)")
	nodeAddCmd.Flags().String("zone", "", "Zone label")
	nodeAddCmd.Flags().String("role", "production", "Role label")
	nodeAddCmd.MarkFlagRequired("endpoint")
	nodeAddCmd.MarkFlagRequired("cpu")
	nodeAddCmd.MarkFlagRequired("memory")

	nodeUpdateCmd.Flags().String("json", "", "Node JSON file path")
	nodeUpdateCmd.MarkFlagRequired("json")
}
