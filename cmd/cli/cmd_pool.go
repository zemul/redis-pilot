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
	Use:   "query",
	Short: "Query server pool",
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/pool/query"))
	},
}

var poolAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Register a server",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
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
			"name": name,
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
	Use:   "remove",
	Short: "Remove a server",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/pool/remove", map[string]string{"name": name}))
	},
}

var poolUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Update server info",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
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
			"name":   name,
			"server": &srv,
		}))
	},
}

func init() {
	poolCmd.AddCommand(poolQueryCmd, poolAddCmd, poolRemoveCmd, poolUpdateCmd)

	poolAddCmd.Flags().String("name", "", "Server name")
	poolAddCmd.Flags().String("endpoint", "", "Server IP")
	poolAddCmd.Flags().Int("agent-port", 8400, "Agent port")
	poolAddCmd.Flags().String("agent-token", "", "Agent token")
	poolAddCmd.Flags().Int("cpu", 0, "CPU cores")
	poolAddCmd.Flags().String("memory", "", "Memory (e.g. 64Gi)")
	poolAddCmd.Flags().String("disk", "", "Disk (e.g. 500Gi)")
	poolAddCmd.Flags().String("zone", "", "Zone label")
	poolAddCmd.Flags().String("role", "production", "Role label")
	poolAddCmd.MarkFlagRequired("name")
	poolAddCmd.MarkFlagRequired("endpoint")
	poolAddCmd.MarkFlagRequired("cpu")
	poolAddCmd.MarkFlagRequired("memory")

	poolRemoveCmd.Flags().String("name", "", "Server name")
	poolRemoveCmd.MarkFlagRequired("name")

	poolUpdateCmd.Flags().String("name", "", "Server name")
	poolUpdateCmd.Flags().String("json", "", "Server JSON file path")
	poolUpdateCmd.MarkFlagRequired("name")
	poolUpdateCmd.MarkFlagRequired("json")
}
