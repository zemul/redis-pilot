package main

import "github.com/spf13/cobra"

var sentinelCmd = &cobra.Command{
	Use:   "sentinel",
	Short: "Redis Sentinel monitoring config",
}

var sentinelStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show configured Sentinel node status",
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/sentinel/status"))
	},
}

var sentinelSyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync monitored masters to pre-deployed Sentinel nodes",
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/sentinel/sync", nil))
	},
}

func init() {
	sentinelCmd.AddCommand(sentinelStatusCmd)
	sentinelCmd.AddCommand(sentinelSyncCmd)
}
