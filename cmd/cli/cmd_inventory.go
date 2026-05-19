package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var inventoryCmd = &cobra.Command{
	Use:   "inventory",
	Short: "Query resource inventory",
	RunE: func(cmd *cobra.Command, args []string) error {
		view, _ := cmd.Flags().GetString("view")
		port, _ := cmd.Flags().GetString("port")
		node, _ := cmd.Flags().GetString("node")
		engine, _ := cmd.Flags().GetString("engine")

		query := fmt.Sprintf("/inventory?view=%s", view)
		if port != "" {
			query += "&port=" + port
		}
		if node != "" {
			query += "&server=" + node
		}
		if engine != "" {
			query += "&engine=" + engine
		}
		return checkResp(client.Get(query))
	},
}

func init() {
	inventoryCmd.Flags().String("port", "", "Filter by Envoy port")
	inventoryCmd.Flags().String("node", "", "Filter by server name")
	inventoryCmd.Flags().String("engine", "", "Filter by engine (redis|kvrocks)")
	inventoryCmd.Flags().String("view", "summary", "View: summary | port | server")
}
