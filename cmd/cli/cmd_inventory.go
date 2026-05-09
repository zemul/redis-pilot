package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var inventoryCmd = &cobra.Command{
	Use:   "inventory",
	Short: "资源清单查询",
	RunE: func(cmd *cobra.Command, args []string) error {
		view, _ := cmd.Flags().GetString("view")
		port, _ := cmd.Flags().GetString("port")
		server, _ := cmd.Flags().GetString("server")
		engine, _ := cmd.Flags().GetString("engine")

		query := fmt.Sprintf("/inventory?view=%s", view)
		if port != "" {
			query += "&port=" + port
		}
		if server != "" {
			query += "&server=" + server
		}
		if engine != "" {
			query += "&engine=" + engine
		}
		return checkResp(client.Get(query))
	},
}

func init() {
	inventoryCmd.Flags().String("port", "", "按 Envoy 端口查询")
	inventoryCmd.Flags().String("server", "", "按服务器名查询")
	inventoryCmd.Flags().String("engine", "", "按引擎过滤 (redis|kvrocks)")
	inventoryCmd.Flags().String("view", "summary", "视图: summary | port | server")
}
