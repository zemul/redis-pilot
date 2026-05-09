package main

import "github.com/spf13/cobra"

var envoyCmd = &cobra.Command{
	Use:   "envoy",
	Short: "Envoy proxy management",
}

var envoyRouteUpdateCmd = &cobra.Command{
	Use:   "route-update",
	Short: "Regenerate Envoy config from current state and reload",
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/envoy/route/update", nil))
	},
}

var envoyConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Show current generated Envoy config",
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/envoy/config"))
	},
}

func init() {
	envoyCmd.AddCommand(envoyRouteUpdateCmd)
	envoyCmd.AddCommand(envoyConfigCmd)
}
