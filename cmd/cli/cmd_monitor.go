package main

import "github.com/spf13/cobra"

var healthCheckCmd = &cobra.Command{
	Use:   "health-check <name>",
	Short: "Check instance health via Agent",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/health-check?name=" + args[0]))
	},
}

var metricsCmd = &cobra.Command{
	Use:   "metrics <name>",
	Short: "Collect instance metrics (INFO + cached metrics)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/metrics?name=" + args[0]))
	},
}

func init() {
	rootCmd.AddCommand(healthCheckCmd)
	rootCmd.AddCommand(metricsCmd)
}
