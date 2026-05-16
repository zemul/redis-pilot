package main

import "github.com/spf13/cobra"

var metricsCmd = &cobra.Command{
	Use:   "metrics <name>",
	Short: "Collect instance metrics (INFO + cached metrics)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/metrics?name=" + args[0]))
	},
}
