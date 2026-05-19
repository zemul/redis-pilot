package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var auditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Query audit logs",
	Long: `Query audit logs with filtering by date, group, level, and action.
Defaults to showing the most recent 30 records across all dates.`,
	Example: `  # Today's audit logs
  redis-pilot-cli audit

  # Filter by level
  redis-pilot-cli audit --level critical

  # Filter by instance group
  redis-pilot-cli audit --group order

  # Date range
  redis-pilot-cli audit --from 20260501 --to 20260510

  # Filter by action
  redis-pilot-cli audit --action instance.create`,
	RunE: func(cmd *cobra.Command, args []string) error {
		from, _ := cmd.Flags().GetString("from")
		to, _ := cmd.Flags().GetString("to")
		group, _ := cmd.Flags().GetString("group")
		instance, _ := cmd.Flags().GetString("instance")
		level, _ := cmd.Flags().GetString("level")
		action, _ := cmd.Flags().GetString("action")

		query := fmt.Sprintf("/audit/query?from=%s&to=%s", from, to)
		if group != "" {
			query += "&group=" + group
		}
		if instance != "" {
			query += "&instance=" + instance
		}
		if level != "" {
			query += "&level=" + level
		}
		if action != "" {
			query += "&action=" + action
		}
		return checkResp(client.Get(query))
	},
}

func init() {
	auditCmd.Flags().String("from", "", "Start date (YYYYMMDD)")
	auditCmd.Flags().String("to", "", "End date (YYYYMMDD)")
	auditCmd.Flags().String("group", "", "Filter by instance group")
	auditCmd.Flags().String("instance", "", "Filter by instance name")
	auditCmd.Flags().String("level", "", "Filter by level: normal | important | critical")
	auditCmd.Flags().String("action", "", "Filter by action type")
}
