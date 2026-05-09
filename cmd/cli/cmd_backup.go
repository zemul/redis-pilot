package main

import "github.com/spf13/cobra"

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup management",
}

var backupExecCmd = &cobra.Command{
	Use:     "exec <name>",
	Short:   "Execute a backup",
	Long:    "Trigger an immediate backup for an instance. Prefers the replica as backup source to avoid impacting the master.",
	Example: `  redis-pilot-cli backup exec order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Post("/backup/exec", map[string]string{"name": args[0]}))
	},
}

var backupRestoreCmd = &cobra.Command{
	Use:   "restore <name>",
	Short: "Restore from backup",
	Long: `Restore an instance from a specific backup. The instance will be stopped, data replaced, then restarted.
Use "backup list" to find available backup timestamps.`,
	Example: `  # List backups first
  redis-pilot-cli backup list order-master

  # Restore from a specific timestamp
  redis-pilot-cli backup restore order-master --backup-ts 2026-05-09T02:00:00`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		backupTs, _ := cmd.Flags().GetString("backup-ts")
		return checkResp(client.Post("/backup/restore", map[string]string{
			"name":      args[0],
			"backup_ts": backupTs,
		}))
	},
}

var backupListCmd = &cobra.Command{
	Use:     "list <name>",
	Short:   "List available backups",
	Long:    "List all available backup snapshots for an instance, sorted by time descending.",
	Example: `  redis-pilot-cli backup list order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/backup/list?name=" + args[0]))
	},
}

var backupGetScheduleCmd = &cobra.Command{
	Use:     "get-schedule <name>",
	Short:   "Get backup schedule for an instance",
	Long:    "Show the current automatic backup schedule and retention policy for an instance.",
	Example: `  redis-pilot-cli backup get-schedule order-master`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/backup/schedule?name=" + args[0]))
	},
}

var backupSetScheduleCmd = &cobra.Command{
	Use:   "set-schedule <name>",
	Short: "Set backup schedule for an instance",
	Long: `Configure the automatic backup schedule for an instance.

The --cron flag accepts a standard 5-field cron expression (minute hour day month weekday).
The --retention flag sets how many backup copies to keep; older ones are deleted automatically.
Omit --retention (or set to 0) to keep the current retention value unchanged.
Set --cron "" to disable automatic backups.`,
	Example: `  # Back up every day at 2am, keep 7 copies
  redis-pilot-cli backup set-schedule order-master --cron "0 2 * * *" --retention 7

  # Back up every 6 hours
  redis-pilot-cli backup set-schedule order-master --cron "0 */6 * * *"

  # Disable automatic backup
  redis-pilot-cli backup set-schedule order-master --cron ""`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cron, _ := cmd.Flags().GetString("cron")
		retention, _ := cmd.Flags().GetInt("retention")
		return checkResp(client.Post("/backup/schedule", map[string]interface{}{
			"name":      args[0],
			"schedule":  cron,
			"retention": retention,
		}))
	},
}

func init() {
	backupCmd.AddCommand(backupExecCmd, backupListCmd, backupSetScheduleCmd, backupGetScheduleCmd, backupRestoreCmd)

	backupRestoreCmd.Flags().String("backup-ts", "", "Backup timestamp")
	backupRestoreCmd.MarkFlagRequired("backup-ts")

	backupSetScheduleCmd.Flags().String("cron", "", "Cron expression (empty to disable)")
	backupSetScheduleCmd.Flags().Int("retention", 0, "Number of backups to retain (0 = keep current)")
}
