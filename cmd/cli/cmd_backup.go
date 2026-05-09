package main

import "github.com/spf13/cobra"

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup management",
}

var backupExecCmd = &cobra.Command{
	Use:   "exec",
	Short: "Execute a backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/backup/exec", map[string]string{"name": name}))
	},
}

var backupRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore from backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		backupTs, _ := cmd.Flags().GetString("backup-ts")
		return checkResp(client.Post("/backup/restore", map[string]string{
			"name":      name,
			"backup_ts": backupTs,
		}))
	},
}

var backupListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available backups",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Get("/backup/list?name=" + name))
	},
}

var backupGetScheduleCmd = &cobra.Command{
	Use:   "get-schedule",
	Short: "Get backup schedule for an instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Get("/backup/schedule?name=" + name))
	},
}

var backupSetScheduleCmd = &cobra.Command{
	Use:   "set-schedule",
	Short: "Set backup schedule for an instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		cron, _ := cmd.Flags().GetString("cron")
		retention, _ := cmd.Flags().GetInt("retention")
		return checkResp(client.Post("/backup/schedule", map[string]interface{}{
			"name":      name,
			"schedule":  cron,
			"retention": retention,
		}))
	},
}

func init() {
	backupCmd.AddCommand(backupExecCmd, backupRestoreCmd, backupListCmd, backupGetScheduleCmd, backupSetScheduleCmd)

	backupExecCmd.Flags().String("name", "", "Instance name")
	backupExecCmd.MarkFlagRequired("name")

	backupRestoreCmd.Flags().String("name", "", "Instance name")
	backupRestoreCmd.Flags().String("backup-ts", "", "Backup timestamp")
	backupRestoreCmd.MarkFlagRequired("name")
	backupRestoreCmd.MarkFlagRequired("backup-ts")

	backupListCmd.Flags().String("name", "", "Instance name")
	backupListCmd.MarkFlagRequired("name")

	backupGetScheduleCmd.Flags().String("name", "", "Instance name")
	backupGetScheduleCmd.MarkFlagRequired("name")

	backupSetScheduleCmd.Flags().String("name", "", "Instance name")
	backupSetScheduleCmd.Flags().String("cron", "", "Cron expression (empty to disable)")
	backupSetScheduleCmd.Flags().Int("retention", 0, "Number of backups to retain (0 = keep current)")
	backupSetScheduleCmd.MarkFlagRequired("name")
}

