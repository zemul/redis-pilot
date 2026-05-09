package main

import "github.com/spf13/cobra"

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "备份管理",
}

var backupExecCmd = &cobra.Command{
	Use:   "exec",
	Short: "执行备份",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Post("/backup/exec", map[string]string{"name": name}))
	},
}

var backupRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "从备份恢复",
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
	Short: "列出可用备份",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		return checkResp(client.Get("/backup/list?name=" + name))
	},
}

func init() {
	backupCmd.AddCommand(backupExecCmd, backupRestoreCmd, backupListCmd)

	backupExecCmd.Flags().String("name", "", "实例名称")
	backupExecCmd.MarkFlagRequired("name")

	backupRestoreCmd.Flags().String("name", "", "实例名称")
	backupRestoreCmd.Flags().String("backup-ts", "", "备份时间戳")
	backupRestoreCmd.MarkFlagRequired("name")
	backupRestoreCmd.MarkFlagRequired("backup-ts")

	backupListCmd.Flags().String("name", "", "实例名称")
	backupListCmd.MarkFlagRequired("name")
}
