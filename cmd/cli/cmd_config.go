package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage CLI configuration (server address and auth token)",
	Long: `Read and write the CLI config file at ~/.redis-pilot-cli/config.yaml.

The file stores the Server address and API token used by all commands.
It is written with 0600 permissions since it contains the token.`,
}

var configSetCmd = &cobra.Command{
	Use:   "set",
	Short: "Set server address and/or auth token",
	Example: `  # Set both server and token
  redis-pilot-cli config set --server 10.0.1.10:8080 --token rpt_xxxxx

  # Set only the server (token unchanged)
  redis-pilot-cli config set --server 10.0.1.10:8080

  # Set only the token
  redis-pilot-cli config set --token rpt_xxxxx`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !cmd.Flags().Changed("server") && !cmd.Flags().Changed("token") {
			return fmt.Errorf("nothing to set: provide --server and/or --token")
		}
		// 读现有配置再合并，只覆盖显式传入的字段，避免清空另一个。
		cfg := cli.LoadConfig()
		if cmd.Flags().Changed("server") {
			s, _ := cmd.Flags().GetString("server")
			cfg.Server = s
		}
		if cmd.Flags().Changed("token") {
			t, _ := cmd.Flags().GetString("token")
			cfg.Token = t
		}
		if err := cli.SaveConfig(cfg); err != nil {
			return fmt.Errorf("save config: %w", err)
		}
		path, _ := cli.ConfigPath()
		fmt.Printf("config saved to %s\n", path)
		fmt.Printf("  server: %s\n", cfg.Server)
		fmt.Printf("  token:  %s\n", maskToken(cfg.Token))
		return nil
	},
}

var configViewCmd = &cobra.Command{
	Use:   "view",
	Short: "Show current CLI configuration (token masked)",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := cli.LoadConfig()
		path, _ := cli.ConfigPath()
		fmt.Printf("config file: %s\n", path)
		fmt.Printf("server: %s\n", cfg.Server)
		fmt.Printf("token:  %s\n", maskToken(cfg.Token))
		return nil
	},
}

// maskToken 对 token 脱敏展示：保留前 4 位和后 4 位，中间打码。
// 避免在终端回显、history 或截屏中泄露完整密钥。
func maskToken(t string) string {
	if t == "" {
		return "(not set)"
	}
	if len(t) <= 8 {
		return "****"
	}
	return t[:4] + "****" + t[len(t)-4:]
}

func init() {
	configSetCmd.Flags().String("server", "", "Server address, e.g. 10.0.1.10:8080")
	configSetCmd.Flags().String("token", "", "API token (rpt_...)")
	configCmd.AddCommand(configSetCmd, configViewCmd)
}
