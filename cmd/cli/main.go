package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var version = "dev"

var client *cli.Client

var rootCmd = &cobra.Command{
	Use:   "redis-pilot-cli",
	Short: "Redis 多实例管理 CLI",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		cfg := cli.LoadConfig()
		if s, _ := cmd.Flags().GetString("server"); s != "" {
			cfg.Server = s
		}
		if t, _ := cmd.Flags().GetString("token"); t != "" {
			cfg.Token = t
		}
		client = cli.NewClient(cfg)
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "显示版本号",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("redis-pilot-cli", version)
	},
}

func init() {
	rootCmd.PersistentFlags().String("server", "", "Server 地址 (default: config file or 127.0.0.1:8080)")
	rootCmd.PersistentFlags().String("token", "", "鉴权 Token")

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(poolCmd)
	rootCmd.AddCommand(instanceCmd)
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(inventoryCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func printJSON(v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

func checkResp(r *apitypes.APIResponse, err error) error {
	if err != nil {
		return err
	}
	if !r.Success {
		return fmt.Errorf("%s", r.Error)
	}
	if r.Data != nil {
		printJSON(r.Data)
	} else {
		fmt.Println("ok")
	}
	return nil
}
