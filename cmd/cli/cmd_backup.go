package main

import (
	"flag"
	"os"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
)

func backupExec(c *cli.Client, args []string) error {
	name := requireName("backup-exec", args)
	return checkResp(c.Post("/backup/exec", map[string]string{"name": name}))
}

func backupRestore(c *cli.Client, args []string) error {
	fs := flag.NewFlagSet("backup-restore", flag.ExitOnError)
	name := fs.String("name", "", "实例名称 (required)")
	backupTs := fs.String("backup-ts", "", "备份时间戳 (required)")
	fs.Parse(args)
	if *name == "" || *backupTs == "" {
		fs.Usage()
		os.Exit(1)
	}
	return checkResp(c.Post("/backup/restore", map[string]string{
		"name":      *name,
		"backup_ts": *backupTs,
	}))
}

func backupList(c *cli.Client, args []string) error {
	name := requireName("backup-list", args)
	return checkResp(c.Get("/backup/list?name=" + name))
}
