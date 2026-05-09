package agent

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// redisCmd 通过 podman exec 执行 redis-cli 命令
func redisCmd(instanceName, password string, args ...string) (string, error) {
	// 尝试 redis- 和 kvrocks- 前缀
	for _, prefix := range []string{"redis-", "kvrocks-"} {
		container := prefix + instanceName
		cliArgs := []string{"exec", container, "redis-cli"}
		if password != "" {
			cliArgs = append(cliArgs, "-a", password, "--no-auth-warning")
		}
		cliArgs = append(cliArgs, args...)
		out, err := exec.Command("podman", cliArgs...).CombinedOutput()
		if err == nil {
			return strings.TrimSpace(string(out)), nil
		}
	}
	return "", fmt.Errorf("instance %s not found or not running", instanceName)
}

// runShell 执行 shell 命令
func runShell(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %v: %w\n%s", name, args, err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

// cleanupBackups 保留最近 n 份备份，删除多余的
func cleanupBackups(dir string, retention int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	// 过滤出备份文件/目录（排除 checkpoint 临时目录）
	var backups []os.DirEntry
	for _, e := range entries {
		name := e.Name()
		if name == "checkpoint" {
			continue
		}
		backups = append(backups, e)
	}
	if len(backups) <= retention {
		return
	}
	// 按名称排序（时间戳格式，字典序即时间序）
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Name() < backups[j].Name()
	})
	// 删除最旧的
	for _, e := range backups[:len(backups)-retention] {
		path := filepath.Join(dir, e.Name())
		if e.IsDir() {
			os.RemoveAll(path)
		} else {
			os.Remove(path)
		}
	}
}

// copyFile 复制文件
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
