package podman

import (
	"fmt"
	"os/exec"
	"strings"
)

// Run 执行 podman 命令，返回 stdout
func Run(args ...string) (string, error) {
	cmd := exec.Command("podman", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("podman %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

// ContainerExists 检查容器是否存在
func ContainerExists(name string) bool {
	_, err := Run("container", "inspect", name)
	return err == nil
}

// ContainerRunning 检查容器是否在运行
func ContainerRunning(name string) bool {
	out, err := Run("inspect", "--format", "{{.State.Running}}", name)
	return err == nil && out == "true"
}

// CreateRedis 创建 Redis 容器
func CreateRedis(name string, port int, memory string, cpus int, dataDir string) (string, error) {
	return Run("run", "-d",
		"--name", name,
		"--memory", memory,
		"--memory-swap", memory,
		"--cpus", fmt.Sprintf("%d", cpus),
		"--restart", "on-failure:5",
		"-p", fmt.Sprintf("%d:6379", port),
		"-v", fmt.Sprintf("%s/conf/redis.conf:/etc/redis/redis.conf:Z", dataDir),
		"-v", fmt.Sprintf("%s/data:/data:Z", dataDir),
		"-v", fmt.Sprintf("%s/backup:/backup:Z", dataDir),
		"docker.io/redis:7",
		"redis-server", "/etc/redis/redis.conf",
	)
}

// CreateKvrocks 创建 Kvrocks 容器
func CreateKvrocks(name string, port int, memory string, cpus int, dataDir string) (string, error) {
	return Run("run", "-d",
		"--name", name,
		"--memory", memory,
		"--memory-swap", memory,
		"--cpus", fmt.Sprintf("%d", cpus),
		"--restart", "on-failure:5",
		"-p", fmt.Sprintf("%d:6666", port),
		"-v", fmt.Sprintf("%s/conf/kvrocks.conf:/etc/kvrocks/kvrocks.conf:Z", dataDir),
		"-v", fmt.Sprintf("%s/data:/data:Z", dataDir),
		"-v", fmt.Sprintf("%s/backup:/backup:Z", dataDir),
		"docker.io/apache/kvrocks:2.9",
		"kvrocks", "--config", "/etc/kvrocks/kvrocks.conf",
	)
}

// Start 启动容器
func Start(name string) error {
	_, err := Run("start", name)
	return err
}

// Stop 停止容器
func Stop(name string) error {
	_, err := Run("stop", name)
	return err
}

// Remove 删除容器
func Remove(name string) error {
	_, err := Run("rm", "-f", name)
	return err
}
