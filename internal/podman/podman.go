package podman

import (
	"fmt"
	"os/exec"
	"strings"
)

// ContainerStatus 容器状态
type ContainerStatus struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
}

// ContainerRuntime 容器运行时接口
type ContainerRuntime interface {
	Create(engine, name, image string, port int, memory string, cpus int, dataDir string) (string, error)
	Start(name string) error
	Stop(name string) error
	Remove(name string) error
	Run(args ...string) (string, error)
	ListAll() ([]ContainerStatus, error)
}

// Runtime 基于 Podman 的 ContainerRuntime 实现
type Runtime struct{}

func NewRuntime() *Runtime { return &Runtime{} }

func (r *Runtime) Run(args ...string) (string, error) {
	cmd := exec.Command("podman", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("podman %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func (r *Runtime) Create(engine, name, image string, port int, memory string, cpus int, dataDir string) (string, error) {
	if engine == "kvrocks" {
		return r.Run("run", "-d",
			"--name", name,
			"--user", "0:0",
			"--memory", memory,
			"--memory-swap", memory,
			"--cpus", fmt.Sprintf("%d", cpus),
			"--restart", "on-failure:5",
			"-p", fmt.Sprintf("%d:6379", port),
			"-v", fmt.Sprintf("%s/conf/kvrocks.conf:/etc/kvrocks/kvrocks.conf:Z", dataDir),
			"-v", fmt.Sprintf("%s/data:/data:Z", dataDir),
			"-v", fmt.Sprintf("%s/backup:/backup:Z", dataDir),
			"--entrypoint", "kvrocks",
			image,
			"-c", "/etc/kvrocks/kvrocks.conf",
		)
	}
	return r.Run("run", "-d",
		"--name", name,
		"--memory", memory,
		"--memory-swap", memory,
		"--cpus", fmt.Sprintf("%d", cpus),
		"--restart", "on-failure:5",
		"-p", fmt.Sprintf("%d:6379", port),
		"-v", fmt.Sprintf("%s/conf/redis.conf:/etc/redis/redis.conf:Z", dataDir),
		"-v", fmt.Sprintf("%s/data:/data:Z", dataDir),
		"-v", fmt.Sprintf("%s/backup:/backup:Z", dataDir),
		image,
		"redis-server", "/etc/redis/redis.conf",
	)
}

func (r *Runtime) Start(name string) error {
	_, err := r.Run("start", name)
	return err
}

func (r *Runtime) Stop(name string) error {
	_, err := r.Run("stop", name)
	return err
}

func (r *Runtime) Remove(name string) error {
	_, err := r.Run("rm", "-f", name)
	return err
}

func (r *Runtime) ListAll() ([]ContainerStatus, error) {
	out, err := r.Run("ps", "-a", "--format", "{{.Names}}\t{{.State}}")
	if err != nil {
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	var result []ContainerStatus
	for _, line := range strings.Split(out, "\n") {
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		result = append(result, ContainerStatus{
			Name:    parts[0],
			Running: parts[1] == "running",
		})
	}
	return result, nil
}
