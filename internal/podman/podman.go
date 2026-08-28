package podman

import (
	"encoding/json"
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
	Create(engine, name, image string, port int, memory string, cpus float64, dataDir string) (string, error)
	// Update 在线调整运行中容器的资源限制（cgroup），不停机、不重建。
	// memory 为空则不改内存；cpus <= 0 则不改 CPU；两者都为空则空操作。
	// 注意：podman update 的变更在容器下次重启/重建时会回退到创建时的值。
	Update(name, memory string, cpus float64) error
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

func (r *Runtime) Create(engine, name, image string, port int, memory string, cpus float64, dataDir string) (string, error) {
	if engine == "kvrocks" {
		return r.Run("run", "-d",
			"--name", name,
			"--user", "0:0",
			"--memory", memory,
			"--memory-swap", memory,
			"--cpus", fmt.Sprintf("%g", cpus),
			"--restart", "on-failure:5",
			"-p", fmt.Sprintf("%d:%d", port, port),
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
		"--cpus", fmt.Sprintf("%g", cpus),
		"--restart", "on-failure:5",
		"-p", fmt.Sprintf("%d:%d", port, port),
		"-v", fmt.Sprintf("%s/conf/redis.conf:/etc/redis/redis.conf:Z", dataDir),
		"-v", fmt.Sprintf("%s/data:/data:Z", dataDir),
		"-v", fmt.Sprintf("%s/backup:/backup:Z", dataDir),
		image,
		"redis-server", "/etc/redis/redis.conf",
	)
}

// Update 在线调整运行中容器的资源限制。对应 `podman update`。
// memory 为空跳过内存;cpus<=0 跳过 CPU;无任何变更时直接返回。
func (r *Runtime) Update(name, memory string, cpus float64) error {
	args := []string{"update"}
	if memory != "" {
		args = append(args, "--memory", memory, "--memory-swap", memory)
	}
	if cpus > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%g", cpus))
	}
	if len(args) == 1 {
		return nil // 没有需要在容器层调整的资源(如仅改 disk)
	}
	args = append(args, name)
	_, err := r.Run(args...)
	return err
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
	out, err := r.Run("ps", "-a", "--format", "json")
	if err != nil {
		return nil, err
	}
	if out == "" || out == "null" || out == "[]" {
		return nil, nil
	}
	// podman json: [{"Names":["name"],"State":"running"/"exited"}, ...]
	var rows []struct {
		Names []string `json:"Names"`
		State string   `json:"State"`
	}
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		return nil, fmt.Errorf("podman ps json: %w", err)
	}
	var result []ContainerStatus
	for _, row := range rows {
		if len(row.Names) == 0 {
			continue
		}
		state := strings.ToLower(row.State)
		result = append(result, ContainerStatus{
			Name:    row.Names[0],
			Running: state == "running" || strings.HasPrefix(state, "up"),
		})
	}
	return result, nil
}
