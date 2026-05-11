package agent

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"
)

type metrics struct {
	Info      string
	UpdatedAt time.Time
}

type hostMetrics struct {
	CPUUsage    float64
	MemTotal    uint64
	MemUsed     uint64
	DiskTotal   uint64
	DiskUsed    uint64
	Instances   []string
	Containers  int
	Running     int
	UpdatedAt   time.Time
}

type monitor struct {
	mu        sync.RWMutex
	instances map[string]*metrics
	host      hostMetrics
}

func newMonitor() *monitor {
	return &monitor{instances: make(map[string]*metrics)}
}

func (m *monitor) get(name string) *metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.instances[name]
}

func (m *monitor) set(name, info string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instances[name] = &metrics{Info: info, UpdatedAt: time.Now()}
}

func (m *monitor) hostResources() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]interface{}{
		"cpu_num":    runtime.NumCPU(),
		"mem_total":  m.host.MemTotal,
		"mem_used":   m.host.MemUsed,
		"disk_total": m.host.DiskTotal,
		"disk_used":  m.host.DiskUsed,
		"instances":  m.host.Instances,
		"containers": m.host.Containers,
		"running":    m.host.Running,
		"updated_at": m.host.UpdatedAt.Format(time.RFC3339),
	}
}

// runHealthCheck 每 30s 检查实例存活，挂了自动重启
func (a *Agent) runHealthCheck() {
	for range time.Tick(30 * time.Second) {
		containers, err := podmanListContainersWithPort()
		if err != nil {
			continue
		}
		for _, c := range containers {
			if c.Port == "" {
				continue
			}
			instName := trimPrefix(c.Name)
			pw := a.getPassword(instName)
			if err := tcpPing("127.0.0.1:"+c.Port, pw); err != nil {
				a.log.Errorf("instance %s unhealthy, restarting", c.Name)
				podmanRestart(c.Name)
			}
		}
	}
}

// runMetricsCollect 每 60s 采集指标缓存
func (a *Agent) runMetricsCollect() {
	for range time.Tick(60 * time.Second) {
		containers, err := podmanListContainers()
		if err != nil {
			continue
		}
		for _, name := range containers {
			instName := trimPrefix(name)
			info, err := redisCmd(instName, a.getPassword(instName), "INFO")
			if err == nil {
				a.mon.set(instName, info)
			}
		}
		a.collectHostMetrics(containers)
	}
}

func (a *Agent) collectHostMetrics(containers []string) {
	a.mon.mu.Lock()
	defer a.mon.mu.Unlock()

	// 实例列表
	var instances []string
	running := 0
	for _, name := range containers {
		instances = append(instances, trimPrefix(name))
		running++
	}
	// 全部容器数（含停止的）
	allOut, _ := runShell("podman", "ps", "-a", "--format", "{{.Names}}")
	allCount := 0
	if allOut != "" {
		allCount = len(strings.Split(allOut, "\n"))
	}

	// 内存
	out, err := runShell("cat", "/proc/meminfo")
	if err == nil {
		var total, available uint64
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fmt.Sscanf(line, "MemTotal: %d kB", &total)
			}
			if strings.HasPrefix(line, "MemAvailable:") {
				fmt.Sscanf(line, "MemAvailable: %d kB", &available)
			}
		}
		a.mon.host.MemTotal = total * 1024
		a.mon.host.MemUsed = (total - available) * 1024
	}

	// 磁盘
	dfOut, err := runShell("df", "-B1", "/data")
	if err == nil {
		lines := strings.Split(dfOut, "\n")
		if len(lines) >= 2 {
			fields := strings.Fields(lines[1])
			if len(fields) >= 4 {
				fmt.Sscanf(fields[1], "%d", &a.mon.host.DiskTotal)
				fmt.Sscanf(fields[2], "%d", &a.mon.host.DiskUsed)
			}
		}
	}

	a.mon.host.Instances = instances
	a.mon.host.Containers = allCount
	a.mon.host.Running = running
	a.mon.host.UpdatedAt = time.Now()
}

type containerInfo struct {
	Name string
	Port string // host port mapped to the container
}

func podmanListContainersWithPort() ([]containerInfo, error) {
	out, err := runShell("podman", "ps", "--format", "{{.Names}}\t{{.Ports}}")
	if err != nil {
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	var result []containerInfo
	for _, line := range strings.Split(out, "\n") {
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) < 2 || parts[0] == "" {
			continue
		}
		port := extractHostPort(parts[1])
		result = append(result, containerInfo{Name: parts[0], Port: port})
	}
	return result, nil
}

// extractHostPort parses "0.0.0.0:6379->6379/tcp" and returns "6379" (the host port).
func extractHostPort(ports string) string {
	// format: "0.0.0.0:6379->6379/tcp" or "0.0.0.0:6380->6666/tcp"
	idx := strings.Index(ports, "->")
	if idx < 0 {
		return ""
	}
	left := ports[:idx]
	colon := strings.LastIndex(left, ":")
	if colon < 0 {
		return ""
	}
	return left[colon+1:]
}

func podmanListContainers() ([]string, error) {
	out, err := runShell("podman", "ps", "--format", "{{.Names}}")
	if err != nil {
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	return strings.Split(out, "\n"), nil
}

func podmanRestart(name string) {
	runShell("podman", "restart", name)
}

// tcpPing connects to addr and sends a Redis PING command via RESP protocol.
func tcpPing(addr, password string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	if password != "" {
		fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(password), password)
		buf := make([]byte, 128)
		conn.Read(buf)
	}

	fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n")
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	resp := string(buf[:n])
	if !strings.Contains(resp, "PONG") && !strings.HasPrefix(resp, "+") {
		return fmt.Errorf("unexpected: %s", resp)
	}
	return nil
}

func trimPrefix(name string) string {
	name = strings.TrimPrefix(name, "redis-")
	name = strings.TrimPrefix(name, "kvrocks-")
	return name
}
