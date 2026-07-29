package agent

import (
	"bufio"
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
	CPUUsage   float64
	MemTotal   uint64
	MemUsed    uint64
	DiskTotal  uint64
	DiskUsed   uint64
	Instances  []string
	Containers int
	Running    int
	UpdatedAt  time.Time
}

// healthState 是某个实例最近一次健康探测的结果。
type healthState struct {
	Health    instanceHealth
	Detail    string
	UpdatedAt time.Time
}

type monitor struct {
	mu        sync.RWMutex
	instances map[string]*metrics
	health    map[string]*healthState
	host      hostMetrics
}

func newMonitor() *monitor {
	return &monitor{
		instances: make(map[string]*metrics),
		health:    make(map[string]*healthState),
	}
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

// setHealth 记录一次健康探测结果。detail 为空表示健康。
func (m *monitor) setHealth(name string, health instanceHealth, detail string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health[name] = &healthState{Health: health, Detail: detail, UpdatedAt: time.Now()}
}

// getHealth 返回最近一次探测结果，从未探测过时返回 nil。
func (m *monitor) getHealth(name string) *healthState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.health[name]
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

// runHealthCheck 每 30s 探测本机实例是否响应，只记录分类结果，不做任何破坏性动作。
//
// 刻意不自动重启：重启 Redis/Kvrocks 实例是破坏性且有全局影响的操作——断开所有客户端
// 连接、丢失未落盘数据、按配置文件里的旧角色重启（导致 Sentinel 刚做完的提升被回退）、
// 可能触发一次本不必要的 failover、replica 可能需要全量重同步。而一次 PING 失败只有
// 一个 bit 的信息量，无法区分「实例真的死了」和「正在 loading / fork 阻塞 / 负载高到
// 超过 3s 超时 / 密码不一致」。用信息量不足的判据去做代价高昂的动作，是净损失。
//
// 各层职责划分：
//   - 进程真正退出：由 podman --restart on-failure:5 处理（响应的是确定事实，无推断）
//   - 超过重启上限后容器停住：由 Server 的容器 reconcile 检测并告警，这是期望行为，
//     不应被 Agent 的静默重启掩盖（一个每 30s 被重启一次的实例，在监控上会看起来
//     「大部分时间是 running」）
//   - 主从组内实例不健康：由 Sentinel failover 把流量切走。切流量无损、可逆、秒级，
//     远优于试图原地修好实例。而且 Agent 的 3s 判定门槛比 Sentinel 的 down-after
//     默认 5000ms 更低，自主重启总会抢在正确机制之前动手。
//   - 容器在跑但实例无响应：探测结果记录在 monitor 里，通过 /instance/status 暴露，
//     由 Server / 运维决策。这个状态此前是观测盲区——容器 reconcile 只问 podman
//     容器在不在跑，看不到「容器活着但实例 hang 住」。
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
			health, pingErr := tcpPing("127.0.0.1:"+c.Port, a.getPassword(instName))
			detail := ""
			if pingErr != nil {
				detail = pingErr.Error()
			}
			a.mon.setHealth(instName, health, detail)
			if health != healthOK {
				a.log.Errorf("instance %s health=%s: %s", c.Name, health, detail)
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

// 注意：这里曾经有一个 podmanRestart(name)，供 runHealthCheck 在 PING 失败时自主
// 重启实例。已删除——Agent 不再基于本机局部观测做重启决策，理由见 runHealthCheck 注释。
// 容器级别的自动重启由 podman --restart on-failure:5 承担（见 internal/podman）。

// instanceHealth 是一次健康探测的结果分类。
//
// 为什么要分类而不是只返回一个 error：探测失败的原因决定了完全不同的处置方式——
// 实例真的不可达需要关注，正在加载数据集只需要等待，认证被拒是配置不一致的问题。
// 早期实现把这些全部归为一个"不健康"并触发 podman restart，导致：正在 load 大
// 数据集的实例被反复重启而永远起不来；改过密码的健康实例被误判后每 30s 重启一次。
type instanceHealth string

const (
	healthOK          instanceHealth = "healthy"
	healthLoading     instanceHealth = "loading"         // -LOADING：正在加载数据集，启动中，不是故障
	healthAuthFailed  instanceHealth = "auth_failed"     // AUTH 被拒或 -NOAUTH：配置问题，实例本身可能是好的
	healthReplLinkErr instanceHealth = "repl_link_error" // -MASTERDOWN 等：复制链路问题，实例本身活着
	healthUnreachable instanceHealth = "unreachable"     // TCP 层就连不上，进程可能已退出
	healthUnexpected  instanceHealth = "unexpected"      // 其他非预期回复
)

// tcpPing 通过 RESP 协议直连实例发送 PING，返回分类后的健康状态。
//
// 这里刻意不用 redisCmd（util.go）：那条路是 podman exec + redis-cli，每次要 fork
// 两个进程，而健康检查每 30s 要把本机所有实例跑一遍，开销不可接受。代价是需要自己
// 手写 RESP，因此下面对回复的解析要足够严格。
func tcpPing(addr, password string) (instanceHealth, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return healthUnreachable, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	if password != "" {
		if _, err := fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(password), password); err != nil {
			return healthUnreachable, err
		}
		// AUTH 的回复必须检查。早期实现读了就丢，导致密码不一致时 AUTH 静默失败，
		// 随后的 PING 收到 -NOAUTH 被当成"实例挂了"，触发重启。
		authResp, err := readRESPLine(conn)
		if err != nil {
			return healthUnreachable, err
		}
		if kind, ok := classifyRESPError(authResp); ok {
			return kind, fmt.Errorf("AUTH rejected: %s", strings.TrimSpace(authResp))
		}
		if !strings.HasPrefix(authResp, "+") {
			return healthUnexpected, fmt.Errorf("unexpected AUTH reply: %q", strings.TrimSpace(authResp))
		}
	}

	if _, err := fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
		return healthUnreachable, err
	}
	resp, err := readRESPLine(conn)
	if err != nil {
		return healthUnreachable, err
	}
	if kind, ok := classifyRESPError(resp); ok {
		return kind, fmt.Errorf("ping rejected: %s", strings.TrimSpace(resp))
	}
	// 只接受 +PONG。早期实现用 HasPrefix(resp, "+") 兜底，任何 + 开头的回复都算健康，
	// 包括上一条命令残留的 +OK —— 那是靠时序侥幸而非逻辑保证。
	if strings.HasPrefix(resp, "+PONG") {
		return healthOK, nil
	}
	return healthUnexpected, fmt.Errorf("unexpected PING reply: %q", strings.TrimSpace(resp))
}

// readRESPLine 读取一行 RESP 回复（到 \n 为止）。健康探测只需要处理简单字符串（+）
// 和错误（-）两种单行回复，不需要完整的 RESP 解析器。
func readRESPLine(conn net.Conn) (string, error) {
	return bufio.NewReader(conn).ReadString('\n')
}

// classifyRESPError 把 RESP 错误回复（- 开头）映射到健康分类。
// 返回 ok=false 表示这不是一个错误回复。
func classifyRESPError(resp string) (instanceHealth, bool) {
	if !strings.HasPrefix(resp, "-") {
		return "", false
	}
	upper := strings.ToUpper(resp)
	switch {
	case strings.HasPrefix(upper, "-LOADING"):
		return healthLoading, true
	case strings.HasPrefix(upper, "-NOAUTH"), strings.HasPrefix(upper, "-WRONGPASS"),
		strings.Contains(upper, "WITHOUT ANY PASSWORD"):
		return healthAuthFailed, true
	case strings.HasPrefix(upper, "-MASTERDOWN"):
		return healthReplLinkErr, true
	default:
		return healthUnexpected, true
	}
}

func trimPrefix(name string) string {
	name = strings.TrimPrefix(name, "redis-")
	name = strings.TrimPrefix(name, "kvrocks-")
	return name
}
