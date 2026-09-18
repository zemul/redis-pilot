package agent

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
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

// replicationState 是某个实例最近一次探测到的运行时复制关系，即实例进程此刻实际在
// 复制谁。这与另外两份记录都不同，三者可以互相偏离，所以必须单独观测：
//
//	Server 记录的 ReplicaOf     —— 意图（跟随 Sentinel 的决定）
//	磁盘配置文件里的 replicaof  —— 容器下次重启会读到的角色
//	这里的 ReplicaOf            —— 此刻正在发生的事实
type replicationState struct {
	Role      string // INFO replication 的 role 字段，"master" 或 "slave"
	ReplicaOf string // 实际在复制的 "ip:port"，Role 为 master 时是空串
	UpdatedAt time.Time
}

// replicationStaleAfter 是运行时复制观测的有效期。
//
// 探测频率是 30s，取 3 倍留出两次失败的余量。超过这个年龄的观测不再上报给 Server：
// Server 会据此下发 REPLICAOF，判据必须是"刚刚看到的事实"而不是"上次开机时的事实"。
// 宁可这一轮没有观测（Server 跳过），也不能给出过期结论。
const replicationStaleAfter = 90 * time.Second

type monitor struct {
	mu        sync.RWMutex
	instances map[string]*metrics
	health    map[string]*healthState
	repl      map[string]*replicationState
	host      hostMetrics
}

func newMonitor() *monitor {
	return &monitor{
		instances: make(map[string]*metrics),
		health:    make(map[string]*healthState),
		repl:      make(map[string]*replicationState),
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

// setRepl 记录一次运行时复制关系观测。
func (m *monitor) setRepl(name string, st *replicationState) {
	if st == nil {
		return
	}
	st.UpdatedAt = time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.repl[name] = st
}

// clearRepl 丢弃某个实例的运行时复制观测。
//
// 探测失败时必须显式丢弃而不是留着上一次的结果：留着会让一个已经不可达的实例继续以
// 陈旧观测的身份被 Server 当作判据。
func (m *monitor) clearRepl(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.repl, name)
}

// replicaOfMap 返回所有观测仍在有效期内的实例的运行时复制目标。
// 空串表示该实例认为自己是 master。过期或从未探测过的实例不出现在结果里。
func (m *monitor) replicaOfMap() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]string, len(m.repl))
	now := time.Now()
	for name, st := range m.repl {
		if st == nil || now.Sub(st.UpdatedAt) > replicationStaleAfter {
			continue
		}
		out[name] = st.ReplicaOf
	}
	return out
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

// usedMemoryMap 返回各实例最近一次 INFO 中解析出的 used_memory（字节）。
// 只包含最近 120s 内有指标采集的实例。
func (m *monitor) usedMemoryMap() map[string]int64 {
	return m.memoryFieldMap("used_memory")
}

func (m *monitor) memoryFieldMap(field string) map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int64, len(m.instances))
	now := time.Now()
	for name, met := range m.instances {
		if met == nil || now.Sub(met.UpdatedAt) > 120*time.Second {
			continue
		}
		if v := parseInfoField(met.Info, field); v > 0 {
			out[name] = v
		}
	}
	return out
}

// parseInfoField 从 Redis INFO 输出中解析一个整数字段。
func parseInfoField(info, field string) int64 {
	prefix := field + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			val := strings.TrimPrefix(line, prefix)
			val = strings.TrimSpace(val)
			n, _ := strconv.ParseInt(val, 10, 64)
			return n
		}
	}
	return 0
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
			// replErrLog 记录"PING 通但 INFO replication 读取失败"——实例健康，只是这一轮
			// 拿不到运行时复制观测。此前静默丢弃，导致 Server 侧"运行时实际 master"视角和
			// 运行时复制漂移自愈拿不到数据时排查无线索。
			health, repl, probeErr := probeInstance("127.0.0.1:"+c.Port, a.getPassword(instName),
				func(replErr error) {
					a.log.Infof("instance %s: replication observation unavailable: %v", instName, replErr)
				})
			detail := ""
			if probeErr != nil {
				detail = probeErr.Error()
			}
			a.mon.setHealth(instName, health, detail)
			// 复制关系只在这次探测确实读到了才更新，读不到就丢弃旧值——Server 会据此
			// 下发 REPLICAOF，用陈旧观测当判据会对着已经变化的拓扑动手。
			if repl != nil {
				a.mon.setRepl(instName, repl)
			} else {
				a.mon.clearRepl(instName)
			}
			if health != healthOK {
				a.log.Errorf("instance %s health=%s: %s", c.Name, health, detail)
			}
		}
	}
}

// runMetricsCollect 每 60s 采集指标缓存并持久化到 SQLite
func (a *Agent) runMetricsCollect() {
	cleanupTicker := time.NewTicker(6 * time.Hour)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-time.After(60 * time.Second):
			containers, err := podmanListContainers()
			if err != nil {
				continue
			}
			for _, name := range containers {
				instName := trimPrefix(name)
				info, err := redisCmd(instName, a.getPassword(instName), "INFO")
				if err == nil {
					a.mon.set(instName, info)
					if a.mstore != nil {
						if err := a.mstore.record(instName, info); err != nil {
							a.log.Errorf("metrics record %s: %v", instName, err)
						}
					}
				}
			}
			a.collectHostMetrics(containers)
		case <-cleanupTicker.C:
			if a.mstore != nil {
				if err := a.mstore.cleanup(30); err != nil {
					a.log.Errorf("metrics cleanup: %v", err)
				}
			}
		}
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
	// 分隔符用 '|' 而非 '\t'：部分 podman 版本的 --format 模板不会把 "\t" 输出成真正的
	// 制表符（0x09），而是输出成空格，导致按 "\t" 分割永远只得到一段、所有容器被跳过、
	// 健康探测与运行时复制观测全部失效（dev 与线上 podman 版本行为不同，曾据此误判）。
	// Ports 字段形如 "0.0.0.0:6379->6379/tcp"，不含 '|'，用它做分隔符安全。
	out, err := runShell("podman", "ps", "--format", "{{.Names}}|{{.Ports}}")
	if err != nil {
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	var result []containerInfo
	for _, line := range strings.Split(out, "\n") {
		parts := strings.SplitN(line, "|", 2)
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
// 是 probeInstance 的窄接口，供只关心健康分类的调用方使用。
func tcpPing(addr, password string) (instanceHealth, error) {
	health, _, err := probeInstance(addr, password, nil)
	return health, err
}

// probeInstance 用一条 TCP 连接完成两件事：健康探测，以及读取实例进程此刻实际在复制谁。
//
// 这里刻意不用 redisCmd（util.go）：那条路是 podman exec + redis-cli，每次要 fork
// 两个进程，而健康检查每 30s 要把本机所有实例跑一遍，开销不可接受。代价是需要自己
// 手写 RESP，因此下面对回复的解析要足够严格。
//
// 复制关系读取失败不影响健康结论：健康由 PING 决定，INFO replication 只是顺路取回的
// 观测信息，读不到就返回 nil 让调用方跳过这一轮。反过来把 INFO 失败当成不健康，会让
// 一个纯观测功能去误报实例故障。
func probeInstance(addr, password string, replErrLog func(error)) (instanceHealth, *replicationState, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return healthUnreachable, nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	// 整条连接共用一个 reader。此前每读一条回复都新建 bufio.NewReader(conn)，会把已经
	// 读进缓冲区、但属于下一条回复的字节连同 reader 一起丢掉——之所以没出问题，是因为
	// 每条命令都等回复读完才发下一条，缓冲区恰好只有一行。那是时序上的侥幸，不是保证，
	// 而 INFO 的回复跨多行，靠不住。
	r := bufio.NewReader(conn)

	if password != "" {
		if _, err := fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(password), password); err != nil {
			return healthUnreachable, nil, err
		}
		// AUTH 的回复必须检查。早期实现读了就丢，导致密码不一致时 AUTH 静默失败，
		// 随后的 PING 收到 -NOAUTH 被当成"实例挂了"，触发重启。
		authResp, err := r.ReadString('\n')
		if err != nil {
			return healthUnreachable, nil, err
		}
		if kind, ok := classifyRESPError(authResp); ok {
			return kind, nil, fmt.Errorf("AUTH rejected: %s", strings.TrimSpace(authResp))
		}
		if !strings.HasPrefix(authResp, "+") {
			return healthUnexpected, nil, fmt.Errorf("unexpected AUTH reply: %q", strings.TrimSpace(authResp))
		}
	}

	if _, err := fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
		return healthUnreachable, nil, err
	}
	resp, err := r.ReadString('\n')
	if err != nil {
		return healthUnreachable, nil, err
	}
	if kind, ok := classifyRESPError(resp); ok {
		return kind, nil, fmt.Errorf("ping rejected: %s", strings.TrimSpace(resp))
	}
	// 只接受 +PONG。早期实现用 HasPrefix(resp, "+") 兜底，任何 + 开头的回复都算健康，
	// 包括上一条命令残留的 +OK —— 那是靠时序侥幸而非逻辑保证。
	if !strings.HasPrefix(resp, "+PONG") {
		return healthUnexpected, nil, fmt.Errorf("unexpected PING reply: %q", strings.TrimSpace(resp))
	}

	// PING 成功但 INFO replication 读取失败：实例是健康的（能应答 PING），只是这一轮
	// 拿不到运行时复制观测。返回值维持原语义——一个纯观测功能不该否定 PING 的结论，
	// 所以 err 仍为 nil、health 仍为 OK、repl=nil 让调用方跳过。但失败原因不再静默丢弃：
	// 通过 replErrLog 回调记录，供排查运行时复制观测为何缺失（此前完全无线索）。
	repl, replErr := readReplicationState(conn, r)
	if replErr != nil {
		if replErrLog != nil {
			replErrLog(replErr)
		}
		return healthOK, nil, nil
	}
	return healthOK, repl, nil
}

// readReplicationState 发一条 INFO replication 并解析出运行时复制关系。
func readReplicationState(conn net.Conn, r *bufio.Reader) (*replicationState, error) {
	if _, err := fmt.Fprintf(conn, "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n"); err != nil {
		return nil, err
	}
	info, err := readRESPBulkString(r)
	if err != nil {
		return nil, err
	}
	return parseReplicationInfo(info)
}

// maxInfoBulkLen 限制 INFO 回复的大小。INFO replication 只有十几行，1MB 已极宽松；
// 上限的作用是不让一个畸形的 $<len> 头把 Agent 的内存拉爆。
const maxInfoBulkLen = 1 << 20

// readRESPBulkString 读一个 RESP 批量字符串（$<len>\r\n<payload>\r\n）。
func readRESPBulkString(r *bufio.Reader) (string, error) {
	header, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	header = strings.TrimRight(header, "\r\n")
	if !strings.HasPrefix(header, "$") {
		return "", fmt.Errorf("expected bulk string reply, got %q", header)
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil {
		return "", fmt.Errorf("bad bulk length: %q", header)
	}
	if n < 0 {
		return "", fmt.Errorf("nil bulk reply")
	}
	if n > maxInfoBulkLen {
		return "", fmt.Errorf("bulk reply too large: %d bytes", n)
	}
	buf := make([]byte, n+2) // payload + CRLF
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

// parseReplicationInfo 从 INFO replication 的输出里取出角色和实际复制目标。
//
// 解析不出 role 时返回错误而不是一个"空的 master 结论"：这个结果会上报给 Server 当作
// 下发 REPLICAOF 的判据，"没读到"和"确实是 master"必须区分开，混同的方向是对一个正常
// 的 replica 动手。role 不是 master 但缺 master_host/master_port 时同理。
func parseReplicationInfo(info string) (*replicationState, error) {
	st := &replicationState{}
	var host, port string
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "role:"):
			st.Role = strings.TrimPrefix(line, "role:")
		case strings.HasPrefix(line, "master_host:"):
			host = strings.TrimPrefix(line, "master_host:")
		case strings.HasPrefix(line, "master_port:"):
			port = strings.TrimPrefix(line, "master_port:")
		}
	}
	if st.Role == "" {
		return nil, fmt.Errorf("no role field in INFO replication output")
	}
	if st.Role != "master" {
		if host == "" || port == "" {
			return nil, fmt.Errorf("role=%s but master_host/master_port missing", st.Role)
		}
		st.ReplicaOf = host + ":" + port
	}
	return st, nil
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
