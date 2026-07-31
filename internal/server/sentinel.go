package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) sentinelEvent(c *gin.Context) {
	var req struct {
		Event     string `json:"event"`
		Group     string `json:"group"`
		OldMaster string `json:"old_master"`
		NewMaster string `json:"new_master"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, 400, err.Error())
		return
	}
	s.log.Infof("sentinel event received: event=%s group=%s old=%s new=%s", req.Event, req.Group, req.OldMaster, req.NewMaster)
	if req.Group != "" && req.NewMaster != "" {
		newMaster := req.NewMaster
		if confirmed, err := s.confirmSentinelMaster(req.Group); err == nil && confirmed != "" {
			newMaster = confirmed
		} else if err != nil {
			s.log.Errorf("sentinel event confirm failed group=%s fallback=%s: %v", req.Group, req.NewMaster, err)
		}
		if err := s.handleSentinelFailover(req.Group, newMaster, "event", operatorFrom(c)); err != nil {
			fail(c, 500, err.Error())
			return
		}
	}
	ok(c, nil)
}

type sentinelNodeStatus struct {
	Name   string                   `json:"name"`
	Status *apitypes.SentinelStatus `json:"status,omitempty"`
	Error  string                   `json:"error,omitempty"`
}

type sentinelClusterStatus struct {
	Enabled bool                 `json:"enabled"`
	Port    int                  `json:"port"`
	Quorum  int                  `json:"quorum"`
	Nodes   []sentinelNodeStatus `json:"nodes"`
}

func (s *Server) sentinelStatus(c *gin.Context) {
	nodeState, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	result := sentinelClusterStatus{
		Enabled: s.cfg.Sentinel.Enabled,
		Port:    s.sentinelPort(),
		Quorum:  s.sentinelQuorum(),
	}
	for _, name := range s.selectSentinelNodes(nodeState) {
		node := sentinelNodeStatus{Name: name}
		srv := nodeState.Servers[name]
		if srv == nil || srv.Endpoint == "" {
			node.Error = "server not found in pool-state"
			result.Nodes = append(result.Nodes, node)
			continue
		}
		status, err := s.querySentinelStatus(srv)
		if err != nil {
			node.Error = err.Error()
		} else {
			node.Status = status
		}
		result.Nodes = append(result.Nodes, node)
	}
	ok(c, result)
}

func (s *Server) sentinelSync(c *gin.Context) {
	if err := s.syncSentinelMasters(); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (s *Server) StartSentinelReconcileLoop() {
	if !s.cfg.Sentinel.Enabled {
		return
	}
	go func() {
		s.reconcileSentinel()
		for range time.Tick(10 * time.Second) {
			s.reconcileSentinel()
		}
	}()
}

func (s *Server) StartSentinelWatchLoop() {
	if !s.cfg.Sentinel.Enabled {
		return
	}
	nodes := uniqueStrings(s.cfg.Sentinel.Nodes)
	if len(nodes) == 0 {
		return
	}
	for _, node := range nodes {
		go s.watchSentinelNode(node)
	}
}

func (s *Server) watchSentinelNode(node string) {
	backoff := time.Second
	for {
		addr, err := s.sentinelNodeAddress(node)
		if err != nil {
			s.log.Errorf("sentinel watcher %s waiting for node address: %v", node, err)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		s.log.Infof("sentinel watcher %s connecting to %s", node, addr)
		backoff = time.Second
		err = s.subscribeSentinelSwitchMaster(addr, func(group string) {
			current, err := s.confirmSentinelMaster(group)
			if err != nil {
				s.log.Errorf("sentinel watcher %s confirm master group=%s failed: %v", node, group, err)
				return
			}
			if current == "" {
				s.log.Errorf("sentinel watcher %s confirm master group=%s returned empty address", node, group)
				return
			}
			if err := s.handleSentinelFailover(group, current, "watch", "system"); err != nil {
				s.log.Errorf("sentinel watcher %s handle failover group=%s new=%s failed: %v", node, group, current, err)
			}
		})
		s.log.Errorf("sentinel watcher %s disconnected: %v", node, err)
		time.Sleep(backoff)
		backoff = nextBackoff(backoff)
	}
}

func (s *Server) sentinelNodeAddress(node string) (string, error) {
	nodeState, err := s.state.ReadNode()
	if err != nil {
		return "", err
	}
	srv := nodeState.Servers[node]
	if srv == nil || srv.Endpoint == "" {
		return "", fmt.Errorf("sentinel node %s not found in pool-state", node)
	}
	return fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort()), nil
}

func (s *Server) subscribeSentinelSwitchMaster(addr string, onSwitch func(group string)) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := writeRESPCommand(conn, "SUBSCRIBE", "+switch-master"); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	for {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
		reply, err := readRESP(reader)
		if err != nil {
			return err
		}
		group, ok := parseSwitchMasterMessage(reply)
		if ok {
			onSwitch(group)
		}
	}
}

func parseSwitchMasterMessage(reply interface{}) (string, bool) {
	values, ok := reply.([]interface{})
	if !ok || len(values) < 3 {
		return "", false
	}
	kind, _ := values[0].(string)
	channel, _ := values[1].(string)
	payload, _ := values[2].(string)
	if kind != "message" || channel != "+switch-master" {
		return "", false
	}
	fields := strings.Fields(payload)
	if len(fields) < 5 {
		return "", false
	}
	return fields[0], true
}

func (s *Server) confirmSentinelMaster(group string) (string, error) {
	node, err := s.state.ReadNode()
	if err != nil {
		return "", err
	}
	nodes := s.selectSentinelNodes(node)
	if len(nodes) == 0 {
		return "", errors.New("no sentinel nodes configured")
	}
	return s.querySentinelMaster(node, nodes, group)
}

func (s *Server) reconcileSentinel() {
	if err := s.reconcileSentinelOnce(); err != nil {
		s.log.Errorf("sentinel reconcile failed: %v", err)
	}
}

func (s *Server) reconcileSentinelOnce() error {
	if !s.cfg.Sentinel.Enabled {
		return nil
	}
	node, err := s.state.ReadNode()
	if err != nil {
		return err
	}
	instances, err := s.state.ReadInstances()
	if err != nil {
		return err
	}
	nodes := s.selectSentinelNodes(node)
	if len(nodes) == 0 {
		s.log.Infof("sentinel reconcile skipped: no sentinel.nodes configured")
		return nil
	}
	if !validSentinelNodeCount(len(nodes)) {
		return fmt.Errorf("invalid configured sentinel node count: got=%d want=3 or 5", len(nodes))
	}
	if len(nodes) < s.sentinelQuorum() {
		return fmt.Errorf("not enough configured sentinel nodes: have=%d quorum=%d", len(nodes), s.sentinelQuorum())
	}
	// 分两趟：先把所有认知不一致的组收集起来，再统一做一次二次确认。
	//
	// 早期实现是在组循环里就地 time.Sleep(3s)，于是耗时是 3 秒 × 不一致的组数。
	// sentinel reconcile 的间隔是 10 秒（server.go），4 个组同时不一致就会让单轮
	// 超过间隔，后续轮次持续挤压；组数再多就变成整个 reconcile 通道被拖垮，
	// 连带那些本来一致、只是排在后面的组也迟迟得不到处理。
	//
	// 二次确认的语义是"隔一段时间后地址仍是同一个新值"，它只要求两次观测之间有间隔，
	// 并不要求每组各睡一次。所以一次 sleep 覆盖全部待确认的组即可，总耗时恒定 3 秒。
	type pendingFailover struct {
		group string
		addr  string
	}
	var pending []pendingFailover

	// 遍历所有 replication group，不依赖 master 是否 running
	for groupName, group := range instances.Groups {
		if group == nil || group.Type != "replication" || group.CurrentMaster == "" {
			continue
		}
		inst := instances.Instances[group.CurrentMaster]
		if inst == nil {
			continue
		}
		endpoint := nodeEndpoint(node, inst.Server)
		if endpoint == "" {
			continue
		}
		expected := fmt.Sprintf("%s:%d", endpoint, inst.Port)
		current, err := s.querySentinelMaster(node, nodes, groupName)
		if err != nil {
			s.log.Errorf("query sentinel master %s failed: %v", groupName, err)
			continue
		}
		if current != "" && current != expected {
			pending = append(pending, pendingFailover{group: groupName, addr: current})
		}
	}
	if len(pending) == 0 {
		return nil
	}

	// GET-MASTER-ADDR-BY-NAME 在 Sentinel 的 failover 状态机走到 reconf-slaves
	// 阶段就已经会返回新地址，但此时 Sentinel 内部还没有完成 failover-end/
	// switch-master 事件，也还没有把旧 master"虚拟加入"新 master 的 slave 列表
	// （Sentinel 依赖这条记录在旧 master 重新可达时自动将其降级，这是它自愈机制的
	// 核心）。如果这时立刻调用 handleSentinelFailover（进而 syncSentinel 触发
	// REMOVE+MONITOR），会打断 Sentinel 还没走完的收尾流程，抹掉这条追踪记录，
	// 导致旧 master 永远不会被自动纠正——这是实测复现的真实竞态。用短暂的二次
	// 确认代替立即处理：只有间隔之后地址依然是同一个新值，才认为 failover 真正
	// 收尾完成，可以安全同步。
	time.Sleep(3 * time.Second)

	for _, p := range pending {
		confirmed, err := s.querySentinelMaster(node, nodes, p.group)
		if err != nil {
			s.log.Errorf("confirm sentinel master %s failed: %v", p.group, err)
			continue
		}
		if confirmed != p.addr {
			// 地址还在变，说明 failover 尚未收尾，交给下一轮
			s.log.Infof("sentinel failover still settling: group=%s observed=%s now=%s",
				p.group, p.addr, confirmed)
			continue
		}
		if err := s.handleSentinelFailover(p.group, p.addr, "reconcile", "system"); err != nil {
			s.log.Errorf("handle sentinel failover group=%s new=%s failed: %v", p.group, p.addr, err)
		}
	}
	return nil
}

func (s *Server) querySentinelMaster(nodeState *apitypes.NodeState, nodes []string, group string) (string, error) {
	var firstErr error
	for _, node := range nodes {
		srv := nodeState.Servers[node]
		if srv == nil || srv.Endpoint == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
		master, err := sentinelGetMasterAddr(addr, group)
		if err == nil {
			return master, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("no sentinel nodes available")
	}
	return "", firstErr
}

func (s *Server) querySentinelStatus(srv *apitypes.NodeServer) (*apitypes.SentinelStatus, error) {
	addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
	reply, err := sentinelCommand(addr, "SENTINEL", "MASTERS")
	if err != nil {
		return nil, err
	}
	return &apitypes.SentinelStatus{
		Running: true,
		Port:    s.sentinelPort(),
		Masters: sentinelMasterNames(reply),
	}, nil
}

func sentinelGetMasterAddr(addr, group string) (string, error) {
	reply, err := sentinelCommand(addr, "SENTINEL", "get-master-addr-by-name", group)
	if err != nil {
		return "", err
	}
	values, ok := reply.([]interface{})
	if !ok {
		return "", fmt.Errorf("expected array, got %T", reply)
	}
	if len(values) < 2 {
		return "", fmt.Errorf("sentinel returned %d values", len(values))
	}
	host, ok := values[0].(string)
	if !ok {
		return "", fmt.Errorf("sentinel returned non-string host: %T", values[0])
	}
	port, ok := values[1].(string)
	if !ok {
		return "", fmt.Errorf("sentinel returned non-string port: %T", values[1])
	}
	return host + ":" + port, nil
}

func sentinelCommand(addr string, args ...string) (interface{}, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeRESPCommand(conn, args...); err != nil {
		return nil, err
	}
	return readRESP(bufio.NewReader(conn))
}

func writeRESPCommand(w io.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

// RESP 解析的资源上限。
//
// 不设上限时，回复里的长度字段直接决定分配多少内存、递归多深，而这些数字来自网络。
// 三种具体的崩法：
//
//   - `$9223372036854775807`：make([]byte, n+2) 里 n+2 溢出成负数，make 直接 panic。
//   - `*1000000000`：一次要求预分配十亿个元素。
//   - `*1\r\n*1\r\n*1\r\n...` 无限嵌套：递归耗尽 goroutine 栈，Go 的栈溢出是
//     不可恢复的 fatal error，recover 接不住。
//
// 后果不止于一个请求失败：watchSentinelNode 跑在裸 goroutine 里，这里 panic 会带走
// 整个 Server 进程。输入虽然来自运维配置的 Sentinel 地址（不是任意攻击者），但端口
// 配错指到别的服务上就足以触发，代价与防护成本完全不对称。
//
// 上限按实际用途留足余量：SENTINEL MASTERS 的回复是"数组套数组套字符串"，深度 2。
const (
	maxRESPBulkLen  = 64 << 20 // 单个 bulk string 最大 64MiB
	maxRESPArrayLen = 1 << 20  // 单个数组最大 1M 个元素
	maxRESPDepth    = 8        // 最大嵌套深度
)

func readRESP(r *bufio.Reader) (interface{}, error) {
	return readRESPDepth(r, 0)
}

func readRESPDepth(r *bufio.Reader, depth int) (interface{}, error) {
	if depth > maxRESPDepth {
		return nil, fmt.Errorf("RESP nesting too deep: exceeds %d", maxRESPDepth)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, errors.New("empty RESP line")
	}
	switch line[0] {
	case '+':
		return strings.TrimPrefix(line, "+"), nil
	case '-':
		return nil, fmt.Errorf("sentinel error: %s", strings.TrimPrefix(line, "-"))
	case ':':
		return strconv.Atoi(strings.TrimPrefix(line, ":"))
	case '$':
		n, err := strconv.Atoi(strings.TrimPrefix(line, "$"))
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return "", nil
		}
		// 必须在 n+2 之前判上限，否则加法本身就可能溢出
		if n > maxRESPBulkLen {
			return nil, fmt.Errorf("RESP bulk string too large: %d exceeds %d", n, maxRESPBulkLen)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		count, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return []interface{}(nil), nil
		}
		if count > maxRESPArrayLen {
			return nil, fmt.Errorf("RESP array too long: %d exceeds %d", count, maxRESPArrayLen)
		}
		// 刻意不按 count 预分配容量：count 是对方声称的数字，声称一百万却只发两个
		// 元素时，预分配就白占了内存。让 append 按实际读到的数量增长，读不够自然在
		// 下一次 readRESPDepth 拿到 EOF。
		var values []interface{}
		for i := 0; i < count; i++ {
			value, err := readRESPDepth(r, depth+1)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		if values == nil {
			values = []interface{}{}
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unknown RESP type %q", line)
	}
}

func sentinelMasterNames(reply interface{}) []string {
	masters, ok := reply.([]interface{})
	if !ok {
		return nil
	}
	names := make([]string, 0, len(masters))
	for _, rawMaster := range masters {
		fields, ok := rawMaster.([]interface{})
		if !ok {
			continue
		}
		for i := 0; i+1 < len(fields); i += 2 {
			key, _ := fields[i].(string)
			if key != "name" {
				continue
			}
			name, ok := fields[i+1].(string)
			if ok {
				names = append(names, name)
			}
			break
		}
	}
	sort.Strings(names)
	return names
}

func (s *Server) handleSentinelFailover(group, newMasterAddr, source, operator string) error {
	// 全程持编排锁：本函数是"改状态 → 写磁盘配置 → 下发 Sentinel"的复合序列，
	// 三步都完成之前不能让另一次故障转移插进来（原因见 Server.topologyMu 的注释）。
	// 四条触发路径都会走到这里：Sentinel 事件回调、watcher 协程、周期 reconcile、
	// 用户手动 failover，它们彼此完全并发。
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()

	sessionID := fmt.Sprintf("sentinel-%s-%d", group, time.Now().UnixNano())
	start := time.Now()
	var oldMasterName, oldMasterServer, newMasterName, newMasterServer string
	var oldMasterFound bool
	var noOp bool

	// Phase 1: 在独立事务中加锁并提交，让并发的 instanceStop/Delete 能看到锁。
	// 此前锁在同一个 WithInstances 回调里通过 defer 释放：回调返回时 defer 触发、
	// 锁被置 nil，WithInstances 提交到 DB 时锁已经是"未持有"--锁从未生效。
	var groupNames []string
	err := s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		groupState := instances.Groups[group]
		if groupState == nil || groupState.Type != "replication" {
			return fmt.Errorf("replication group not found: %s", group)
		}
		oldMasterName = groupState.CurrentMaster
		oldMaster := instances.Instances[oldMasterName]
		if oldMaster == nil {
			return fmt.Errorf("old master not found for group %s", group)
		}
		oldMasterFound = true
		oldMasterServer = oldMaster.Server
		groupNames = instanceNamesByGroup(instances, group)
		for _, name := range groupNames {
			if inst := instances.Instances[name]; inst != nil {
				if err := state.TryAcquireLock(inst, sessionID, "sentinel-failover", 60); err != nil {
					return fmt.Errorf("instance %s: %w", name, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		s.audit.Log(audit.Record{
			Operator: operator,
			Action:   "topology.failover", Level: audit.LevelCritical, Result: "failed",
			Duration: time.Since(start).Milliseconds(),
			Target:   map[string]interface{}{"instance_group": group, "new_master": newMasterAddr},
			Params:   map[string]interface{}{"source": source},
			Detail:   err.Error(),
		})
		return err
	}
	// 在函数返回时释放锁（独立事务，与 resolveAndLock 的模式一致）
	defer s.releaseLockGroup(groupNames, sessionID)

	// Phase 2: 解析新 master 地址 + 修改角色
	node, err := s.state.ReadNode()
	if err != nil {
		s.audit.Log(audit.Record{
			Operator: operator,
			Action:   "topology.failover", Level: audit.LevelCritical, Result: "failed",
			Duration: time.Since(start).Milliseconds(),
			Target:   map[string]interface{}{"instance_group": group, "new_master": newMasterAddr},
			Params:   map[string]interface{}{"source": source},
			Detail:   err.Error(),
		})
		return err
	}

	err = s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		groupState := instances.Groups[group]
		if groupState == nil {
			return fmt.Errorf("group not found: %s", group)
		}
		newMasterName = findInstanceByAddress(node, instances, newMasterAddr)
		if newMasterName == "" {
			return fmt.Errorf("new master %s not found in instances-state", newMasterAddr)
		}
		newMaster := instances.Instances[newMasterName]
		if newMaster == nil {
			return fmt.Errorf("new master instance missing: %s", newMasterName)
		}
		if newMaster.Group != group {
			return fmt.Errorf("new master %s belongs to group %s, not %s", newMasterName, newMaster.Group, group)
		}
		// 已经同步过了就跳过。这里读观测字段：判据是"新 master 确实在跑"，
		// 是事实问题而非意图问题。
		if groupState.CurrentMaster == newMasterName && newMaster.Role == "master" && newMaster.IsServing() {
			noOp = true
			return nil
		}
		newMasterServer = newMaster.Server

		oldMaster := instances.Instances[oldMasterName]
		if oldMaster != nil {
			oldMaster.Role = "replica"
			oldMaster.ReplicaOf = newMasterName
			// 刻意不写 oldMaster 的 ObservedStatus。故障转移的原因是未知的：
			// Sentinel 自动触发时旧 master 通常确实挂了，但 instance failover 手动触发时
			// 它可能完全健康。把它一律标成 failed 是在猜测而不是观测--交给 reconcile
			// 在 30 秒内如实观测。
		}

		newMaster.Role = "master"
		newMaster.ReplicaOf = ""
		// 这一处写观测字段是有依据的：Sentinel 能把它选为新 master，说明 Sentinel
		// 探测到它是健康的。不写的话，Envoy 要等到下一轮 reconcile 才会把写流量
		// 指向新 master（proxy_group 只挂 IsServing() 的实例），白丢最多 30 秒写入。
		newMaster.ObservedStatus = apitypes.ObservedRunning
		groupState.CurrentMaster = newMasterName

		for name, inst := range instances.Instances {
			if inst == nil || name == newMasterName {
				continue
			}
			if name == oldMasterName || containsString(groupNames, name) {
				inst.Group = group
				// 这里此前有一道 `if inst.Status == "running"` 门禁，导致故障转移发生时
				// 恰好不 running 的 replica 的 Role/ReplicaOf 不被更新，永久停留在旧
				// master 上（探针实证：三节点组里先崩一个 replica 再 failover，该 replica
				// 的 ReplicaOf 一直指向已经降级的旧 master，形成一条 Sentinel 从未创建
				// 过的级联复制，且没有任何自愈路径会纠正它--reconcileSentinelOnce 只比对
				// master 地址，runReconcile 只管观测状态）。
				//
				// 门禁本身就是本次重构要消除的那类错误：Role/ReplicaOf 表达的是"这个实例
				// 在组里应该扮演什么角色"，属于意图；而 Status 由 reconcile 按观测写入。
				// 拿观测结果给意图写入把门，等于让"它现在活不活"决定"它应该是什么角色"。
				// 一个挂掉的 replica 恢复后当然应该去复制新 master，而不是复制一个已经
				// 降级的旧 master。
				inst.Role = "replica"
				inst.ReplicaOf = newMasterName
			}
		}
		state.RecalculateGroupTopology(instances, group)
		return nil
	})
	if err != nil {
		s.audit.Log(audit.Record{
			Operator: operator,
			Action:   "topology.failover", Level: audit.LevelCritical, Result: "failed",
			Duration: time.Since(start).Milliseconds(),
			Target:   map[string]interface{}{"instance_group": group, "new_master": newMasterAddr},
			Params:   map[string]interface{}{"source": source},
			Detail:   err.Error(),
		})
		return err
	}
	if !oldMasterFound {
		return fmt.Errorf("old master not found for group %s", group)
	}
	if noOp {
		s.log.Infof("sentinel failover already synchronized: group=%s master=%s source=%s", group, newMasterName, source)
		return nil
	}

	result := "success"
	detail := "sentinel failover synchronized"
	s.audit.Log(audit.Record{
		Operator: operator,
		Action:   "topology.failover", Level: audit.LevelCritical, Result: result,
		Duration: time.Since(start).Milliseconds(),
		Target: map[string]interface{}{
			"instance_group":    group,
			"old_master_server": oldMasterServer,
			"new_master":        newMasterName,
			"new_master_addr":   newMasterAddr,
			"new_master_server": newMasterServer,
		},
		Params: map[string]interface{}{"source": source},
		Detail: detail,
	})
	// 把新角色写进各实例的磁盘配置文件。运行时角色已由 Sentinel 改好，这里修正的是
	// 磁盘上那份，避免容器重启（podman --restart on-failure:5）后角色回退。
	// 失败不回滚故障转移--reconcileRoleDrift 会在下一轮 reconcile 补推。
	s.persistGroupRolesLocked(group)
	// 调不加锁变体：本函数已持 topologyMu，Go 的 Mutex 不可重入。
	if err := s.syncSentinelMastersLocked(); err != nil {
		s.log.Errorf("sentinel sync failed: %v", err)
	}
	return nil
}

func findInstanceByAddress(node *apitypes.NodeState, instances *apitypes.InstancesState, addr string) string {
	for name, inst := range instances.Instances {
		if inst == nil {
			continue
		}
		if fmt.Sprintf("%s:%d", nodeEndpoint(node, inst.Server), inst.Port) == addr {
			return name
		}
	}
	return ""
}

func instanceNamesByGroup(instances *apitypes.InstancesState, group string) []string {
	names := make([]string, 0)
	if group == "" {
		return names
	}
	for name, inst := range instances.Instances {
		if inst != nil && inst.Group == group {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (s *Server) syncSentinel() {
	if err := s.syncSentinelMasters(); err != nil {
		s.log.Errorf("sentinel sync failed: %v", err)
	}
}

// syncSentinelMasters 是给未持锁的调用方用的入口。已持 topologyMu 的调用方
// （目前只有 handleSentinelFailover）必须直接调 syncSentinelMastersLocked。
func (s *Server) syncSentinelMasters() error {
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()
	return s.syncSentinelMastersLocked()
}

// syncSentinelMastersLocked 要求调用方已持有 topologyMu。
//
// 锁必须覆盖"读状态快照"到"下发完成"的整段，而不只是下发动作：只锁下发的话，
// 两个协程仍可能各自读到不同快照，然后按快照顺序错乱地下发。
func (s *Server) syncSentinelMastersLocked() error {
	if !s.cfg.Sentinel.Enabled {
		return nil
	}
	node, err := s.state.ReadNode()
	if err != nil {
		return err
	}
	instances, err := s.state.ReadInstances()
	if err != nil {
		return err
	}

	nodes := s.selectSentinelNodes(node)
	if !validSentinelNodeCount(len(nodes)) {
		return fmt.Errorf("sentinel disabled: configure exactly 3 or 5 sentinel.nodes, got %d", len(nodes))
	}

	masters := s.buildSentinelMasters(node, instances)
	known := knownSentinelGroups(instances)

	var firstErr error
	success := 0
	for _, name := range nodes {
		srv := node.Servers[name]
		if srv == nil || srv.Endpoint == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
		if err := syncSentinelNode(addr, masters, known, s.sentinelQuorum()); err != nil {
			s.log.Errorf("sentinel sync failed on %s: %v", name, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		success++
	}
	if success < s.sentinelQuorum() {
		return fmt.Errorf("sentinel quorum not reached: success=%d quorum=%d: %w", success, s.sentinelQuorum(), firstErr)
	}
	return nil
}

func (s *Server) removeSentinelMaster(group string) {
	if !s.cfg.Sentinel.Enabled || group == "" {
		return
	}
	// 与 syncSentinelMasters 互斥：否则删组的 REMOVE 可能夹在另一次同步的
	// REMOVE+MONITOR 之间，把刚建立的监控又删掉。
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()
	node, err := s.state.ReadNode()
	if err != nil {
		s.log.Errorf("sentinel remove read node: %v", err)
		return
	}
	for _, name := range s.selectSentinelNodes(node) {
		srv := node.Servers[name]
		if srv == nil || srv.Endpoint == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
		if _, err := sentinelCommand(addr, "SENTINEL", "REMOVE", group); err != nil {
			s.log.Errorf("sentinel remove %s on %s failed: %v", group, name, err)
		}
	}
}

// syncSentinelNode 把期望配置同步到单个 Sentinel 节点。
//
// desired 是"需要下发 MONITOR/SET 配置"的组；known 是"Server 认识的全部 replication
// 组"，用于判断 Sentinel 上的哪些组已经不该存在。两者必须分开：
// desired 会因为 master 暂时不健康、地址暂时解析不出来而缺项，而这类缺项绝不能被理解成
// "该组应当从 Sentinel 上删除"。只有 known 里没有的组才是真正被删除/改类型的组。
func syncSentinelNode(addr string, desired []apitypes.SentinelMaster, known map[string]struct{}, quorum int) error {
	reply, err := sentinelCommand(addr, "SENTINEL", "MASTERS")
	if err != nil {
		return err
	}
	desiredByGroup := map[string]apitypes.SentinelMaster{}
	for _, master := range desired {
		desiredByGroup[master.Group] = master
	}

	// 逐组独立处理，单组失败不影响其余组。
	//
	// 早期实现在组循环里直接 return err，于是任意一组出问题（比如某组的密码设置失败），
	// 排在它后面的组这一轮就完全不会被配置。最坏的一种：REMOVE 已经执行、MONITOR 失败
	// 时直接返回，该组在 Sentinel 上处于"已删除且未重建"的无监控状态，同时后面的组也
	// 没被处理——一次局部故障扩散成整个节点的配置停滞。
	var errs []string
	fail := func(group string, err error) {
		errs = append(errs, fmt.Sprintf("%s: %v", group, err))
	}

	for _, group := range sentinelMasterNames(reply) {
		if _, ok := known[group]; !ok {
			if _, err := sentinelCommand(addr, "SENTINEL", "REMOVE", group); err != nil {
				fail(group, fmt.Errorf("remove stale group: %w", err))
			}
		}
	}
	for _, master := range desired {
		// 仅当 Sentinel 当前认知的 master 地址与期望地址不一致（组未注册，或地址真的变了，
		// 例如手工修复配置错误）时才 REMOVE + 重新 MONITOR。
		//
		// 关键：REMOVE 会清空 Sentinel 对该组的全部历史状态，包括 failover 完成后
		// Sentinel 自己"虚拟加入"的旧 master 记录（Sentinel 依赖这条记录在旧 master
		// 重新可达时自动把它降级为 replica，这是官方文档描述的核心自愈机制）。如果每次
		// syncSentinel 都无条件 REMOVE+MONITOR，就会在 failover 刚完成、Sentinel 刚建立
		// 这条追踪记录时立刻把它抹掉，导致旧 master 永远不会被自动纠正——这是本函数曾经
		// 的实现方式，也是脑裂 bug 的直接原因之一。
		// 当地址已经一致时，只允许通过 SENTINEL SET 调整不影响拓扑追踪状态的参数。
		currentAddr, hasCurrentAddr := sentinelCurrentAddr(reply, master.Group)
		wantAddr := fmt.Sprintf("%s:%d", master.Host, master.Port)
		if !hasCurrentAddr || currentAddr != wantAddr {
			_, _ = sentinelCommand(addr, "SENTINEL", "REMOVE", master.Group)
			if _, err := sentinelCommand(addr, "SENTINEL", "MONITOR", master.Group, master.Host, fmt.Sprintf("%d", master.Port), fmt.Sprintf("%d", quorum)); err != nil {
				// 这一条格外严重：REMOVE 已经执行完，MONITOR 又失败，该组此刻在这个
				// Sentinel 节点上完全没有监控，失去故障转移能力，直到下一轮同步补上。
				// 不 return，让其余组照常配置——它们的监控还是好的，没必要一起牺牲。
				fail(master.Group, fmt.Errorf("MONITOR failed after REMOVE, group is left unmonitored on this node: %w", err))
				continue
			}
		}
		if master.Password != "" {
			if _, err := sentinelCommand(addr, "SENTINEL", "SET", master.Group, "auth-pass", master.Password); err != nil {
				fail(master.Group, fmt.Errorf("SET auth-pass: %w", err))
				continue
			}
		}
		for _, kv := range [][2]string{
			{"down-after-milliseconds", fmt.Sprintf("%d", master.DownAfterMilliseconds)},
			{"failover-timeout", fmt.Sprintf("%d", master.FailoverTimeout)},
			{"parallel-syncs", fmt.Sprintf("%d", master.ParallelSyncs)},
		} {
			if _, err := sentinelCommand(addr, "SENTINEL", "SET", master.Group, kv[0], kv[1]); err != nil {
				fail(master.Group, fmt.Errorf("SET %s: %w", kv[0], err))
				break
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d group(s) failed: %s", len(errs), strings.Join(errs, "; "))
	}
	return nil
}

// sentinelCurrentAddr 从 SENTINEL MASTERS 的回复中提取指定 group 当前的 master 地址。
func sentinelCurrentAddr(reply interface{}, group string) (string, bool) {
	masters, ok := reply.([]interface{})
	if !ok {
		return "", false
	}
	for _, rawMaster := range masters {
		fields, ok := rawMaster.([]interface{})
		if !ok {
			continue
		}
		m := map[string]string{}
		for i := 0; i+1 < len(fields); i += 2 {
			key, _ := fields[i].(string)
			val, _ := fields[i+1].(string)
			m[key] = val
		}
		if m["name"] != group {
			continue
		}
		if m["ip"] == "" || m["port"] == "" {
			return "", false
		}
		return m["ip"] + ":" + m["port"], true
	}
	return "", false
}

// knownSentinelGroups 返回"按拓扑意图应当由 Sentinel 监控"的组名集合。
//
// 关键：这里刻意不看 master 的健康状态（Status/Role）。是否需要监控是一个拓扑意图，
// 只在建组/删组/改组类型时变化；而 master 是否健康是随时在变的瞬时观测。master 不健康
// 恰恰是 Sentinel 最需要继续监控、准备发起 failover 的时刻，绝不能因此撤掉监控。
//
// 这个集合专门用于回答"哪些组允许从 Sentinel 上 REMOVE"，与 buildSentinelMasters
// 返回的"需要下发 MONITOR/SET 配置的组"是两件不同的事：后者需要能解析出具体地址，
// 前者不需要。地址暂时解析不出来只意味着"这一轮没法下发配置"，不意味着"该删掉监控"。
func knownSentinelGroups(instances *apitypes.InstancesState) map[string]struct{} {
	groups := map[string]struct{}{}
	if instances == nil {
		return groups
	}
	for name, group := range instances.Groups {
		if group == nil || group.Type != "replication" || group.CurrentMaster == "" {
			continue
		}
		groups[name] = struct{}{}
	}
	return groups
}

func (s *Server) buildSentinelMasters(node *apitypes.NodeState, instances *apitypes.InstancesState) []apitypes.SentinelMaster {
	var masters []apitypes.SentinelMaster
	for groupName, group := range instances.Groups {
		if group == nil || group.Type != "replication" || group.CurrentMaster == "" {
			continue
		}
		// 只要求 CurrentMaster 记录存在且地址可解析，不要求它当前健康。
		// 曾经这里额外要求 inst.Role == "master" && inst.Status == "running"，
		// 导致 master 容器崩溃（reconcile 把 Status 改成 failed）后该组被剔出 desired，
		// 再经 syncSentinelNode 的清理逻辑被 SENTINEL REMOVE——恰好在最需要故障转移的
		// 时刻摘掉了故障转移能力。健康状态只应影响"要不要重新 MONITOR 到新地址"，
		// 不应影响"要不要监控这个组"。
		inst := instances.Instances[group.CurrentMaster]
		if inst == nil {
			continue
		}
		endpoint := nodeEndpoint(node, inst.Server)
		if endpoint == "" {
			continue
		}
		masters = append(masters, apitypes.SentinelMaster{
			Group:                 groupName,
			Host:                  endpoint,
			Port:                  inst.Port,
			Password:              inst.Password,
			DownAfterMilliseconds: s.sentinelDownAfter(),
			FailoverTimeout:       s.sentinelFailoverTimeout(),
			ParallelSyncs:         s.sentinelParallelSyncs(),
		})
	}
	sort.Slice(masters, func(i, j int) bool { return masters[i].Group < masters[j].Group })
	return masters
}

func (s *Server) selectSentinelNodes(node *apitypes.NodeState) []string {
	var selected []string
	seen := map[string]bool{}
	for _, name := range s.cfg.Sentinel.Nodes {
		if name == "" || seen[name] {
			continue
		}
		if node.Servers[name] == nil {
			s.log.Errorf("configured sentinel node %s not found in pool-state", name)
			continue
		}
		selected = append(selected, name)
		seen[name] = true
	}
	return selected
}

func (s *Server) sentinelReplicas() int {
	if len(s.cfg.Sentinel.Nodes) > 0 {
		return len(s.cfg.Sentinel.Nodes)
	}
	if s.cfg.Sentinel.Replicas == 5 {
		return 5
	}
	return 3
}

func validSentinelNodeCount(count int) bool {
	return count == 3 || count == 5
}

func (s *Server) sentinelQuorum() int {
	if s.cfg.Sentinel.Quorum > 0 {
		return s.cfg.Sentinel.Quorum
	}
	if s.sentinelReplicas() == 5 {
		return 3
	}
	return 2
}

func (s *Server) sentinelPort() int {
	if s.cfg.Sentinel.Port > 0 {
		return s.cfg.Sentinel.Port
	}
	return 26379
}

func (s *Server) sentinelDownAfter() int {
	if s.cfg.Sentinel.DownAfterMilliseconds > 0 {
		return s.cfg.Sentinel.DownAfterMilliseconds
	}
	return 5000
}

func (s *Server) sentinelFailoverTimeout() int {
	if s.cfg.Sentinel.FailoverTimeout > 0 {
		return s.cfg.Sentinel.FailoverTimeout
	}
	return 30000
}

func (s *Server) sentinelParallelSyncs() int {
	if s.cfg.Sentinel.ParallelSyncs > 0 {
		return s.cfg.Sentinel.ParallelSyncs
	}
	return 1
}

func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}
