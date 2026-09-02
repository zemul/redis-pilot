// Package session 是一条客户端连接的状态机。
//
// 职责：读客户端命令 -> route.Classify -> 本地处理或转发后端 -> 中继响应。
//
// M5：使用共享连接池（Pool + FIFO 解复用）。SHARED 命令走 pool.RoundTrip，
// EXCLUSIVE（MULTI/BLPOP/SUBSCRIBE/SELECT）走 pool.AcquireExclusive 直连。
// 多个 session 共享同一组 Pool（由 proxysvc 创建管理）。
package session

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/backend"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/metrics"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/readpool"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/resp"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/route"
)

// Mode 是 ClientSession 的当前模式。
type Mode int

const (
	ModeNormal    Mode = iota // 常规：SHARED 池多路复用
	ModeExclusive             // 独占：pin 在一条上游连接上
	ModeSubscribe             // 订阅：双向中继
)

// Config 是 session 创建参数。
type Config struct {
	Group             string // group 名称（用于指标）
	MasterAddr        string
	ReplicaAddrs      []string
	Password          string        // group 密码（D7：空=不鉴权）
	ConnectTimeout    time.Duration // 建连超时（§15: 3s）
	MasterReadWeight  int           // master 读权重（默认 0）
	MaxRequestBytes   int           // 单条命令最大字节数（D13，默认 16MB）
}

// Session 管理一条客户端连接的生命周期。
type Session struct {
	clientConn   net.Conn
	clientReader *resp.Reader
	clientWriter *resp.Writer
	cfg          Config
	metrics      *metrics.Metrics

	masterPool   *backend.Pool
	replicaPools map[string]*backend.Pool // addr -> pool（proxysvc 传入的每会话快照，只读，不与拓扑更新共享同一 map）
	readPool     *readpool.Pool           // 选择逻辑（per-session）

	mode          Mode
	authed        bool
	exclusiveConn *backend.UpstreamConn // EXCLUSIVE 模式下的独占连接
	pinnedBySelect bool                 // SELECT 触发的永久 pin，事务结束后不释放连接

	// Pipeline read-ahead: command read from buffer but not yet processed
	// (because it couldn't be batched with the previous group).
	pendingRaw  []byte
	pendingArgv [][]byte
}

var ErrQuit = errors.New("client sent QUIT")

// New 创建 session。masterPool 由 proxysvc 创建并共享；replicaPools 是 proxysvc
// 传入的每会话快照（浅拷贝，见 Proxy.snapshotReplicas），session 只读不改。
func New(conn net.Conn, cfg Config, masterPool *backend.Pool, replicaPools map[string]*backend.Pool, m *metrics.Metrics) (*Session, error) {
	// 构建读池：master + 所有 replica
	entries := []readpool.Entry{
		{Addr: cfg.MasterAddr, Weight: cfg.MasterReadWeight, Role: readpool.RoleMaster},
	}
	for _, addr := range cfg.ReplicaAddrs {
		entries = append(entries, readpool.Entry{Addr: addr, Weight: 1, Role: readpool.RoleReplica})
	}
	rp := readpool.New(entries)

	s := &Session{
		clientConn:   conn,
		clientReader: resp.NewReader(conn),
		clientWriter: resp.NewWriter(conn),
		cfg:          cfg,
		metrics:      m,
		masterPool:   masterPool,
		replicaPools: replicaPools,
		readPool:     rp,
		mode:         ModeNormal,
		authed:       cfg.Password == "",
	}
	if cfg.MaxRequestBytes > 0 {
		s.clientReader.SetMaxBytes(cfg.MaxRequestBytes)
	}
	return s, nil
}

// Serve 运行 session 主循环，直到客户端断连或出错。
func (s *Session) Serve() error {
	defer s.cleanup()

	for {
		// Get next command: either from pipeline read-ahead or read from client.
		var raw []byte
		var argv [][]byte
		if s.pendingRaw != nil {
			raw, argv = s.pendingRaw, s.pendingArgv
			s.pendingRaw, s.pendingArgv = nil, nil
		} else {
			var err error
			raw, argv, err = s.clientReader.ReadCommand()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}

		// AUTH 检查
		if !s.authed && !isPreAuthCommand(argv) {
			s.clientWriter.WriteError("NOAUTH Authentication required.")
			continue
		}

		s.metrics.IncCommand(s.cfg.Group, route.NormalizeCommand(firstArg(argv)))

		d := route.Classify(argv)

		switch d.Class {
		case route.ClassDeny:
			s.clientWriter.WriteError("ERR command not allowed: " + string(argv[0]))

		case route.ClassConnState:
			if err := s.handleConnState(argv, raw, d); err != nil {
				if err == ErrQuit {
					return nil
				}
				return err
			}

		case route.ClassSubscribe:
			// SUBSCRIBE：获取独占连接，转发后进入双向中继
			conn, err := s.masterPool.AcquireExclusive()
			if err != nil {
				return err
			}
			s.exclusiveConn = conn
			s.metrics.IncExclusive(s.cfg.Group)
			s.mode = ModeSubscribe
			if err := s.forwardExclusive(raw, d, argv[0]); err != nil {
				return err
			}
			s.relay(conn)
			return nil // relay 退出 = 连接结束

		case route.ClassTxn:
			// MULTI/WATCH 触发 pin；EXEC/DISCARD 解 pin
			if d.Pin {
				if s.exclusiveConn == nil {
					conn, err := s.masterPool.AcquireExclusive()
					if err != nil {
						return err
					}
					s.exclusiveConn = conn
					s.metrics.IncExclusive(s.cfg.Group)
				}
				s.mode = ModeExclusive
			}
			if err := s.forwardAndRelay(raw, s.masterPool, d, argv[0]); err != nil {
				return err
			}
			if !d.Pin {
				// EXEC/DISCARD/UNWATCH：退出事务模式
				if s.pinnedBySelect {
					// SELECT 触发的永久 pin：保持独占连接，仅退出事务状态
					// 连接仍在非默认 DB 上，不能还池
					s.mode = ModeExclusive
				} else if s.exclusiveConn != nil {
					s.masterPool.ReleaseExclusive(s.exclusiveConn)
					s.metrics.DecExclusive(s.cfg.Group)
					s.exclusiveConn = nil
					s.mode = ModeNormal
				} else {
					s.mode = ModeNormal
				}
			}

		case route.ClassBlocking:
			// 阻塞命令：获取独占连接 -> 转发 -> 等响应 -> 还池
			if s.exclusiveConn == nil {
				conn, err := s.masterPool.AcquireExclusive()
				if err != nil {
					return err
				}
				s.exclusiveConn = conn
				s.metrics.IncExclusive(s.cfg.Group)
			}
			s.mode = ModeExclusive
			if err := s.forwardAndRelay(raw, s.masterPool, d, argv[0]); err != nil {
				return err
			}
			if !s.pinnedBySelect {
				s.masterPool.ReleaseExclusive(s.exclusiveConn)
				s.metrics.DecExclusive(s.cfg.Group)
				s.exclusiveConn = nil
				s.mode = ModeNormal
			}

		default:
			// WRITE / READ / MASTER_ONLY / ADMIN
			pool := s.selectTarget(d)

			// Pipeline: read ahead while commands are buffered.
			// Batch commands of the same class going to the same pool.
			batch := [][]byte{raw}
			for s.clientReader.PendingCommand() {
				raw2, argv2, err := s.clientReader.ReadCommand()
				if err != nil {
					break
				}
				d2 := route.Classify(argv2)
				if d2.Class != d.Class {
					// Different routing class — save for next iteration.
					s.pendingRaw = raw2
					s.pendingArgv = argv2
					break
				}
				s.metrics.IncCommand(s.cfg.Group, route.NormalizeCommand(firstArg(argv2)))
				batch = append(batch, raw2)
			}

			if len(batch) == 1 {
				if err := s.forwardAndRelay(raw, pool, d, argv[0]); err != nil {
					return err
				}
			} else {
				if err := s.forwardBatchAndRelay(batch, pool, d, argv[0]); err != nil {
					return err
				}
			}
		}
	}
}

// selectTarget 根据命令分类和会话状态选择目标后端池。
// 返回 nil 表示在 EXCLUSIVE 模式下，使用 exclusiveConn 直接转发。
func (s *Session) selectTarget(d route.Decision) *backend.Pool {
	if s.exclusiveConn != nil {
		return nil // EXCLUSIVE 模式，forwardAndRelay 会用 exclusiveConn
	}

	switch d.Class {
	case route.ClassWrite:
		return s.masterPool

	case route.ClassRead:
		if s.rwSplitEnabled() {
			return s.selectFromReadPool()
		}
		return s.masterPool

	default:
		return s.masterPool
	}
}

// selectFromReadPool 从读池选择后端池，失败时回落 master（D10）。
func (s *Session) selectFromReadPool() *backend.Pool {
	entry := s.readPool.Select()

	if entry.Role == readpool.RoleMaster || entry.Addr == s.cfg.MasterAddr {
		// 选中 master 有两种情形：master 配了读权重被正常加权命中（预期分流，
		// 非回落）；或所有 replica 不可用被迫回落（D10）。仅后者计入回落指标，
		// 用 HasAvailableReplica 区分，避免污染 proxy_read_fallback_master_total。
		if !s.readPool.HasAvailableReplica() {
			s.metrics.IncReadFallback(s.cfg.Group)
		}
		return s.masterPool
	}

	if pool, ok := s.replicaPools[entry.Addr]; ok {
		return pool
	}

	// 不应发生（replicaPools 由 proxysvc 预建）
	slog.Warn("replica pool not found, falling back to master", "addr", entry.Addr)
	s.metrics.IncReadFallback(s.cfg.Group)
	return s.masterPool
}

// rwSplitEnabled 返回是否启用了读写分离。
func (s *Session) rwSplitEnabled() bool {
	return len(s.cfg.ReplicaAddrs) > 0
}

// slowCommandThreshold 是慢命令阈值：单次后端 round-trip 超过此值即记入
// proxy_slow_command_total。10ms 对 Redis O(1) 命令已属明显偏慢。
const slowCommandThreshold = 10 * time.Millisecond

// firstArg 返回 argv 的首元素（命令名），argv 为空时返回 nil，避免越界。
// RESP 允许 *0 空数组或空 inline，此时 argv 为空但无错误。
func firstArg(argv [][]byte) []byte {
	if len(argv) == 0 {
		return nil
	}
	return argv[0]
}

// kindOf 把路由分类映射为延迟指标的 kind 标签（read/write/other）。
func kindOf(c route.Class) string {
	switch c {
	case route.ClassRead:
		return "read"
	case route.ClassWrite:
		return "write"
	default:
		return "other"
	}
}

// observeCommand 记录一次后端 round-trip 的延迟，并在超阈值时按命令记慢命令。
// firstArg 为该次转发的首条命令名（argv[0]，pipeline 批取批首）；nil 时跳过慢命令归一化。
func (s *Session) observeCommand(start time.Time, kind string, firstArg []byte) {
	elapsed := time.Since(start)
	s.metrics.ObserveCommandDuration(s.cfg.Group, kind, elapsed.Seconds())
	if elapsed >= slowCommandThreshold && len(firstArg) > 0 {
		s.metrics.IncSlowCommand(s.cfg.Group, route.NormalizeCommand(firstArg))
	}
}

// forwardAndRelay 转发命令到后端，读取响应，转发给客户端。
// EXCLUSIVE 模式用 exclusiveConn 直连；SHARED 模式用 pool.RoundTrip（FIFO 解复用）。
// d/firstArg 供延迟与慢命令指标使用（EXCLUSIVE 路径由 forwardExclusive 记录）。
func (s *Session) forwardAndRelay(raw []byte, pool *backend.Pool, d route.Decision, firstArg []byte) error {
	if s.exclusiveConn != nil {
		return s.forwardExclusive(raw, d, firstArg)
	}
	if pool == nil {
		return errors.New("no backend pool available")
	}
	start := time.Now()
	resp, err := pool.RoundTrip(raw)
	if err != nil {
		return err
	}
	s.observeCommand(start, kindOf(d.Class), firstArg)
	return s.clientWriter.WriteRaw(resp)
}

// forwardBatchAndRelay forwards a batch of commands to the same pool,
// collecting all responses before writing them to the client in one flush.
// Enables pipelining: all commands are sent before any response is read.
// d/firstArg 供延迟与慢命令指标使用；整批记为单次观测。
func (s *Session) forwardBatchAndRelay(batch [][]byte, pool *backend.Pool, d route.Decision, firstArg []byte) error {
	// EXCLUSIVE mode: send all, then recv all (pipeline on exclusive conn).
	if s.exclusiveConn != nil {
		w := s.exclusiveConn.Writer()
		for _, raw := range batch {
			if err := w.WriteRawNoFlush(raw); err != nil {
				return err
			}
		}
		if err := w.Flush(); err != nil {
			return err
		}
		for range batch {
			resp, err := s.exclusiveConn.Recv()
			if err != nil {
				return err
			}
			if err := s.clientWriter.WriteRawNoFlush(resp); err != nil {
				return err
			}
		}
		return s.clientWriter.Flush()
	}

	if pool == nil {
		return errors.New("no backend pool available")
	}

	start := time.Now()
	resps, err := pool.RoundTripBatch(batch)
	if err != nil {
		return err
	}
	s.observeCommand(start, kindOf(d.Class), firstArg)
	for _, resp := range resps {
		if err := s.clientWriter.WriteRawNoFlush(resp); err != nil {
			return err
		}
	}
	return s.clientWriter.Flush()
}

// forwardExclusive 在独占连接上直接转发（不走 FIFO）。
// d/firstArg 供延迟与慢命令指标使用。
func (s *Session) forwardExclusive(raw []byte, d route.Decision, firstArg []byte) error {
	if err := s.exclusiveConn.Writer().WriteRaw(raw); err != nil {
		return err
	}
	start := time.Now()
	resp, err := s.exclusiveConn.Recv()
	if err != nil {
		return err
	}
	s.observeCommand(start, kindOf(d.Class), firstArg)
	return s.clientWriter.WriteRaw(resp)
}

// relay 进入双向中继模式（SUBSCRIBE 后）。
func (s *Session) relay(target *backend.UpstreamConn) {
	done := make(chan struct{}, 2)

	go func() {
		io.Copy(target.Conn(), s.clientConn)
		done <- struct{}{}
	}()

	go func() {
		io.Copy(s.clientConn, target.Conn())
		done <- struct{}{}
	}()

	<-done
}

// handleConnState 处理连接状态命令。
func (s *Session) handleConnState(argv [][]byte, raw []byte, d route.Decision) error {
	cmd := strings.ToUpper(string(argv[0]))

	switch cmd {
	case "AUTH":
		return s.handleAuth(argv)

	case "PING":
		if len(argv) > 1 {
			return s.clientWriter.WriteBulkString(string(argv[1]))
		}
		return s.clientWriter.WriteSimpleString("PONG")

	case "ECHO":
		if len(argv) > 1 {
			return s.clientWriter.WriteBulkString(string(argv[1]))
		}
		return s.clientWriter.WriteError("ERR wrong number of arguments for 'echo' command")

	case "HELLO":
		return s.handleHello(argv)

	case "QUIT":
		s.clientWriter.WriteSimpleString("OK")
		return ErrQuit

	case "RESET":
		if s.exclusiveConn != nil {
			s.exclusiveConn.Close()
			s.metrics.DecExclusive(s.cfg.Group)
			s.exclusiveConn = nil
		}
		s.mode = ModeNormal
		s.pinnedBySelect = false
		s.authed = s.cfg.Password == ""
		return s.clientWriter.WriteSimpleString("RESET")

	case "CLUSTER":
		return s.handleCluster(argv)

	case "SELECT":
		// SELECT：获取独占连接（永久 pin，§5.8），转发到 master
		if s.exclusiveConn == nil {
			conn, err := s.masterPool.AcquireExclusive()
			if err != nil {
				return err
			}
			s.exclusiveConn = conn
			s.metrics.IncExclusive(s.cfg.Group)
		}
		s.mode = ModeExclusive
		s.pinnedBySelect = true
		return s.forwardExclusive(raw, d, argv[0])

	default:
		if cmd == "CLIENT" {
			return s.handleClient(argv, raw)
		}
		return s.forwardAndRelay(raw, s.masterPool, d, argv[0])
	}
}

// handleAuth 校验密码（D7：proxy 终结，不透传）。
func (s *Session) handleAuth(argv [][]byte) error {
	if s.cfg.Password == "" {
		s.authed = true
		return s.clientWriter.WriteSimpleString("OK")
	}
	if len(argv) == 2 {
		if string(argv[1]) == s.cfg.Password {
			s.authed = true
			return s.clientWriter.WriteSimpleString("OK")
		}
		return s.clientWriter.WriteError("ERR invalid password")
	}
	if len(argv) == 3 {
		return s.clientWriter.WriteError("ERR single parameter AUTH only")
	}
	return s.clientWriter.WriteError("ERR wrong number of arguments for 'auth' command")
}

// handleHello 处理 HELLO 命令（D8：拒绝 RESP3）。
func (s *Session) handleHello(argv [][]byte) error {
	if len(argv) >= 2 {
		ver := strings.ToUpper(string(argv[1]))
		if ver == "3" {
			return s.clientWriter.WriteError("NOPROTO sorry, this protocol version is not supported.")
		}
	}
	w := s.clientWriter
	w.WriteArrayHeader(14)
	w.WriteRaw([]byte("$6\r\nserver\r\n"))
	w.WriteRaw([]byte("$5\r\nredis\r\n"))
	w.WriteRaw([]byte("$7\r\nversion\r\n"))
	w.WriteRaw([]byte("$3\r\n7.0\r\n"))
	w.WriteRaw([]byte("$5\r\nproto\r\n"))
	w.WriteInteger(2)
	w.WriteRaw([]byte("$2\r\nid\r\n"))
	w.WriteInteger(1)
	w.WriteRaw([]byte("$4\r\nmode\r\n"))
	w.WriteRaw([]byte("$10\r\nstandalone\r\n"))
	w.WriteRaw([]byte("$4\r\nrole\r\n"))
	w.WriteRaw([]byte("$6\r\nmaster\r\n"))
	w.WriteRaw([]byte("$7\r\nmodules\r\n"))
	w.WriteRaw([]byte("*0\r\n"))
	return w.Flush()
}

// handleClient 处理 CLIENT 子命令。
func (s *Session) handleClient(argv [][]byte, raw []byte) error {
	if len(argv) < 2 {
		return s.clientWriter.WriteError("ERR wrong number of arguments for 'client' command")
	}
	sub := strings.ToUpper(string(argv[1]))

	switch sub {
	case "GETNAME":
		return s.clientWriter.WriteBulkString("")
	case "SETNAME":
		return s.clientWriter.WriteSimpleString("OK")
	case "ID":
		return s.clientWriter.WriteInteger(1)
	case "INFO":
		return s.clientWriter.WriteBulkString("id=1 addr=proxy")
	case "SETINFO":
		return s.clientWriter.WriteSimpleString("OK")
	case "LIST":
		return s.forwardAndRelay(raw, s.masterPool, route.Classify(argv), argv[0])
	default:
		return s.clientWriter.WriteError("ERR command not allowed: CLIENT " + sub)
	}
}

// handleCluster 处理 CLUSTER 子命令（本地应答，不透传后端）。
// 支持 INFO / NODES / SLOTS / SHARDS / COUNTKEYSINSLOT / KEYSINSLOT / COUNT-FAILURE-REPORTS。
// 危险子命令（FAILOVER / FORGET / MEET / RESET / SET-CONFIG-EPOCH / BUMPEPOCH）直接拒绝。
func (s *Session) handleCluster(argv [][]byte) error {
	if len(argv) < 2 {
		return s.clientWriter.WriteError("ERR wrong number of arguments for 'cluster' command")
	}
	sub := strings.ToUpper(string(argv[1]))

	switch sub {
	case "INFO":
		info := "cluster_enabled:0\r\ncluster_state:ok\r\ncluster_slots_assigned:0\r\n" +
			"cluster_slots_ok:0\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n" +
			"cluster_known_nodes:1\r\ncluster_size:0\r\n" +
			"cluster_current_epoch:0\r\ncluster_my_epoch:0\r\n" +
			"cluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n"
		return s.clientWriter.WriteBulkString(info)

	case "NODES":
		// 返回单节点视图
		return s.clientWriter.WriteBulkString(
			s.cfg.MasterAddr + " myself,master - 0 0 0 connected 0-16383\n")

	case "SLOTS", "SHARDS":
		// 非集群模式返回空数组
		return s.clientWriter.WriteArrayHeader(0)

	case "COUNTKEYSINSLOT":
		return s.clientWriter.WriteInteger(0)

	case "KEYSINSLOT":
		return s.clientWriter.WriteArrayHeader(0)

	case "COUNT-FAILURE-REPORTS":
		return s.clientWriter.WriteInteger(0)

	case "MYID":
		return s.clientWriter.WriteBulkString(strings.Repeat("0", 40))

	default:
		return s.clientWriter.WriteError("ERR CLUSTER " + sub + " not allowed")
	}
}

// isPreAuthCommand 判断命令是否在未认证状态下允许。
func isPreAuthCommand(argv [][]byte) bool {
	if len(argv) == 0 {
		return false
	}
	switch strings.ToUpper(string(argv[0])) {
	case "AUTH", "PING", "HELLO", "QUIT", "RESET":
		return true
	}
	return false
}

// cleanup 关闭客户端连接，销毁独占连接。
// 独占连接到达 cleanup 说明要么 SELECT 改了 DB、要么出错中断（MULTI 未提交 / BLPOP 失败 / SUBSCRIBE relay 结束），
// 连接状态不可控，一律 Close 不还池，避免串库和坏连接复用。
// Pool 由 proxysvc 管理，不在此关闭。
func (s *Session) cleanup() {
	s.clientConn.Close()
	if s.exclusiveConn != nil {
		s.exclusiveConn.Close()
		s.metrics.DecExclusive(s.cfg.Group)
		s.exclusiveConn = nil
	}
}

// Close 外部关闭 session（用于 failover 等场景，§12）。
func (s *Session) Close() {
	s.clientConn.Close()
	if s.exclusiveConn != nil {
		s.exclusiveConn.Close()
		s.metrics.DecExclusive(s.cfg.Group)
		s.exclusiveConn = nil
	}
}
