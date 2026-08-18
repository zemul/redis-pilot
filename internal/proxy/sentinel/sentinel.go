package sentinel

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/discovery"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/resp"
)

// TopoFunc 返回当前拓扑快照（线程安全）。由 discovery.Manager.Topology 提供。
type TopoFunc func() *discovery.Topology

// Config 是伪 Sentinel 的配置。
type Config struct {
	Listen          string // 监听地址，如 ":26379"；空 = 不启用
	AnnounceIP      string // 返回给客户端的 host；空 = 用连接命中的本地 IP
	Password        string // 发现端口鉴权密码；空 = 不鉴权
	MaxRequestBytes int    // 单命令上限
}

// Server 是伪 Sentinel 协议服务器。
type Server struct {
	cfg  Config
	topo TopoFunc

	mu      sync.Mutex
	ln      net.Listener // 受 mu 保护
	stopped bool         // 受 mu 保护：Stop 已调用
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup
}

// New 创建伪 Sentinel 服务器。
func New(cfg Config, topo TopoFunc) *Server {
	return &Server{cfg: cfg, topo: topo, conns: make(map[net.Conn]struct{})}
}

// Serve 在已就绪的 listener 上 accept；阻塞直到 listener 关闭。
func (s *Server) Serve(ln net.Listener) {
	// 记录 listener 供 Stop 关闭。若 Stop 已先一步调用，直接关掉 ln 并退出，
	// 避免在已停止的 server 上继续 accept。
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		ln.Close()
		return
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener 关闭 -> 退出
		}
		if !s.trackConn(conn) {
			// Stop 已介入：拒绝这条晚到的连接，避免 wg 永远等不到它退出。
			conn.Close()
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrackConn(conn)
			s.handleConn(conn)
		}()
	}
}

// Stop 关闭 listener 并断开所有在连接，然后等待处理 goroutine 退出。
func (s *Server) Stop() {
	s.mu.Lock()
	s.stopped = true
	ln := s.ln
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()

	if ln != nil {
		ln.Close()
	}
	s.wg.Wait()
}

// trackConn 登记连接。若 server 已 Stop，返回 false 表示不应继续服务该连接。
func (s *Server) trackConn(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrackConn(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// handleConn 处理单个伪 Sentinel 连接的请求循环。
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	r := resp.NewReader(conn)
	if s.cfg.MaxRequestBytes > 0 {
		r.SetMaxBytes(s.cfg.MaxRequestBytes)
	}
	w := resp.NewWriter(conn)

	authed := s.cfg.Password == "" // 无密码 = 默认已认证

	for {
		_, argv, err := r.ReadCommand()
		if err != nil {
			return // EOF / 协议错误 / 超限 -> 断连
		}
		if len(argv) == 0 {
			continue
		}
		cmd := strings.ToUpper(string(argv[0]))

		// 未认证时只放行 AUTH/PING/HELLO/QUIT
		if !authed && cmd != "AUTH" && cmd != "PING" && cmd != "HELLO" && cmd != "QUIT" {
			_ = w.WriteError("NOAUTH Authentication required.")
			continue
		}

		switch cmd {
		case "AUTH":
			authed = s.handleAuth(w, argv)
		case "PING":
			_ = w.WriteSimpleString("PONG")
		case "QUIT":
			_ = w.WriteSimpleString("OK")
			return
		case "HELLO":
			// D8：不应答 RESP3，逼客户端回落 RESP2
			_ = w.WriteError("NOPROTO unsupported protocol version")
		case "INFO":
			_ = w.WriteBulkString(minimalInfo())
		case "COMMAND":
			_ = w.WriteRaw(emptyArray) // best-effort：*0\r\n
		case "CLIENT":
			_ = w.WriteSimpleString("OK") // setname/reply 等兜底
		case "SUBSCRIBE", "PSUBSCRIBE":
			s.handleSubscribe(w, cmd, argv)
		case "UNSUBSCRIBE", "PUNSUBSCRIBE":
			s.handleUnsubscribe(w, cmd, argv)
		case "SENTINEL":
			s.handleSentinel(conn, w, argv)
		default:
			_ = w.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmd))
		}
	}
}

// handleAuth 处理 AUTH 命令。
func (s *Server) handleAuth(w *resp.Writer, argv [][]byte) bool {
	if s.cfg.Password == "" {
		// 对齐 redis：无密码时 AUTH 报错
		_ = w.WriteError("ERR Client sent AUTH, but no password is set")
		return true // 仍视为已认证（无密码）
	}
	if len(argv) == 2 && string(argv[1]) == s.cfg.Password {
		_ = w.WriteSimpleString("OK")
		return true
	}
	if len(argv) == 3 {
		_ = w.WriteError("ERR single parameter AUTH only") // v1 不支持 ACL 双参
		return false
	}
	_ = w.WriteError("ERR invalid password")
	return false
}

// handleSentinel 分发 SENTINEL 子命令。
func (s *Server) handleSentinel(conn net.Conn, w *resp.Writer, argv [][]byte) {
	if len(argv) < 2 {
		_ = w.WriteError("ERR wrong number of arguments for 'sentinel'")
		return
	}
	sub := strings.ToLower(string(argv[1]))
	switch sub {
	case "get-master-addr-by-name":
		s.handleGetMasterAddr(conn, w, argv)
	case "master":
		s.handleMaster(conn, w, argv)
	case "masters":
		s.handleMasters(conn, w)
	case "slaves", "replicas":
		_ = w.WriteRaw(emptyArray) // 隐藏后端拓扑，流量收敛到 proxy
	case "sentinels":
		_ = w.WriteRaw(emptyArray) // v1 不通告 peer
	case "failover", "monitor", "remove", "set", "reset", "ckquorum", "flushconfig":
		_ = w.WriteError("ERR unsupported: pseudo-sentinel is read-only discovery")
	default:
		_ = w.WriteError(fmt.Sprintf("ERR unknown sentinel subcommand '%s'", sub))
	}
}

// resolve 返回该 group 对客户端暴露的入口地址。
// resolve 返回 group 的伪 Sentinel 发现地址。
// 端口按 access_mode 选择：standard → MasterPort，rw-split/空 → AutoPort。
// 若首选端口为 0 则兜底到另一个存在的端口。
// ip 优先配置的 AnnounceIP，否则用连接命中的本地 IP。
func (s *Server) resolve(group string, conn net.Conn) (ip string, port int, ok bool) {
	topo := s.topo()
	if topo == nil {
		return "", 0, false
	}
	g := topo.Groups[group]
	if g == nil {
		return "", 0, false
	}
	switch g.AccessMode {
	case "standard":
		port = g.MasterPort
		if port == 0 {
			port = g.AutoPort
		}
	default: // rw-split 或空
		port = g.AutoPort
		if port == 0 {
			port = g.MasterPort
		}
	}
	if port == 0 {
		return "", 0, false
	}
	ip = s.cfg.AnnounceIP
	if ip == "" {
		ip = localIP(conn)
	}
	return ip, port, true
}

// handleGetMasterAddr 处理 SENTINEL get-master-addr-by-name <group>。
func (s *Server) handleGetMasterAddr(conn net.Conn, w *resp.Writer, argv [][]byte) {
	if len(argv) != 3 {
		_ = w.WriteError("ERR wrong number of arguments")
		return
	}
	group := string(argv[2])
	ip, port, ok := s.resolve(group, conn)
	if !ok {
		_ = w.WriteRaw(nullArray) // *-1\r\n
		slog.Debug("sentinel get-master-addr-by-name: group not found", "group", group)
		return
	}
	_ = w.WriteRaw(encodeStringArray([]string{ip, strconv.Itoa(port)}))
	slog.Debug("sentinel get-master-addr-by-name", "group", group, "ip", ip, "port", port)
}

// handleMaster 处理 SENTINEL master <group>。
func (s *Server) handleMaster(conn net.Conn, w *resp.Writer, argv [][]byte) {
	if len(argv) != 3 {
		_ = w.WriteError("ERR wrong number of arguments")
		return
	}
	group := string(argv[2])
	ip, port, ok := s.resolve(group, conn)
	if !ok {
		_ = w.WriteError("ERR No such master with that name")
		return
	}
	_ = w.WriteRaw(encodeMasterInfo(group, ip, port))
}

// handleMasters 处理 SENTINEL masters。
func (s *Server) handleMasters(conn net.Conn, w *resp.Writer) {
	topo := s.topo()
	if topo == nil {
		_ = w.WriteRaw(emptyArray)
		return
	}
	var buf []byte
	n := 0
	for name := range topo.Groups {
		ip, port, ok := s.resolve(name, conn)
		if !ok {
			continue
		}
		buf = append(buf, encodeMasterInfo(name, ip, port)...)
		n++
	}
	out := encodeArrayHeader(n)
	out = append(out, buf...)
	_ = w.WriteRaw(out)
}

// handleSubscribe 处理 SUBSCRIBE/PSUBSCRIBE。
// v1 仅应答确认帧，不推送事件（Proxy 地址稳定，failover 对客户端透明）。
func (s *Server) handleSubscribe(w *resp.Writer, cmd string, argv [][]byte) {
	kind := "subscribe"
	if cmd == "PSUBSCRIBE" {
		kind = "psubscribe"
	}
	for i, ch := range argv[1:] {
		out := encodeSubscribeReply(kind, string(ch), i+1)
		_ = w.WriteRaw(out)
	}
}

// handleUnsubscribe 处理 UNSUBSCRIBE/PUNSUBSCRIBE。
func (s *Server) handleUnsubscribe(w *resp.Writer, cmd string, argv [][]byte) {
	kind := "unsubscribe"
	if cmd == "PUNSUBSCRIBE" {
		kind = "punsubscribe"
	}
	chans := argv[1:]
	if len(chans) == 0 {
		_ = w.WriteRaw(encodeUnsubscribeReply(kind, "", 0))
		return
	}
	for _, ch := range chans {
		_ = w.WriteRaw(encodeUnsubscribeReply(kind, string(ch), 0))
	}
}

// localIP 取连接命中的本地 IP，作为 AnnounceIP 兜底。
func localIP(conn net.Conn) string {
	if la, ok := conn.LocalAddr().(*net.TCPAddr); ok {
		return la.IP.String()
	}
	return "127.0.0.1"
}
