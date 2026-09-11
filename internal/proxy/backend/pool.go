package backend

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Pool 管理到一个 Backend 的共享连接集合。
//
// SHARED 连接：带 readLoop 的 UpstreamConn，多个 session 复用。
// RoundTrip(raw) 处理"选连接 -> 发命令 -> 等响应"全流程。
//
// EXCLUSIVE 连接：通过 AcquireExclusive 获取，直接读写，用完 ReleaseExclusive 归还。
//
// 池上限 maxSize 限制 SHARED 连接数（§13）；超限时复用已有连接（FIFO 排队）。
type Pool struct {
	addr           string
	password       string
	connectTimeout time.Duration
	maxSize        int
	idleTimeout    time.Duration

	mu          sync.Mutex
	shared      []*UpstreamConn // SHARED 连接（readLoop 运行中）
	idle        []*UpstreamConn // 空闲连接（EXCLUSIVE 归还，无 readLoop）
	totalShared int
	stopCh      chan struct{} // 停止驱逐 goroutine
}

// NewPool 创建连接池。
func NewPool(addr, password string, connectTimeout time.Duration, maxSize int, idleTimeout time.Duration) *Pool {
	if maxSize <= 0 {
		maxSize = 128
	}
	if idleTimeout <= 0 {
		idleTimeout = 5 * time.Minute
	}
	p := &Pool{
		addr:           addr,
		password:       password,
		connectTimeout: connectTimeout,
		maxSize:        maxSize,
		idleTimeout:    idleTimeout,
		stopCh:         make(chan struct{}),
	}
	p.startEvictor()
	return p
}

func (p *Pool) startEvictor() {
	interval := p.idleTimeout / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-ticker.C:
				p.evictIdle()
			}
		}
	}()
}

// RoundTrip 发送命令并等待响应（SHARED 模式，§5.3/§5.4）。
func (p *Pool) RoundTrip(raw []byte) ([]byte, error) {
	conn := p.getSharedConn()
	if conn == nil {
		return nil, ErrPoolExhausted
	}
	ch := conn.Send(raw)
	resp := <-ch
	chanPool.Put(ch)
	if resp.err != nil {
		// 连接可能已断开，清理
		p.removeClosedConns()
	}
	return resp.data, resp.err
}

// RoundTripBatch sends multiple commands as a pipeline and collects
// responses in order. All commands are sent before any response is read,
// enabling backend-side pipelining.
func (p *Pool) RoundTripBatch(raws [][]byte) ([][]byte, error) {
	conn := p.getSharedConn()
	if conn == nil {
		return nil, ErrPoolExhausted
	}
	channels := conn.SendBatch(raws)

	resps := make([][]byte, len(raws))
	for i, ch := range channels {
		resp := <-ch
		chanPool.Put(ch)
		if resp.err != nil {
			p.removeClosedConns()
			// Drain remaining channels: those with data can be returned to pool;
			// those still in inflight (no data yet) will be GC'd safely.
			for j := i + 1; j < len(channels); j++ {
				select {
				case <-channels[j]:
					chanPool.Put(channels[j])
				default:
					// Still in inflight — leak is safe, readLoop will send and GC.
				}
			}
			return nil, resp.err
		}
		resps[i] = resp.data
	}
	return resps, nil
}

// AcquireExclusive 取一条连接用于独占模式（§7.1）。
// 优先从空闲池取，否则新建。
func (p *Pool) AcquireExclusive() (*UpstreamConn, error) {
	p.mu.Lock()

	// 优先复用空闲连接
	if len(p.idle) > 0 {
		conn := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.mu.Unlock()
		// Idle exclusive connections have no readLoop to notice a backend
		// restart. Probe before sending ANY business command; never replay a
		// blocking pop/transaction after an ambiguous transport failure.
		timeout := p.connectTimeout
		if timeout <= 0 {
			timeout = 3 * time.Second
		}
		conn.Conn().SetDeadline(time.Now().Add(timeout))
		err := conn.Writer().WriteCommand([][]byte{[]byte("PING")})
		var reply []byte
		if err == nil {
			reply, err = conn.Recv()
		}
		conn.Conn().SetDeadline(time.Time{})
		if err == nil && string(reply) == "+PONG\r\n" {
			return conn, nil
		}
		conn.Close()
		return p.dial()
	}
	p.mu.Unlock()

	// 新建
	return p.dial()
}

// ReleaseExclusive 归还独占连接到空闲池（§7.2）。
func (p *Pool) ReleaseExclusive(conn *UpstreamConn) {
	if conn.IsClosed() {
		return // 连接已断开，不回收
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	conn.markUsed()
	p.idle = append(p.idle, conn)
}

// Addr 返回后端地址。
func (p *Pool) Addr() string { return p.addr }

// Close 关闭所有连接并停止驱逐 goroutine。
func (p *Pool) Close() {
	close(p.stopCh)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.shared {
		c.Close()
	}
	for _, c := range p.idle {
		c.Close()
	}
	p.shared = nil
	p.idle = nil
	p.totalShared = 0
}

// SharedConnCount 返回当前 SHARED 连接数（用于指标）。
func (p *Pool) SharedConnCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalShared
}

// getSharedConn 获取一条可用的 SHARED 连接，必要时新建。
func (p *Pool) getSharedConn() *UpstreamConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 清理已断开的连接
	var alive []*UpstreamConn
	for _, c := range p.shared {
		if c.IsClosed() {
			p.totalShared--
		} else {
			alive = append(alive, c)
		}
	}
	p.shared = alive

	// 有可用连接，选 inflight 最少的
	if len(p.shared) > 0 {
		best := p.shared[0]
		bestN := best.inflightCount()
		for _, c := range p.shared[1:] {
			n := c.inflightCount()
			if n < bestN {
				best = c
				bestN = n
			}
		}
		return best
	}

	// 无可用连接，尝试新建
	if p.totalShared < p.maxSize {
		conn, err := p.dialLocked()
		if err != nil {
			slog.Warn("pool dial failed", "addr", p.addr, "pool_size", p.totalShared, "max", p.maxSize, "err", err)
			return nil
		}
		conn.StartReadLoop()
		p.shared = append(p.shared, conn)
		p.totalShared++
		return conn
	}

	// 达到上限，返回第一个（FIFO 排队）
	if len(p.shared) > 0 {
		return p.shared[0]
	}
	return nil
}

// removeClosedConns 清理已断开的 SHARED 连接。
func (p *Pool) removeClosedConns() {
	p.mu.Lock()
	defer p.mu.Unlock()
	var alive []*UpstreamConn
	for _, c := range p.shared {
		if c.IsClosed() {
			p.totalShared--
		} else {
			alive = append(alive, c)
		}
	}
	p.shared = alive
}

// evictIdle 驱逐空闲连接（§13）。
// 扫描 idle 和 shared 两个池，关闭超过 idleTimeout 未使用的连接。
// shared 连接额外要求 inflight == 0（不能关有未完成请求的连接）。
func (p *Pool) evictIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()

	// 驱逐 idle（EXCLUSIVE 归还的连接）
	var aliveIdle []*UpstreamConn
	for _, c := range p.idle {
		if now.Sub(c.lastUsed()) > p.idleTimeout {
			c.Close()
		} else {
			aliveIdle = append(aliveIdle, c)
		}
	}
	p.idle = aliveIdle

	// 驱逐 shared（无 inflight 且超时）
	var aliveShared []*UpstreamConn
	for _, c := range p.shared {
		if c.IsClosed() {
			p.totalShared--
			continue
		}
		if c.inflightCount() == 0 && now.Sub(c.lastUsed()) > p.idleTimeout {
			c.Close()
			p.totalShared--
		} else {
			aliveShared = append(aliveShared, c)
		}
	}
	p.shared = aliveShared
}

// dial 新建一条连接并 AUTH（不加锁，调用方自行加锁或在 AcquireExclusive 中调用）。
func (p *Pool) dial() (*UpstreamConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dialLocked()
}

// dialLocked 新建连接（调用方已持有锁）。
func (p *Pool) dialLocked() (*UpstreamConn, error) {
	conn, err := net.DialTimeout("tcp", p.addr, p.connectTimeout)
	if err != nil {
		return nil, err
	}

	// TCP keepalive 60s（§15）
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(60 * time.Second)
	}

	uc := newUpstreamConn(conn, p.addr)

	// AUTH（D7：proxy 维护后端鉴权）
	if p.password != "" {
		if err := uc.writer.WriteCommand([][]byte{[]byte("AUTH"), []byte(p.password)}); err != nil {
			conn.Close()
			return nil, err
		}
		raw, err := uc.reader.ReadFrame()
		if err != nil {
			conn.Close()
			return nil, err
		}
		if len(raw) == 0 || raw[0] != '+' {
			conn.Close()
			return nil, &AuthError{Raw: raw}
		}
	}

	return uc, nil
}

// inflightCount 返回当前 inflight 请求数。
func (c *UpstreamConn) inflightCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inflight)
}

// ErrPoolExhausted 池连接耗尽。
var ErrPoolExhausted = errors.New("backend pool exhausted")
