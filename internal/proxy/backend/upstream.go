package backend

import (
	"net"
	"sync"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/resp"
)

// response 是 FIFO 解复用的响应传递单元。
type response struct {
	data []byte
	err  error
}

// chanPool 复用 chan response，避免每条命令分配 channel（高吞吐下 GC 压力大）。
// 使用约定：Send/SendBatch 从 pool Get，RoundTrip/RoundTripBatch 收到响应后 Put。
var chanPool = sync.Pool{
	New: func() any { return make(chan response, 1) },
}

// UpstreamConn 是一条 proxy->redis 的真实连接。
//
// 两种使用模式：
//   - SHARED：多个 session 共享。Send(raw) 写入命令并压 FIFO，readLoop 读响应弹队头分发。
//   - EXCLUSIVE：单个 session 独占。直接用 Writer()/Recv()/Conn()，不经过 FIFO。
//
// §5.4 物理定律：单条 redis 连接上响应严格按请求顺序返回，所以 FIFO 弹队头即得属主。
type UpstreamConn struct {
	conn   net.Conn
	reader *resp.Reader
	writer *resp.Writer
	addr   string

	// SHARED 模式：FIFO + readLoop
	inflight   []chan response // 队头是最早未完成请求
	mu         sync.Mutex      // 保护 writer + inflight + lastUsedAt + closed
	closed     bool
	lastUsedAt time.Time // 最后活跃时间（用于空闲驱逐）
}

// newUpstreamConn 创建一条上游连接（不启动 readLoop）。
func newUpstreamConn(conn net.Conn, addr string) *UpstreamConn {
	return &UpstreamConn{
		conn:       conn,
		reader:     resp.NewReader(conn),
		writer:     resp.NewWriter(conn),
		addr:       addr,
		lastUsedAt: time.Now(),
	}
}

// Send 写入命令并返回响应通道（SHARED 模式）。
// 调用方在通道上等待响应，收到后应调用 chanPool.Put(ch) 归还。
func (c *UpstreamConn) Send(raw []byte) chan response {
	ch := chanPool.Get().(chan response)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		ch <- response{err: errConnClosed}
		return ch
	}

	if err := c.writer.WriteRaw(raw); err != nil {
		c.mu.Unlock()
		ch <- response{err: err}
		return ch
	}

	c.lastUsedAt = time.Now()
	c.inflight = append(c.inflight, ch)
	c.mu.Unlock()

	return ch
}

// SendBatch writes multiple commands in a single flush and returns
// response channels in order. Enables pipelining: all commands are
// sent to the backend before waiting for any response.
// Caller must receive from all channels and return them via chanPool.Put.
func (c *UpstreamConn) SendBatch(raws [][]byte) []chan response {
	channels := make([]chan response, len(raws))

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		for i := range channels {
			ch := chanPool.Get().(chan response)
			ch <- response{err: errConnClosed}
			channels[i] = ch
		}
		return channels
	}

	for i, raw := range raws {
		ch := chanPool.Get().(chan response)
		if err := c.writer.WriteRawNoFlush(raw); err != nil {
			ch <- response{err: err}
		} else {
			c.inflight = append(c.inflight, ch)
		}
		channels[i] = ch
	}

	c.writer.Flush()
	c.lastUsedAt = time.Now()

	return channels
}

// readLoop 持续读响应，弹 FIFO 队头分发（SHARED 模式）。
// 连接断开时通知所有 inflight 等待者（§7.4）。
func (c *UpstreamConn) readLoop() {
	for {
		data, err := c.reader.ReadFrame()

		c.mu.Lock()
		if err != nil {
			// 通知所有 inflight 等待者
			for _, ch := range c.inflight {
				ch <- response{err: err}
			}
			c.inflight = nil
			c.closed = true
			c.mu.Unlock()
			return
		}

		if len(c.inflight) == 0 {
			// 不应发生：收到响应但没有 inflight 请求
			c.mu.Unlock()
			continue
		}

		ch := c.inflight[0]
		c.inflight = c.inflight[1:]
		c.mu.Unlock()

		ch <- response{data: data}
	}
}

// --- EXCLUSIVE 模式：直接读写 ---

// Writer 暴露 writer（EXCLUSIVE 模式直接写）。
func (c *UpstreamConn) Writer() *resp.Writer { return c.writer }

// Recv 直接读一帧（EXCLUSIVE 模式）。
func (c *UpstreamConn) Recv() ([]byte, error) { return c.reader.ReadFrame() }

// Conn 暴露底层连接（用于 io.Copy 等）。
func (c *UpstreamConn) Conn() net.Conn { return c.conn }

// Close 关闭连接并标记 closed。
func (c *UpstreamConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.conn.Close()
}

// markUsed 更新最后活跃时间（用于空闲驱逐）。
func (c *UpstreamConn) markUsed() {
	c.mu.Lock()
	c.lastUsedAt = time.Now()
	c.mu.Unlock()
}

// lastUsed 返回最后活跃时间。
func (c *UpstreamConn) lastUsed() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastUsedAt
}

// Addr 返回后端地址。
func (c *UpstreamConn) Addr() string { return c.addr }

// IsClosed 返回连接是否已关闭。
func (c *UpstreamConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// StartReadLoop 启动 reader goroutine（SHARED 模式调用）。
func (c *UpstreamConn) StartReadLoop() {
	go c.readLoop()
}

var errConnClosed = &connClosedError{}

type connClosedError struct{}

func (e *connClosedError) Error() string { return "upstream connection closed" }
