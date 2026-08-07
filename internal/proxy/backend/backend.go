// Package backend 管理到 Redis 后端的连接。
//
// M2 是退化版：每 client 一条上游连接（1:1），不收敛。但接口按终态设计，
// M5 在同一接口上叠加池化 + FIFO 解复用。
package backend

import (
	"net"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/resp"
)

// Backend 是一条到 Redis 实例的连接。
type Backend struct {
	conn   net.Conn
	reader *resp.Reader
	writer *resp.Writer
	addr   string
}

// Dial 连接到 Redis 后端。
// connectTimeout 控制建连超时（§15: 3s）。
// password 非空时自动 AUTH。
func Dial(addr, password string, connectTimeout time.Duration) (*Backend, error) {
	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, err
	}

	// TCP keepalive 60s（§15），不能依赖 OS 默认的 2 小时
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(60 * time.Second)
	}

	b := &Backend{
		conn:   conn,
		reader: resp.NewReader(conn),
		writer: resp.NewWriter(conn),
		addr:   addr,
	}

	if password != "" {
		// AUTH 不透传，proxy 自己维护后端鉴权（D7）
		if err := b.sendRaw("AUTH", password); err != nil {
			conn.Close()
			return nil, err
		}
		raw, err := b.reader.ReadFrame()
		if err != nil {
			conn.Close()
			return nil, err
		}
		if len(raw) == 0 || raw[0] != '+' {
			conn.Close()
			return nil, &AuthError{Raw: raw}
		}
	}

	return b, nil
}

// Send 发送一条命令到后端。
func (b *Backend) Send(argv [][]byte) error {
	return b.writer.WriteCommand(argv)
}

// sendRaw 发送一条命令（参数为 string，内部转 []byte）。
func (b *Backend) sendRaw(args ...string) error {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	return b.writer.WriteCommand(argv)
}

// Recv 读取一帧完整响应（原始字节，不解释内容）。
// 阻塞直到读到一帧或连接断开。不设 read 超时（§15 红线：EXCLUSIVE/阻塞连接不设超时）。
func (b *Backend) Recv() ([]byte, error) {
	return b.reader.ReadFrame()
}

// Reader 暴露给需要直接读帧的场景（如 subscribe relay）。
func (b *Backend) Reader() *resp.Reader { return b.reader }

// Writer 暴露给需要直接写的场景。
func (b *Backend) Writer() *resp.Writer { return b.writer }

// Conn 暴露底层连接（用于 io.Copy 等）。
func (b *Backend) Conn() net.Conn { return b.conn }

// Close 关闭后端连接。
func (b *Backend) Close() error {
	return b.conn.Close()
}

// Addr 返回后端地址。
func (b *Backend) Addr() string { return b.addr }

// AuthError 表示后端 AUTH 失败。
type AuthError struct{ Raw []byte }

func (e *AuthError) Error() string {
	return "backend AUTH failed: " + string(e.Raw)
}
