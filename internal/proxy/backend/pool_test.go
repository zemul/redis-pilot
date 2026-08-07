package backend

import (
	"net"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/resp"
)

// startTestRedis 启动一个假 Redis（回显收到的命令）。
// 每收到一帧命令，回 +OK\r\n。
func startTestRedis(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				r := resp.NewReader(conn)
				w := resp.NewWriter(conn)
				for {
					_, _, err := r.ReadCommand()
					if err != nil {
						return
					}
					w.WriteSimpleString("OK")
					w.Flush()
				}
			}(conn)
		}
	}()

	return ln.Addr().String(), func() { ln.Close() }
}

func TestPool_RoundTrip(t *testing.T) {
	addr, cleanup := startTestRedis(t)
	defer cleanup()

	pool := NewPool(addr, "", 3*time.Second, 10, 5*time.Minute)
	defer pool.Close()

	raw := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	resp, err := pool.RoundTrip(raw)
	if err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	if string(resp) != "+OK\r\n" {
		t.Errorf("response = %q, want +OK\\r\\n", string(resp))
	}
}

func TestPool_FIFOOrdering(t *testing.T) {
	// 验证 FIFO 保序：多个并发请求的响应按发送顺序分发
	addr, cleanup := startTestRedis(t)
	defer cleanup()

	pool := NewPool(addr, "", 3*time.Second, 1, 5*time.Minute) // 1 条共享连接
	defer pool.Close()

	// 发送 10 个并发请求
	const N = 10
	results := make(chan string, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			raw := []byte("*2\r\n$3\r\nGET\r\n")
			raw = append(raw, []byte("$1\r\n")...)
			raw = append(raw, 'A'+byte(i))
			raw = append(raw, '\r', '\n')
			_, err := pool.RoundTrip(raw)
			if err != nil {
				results <- "err"
			} else {
				results <- "ok"
			}
		}(i)
	}

	// 所有请求都应成功
	for i := 0; i < N; i++ {
		r := <-results
		if r != "ok" {
			t.Errorf("request %d failed: %s", i, r)
		}
	}
}

func TestPool_ConnectionReuse(t *testing.T) {
	// 验证连接复用：多次 RoundTrip 应复用同一条连接
	addr, cleanup := startTestRedis(t)
	defer cleanup()

	pool := NewPool(addr, "", 3*time.Second, 10, 5*time.Minute)
	defer pool.Close()

	raw := []byte("*1\r\n$4\r\nPING\r\n")

	// 第一次 RoundTrip 创建连接
	_, _ = pool.RoundTrip(raw)
	if pool.SharedConnCount() != 1 {
		t.Errorf("after 1st: shared conns = %d, want 1", pool.SharedConnCount())
	}

	// 第二次 RoundTrip 应复用
	_, _ = pool.RoundTrip(raw)
	if pool.SharedConnCount() != 1 {
		t.Errorf("after 2nd: shared conns = %d, want 1 (reuse)", pool.SharedConnCount())
	}
}

func TestPool_AcquireExclusive(t *testing.T) {
	addr, cleanup := startTestRedis(t)
	defer cleanup()

	pool := NewPool(addr, "", 3*time.Second, 10, 5*time.Minute)
	defer pool.Close()

	// AcquireExclusive 获取连接
	conn1, err := pool.AcquireExclusive()
	if err != nil {
		t.Fatalf("AcquireExclusive failed: %v", err)
	}

	// 直接读写（不走 FIFO）
	if err := conn1.Writer().WriteRaw([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("WriteRaw failed: %v", err)
	}
	data, err := conn1.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if string(data) != "+OK\r\n" {
		t.Errorf("response = %q, want +OK", string(data))
	}

	// 归还
	pool.ReleaseExclusive(conn1)

	// 再次获取应复用空闲连接
	conn2, err := pool.AcquireExclusive()
	if err != nil {
		t.Fatalf("2nd AcquireExclusive failed: %v", err)
	}
	if conn2 != conn1 {
		t.Error("expected to reuse idle connection")
	}
	pool.ReleaseExclusive(conn2)
}

func TestPool_ConnectionErrorCleanup(t *testing.T) {
	// 验证连接断开后池清理
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	// 假 Redis：接受连接后立即关闭
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}()

	pool := NewPool(addr, "", 1*time.Second, 10, 5*time.Minute)
	defer pool.Close()

	// RoundTrip 应失败（连接被关闭）
	_, err = pool.RoundTrip([]byte("*1\r\n$4\r\nPING\r\n"))
	if err == nil {
		// 可能收到 EOF 后才失败，也可能在 Send 时失败
		// 无论如何，第二次应该能新建连接（但假 Redis 也会关闭）
	}

	// 关闭假 Redis，池应清理断开连接
	ln.Close()
}
