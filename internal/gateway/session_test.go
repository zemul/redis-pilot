package gateway

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Each test backend speaks RESP on an ephemeral loopback port; no real data.
func fakeBackend(t *testing.T, fn func(net.Conn, [][]byte) bool) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				c.SetDeadline(time.Now().Add(2 * time.Second))
				r, _ := NewReader(c, DefaultConfig().Limits)
				for {
					_, a, e := r.ReadCommand()
					if e != nil || !fn(c, a) {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { l.Close(); wg.Wait() })
	return l.Addr().String()
}
func infoReply(c net.Conn, role string) {
	b := "redis_version:7.0.0\r\nrole:" + role + "\r\ncluster_enabled:0\r\n"
	writeAll(c, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(b), b)))
}
func passConfig(addr string) Config {
	c := DefaultConfig()
	c.Mode = "passthrough"
	c.RedisAddress = addr
	c.AllowUnauthenticated = true
	c.RequestTimeout = 100 * time.Millisecond
	return c
}
func startSession(t *testing.T, s *Service) (net.Conn, *Reader, context.CancelFunc) {
	t.Helper()
	client, server := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.session(ctx, server); close(done) }()
	t.Cleanup(func() {
		cancel()
		client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("session leaked")
		}
	})
	client.SetDeadline(time.Now().Add(time.Second))
	r, _ := NewReader(client, DefaultConfig().Limits)
	return client, r, cancel
}
func expect(t *testing.T, r *Reader, want string) {
	t.Helper()
	b, e := r.ReadFrame()
	if e != nil || string(b) != want {
		t.Fatalf("got %q, %v; want %q", b, e, want)
	}
}
func TestPassthroughPipelineOrdering(t *testing.T) {
	addr := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
		switch string(a[0]) {
		case "INFO":
			infoReply(c, "master")
		case "PING":
			writeAll(c, []byte("+PONG\r\n"))
		case "GET":
			writeAll(c, []byte("$-1\r\n"))
		case "SET":
			writeAll(c, []byte("-ERR backend test\r\n"))
		default:
			t.Errorf("unexpected forwarding %q", a)
			return false
		}
		return true
	})
	s, e := NewService(passConfig(addr))
	if e != nil {
		t.Fatal(e)
	}
	c, r, _ := startSession(t, s)
	batch := append(encodeCommand("PING"), encodeCommand("EVAL", "anything")...)
	batch = append(batch, encodeCommand("GET", "missing")...)
	batch = append(batch, encodeCommand("SET", "x")...)
	go writeAll(c, batch)
	expect(t, r, "+PONG\r\n")
	expect(t, r, "-ERR command not supported by gateway passthrough\r\n")
	expect(t, r, "$-1\r\n")
	expect(t, r, "-ERR backend test\r\n")
}
func secretFile(t *testing.T, secret string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "secret")
	if e := os.WriteFile(p, []byte(secret), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func TestIndependentAuth(t *testing.T) {
	var auth atomic.Int32
	addr := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
		switch string(a[0]) {
		case "AUTH":
			if len(a) != 2 || string(a[1]) != "backend-secret" {
				t.Errorf("wrong backend credentials")
				return false
			}
			auth.Add(1)
			writeAll(c, []byte("+OK\r\n"))
		case "INFO":
			infoReply(c, "master")
		case "PING":
			writeAll(c, []byte("+PONG\r\n"))
		default:
			t.Error("unexpected command")
			return false
		}
		return true
	})
	cfg := passConfig(addr)
	cfg.AllowUnauthenticated = false
	cfg.FrontendPasswordFile = secretFile(t, "front-secret\n")
	cfg.BackendPasswordFile = secretFile(t, "backend-secret\n")
	s, e := NewService(cfg)
	if e != nil {
		t.Fatal(e)
	}
	c, r, _ := startSession(t, s)
	writeAll(c, encodeCommand("PING"))
	expect(t, r, "-NOAUTH Authentication required.\r\n")
	if auth.Load() != 0 {
		t.Fatal("unauthenticated request connected to backend")
	}
	writeAll(c, encodeCommand("AUTH", "bad"))
	expect(t, r, "-WRONGPASS invalid credentials\r\n")
	writeAll(c, encodeCommand("AUTH", "front-secret"))
	expect(t, r, "+OK\r\n")
	writeAll(c, encodeCommand("PING"))
	expect(t, r, "+PONG\r\n")
	if auth.Load() != 1 {
		t.Fatal("missing backend auth")
	}
}
func TestBackendRejection(t *testing.T) {
	for _, kind := range []string{"auth", "replica", "cluster"} {
		t.Run(kind, func(t *testing.T) {
			var business atomic.Int32
			addr := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
				if string(a[0]) == "AUTH" {
					writeAll(c, []byte("-WRONGPASS denied\r\n"))
					return true
				}
				if string(a[0]) == "INFO" {
					if kind == "replica" {
						infoReply(c, "slave")
					} else {
						b := "redis_version:7.0.0\r\nrole:master\r\ncluster_enabled:1\r\n"
						writeAll(c, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(b), b)))
					}
					return true
				}
				business.Add(1)
				return false
			})
			cfg := passConfig(addr)
			if kind == "auth" {
				cfg.BackendPasswordFile = secretFile(t, "bad")
			}
			s, e := NewService(cfg)
			if e != nil {
				t.Fatal(e)
			}
			c, r, _ := startSession(t, s)
			writeAll(c, encodeCommand("INCR", "x"))
			expect(t, r, "-ERR backend unavailable; command not sent\r\n")
			if business.Load() != 0 {
				t.Fatal("sent business command to invalid backend")
			}
		})
	}
}
func TestNoReplayAndCancellation(t *testing.T) {
	for _, kind := range []string{"disconnect", "partial", "timeout", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			var count atomic.Int32
			sent := make(chan struct{})
			addr := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
				if string(a[0]) == "INFO" {
					infoReply(c, "master")
					return true
				}
				count.Add(1)
				close(sent)
				switch kind {
				case "partial":
					writeAll(c, []byte("$5\r\nab"))
				case "timeout", "cancel":
					var b [1]byte
					c.Read(b[:])
				}
				return false
			})
			s, _ := NewService(passConfig(addr))
			c, r, cancel := startSession(t, s)
			writeAll(c, encodeCommand("INCR", "x"))
			select {
			case <-sent:
			case <-time.After(time.Second):
				t.Fatal("backend did not receive request")
			}
			if kind == "cancel" {
				cancel()
			}
			if _, e := r.ReadFrame(); e == nil {
				t.Fatal("fabricated reply after ambiguous result")
			}
			if count.Load() != 1 {
				t.Fatalf("replayed request %d", count.Load())
			}
		})
	}
}
func TestPasswordValidation(t *testing.T) {
	for _, secret := range []string{"", "a\nb", strings.Repeat("x", 4097)} {
		if _, e := readPassword(secretFile(t, secret)); e == nil {
			t.Error("accepted invalid password file")
		}
	}
	c := passConfig("127.0.0.1:6379")
	c.AllowUnauthenticated = false
	if e := c.Validate(); e == nil {
		t.Error("implicit anonymous access allowed")
	}
	c = passConfig("invalid")
	if e := c.Validate(); e == nil {
		t.Error("invalid backend allowed")
	}
}

func TestConcurrentSessions(t *testing.T) {
	addr := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
		if string(a[0]) == "INFO" {
			infoReply(c, "master")
			return true
		}
		b := string(a[1])
		writeAll(c, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(b), b)))
		return true
	})
	s, _ := NewService(passConfig(addr))
	for i := 0; i < 8; i++ {
		i := i
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			c, r, _ := startSession(t, s)
			writeAll(c, encodeCommand("ECHO", fmt.Sprint(i)))
			expect(t, r, fmt.Sprintf("$1\r\n%d\r\n", i))
		})
	}
}
