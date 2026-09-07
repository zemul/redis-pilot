package gateway

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

// Opt-in: starts its own Redis, persistence disabled, in a temporary directory.
func TestRedisIntegration(t *testing.T) {
	binary := os.Getenv("GATEWAY_TEST_REDIS_SERVER")
	if binary == "" {
		t.Skip("set GATEWAY_TEST_REDIS_SERVER to a redis-server executable")
	}
	reserve, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := reserve.Addr().String()
	port := reserve.Addr().(*net.TCPAddr).Port
	reserve.Close()
	proc := exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--dir", t.TempDir(), "--requirepass", "gateway-integration-backend", "--loglevel", "warning")
	if e = proc.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { proc.Process.Kill(); proc.Wait() })
	cfg := passConfig(addr)
	cfg.MaxConnections = 1
	cfg.RequestTimeout = time.Second
	cfg.BackendPasswordFile = secretFile(t, "gateway-integration-backend")
	cfg.FrontendPasswordFile = secretFile(t, "gateway-integration-front")
	cfg.AllowUnauthenticated = false
	s, e := NewService(cfg)
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, _, err := s.openBackend(context.Background())
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("isolated Redis did not become available")
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer data.Close()
	admin, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, data, admin) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(3 * time.Second):
			t.Error("gateway stop timed out")
		}
	})
	c, e := net.DialTimeout("tcp", data.Addr().String(), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	r, _ := NewReader(c, cfg.Limits)
	type exchange struct {
		args []string
		want string
	}
	for _, x := range []exchange{
		{[]string{"GET", "counter"}, "-NOAUTH Authentication required.\r\n"},
		{[]string{"AUTH", "gateway-integration-front"}, "+OK\r\n"},
		{[]string{"SET", "counter", "100"}, "+OK\r\n"},
		{[]string{"INCR", "counter"}, ":101\r\n"},
		{[]string{"GET", "counter"}, "$3\r\n101\r\n"},
		{[]string{"PEXPIRE", "counter", "60000"}, ":1\r\n"},
		{[]string{"PERSIST", "counter"}, ":1\r\n"},
		{[]string{"PTTL", "counter"}, ":-1\r\n"},
		{[]string{"EVAL", "return 1", "0"}, "-ERR command not supported by gateway passthrough\r\n"},
	} {
		if e = writeAll(c, encodeCommand(x.args...)); e != nil {
			t.Fatal(e)
		}
		expect(t, r, x.want)
	}
	batch := append(encodeCommand("GET", "counter"), encodeCommand("DEL", "counter")...)
	batch = append(batch, encodeCommand("GET", "counter")...)
	writeAll(c, batch)
	expect(t, r, "$3\r\n101\r\n")
	expect(t, r, ":1\r\n")
	expect(t, r, "$-1\r\n")
	// The first connection is still active; a second one must be rejected.
	extra, err := net.DialTimeout("tcp", data.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	extra.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	_, err = extra.Read(one[:])
	extra.Close()
	if err != io.EOF {
		t.Fatalf("expected connection limit rejection, got %v", err)
	}
	hc := http.Client{Timeout: time.Second}
	deadline = time.Now().Add(3 * time.Second)
	for {
		resp, err := hc.Get(fmt.Sprintf("http://%s/readyz", admin.Addr()))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway not ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
