package gateway

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	c, e := DecodeConfig(strings.NewReader("mode: skeleton\nshutdown_timeout: 2s\n"))
	if e != nil || c.ShutdownTimeout != 2*time.Second {
		t.Fatalf("%+v %v", c, e)
	}
	for _, input := range []string{"typo: true", "mode: passthrough", "listen: 0.0.0.0:17379", "shutdown_timeout: 0s", "limits: {max_depth: 0}", "mode: skeleton\n---\nmode: skeleton", strings.Repeat("#", 65537)} {
		if _, e = DecodeConfig(strings.NewReader(input)); e == nil {
			t.Errorf("accepted invalid config %.80q", input)
		}
	}
}

// Retired tiering configuration must not silently serve a partial hot database
// through the remaining single-backend passthrough mode.
func TestRetiredTieringConfigRejected(t *testing.T) {
	base := "mode: passthrough\nredis_address: 127.0.0.1:16390\nallow_unauthenticated: true\n"
	if _, err := DecodeConfig(strings.NewReader(base)); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"hot_cold: {}\n", "hot_cold: null\n", "state_dir: /old/state\n", "state_dir: ''\n", "hot_cold:\n  auto_eviction:\n    enabled: true\n"} {
		if _, err := DecodeConfig(strings.NewReader(base + field)); err == nil {
			t.Fatalf("retired config accepted: %s", field)
		}
	}
}

func reader(t *testing.T, s string, l Limits) *Reader {
	t.Helper()
	r, e := NewReader(strings.NewReader(s), l)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestCommandAndPipeline(t *testing.T) {
	l := DefaultConfig().Limits
	first := "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$3\r\na\x00b\r\n"
	r := reader(t, first+"*1\r\n$4\r\nPING\r\n", l)
	raw, a, e := r.ReadCommand()
	if e != nil || string(raw) != first || len(a) != 3 || string(a[2]) != "a\x00b" {
		t.Fatalf("%q %q %v", raw, a, e)
	}
	_, a, e = r.ReadCommand()
	if e != nil || string(a[0]) != "PING" {
		t.Fatalf("%q %v", a, e)
	}
	if string(raw) != first {
		t.Fatal("previous frame mutated")
	}
}
func TestInvalidCommands(t *testing.T) {
	l := Limits{MaxBytes: 64, MaxElements: 4, MaxDepth: 3}
	for _, s := range []string{"PING\r\n", "*0\r\n", "*-1\r\n", "*1\r\n$-1\r\n", "*1\r\n$0\r\n\r\n", "*5\r\n", "*1\r\n$9223372036854775808\r\n", "*1\r\n$64\r\n", "*1\r\n$3\r\nGETxx", "*1\n$3\r\nGET\r\n", "*1\r\n$3\r\nGE"} {
		if _, _, e := reader(t, s, l).ReadCommand(); e == nil {
			t.Errorf("accepted %q", s)
		}
	}
}
func TestFrames(t *testing.T) {
	l := Limits{MaxBytes: 128, MaxElements: 6, MaxDepth: 3}
	for _, s := range []string{"+OK\r\n", "-ERR failure\r\n", ":-1\r\n", "$-1\r\n", "*-1\r\n", "*0\r\n", "*2\r\n$0\r\n\r\n*1\r\n:2\r\n"} {
		b, e := reader(t, s, l).ReadFrame()
		if e != nil || string(b) != s {
			t.Errorf("%q: %q %v", s, b, e)
		}
	}
	for _, s := range []string{"$-2\r\n", "*-2\r\n", ":x\r\n", "+a\nb\r\n", "*1\r\n*1\r\n*1\r\n:1\r\n", "*7\r\n", "$99999999999999\r\n", strings.Repeat("+", 129)} {
		if _, e := reader(t, s, l).ReadFrame(); e == nil {
			t.Errorf("accepted %q", s)
		}
	}
}
func TestAggregateBudget(t *testing.T) {
	l := Limits{MaxBytes: 32, MaxElements: 4, MaxDepth: 3}
	if _, _, e := reader(t, "*3\r\n$3\r\nSET\r\n$8\r\nabcdefgh\r\n$8\r\nabcdefgh\r\n", l).ReadCommand(); e == nil {
		t.Fatal("aggregate bytes not bounded")
	}
	if _, e := reader(t, "*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n", Limits{MaxBytes: 128, MaxElements: 4, MaxDepth: 3}).ReadFrame(); e == nil {
		t.Fatal("aggregate elements not bounded")
	}
}
func TestHealth(t *testing.T) {
	s, e := NewService(DefaultConfig())
	if e != nil {
		t.Fatal(e)
	}
	for p, want := range map[string]int{"/livez": 200, "/readyz": 503, "/metrics": 200} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", p, nil))
		if w.Code != want {
			t.Errorf("%s: %d", p, w.Code)
		}
	}
}
func TestServeRejectAndStop(t *testing.T) {
	c := DefaultConfig()
	s, _ := NewService(c)
	d, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	a, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, d, a) }()
	conn, e := net.DialTimeout("tcp", d.Addr().String(), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	line, e := bufio.NewReader(conn).ReadString('\n')
	if e != nil || !strings.HasPrefix(line, "-ERR gateway skeleton") {
		t.Fatalf("%q %v", line, e)
	}
	cancel()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown stuck")
	}
}
func TestRunBindFailure(t *testing.T) {
	occupied, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer occupied.Close()
	c := DefaultConfig()
	c.Listen = occupied.Addr().String()
	s, _ := NewService(c)
	if e = s.Run(context.Background()); e == nil {
		t.Fatal("expected bind failure")
	}
}
func FuzzRESP(f *testing.F) {
	f.Add("*1\r\n$4\r\nPING\r\n")
	f.Add("*2\r\n$-1\r\n:1\r\n")
	f.Fuzz(func(t *testing.T, s string) {
		l := Limits{MaxBytes: 4096, MaxElements: 64, MaxDepth: 8}
		r, _ := NewReader(strings.NewReader(s), l)
		r.ReadCommand()
		r, _ = NewReader(strings.NewReader(s), l)
		r.ReadFrame()
	})
}

// Force fragmented reads without buffering the whole transport response.
type byteReader struct{ r io.Reader }

func (b byteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return b.r.Read(p)
}
func TestFragmented(t *testing.T) {
	s := "*1\r\n$4\r\nPING\r\n"
	r, _ := NewReader(byteReader{strings.NewReader(s)}, DefaultConfig().Limits)
	raw, _, e := r.ReadCommand()
	if e != nil || string(raw) != s {
		t.Fatalf("%q %v", raw, e)
	}
}
