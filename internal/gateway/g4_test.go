package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	l.Close()
	return a
}
func ownProcess(t *testing.T, binary string, args ...string) *exec.Cmd {
	t.Helper()
	p := exec.Command(binary, args...)
	if e := p.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { p.Process.Kill(); p.Wait() })
	return p
}
func proxyBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("GATEWAY_TEST_PROXY_BINARY"); p != "" {
		return p // Cross-compiled test runs need no Go toolchain on the target.
	}
	p := filepath.Join(t.TempDir(), "redis-proxy")
	c := exec.Command("go", "build", "-o", p, "../../cmd/proxy")
	if out, e := c.CombinedOutput(); e != nil {
		t.Fatalf("build test proxy: %v %s", e, out)
	}
	return p
}
func startTestProxy(t *testing.T, binary, backend, password string) string {
	t.Helper()
	addr := freeAddress(t)
	ownProcess(t, binary, "-listen", addr, "-master", backend, "-password", password, "-metrics-listen", "")
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, e := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if e == nil {
			c.Close()
			return addr
		}
		if time.Now().After(deadline) {
			t.Fatal("test proxy did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func startTestGateway(t *testing.T, cfg Config) (*Service, string, context.CancelFunc) {
	t.Helper()
	s, e := NewService(cfg)
	if e != nil {
		t.Fatal(e)
	}
	d, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		d.Close()
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, d, a) }()
	t.Cleanup(func() {
		cancel()
		d.Close()
		a.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("test gateway did not stop")
		}
	})
	return s, d.Addr().String(), cancel
}
func authenticated(addr, password string) (net.Conn, *Reader, error) {
	c, e := net.DialTimeout("tcp", addr, time.Second)
	if e != nil {
		return nil, nil, e
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	r, _ := NewReader(c, DefaultConfig().Limits)
	if password != "" {
		if e = writeAll(c, encodeCommand("AUTH", password)); e != nil {
			c.Close()
			return nil, nil, e
		}
		b, e := r.ReadFrame()
		if e != nil || string(b) != "+OK\r\n" {
			c.Close()
			return nil, nil, fmt.Errorf("test auth failed: %v", e)
		}
	}
	return c, r, nil
}
func g4Redis(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("GATEWAY_TEST_REDIS_SERVER")
	if binary == "" {
		t.Skip("set GATEWAY_TEST_REDIS_SERVER for isolated G4 tests")
	}
	addr := freeAddress(t)
	_, port, _ := net.SplitHostPort(addr)
	ownProcess(t, binary, "--bind", "127.0.0.1", "--port", port, "--save", "", "--appendonly", "no", "--dir", t.TempDir(), "--requirepass", "g4-backend-test", "--loglevel", "warning")
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, _, e := authenticated(addr, "g4-backend-test")
		if e == nil {
			c.Close()
			return addr
		}
		if time.Now().After(deadline) {
			t.Fatal("isolated G4 Redis did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestG4DoubleProxy(t *testing.T) {
	redis := g4Redis(t)
	binary := proxyBinary(t)
	cfg := passConfig(redis)
	cfg.RequestTimeout = time.Second
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	cfg.FrontendPasswordFile = secretFile(t, "g4-front-test")
	_, gateway, _ := startTestGateway(t, cfg)
	proxy := startTestProxy(t, binary, gateway, "g4-front-test")
	c, r, e := authenticated(proxy, "g4-front-test")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	batch := append(encodeCommand("SET", "g4-counter", "10"), encodeCommand("INCR", "g4-counter")...)
	batch = append(batch, encodeCommand("EVAL", "return 1", "0")...)
	batch = append(batch, encodeCommand("GET", "g4-counter")...)
	if e = writeAll(c, batch); e != nil {
		t.Fatal(e)
	}
	expect(t, r, "+OK\r\n")
	expect(t, r, ":11\r\n")
	expect(t, r, "-ERR command not supported by gateway passthrough\r\n")
	expect(t, r, "$2\r\n11\r\n")
	d, dr, e := authenticated(redis, "g4-backend-test")
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	writeAll(d, encodeCommand("GET", "g4-counter"))
	expect(t, dr, "$2\r\n11\r\n")
	// Wrong frontend credentials must not create a usable client session.
	if bad, _, e := authenticated(proxy, "wrong-test-password"); e == nil {
		bad.Close()
		t.Fatal("wrong password accepted")
	}
}

func TestG4DoubleProxyAmbiguousWrite(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_REDIS_SERVER") == "" {
		t.Skip("opt-in G4 process test")
	}
	received := make(chan struct{})
	var mu sync.Mutex
	count := 0
	backend := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
		if string(a[0]) == "INFO" {
			infoReply(c, "master")
			return true
		}
		mu.Lock()
		count++
		if count == 1 {
			close(received)
		}
		mu.Unlock()
		var b [1]byte
		c.Read(b[:])
		return false
	})
	cfg := passConfig(backend)
	cfg.RequestTimeout = time.Second
	cfg.ShutdownTimeout = 50 * time.Millisecond
	_, gw, cancel := startTestGateway(t, cfg)
	proxy := startTestProxy(t, proxyBinary(t), gw, "")
	c, r, e := authenticated(proxy, "")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	writeAll(c, encodeCommand("INCR", "x"))
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("write not received")
	}
	cancel()
	b, e := r.ReadFrame()
	if e == nil && (len(b) == 0 || b[0] != '-') {
		t.Fatalf("unexpected successful result %q", b)
	}
	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("request replayed %d times", count)
	}
}

type g4Result struct {
	Route      string  `json:"route"`
	Pipeline   int     `json:"pipeline"`
	ValueBytes int     `json:"value_bytes"`
	Repeat     int     `json:"repeat"`
	Commands   int     `json:"commands"`
	Seconds    float64 `json:"seconds"`
	QPS        float64 `json:"qps"`
	BatchP50MS float64 `json:"batch_p50_ms"`
	BatchP95MS float64 `json:"batch_p95_ms"`
	BatchP99MS float64 `json:"batch_p99_ms"`
}

// net.Pipe has no socket send buffer: a non-reading peer deterministically
// blocks reply delivery, independent of OS TCP buffer sizes.
func TestG4SlowReaderReleasesWorkspace(t *testing.T) {
	backend := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
		if string(a[0]) == "INFO" {
			infoReply(c, "master")
		} else {
			writeAll(c, []byte("$1\r\nv\r\n"))
		}
		return true
	})
	s, err := NewService(passConfig(backend))
	if err != nil {
		t.Fatal(err)
	}
	// Repeated sessions must release their reservation without a client read.
	for i := 0; i < 8; i++ {
		c, _, _ := startSession(t, s)
		if err := writeAll(c, encodeCommand("GET", "x")); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for metricValue(s.errors.WithLabelValues("client_write")) < float64(i+1) || metricValue(s.bufferReserved) != 0 || metricValue(s.active) != 0 || metricValue(s.inflight) != 0 {
			if time.Now().After(deadline) {
				t.Fatal("slow reader retained session or workspace")
			}
			time.Sleep(5 * time.Millisecond)
		}
		c.Close()
	}
}

func g4Load(addr, password string, pipeline, valueBytes int) (g4Result, error) {
	const clients = 8
	const commandsPerClient = 512
	var wg sync.WaitGroup
	var mu sync.Mutex
	var samples []float64
	var firstErr error
	start := time.Now()
	for id := 0; id < clients; id++ {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, r, e := authenticated(addr, password)
			if e != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = e
				}
				mu.Unlock()
				return
			}
			defer c.Close()
			key := "g4-load:" + strconv.Itoa(id)
			value := strings.Repeat("v", valueBytes)
			expected := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
			local := make([]float64, 0, commandsPerClient/pipeline)
			for offset := 0; offset < commandsPerClient; offset += pipeline {
				var raw []byte
				for j := 0; j < pipeline; j++ {
					if (offset+j)%2 == 0 {
						raw = append(raw, encodeCommand("SET", key, value)...)
					} else {
						raw = append(raw, encodeCommand("GET", key)...)
					}
				}
				c.SetDeadline(time.Now().Add(5 * time.Second))
				sent := time.Now()
				e = writeAll(c, raw)
				for j := 0; j < pipeline && e == nil; j++ {
					var b []byte
					b, e = r.ReadFrame()
					want := "+OK\r\n"
					if (offset+j)%2 == 1 {
						want = expected
					}
					if e == nil && string(b) != want {
						e = fmt.Errorf("unexpected response for operation %d", offset+j)
					}
				}
				if e != nil {
					break
				}
				local = append(local, float64(time.Since(sent).Microseconds())/1000)
			}
			mu.Lock()
			samples = append(samples, local...)
			if firstErr == nil {
				firstErr = e
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	if firstErr != nil {
		return g4Result{}, firstErr
	}
	sort.Float64s(samples)
	pct := func(p float64) float64 { return samples[int(float64(len(samples)-1)*p)] }
	return g4Result{Pipeline: pipeline, ValueBytes: valueBytes, Commands: clients * commandsPerClient, Seconds: elapsed, QPS: float64(clients*commandsPerClient) / elapsed, BatchP50MS: pct(.5), BatchP95MS: pct(.95), BatchP99MS: pct(.99)}, nil
}

// Exploratory local measurements, not a production benchmark or SLO assertion.
func TestG4Comparison(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_G4_LOAD") != "1" {
		t.Skip("set GATEWAY_TEST_G4_LOAD=1 for short four-path comparison")
	}
	redis := g4Redis(t)
	binary := proxyBinary(t)
	cfg := passConfig(redis)
	cfg.RequestTimeout = 5 * time.Second
	cfg.MaxConnections = 32
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	cfg.FrontendPasswordFile = secretFile(t, "g4-front-test")
	s, gw, _ := startTestGateway(t, cfg)
	p := startTestProxy(t, binary, redis, "g4-backend-test")
	chain := startTestProxy(t, binary, gw, "g4-front-test")
	routes := []struct{ name, addr, password string }{{"direct", redis, "g4-backend-test"}, {"proxy", p, "g4-backend-test"}, {"gateway", gw, "g4-front-test"}, {"proxy_gateway", chain, "g4-front-test"}}
	for repeat := 1; repeat <= 2; repeat++ {
		for _, size := range []int{128, 4096} {
			for _, pipeline := range []int{1, 8} {
				for i := range routes {
					route := routes[(i+repeat-1)%len(routes)]
					result, e := g4Load(route.addr, route.password, pipeline, size)
					if e != nil {
						t.Fatalf("%s p=%d size=%d: %v", route.name, pipeline, size, e)
					}
					result.Route = route.name
					result.Repeat = repeat
					b, _ := json.Marshal(result)
					t.Logf("G4_RESULT %s", b)
				}
			}
		}
	}
	metrics, e := s.registry.Gather()
	if e != nil {
		t.Fatal(e)
	}
	for _, m := range metrics {
		if m.GetName() == "redis_pilot_gateway_batch_requests" {
			t.Logf("G4_BATCH_HISTOGRAM %s", m.String())
		}
	}
}
