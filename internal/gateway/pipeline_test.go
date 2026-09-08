package gateway

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestBatchWritesBeforeReplies(t *testing.T) {
	cfg := passConfig("127.0.0.1:1")
	cfg.RequestTimeout = time.Second
	s, _ := NewService(cfg)
	client, backend := net.Pipe()
	defer client.Close()
	defer backend.Close()
	done := make(chan error, 1)
	go func() {
		r, _ := NewReader(backend, cfg.Limits)
		for i := 0; i < 2; i++ {
			if _, _, e := r.ReadCommand(); e != nil {
				done <- e
				return
			}
		}
		done <- writeAll(backend, []byte("+ONE\r\n+TWO\r\n"))
	}()
	r, _ := NewReader(client, cfg.Limits)
	a := encodeCommand("PING")
	var replies []string
	batch := []request{{raw: a, started: time.Now()}, {local: []byte("-ERR local\r\n"), started: time.Now()}, {raw: a, started: time.Now()}}
	if !s.executeBatch(context.Background(), client, r, batch, [][]byte{a, a}, func(b []byte) bool { replies = append(replies, string(b)); return true }) {
		t.Fatal("batch failed: writer must pipeline before waiting")
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if strings.Join(replies, "") != "+ONE\r\n-ERR local\r\n+TWO\r\n" {
		t.Fatal(replies)
	}
	if metricValue(s.inflight) != 0 || metricValue(s.uncertain) != 0 {
		t.Fatal("batch gauges not released")
	}
}

func TestWorkspaceBudget(t *testing.T) {
	cfg := passConfig("127.0.0.1:1")
	cfg.BufferBudgetBytes = cfg.workspaceBytes() + cfg.probeWorkspaceBytes()
	s, e := NewService(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if !s.acquireWorkspace(cfg.probeWorkspaceBytes()) {
		t.Fatal("probe reservation")
	}
	defer s.releaseWorkspace(cfg.probeWorkspaceBytes())
	if !s.acquireWorkspace(cfg.workspaceBytes()) {
		t.Fatal("session reservation")
	}
	if s.acquireWorkspace(1) {
		t.Fatal("budget exceeded")
	}
	s.releaseWorkspace(cfg.workspaceBytes())
	if !s.acquireWorkspace(cfg.workspaceBytes()) {
		t.Fatal("budget not reusable")
	}
	s.releaseWorkspace(cfg.workspaceBytes())
}

func TestDrainAndDeadline(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "complete"
		if timeout {
			name = "forced"
		}
		t.Run(name, func(t *testing.T) {
			received := make(chan struct{})
			release := make(chan struct{})
			addr := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
				if string(a[0]) == "INFO" {
					infoReply(c, "master")
					return true
				}
				close(received)
				if timeout {
					var b [1]byte
					c.Read(b[:])
					return false
				}
				select {
				case <-release:
				case <-time.After(2 * time.Second):
					return false
				}
				writeAll(c, []byte(":1\r\n"))
				return true
			})
			cfg := passConfig(addr)
			cfg.RequestTimeout = time.Second
			cfg.ShutdownTimeout = 150 * time.Millisecond
			s, _ := NewService(cfg)
			s.admission = testKeyBarrierSet(t, 4, 100, 4)
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
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.serve(ctx, data, admin) }()
			c, e := net.Dial("tcp", data.Addr().String())
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(2 * time.Second))
			writeAll(c, encodeCommand("INCR", "x"))
			select {
			case <-received:
			case <-time.After(time.Second):
				t.Fatal("request not sent")
			}
			cancel()
			select {
			case <-done:
				t.Fatal("did not drain active request")
			case <-time.After(20 * time.Millisecond):
			}
			r, _ := NewReader(c, cfg.Limits)
			if timeout {
				if _, e := r.ReadFrame(); e == nil {
					t.Fatal("fabricated response")
				}
			} else {
				close(release)
				expect(t, r, ":1\r\n")
			}
			select {
			case e := <-done:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				t.Fatal("drain never finished")
			}
			if metricValue(s.active) != 0 || metricValue(s.bufferReserved) != 0 || metricValue(s.inflight) != 0 {
				t.Fatal("shutdown leaked reservations")
			}
			if timeout && metricValue(s.errors.WithLabelValues("drain_timeout")) != 1 {
				t.Fatal("missing forced drain metric")
			}
			if timeout {
				_, err := s.admission.enter("x")
				requireBarrierError(t, err, errKeyBarrierUncertain)
				requireBarrierAccounting(t, s.admission, 1, 1, 0)
			} else {
				requireBarrierAccounting(t, s.admission, 0, 0, 0)
			}
		})
	}
}

func TestPipelineLimitConfig(t *testing.T) {
	for _, apply := range []func(*Config){func(c *Config) { c.MaxPipeline = 0 }, func(c *Config) { c.MaxPipeline = 65 }, func(c *Config) { c.MaxBatchBytes = 0 }, func(c *Config) { c.BufferBudgetBytes = 1 }} {
		c := passConfig("127.0.0.1:1")
		apply(&c)
		if c.Validate() == nil {
			t.Fatal("invalid pipeline config accepted")
		}
	}
}

func metricValue(m prometheus.Metric) float64 {
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		panic(err)
	}
	if out.Gauge != nil {
		return out.Gauge.GetValue()
	}
	return out.Counter.GetValue()
}

type failedFlushConn struct{ net.Conn }

func (c failedFlushConn) Write(p []byte) (int, error) { return len(p) / 2, io.ErrUnexpectedEOF }
func TestBatchFlushFailureNoReplay(t *testing.T) {
	cfg := passConfig("127.0.0.1:1")
	s, _ := NewService(cfg)
	s.admission = testKeyBarrierSet(t, 4, 100, 4)
	client, backend := net.Pipe()
	defer client.Close()
	defer backend.Close()
	c := failedFlushConn{client}
	r, _ := NewReader(c, cfg.Limits)
	raw := encodeCommand("INCR", "x")
	batch := []request{{raw: raw, started: time.Now()}, {raw: raw, started: time.Now()}}
	for i := range batch {
		batch[i].args = admissionArgs("INCR", "x")
		s.admitRequest(&batch[i])
	}
	if s.executeBatch(context.Background(), c, r, batch, [][]byte{raw, raw}, func([]byte) bool { t.Error("unexpected response"); return true }) {
		t.Fatal("flush failure accepted")
	}
	if metricValue(s.errors.WithLabelValues("backend_write")) != 1 || metricValue(s.uncertain) != 2 || metricValue(s.inflight) != 0 {
		t.Fatal("failure metrics / cleanup incorrect")
	}
	_, err := s.admission.enter("x")
	requireBarrierError(t, err, errKeyBarrierUncertain)
	requireBarrierAccounting(t, s.admission, 1, 1, 0)
}
