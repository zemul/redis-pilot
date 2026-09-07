package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func reportingConfig(t *testing.T, endpoint string) Config {
	c := DefaultConfig()
	c.ControlPlane = ControlPlaneConfig{ServerEndpoint: endpoint, TokenFile: secretFile(t, "test-control-token-long-enough"), NodeID: "node", ClusterID: "cluster", ShardIDs: []string{"shard0"}, HeartbeatInterval: time.Second, RequestTimeout: 100 * time.Millisecond}
	return c
}

func TestControlPlaneConfigAndCredentials(t *testing.T) {
	c := reportingConfig(t, "http://127.0.0.1:8080")
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"http://example.test", "http://10.0.0.1", "ftp://127.0.0.1", "https://user:password@example.test", "https://example.test/path", "https://example.test?secret=x", "https://example.test#x"} {
		bad := c
		bad.ControlPlane.ServerEndpoint = endpoint
		if err := bad.Validate(); err == nil {
			t.Fatalf("accepted endpoint %s", endpoint)
		}
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.ControlPlane.TokenFile = "relative" },
		func(c *Config) { c.ControlPlane.ServerEndpoint = "" },
		func(c *Config) { c.ControlPlane.ShardIDs = []string{"x", "x"} },
		func(c *Config) { c.ControlPlane.HeartbeatInterval = time.Minute },
		func(c *Config) { c.GatewayID = "../bad" },
		func(c *Config) { c.ControlPlane.NodeID = "" },
	} {
		bad := c
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("invalid config accepted")
		}
	}
	if err := os.Chmod(c.ControlPlane.TokenFile, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(c); err == nil {
		t.Fatal("world-readable token accepted")
	}
	c.ControlPlane.TokenFile = secretFile(t, "short")
	if _, err := NewService(c); err == nil {
		t.Fatal("short token accepted")
	}
	c.ControlPlane.TokenFile = secretFile(t, "long-enough-but\nnot-single-line")
	if _, err := NewService(c); err == nil {
		t.Fatal("multiline token accepted")
	}
	s, err := NewService(DefaultConfig())
	if err != nil || s.reporter != nil {
		t.Fatal("reporter not disabled by default", err)
	}
}

func TestControlPlaneAcknowledgements(t *testing.T) {
	var redirects atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirects.Add(1) }))
	defer target.Close()
	var sequence int64
	var responseMode atomic.Int32
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-control-token-long-enough" {
			t.Error("wrong auth")
		}
		if r.URL.Path != "/api/v1/gateway/heartbeat/gateway-local" {
			t.Error("wrong path")
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(string(raw), "token-long") || strings.Contains(string(raw), "token_file") {
			t.Error("credential leak")
		}
		var v apitypes.GatewayHeartbeat
		if err := json.Unmarshal(raw, &v); err != nil || v.Validate() != nil {
			t.Error("invalid report", err)
			return
		}
		if v.Sequence <= sequence {
			t.Error("non-monotonic report")
		}
		sequence = v.Sequence
		switch responseMode.Load() {
		case 1:
			http.Error(w, "credential error", 401)
		case 2:
			http.Redirect(w, r, target.URL, 307)
		case 3:
			fmt.Fprint(w, `{"success":true,"data":{"accepted_sequence":0}}`)
		case 4:
			fmt.Fprint(w, strings.Repeat("x", 4097))
		default:
			fmt.Fprintf(w, `{"success":true,"data":{"accepted_sequence":%d}}`, v.Sequence)
		}
	}))
	defer host.Close()
	s, err := NewService(reportingConfig(t, host.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer s.reporter.client.CloseIdleConnections()
	for mode := int32(0); mode < 5; mode++ {
		responseMode.Store(mode)
		s.reporter.send(context.Background(), s)
	}
	if got := metricValue(s.heartbeats.WithLabelValues("acknowledged")); got != 1 {
		t.Fatalf("acks=%v", got)
	}
	if got := metricValue(s.heartbeats.WithLabelValues("error")); got != 4 {
		t.Fatalf("errors=%v", got)
	}
	if redirects.Load() != 0 || metricValue(s.lastHeartbeatAck) == 0 {
		t.Fatal("redirect followed or no ack timestamp")
	}
	// Same config gives the same non-secret fingerprint but a different process ID.
	s2, err := NewService(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.reporter.client.CloseIdleConnections()
	if s2.reporter.report.BootID == s.reporter.report.BootID || s2.reporter.report.LocalConfigDigest != s.reporter.report.LocalConfigDigest {
		t.Fatal("bad process identity or fingerprint")
	}
}

func TestControlPlaneOutageDoesNotBlockForwarding(t *testing.T) {
	blocked := make(chan struct{}, 1)
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case blocked <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer host.Close()
	backend := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
		if string(a[0]) == "INFO" {
			infoReply(c, "master")
		} else {
			writeAll(c, []byte("+PONG\r\n"))
		}
		return true
	})
	c := passConfig(backend)
	c.ControlPlane = reportingConfig(t, host.URL).ControlPlane
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, data, admin) }()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("reporter did not start")
	}
	client, err := net.DialTimeout("tcp", data.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(time.Second))
	if err := writeAll(client, encodeCommand("PING")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 7)
	if _, err := io.ReadFull(client, buf); err != nil || string(buf) != "+PONG\r\n" {
		t.Fatalf("forwarding during outage: %q %v", buf, err)
	}
	client.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown blocked by control plane")
	}
	if metricValue(s.heartbeats.WithLabelValues("error")) == 0 {
		t.Fatal("no failed reports recorded")
	}
}

func TestControlPlanePeriodicRecovery(t *testing.T) {
	var attempts atomic.Int32
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v apitypes.GatewayHeartbeat
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			t.Error(err)
			return
		}
		if attempts.Add(1) == 1 {
			http.Error(w, "unavailable", 503)
			return
		}
		fmt.Fprintf(w, `{"success":true,"data":{"accepted_sequence":%d}}`, v.Sequence)
	}))
	defer host.Close()
	s, err := NewService(reportingConfig(t, host.URL))
	if err != nil {
		t.Fatal(err)
	}
	s.reporter.interval = 10 * time.Millisecond // test clock acceleration, not accepted production config
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.reporter.run(ctx, s) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	for metricValue(s.lastHeartbeatAck) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("reporting did not recover")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if metricValue(s.heartbeats.WithLabelValues("error")) != 1 || attempts.Load() < 2 {
		t.Fatal("periodic retry missing")
	}
}
