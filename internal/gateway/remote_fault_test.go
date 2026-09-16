package gateway

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// Test-only, one backend connection per incoming connection. Unlike restarting
// an engine, cutting this link leaves the remote database and its data intact.
type remoteFaultLink struct {
	address     string
	mu          sync.Mutex
	down        bool
	dropKey     string
	dropped     bool
	connections map[net.Conn]bool
}

func newRemoteFaultLink(t *testing.T, host, backend string) *remoteFaultLink {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatal(err)
	}
	f := &remoteFaultLink{address: l.Addr().String(), connections: map[net.Conn]bool{}}
	t.Cleanup(func() { l.Close(); f.cut(true) })
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go f.serve(c, backend)
		}
	}()
	return f
}

func (f *remoteFaultLink) cut(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
	if down {
		for c := range f.connections {
			c.Close()
		}
	}
}

func (f *remoteFaultLink) serve(c net.Conn, backend string) {
	defer c.Close()
	peer, _, _ := net.SplitHostPort(c.RemoteAddr().String())
	local, _, _ := net.SplitHostPort(f.address)
	if !net.ParseIP(peer).Equal(net.ParseIP(local)) {
		return
	}
	f.mu.Lock()
	if f.down {
		f.mu.Unlock()
		return
	}
	f.connections[c] = true
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.connections, c); f.mu.Unlock() }()
	b, err := net.DialTimeout("tcp", backend, time.Second)
	if err != nil {
		return
	}
	defer b.Close()
	f.mu.Lock()
	if f.down {
		f.mu.Unlock()
		return
	}
	f.connections[b] = true
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.connections, b); f.mu.Unlock() }()
	limits := Limits{MaxBytes: 4 << 20, MaxElements: 65536, MaxDepth: 32}
	in, _ := NewReader(c, limits)
	out, _ := NewReader(b, limits)
	for {
		raw, args, e := in.ReadCommand()
		if e != nil {
			return
		}
		if writeAll(b, raw) != nil {
			return
		}
		reply, e := out.ReadFrame()
		if e != nil {
			return
		}
		// Drop exactly one executed INCR reply, not a probe or ownership script.
		f.mu.Lock()
		key, increment := false, false
		for _, v := range args {
			key = key || (f.dropKey != "" && string(v) == f.dropKey)
			increment = increment || bytes.Equal(v, []byte("INCR"))
		}
		drop := key && increment && !f.dropped
		if drop {
			f.dropped = true
		}
		f.mu.Unlock()
		if drop {
			return
		}
		if writeAll(c, reply) != nil {
			return
		}
	}
}

func remoteRequest(t *testing.T, address string, args ...string) string {
	t.Helper()
	c, r, err := authenticated(address, "shard-config-front")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(6 * time.Second))
	if err = writeAll(c, encodeCommand(args...)); err != nil {
		t.Fatal(err)
	}
	b, err := r.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func awaitRemoteReply(t *testing.T, address, want string, args ...string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		got := remoteRequest(t, address, args...)
		if got == want {
			return
		}
		if len(got) == 0 || got[0] != '-' || time.Now().After(deadline) {
			t.Fatalf("%v: %q want %q", args, got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func verifyRemoteLinkFailures(t *testing.T, address string, hot, cold *remoteFaultLink) {
	t.Helper()
	awaitRemoteReply(t, address, "+OK\r\n", "SET", "fault-hot", "old")
	awaitRemoteReply(t, address, "$3\r\nold\r\n", "GET", "fault-hot")
	hot.cut(true)
	awaitRemoteReply(t, address, "+OK\r\n", "SET", "fault-hot", "new")
	awaitRemoteReply(t, address, "$3\r\nnew\r\n", "GET", "fault-hot")
	hot.cut(false)
	deadline := time.Now().Add(10 * time.Second)
	for {
		awaitRemoteReply(t, address, "$3\r\nnew\r\n", "GET", "fault-hot")
		c, err := net.DialTimeout("tcp", hot.address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(time.Second))
		r, _ := NewReader(c, Limits{MaxBytes: 1 << 20, MaxElements: 4096, MaxDepth: 16})
		err = writeAll(c, encodeCommand("GET", "fault-hot"))
		if err != nil {
			c.Close()
			t.Fatal(err)
		}
		b, err := r.ReadFrame()
		c.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(b) == "$3\r\nnew\r\n" {
			break
		}
		// An isolated hot DB may still contain old bytes until recovery clears
		// it; only Gateway reads must never expose them. Require eventual reuse.
		if (string(b) != "$-1\r\n" && string(b) != "$3\r\nold\r\n") || time.Now().After(deadline) {
			t.Fatalf("hot cache did not recover safely: %q", b)
		}
		time.Sleep(25 * time.Millisecond)
	}
	awaitRemoteReply(t, address, "+OK\r\n", "SET", "fault-counter", "0")
	cold.cut(true)
	for _, args := range [][]string{{"INCR", "fault-counter"}, {"GET", "fault-never-cached"}} {
		got := remoteRequest(t, address, args...)
		if len(got) == 0 || got[0] != '-' {
			t.Fatalf("unavailable cold returned non-error: %q", got)
		}
	}
	cold.cut(false)
	awaitRemoteReply(t, address, "$1\r\n0\r\n", "GET", "fault-counter")
	cold.mu.Lock()
	cold.dropKey = "fault-counter"
	cold.mu.Unlock()
	got := remoteRequest(t, address, "INCR", "fault-counter")
	if len(got) == 0 || got[0] != '-' {
		t.Fatalf("lost write reply reported success: %q", got)
	}
	cold.mu.Lock()
	dropped := cold.dropped
	cold.mu.Unlock()
	if !dropped {
		t.Fatal("write-reply fault was not injected")
	}
	awaitRemoteReply(t, address, "$1\r\n1\r\n", "GET", "fault-counter")
	for i := 0; i < 5; i++ {
		awaitRemoteReply(t, address, "$1\r\n1\r\n", "GET", "fault-counter")
	}
	t.Log("hot link recovery, cold link errors and executed INCR reply loss passed")
}

func TestRemoteFaultLink(t *testing.T) {
	backend := g4Redis(t)
	f := newRemoteFaultLink(t, "127.0.0.1", backend)
	c, r, err := authenticated(f.address, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	shardWant(t, c, r, "+OK\r\n", "SET", "counter", "0")
	f.mu.Lock()
	f.dropKey = "counter"
	f.mu.Unlock()
	if err := writeAll(c, encodeCommand("INCR", "counter")); err != nil {
		t.Fatal(err)
	}
	if reply, err := r.ReadFrame(); err == nil {
		t.Fatalf("executed reply not dropped: %q", reply)
	}
	c2, r2, err := authenticated(backend, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	shardWant(t, c2, r2, "$1\r\n1\r\n", "GET", "counter")
	c3, r3, err := authenticated(f.address, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	f.cut(true)
	c3.SetDeadline(time.Now().Add(time.Second))
	writeAll(c3, encodeCommand("PING"))
	if _, err := r3.ReadFrame(); err == nil {
		t.Fatal("existing connection survived cut")
	}
	f.cut(false)
	c4, r4, err := authenticated(f.address, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c4.Close()
	shardWant(t, c4, r4, "$1\r\n1\r\n", "GET", "counter")
}
