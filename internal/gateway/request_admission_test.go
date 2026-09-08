package gateway

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func admissionArgs(args ...string) [][]byte {
	var out [][]byte
	for _, a := range args {
		out = append(out, []byte(a))
	}
	return out
}

func TestRequestKeysWhitelist(t *testing.T) {
	tests := []struct {
		args []string
		keys []string
	}{
		{[]string{"PING"}, nil}, {[]string{"ECHO", "hello"}, nil},
		{[]string{"GET", ""}, []string{""}},
		{[]string{"SET", "k", "v", "NX", "GET"}, []string{"k"}},
		{[]string{"INCR", "k"}, []string{"k"}}, {[]string{"DECR", "k"}, []string{"k"}},
		{[]string{"APPEND", "k", "v"}, []string{"k"}},
		{[]string{"DEL", "a", "b", "a"}, []string{"a", "b", "a"}},
		{[]string{"UNLINK", "a", "b"}, []string{"a", "b"}},
		{[]string{"EXISTS", "a", "b"}, []string{"a", "b"}},
		{[]string{"TYPE", "k"}, []string{"k"}}, {[]string{"TTL", "k"}, []string{"k"}},
		{[]string{"PTTL", "k"}, []string{"k"}}, {[]string{"PERSIST", "k"}, []string{"k"}},
		{[]string{"EXPIRE", "k", "10", "NX"}, []string{"k"}},
		{[]string{"PEXPIRE", "k", "10"}, []string{"k"}},
		{[]string{"MGET", "\x00\xff", "b"}, []string{"\x00\xff", "b"}},
		{[]string{"MSET", "a", "v", "b", "v"}, []string{"a", "b"}},
	}
	covered := make(map[string]bool)
	for _, tt := range tests {
		covered[tt.args[0]] = true
		for _, lower := range []bool{false, true} {
			args := admissionArgs(tt.args...)
			if lower {
				args[0] = []byte(strings.ToLower(tt.args[0]))
			}
			got, err := requestKeys(args)
			if err != nil || !reflect.DeepEqual(got, admissionArgs(tt.keys...)) {
				t.Fatalf("%q: keys %q, err %v", args, got, err)
			}
		}
	}
	for cmd := range forwardCommands {
		if !covered[cmd] {
			t.Errorf("forwarded command %s needs an admission specification test", cmd)
		}
	}
	for _, args := range [][]string{
		nil, {"GET"}, {"GET", "a", "b"}, {"SET", "a"}, {"MSET", "a", "v", "b"},
		{"DEL"}, {"APPEND", "a"}, {"PING", "a", "b"}, {"ECHO"},
		{"AUTH", "secret"}, {"EVAL", "return 1", "0"}, {"FUTURECOMMAND", "k"},
	} {
		if _, err := requestKeys(admissionArgs(args...)); !errors.Is(err, errAdmissionCommand) {
			t.Fatalf("accepted unclassified/malformed command %q: %v", args, err)
		}
	}
	for _, args := range [][]string{
		{"GET", syncMetadataPrefix + "x"}, {"MGET", "a", syncMetadataPrefix + "x"},
		{"MSET", "a", "v", syncMetadataPrefix + "x", "v"},
	} {
		if _, err := requestKeys(admissionArgs(args...)); !errors.Is(err, errAdmissionMetadata) {
			t.Fatalf("metadata access accepted: %v", err)
		}
	}
	// Values and keyless payloads are not keys.
	for _, args := range [][]string{{"SET", "a", syncMetadataPrefix}, {"ECHO", syncMetadataPrefix}} {
		if _, err := requestKeys(admissionArgs(args...)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKeyBarrierManyAtomicRollback(t *testing.T) {
	for _, reason := range []string{"busy", "uncertain", "bytes", "active", "keys", "long"} {
		t.Run(reason, func(t *testing.T) {
			g := testKeyBarrierSet(t, 3, 3, 3)
			keys := admissionArgs("a", "b")
			want := errKeyBarrierBudget
			switch reason {
			case "busy":
				testKeyBarrier(t, g, "b")
				want = errKeyBarrierBusy
			case "uncertain":
				testKeyRequest(t, g, "b").settle(false)
				want = errKeyBarrierUncertain
			case "bytes":
				keys[1] = []byte("bbb")
			case "active":
				g.maxActive = 1
			case "keys":
				g.maxKeys = 1
			case "long":
				keys[1] = []byte(strings.Repeat("b", (64<<10)+1))
				want = errKeyBarrierKey
			}
			n, bytes := len(g.entries), g.keyBytes
			r, err := g.enterMany(keys)
			requireBarrierError(t, err, want)
			if len(r) != 0 {
				t.Fatal("partial admission returned")
			}
			requireBarrierAccounting(t, g, n, bytes, 0)
			testKeyRequest(t, g, "a").settle(true)
		})
	}
	g := testKeyBarrierSet(t, 2, 3, 3)
	old := testKeyRequest(t, g, "a")
	testKeyBarrier(t, g, "b")
	_, err := g.enterMany(admissionArgs("a", "b"))
	requireBarrierError(t, err, errKeyBarrierBusy)
	requireBarrierAccounting(t, g, 2, 2, 1) // Rollback must retain the older request.
	old.settle(true)
	g = testKeyBarrierSet(t, 2, 3, 2)
	r, err := g.enterMany(admissionArgs("", "\x00\xffk", "", "\x00\xffk"))
	if err != nil || len(r) != 2 {
		t.Fatalf("dedup: %d, %v", len(r), err)
	}
	requireBarrierAccounting(t, g, 2, 3, 2)
	for _, h := range r {
		h.settle(true)
	}
	requireBarrierAccounting(t, g, 0, 0, 0)
	g.close()
	_, err = g.enterMany(nil)
	requireBarrierError(t, err, errKeyBarrierClosed)
}

// Exercises the real Service TCP listener and session loop with a controlled
// RESP backend. Admission is installed before serving, never swapped live.
func TestGuardedServiceConcurrentBarrier(t *testing.T) {
	received, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	addr := fakeBackend(t, func(c net.Conn, args [][]byte) bool {
		if string(args[0]) == "INFO" {
			infoReply(c, "master")
			return true
		}
		if string(args[0]) == "INCR" && string(args[1]) == "hot" {
			close(received)
			select {
			case <-release:
			case <-time.After(2 * time.Second):
				return false
			}
			return writeAll(c, []byte(":1\r\n")) == nil
		}
		if string(args[0]) != "GET" || string(args[1]) != "other" {
			t.Errorf("barrier bypass: %q", args)
			return false
		}
		return writeAll(c, []byte("$-1\r\n")) == nil
	})
	cfg := passConfig(addr)
	cfg.RequestTimeout = time.Second
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admission = testKeyBarrierSet(t, 8, 100, 16)
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
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, data, admin) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("service did not stop")
		}
	})
	connect := func() (net.Conn, *Reader) {
		c, err := net.DialTimeout("tcp", data.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(2 * time.Second))
		r, _ := NewReader(c, cfg.Limits)
		return c, r
	}
	c1, r1 := connect()
	if err := writeAll(c1, encodeCommand("INCR", "hot")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("missing in-flight request")
	}
	b := testKeyBarrier(t, s.admission, "hot")
	requireBarrierError(t, b.beginDispatch(), errKeyBarrierDraining)
	c2, r2 := connect()
	var pipeline []byte
	for _, args := range [][]string{{"GET", "hot"}, {"MSET", "other", "v", "hot", "v"}, {"GET", "other"}} {
		pipeline = append(pipeline, encodeCommand(args...)...)
	}
	if err := writeAll(c2, pipeline); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		expect(t, r2, "-TRYAGAIN hot key admission unavailable; command not sent\r\n")
	}
	expect(t, r2, "$-1\r\n")
	requireBarrierWake(t, b, false)
	once.Do(func() { close(release) })
	expect(t, r1, ":1\r\n")
	requireBarrierError(t, b.beginDispatch(), nil)
	if err := writeAll(c1, encodeCommand("GET", "hot")); err != nil {
		t.Fatal(err)
	}
	expect(t, r1, "-TRYAGAIN hot key admission unavailable; command not sent\r\n")
	requireBarrierAccounting(t, s.admission, 1, 3, 0)
}

func TestGuardedBatchOutcomes(t *testing.T) {
	for _, mode := range []string{"success", "redis_error", "lost_reply", "client_failure", "local_client_failure", "canceled_before_send"} {
		t.Run(mode, func(t *testing.T) {
			s, err := NewService(passConfig("127.0.0.1:1"))
			if err != nil {
				t.Fatal(err)
			}
			s.admission = testKeyBarrierSet(t, 4, 100, 4)
			var batch []request
			var raws [][]byte
			if mode == "local_client_failure" {
				batch = append(batch, request{local: []byte("-ERR local\r\n")})
			}
			for _, key := range []string{"a", "b"} {
				q := request{args: admissionArgs("INCR", key), raw: encodeCommand("INCR", key), started: time.Now()}
				s.admitRequest(&q)
				if q.local != nil {
					t.Fatal(string(q.local))
				}
				batch = append(batch, q)
				raws = append(raws, q.raw)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled_before_send" {
				cancel()
				if s.executeBatch(ctx, nil, nil, batch, raws, func([]byte) bool { t.Error("unexpected reply"); return true }) {
					t.Fatal("canceled batch succeeded")
				}
				requireBarrierAccounting(t, s.admission, 0, 0, 0)
				return
			}
			c, backend := net.Pipe()
			defer c.Close()
			defer backend.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer backend.Close()
				backend.SetDeadline(time.Now().Add(time.Second))
				r, _ := NewReader(backend, s.cfg.Limits)
				for i := 0; i < 2; i++ {
					if _, _, err := r.ReadCommand(); err != nil {
						return
					}
				}
				replies := ":1\r\n:2\r\n"
				if mode == "lost_reply" {
					replies = ":1\r\n"
				} else if mode == "redis_error" {
					replies = "-ERR not an integer\r\n:2\r\n"
				}
				writeAll(backend, []byte(replies))
			}()
			r, _ := NewReader(c, s.cfg.Limits)
			ok := s.executeBatch(ctx, c, r, batch, raws, func([]byte) bool {
				return mode != "client_failure" && mode != "local_client_failure"
			})
			<-done
			if ok != (mode == "success" || mode == "redis_error") {
				t.Fatal("unexpected completion", ok)
			}
			for _, key := range []string{"a", "b"} {
				h, err := s.admission.enter(key)
				unknown := mode == "local_client_failure" || (key == "b" && (mode == "lost_reply" || mode == "client_failure"))
				if unknown {
					requireBarrierError(t, err, errKeyBarrierUncertain)
				} else if err != nil {
					t.Fatal(err)
				} else {
					h.settle(true)
				}
			}
		})
	}
}

func TestGuardedBackendInitFailureAndLocalRejection(t *testing.T) {
	addr := fakeBackend(t, func(c net.Conn, args [][]byte) bool {
		if string(args[0]) != "INFO" {
			t.Errorf("business command sent: %q", args)
		}
		return false
	})
	s, _ := NewService(passConfig(addr))
	s.admission = testKeyBarrierSet(t, 4, 100, 4)
	c, r, _ := startSession(t, s)
	writeAll(c, encodeCommand("INCR", "a"))
	expect(t, r, "-ERR backend unavailable; command not sent\r\n")
	requireBarrierAccounting(t, s.admission, 0, 0, 0)
	other, _ := NewService(passConfig(addr))
	other.admission = testKeyBarrierSet(t, 4, 100, 4)
	other.frontendPassword = "secret"
	c2, r2, _ := startSession(t, other)
	writeAll(c2, encodeCommand("GET", "a"))
	expect(t, r2, "-NOAUTH Authentication required.\r\n")
	requireBarrierAccounting(t, other.admission, 0, 0, 0)
}

func TestGuardedMultiKeyUnknownAndClosedSet(t *testing.T) {
	s, _ := NewService(passConfig("127.0.0.1:1"))
	s.admission = testKeyBarrierSet(t, 4, 100, 4)
	q := request{args: admissionArgs("MSET", "a", "1", "b", "2", "a", "3")}
	s.admitRequest(&q)
	if len(q.admitted) != 2 || q.local != nil {
		t.Fatal("multi-key command not admitted as one unit")
	}
	b := testKeyBarrier(t, s.admission, "b")
	settleRequest(q, false)
	settleRequest(q, true)
	requireBarrierError(t, b.beginDispatch(), errKeyBarrierUncertain)
	for _, key := range []string{"a", "b"} {
		_, err := s.admission.enter(key)
		requireBarrierError(t, err, errKeyBarrierUncertain)
	}
	s.admission.close()
	for _, args := range [][]string{{"PING"}, {"GET", "other"}} {
		q := request{args: admissionArgs(args...)}
		s.admitRequest(&q)
		if q.local == nil || len(q.admitted) != 0 {
			t.Fatal("closed set accepted request")
		}
	}
}

func TestKeyBarrierManyConcurrentReserve(t *testing.T) {
	g := testKeyBarrierSet(t, 3, 10, 64)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r, err := g.enterMany(admissionArgs("a", "b", "a"))
				if err == nil {
					for _, h := range r {
						h.settle(true)
					}
				} else if err != errKeyBarrierBusy && err != errKeyBarrierBudget {
					t.Error(err)
				}
				b, err := g.reserve("b")
				if err == nil {
					r, err := g.enterMany(admissionArgs("c", "b"))
					if len(r) != 0 || err == nil {
						t.Error("partial multi-key admission under a barrier")
					}
					if err := b.cancel(); err != nil {
						t.Error(err)
					}
				} else if err != errKeyBarrierBusy {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	requireBarrierAccounting(t, g, 0, 0, 0)
}
