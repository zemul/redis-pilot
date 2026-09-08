package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSyncTransportClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"EOF", io.EOF, true},
		{"partial", io.ErrUnexpectedEOF, true},
		{"network", &net.OpError{Op: "read", Net: "tcp", Err: io.EOF}, true},
		{"network-timeout", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, true},
		{"context-cancel", context.Canceled, false},
		{"context-deadline", context.DeadlineExceeded, false},
		{"wrapped-context", &net.OpError{Op: "read", Net: "tcp", Err: context.DeadlineExceeded}, false},
		{"buffer-limit", bufio.ErrBufferFull, false},
		{"short-write", io.ErrShortWrite, false},
		{"protocol", errors.New("invalid protocol"), false},
		{"history", errSyncHistoryGap, false},
		{"journal", errSyncJournalCorrupt, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSyncTransportError(tc.err); got != tc.want {
				t.Fatalf("classification=%v want=%v", got, tc.want)
			}
		})
	}
	cause := &net.OpError{Op: "SECRET-REMOTE-ERROR", Net: "tcp", Err: io.EOF}
	err := syncTransportFailure(context.Background(), errSyncSourceTransport, "read", cause)
	if !errors.Is(err, errSyncSourceTransport) || !errors.Is(err, io.EOF) || strings.Contains(err.Error(), "SECRET") {
		t.Fatal("transport wrapper lost class/cause or exposed underlying message", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := syncTransportFailure(ctx, errSyncColdConnect, "connect", cause); !errors.Is(err, context.Canceled) || errors.Is(err, errSyncColdConnect) {
		t.Fatal("cancellation was granted retry authority", err)
	}
	for _, fatal := range []error{nil, errSyncTargetDrift, errSyncJournalFull, errSyncJournalCorrupt, bufio.ErrBufferFull, errors.New("invalid protocol")} {
		if err := syncTransportFailure(ctx, errSyncColdConnect, "read", fatal); err != nil {
			t.Fatal("cancellation masked a terminal non-network outcome", err)
		}
	}
}

func TestSyncTransportControlLine(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		retry       bool
	}{
		{"empty-EOF", "", true},
		{"partial-EOF", "+CONT", true},
		{"invalid-LF", "+CONTINUE\n", false},
		{"oversized-line", strings.Repeat("x", 1100) + "\r\n", false},
		{"oversized-partial", strings.Repeat("x", 1100), false},
		{"buffer-full", strings.Repeat("x", 5000), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := syncLine(bufio.NewReaderSize(strings.NewReader(tc.input), 4096))
			if err == nil || isSyncTransportError(err) != tc.retry {
				t.Fatal("unexpected control-line classification", err)
			}
		})
	}
}

func syncTransportServer(t *testing.T, serve func(net.Conn, *Reader) error) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	t.Cleanup(func() { listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		defer listener.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		r, _ := NewReader(conn, DefaultConfig().Limits)
		done <- serve(conn, r)
	}()
	return listener.Addr().String(), done
}

func syncTransportExpect(r *Reader, name string) error {
	_, args, err := r.ReadCommand()
	if err != nil {
		return err
	}
	if len(args) == 0 || string(args[0]) != name {
		return errors.New("unexpected test command")
	}
	return nil
}

func syncTransportAwait(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("fake server failed", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("fake server did not stop")
	}
}

func syncTransportPassword(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("SECRET-PASSWORD"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSyncSourceTransportReplies(t *testing.T) {
	for _, tc := range []struct {
		name, phase, reply           string
		auth, seeded, retry, history bool
	}{
		{name: "auth-rejected", phase: "AUTH", reply: "-ERR SECRET-PASSWORD denied\r\n", auth: true},
		{name: "auth-disconnected", phase: "AUTH", auth: true, retry: true},
		{name: "handshake-disconnected", phase: "REPLCONF", retry: true},
		{name: "handshake-rejected", phase: "REPLCONF", reply: "-ERR unsupported\r\n"},
		{name: "PSYNC-disconnected", phase: "PSYNC", retry: true},
		{name: "PSYNC-partial", phase: "PSYNC", reply: "+CONT", retry: true},
		{name: "PSYNC-malformed", phase: "PSYNC", reply: "+nonsense\r\n"},
		{name: "PSYNC-oversized", phase: "PSYNC", reply: strings.Repeat("x", 1100) + "\r\n"},
		{name: "history-gap", phase: "PSYNC", reply: "+FULLRESYNC " + strings.Repeat("b", 40) + " 1\r\n", seeded: true, history: true},
		{name: "continued-new-history", phase: "PSYNC", reply: "+CONTINUE " + strings.Repeat("b", 40) + "\r\n", seeded: true, history: true},
		{name: "RDB-header-disconnected", phase: "RDB", retry: true},
		{name: "RDB-too-large", phase: "RDB", reply: "$999999999\r\n"},
		{name: "RDB-half-read", phase: "RDB", reply: "$100\r\nREDIS0011partial", retry: true},
		{name: "command-half-read", phase: "COMMAND", reply: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$8\r\npar", retry: true},
		{name: "command-invalid-type", phase: "COMMAND", reply: "+OK\r\n"},
		{name: "command-invalid-CRLF", phase: "COMMAND", reply: "*1\r\n$4\r\nPINGxx"},
		{name: "command-oversized-array", phase: "COMMAND", reply: "*999999\r\n"},
		{name: "command-oversized-bulk", phase: "COMMAND", reply: "*1\r\n$999999999\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, done := syncTransportServer(t, func(c net.Conn, r *Reader) error {
				commands := []string{"REPLCONF", "REPLCONF", "PSYNC"}
				if tc.auth {
					commands = append([]string{"AUTH"}, commands...)
				}
				for _, command := range commands {
					if err := syncTransportExpect(r, command); err != nil {
						return err
					}
					if tc.phase == command {
						return writeAll(c, []byte(tc.reply))
					}
					if command != "PSYNC" {
						if err := writeAll(c, []byte("+OK\r\n")); err != nil {
							return err
						}
					}
				}
				if err := writeAll(c, []byte("+FULLRESYNC "+strings.Repeat("a", 40)+" 0\r\n")); err != nil {
					return err
				}
				if tc.phase == "RDB" {
					return writeAll(c, []byte(tc.reply))
				}
				if err := writeAll(c, []byte("$9\r\nREDIS0011")); err != nil {
					return err
				}
				if err := syncTransportExpect(r, "REPLCONF"); err != nil {
					return err
				}
				return writeAll(c, []byte(tc.reply))
			})
			j, _ := captureJournal(t, 1<<20)
			if tc.seeded {
				if err := j.append(captureSnapshot(address)); err != nil {
					t.Fatal(err)
				}
			}
			source := syncSource{address: address, timeout: time.Second, maxSnapshot: 1 << 20}
			if tc.auth {
				source.passwordFile = syncTransportPassword(t)
			}
			err := source.capture(context.Background(), j)
			if err == nil || errors.Is(err, errSyncSourceTransport) != tc.retry || errors.Is(err, errSyncHistoryGap) != tc.history {
				t.Fatalf("retry=%v history=%v error=%v", tc.retry, tc.history, err)
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatal("source exposed remote reply/password")
			}
			if tc.phase == "COMMAND" {
				if j.durablePosition().History == "" || j.durablePosition().Offset != 0 {
					t.Fatal("invalid/partial command advanced durable capture")
				}
			}
			syncTransportAwait(t, done)
		})
	}
}

type syncTransportFailLog struct{ failure error }

func (j syncTransportFailLog) durablePosition() syncPosition { return syncPosition{} }
func (j syncTransportFailLog) append(syncRecord) error       { return j.failure }

type syncTransportCancelLog struct {
	mu       sync.Mutex
	position syncPosition
	cancel   context.CancelFunc
	failure  error
}

func (j *syncTransportCancelLog) durablePosition() syncPosition {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.position
}

func (j *syncTransportCancelLog) append(record syncRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if record.Kind == "command" {
		// Deterministically race a terminal append failure with cancellation
		// after the ACK goroutine has started. Its deferred join must not
		// replace the terminal result with context.Canceled.
		j.cancel()
		return j.failure
	}
	j.position = record.Position
	return nil
}

func TestSyncSourceTransportCancellationPreservesTerminal(t *testing.T) {
	for _, fatal := range []error{errSyncJournalFull, errSyncJournalCorrupt, errSyncUnsupported} {
		t.Run(fatal.Error(), func(t *testing.T) {
			address, done := syncTransportServer(t, func(c net.Conn, r *Reader) error {
				for _, command := range []string{"REPLCONF", "REPLCONF", "PSYNC"} {
					if err := syncTransportExpect(r, command); err != nil {
						return err
					}
					if command != "PSYNC" {
						if err := writeAll(c, []byte("+OK\r\n")); err != nil {
							return err
						}
					}
				}
				if err := writeAll(c, []byte("+FULLRESYNC "+strings.Repeat("a", 40)+" 0\r\n$9\r\nREDIS0011")); err != nil {
					return err
				}
				if err := syncTransportExpect(r, "REPLCONF"); err != nil {
					return err
				}
				return writeAll(c, encodeCommand("SET", "key", "value"))
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log := &syncTransportCancelLog{cancel: cancel, failure: fatal}
			err := (syncSource{address: address, timeout: time.Second, maxSnapshot: 1 << 20}).capture(ctx, log)
			if ctx.Err() == nil || err != fatal || errors.Is(err, context.Canceled) || errors.Is(err, errSyncSourceTransport) {
				t.Fatal("terminal capture outcome was masked by cancellation", err)
			}
			syncTransportAwait(t, done)
		})
	}
}

func TestSyncSourceTransportJournalFailureIsFatal(t *testing.T) {
	address, done := syncTransportServer(t, func(c net.Conn, r *Reader) error {
		for _, command := range []string{"REPLCONF", "REPLCONF", "PSYNC"} {
			if err := syncTransportExpect(r, command); err != nil {
				return err
			}
			if command != "PSYNC" {
				if err := writeAll(c, []byte("+OK\r\n")); err != nil {
					return err
				}
			}
		}
		return writeAll(c, []byte("+FULLRESYNC "+strings.Repeat("a", 40)+" 0\r\n$9\r\nREDIS0011"))
	})
	// Even an error whose concrete type resembles a transport failure came
	// from the journal, not the network; the capture layer may not relabel it.
	cause := &net.OpError{Op: "journal", Net: "local", Err: io.ErrUnexpectedEOF}
	err := (syncSource{address: address, timeout: time.Second, maxSnapshot: 1 << 20}).capture(context.Background(), syncTransportFailLog{cause})
	if err != cause || errors.Is(err, errSyncSourceTransport) {
		t.Fatal("journal failure was incorrectly made retryable", err)
	}
	syncTransportAwait(t, done)
}

func syncTransportBulk(body string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(body), body))
}

func TestSyncColdConnectTransport(t *testing.T) {
	goodInfo := "redis_version:7.0.0\r\nrole:master\r\ncluster_enabled:0\r\nkvrocks_version:2.15.0\r\n"
	for _, tc := range []struct {
		name, phase, reply             string
		auth, retry, success, business bool
	}{
		{name: "AUTH-denied", phase: "AUTH", reply: "-ERR SECRET-PASSWORD wrong\r\n", auth: true},
		{name: "AUTH-disconnected", phase: "AUTH", auth: true, retry: true},
		{name: "INFO-disconnected", phase: "INFO", retry: true},
		{name: "INFO-partial", phase: "INFO", reply: "$100\r\npartial", retry: true},
		{name: "INFO-invalid", phase: "INFO", reply: "!bad\r\n"},
		{name: "INFO-oversized", phase: "INFO", reply: "$999999999\r\n"},
		{name: "wrong-version", phase: "INFO", reply: string(syncTransportBulk(strings.ReplaceAll(goodInfo, "2.15.0", "2.14.0")))},
		{name: "replica", phase: "INFO", reply: string(syncTransportBulk(strings.ReplaceAll(goodInfo, "role:master", "role:slave")))},
		{name: "cluster", phase: "INFO", reply: string(syncTransportBulk(strings.ReplaceAll(goodInfo, "cluster_enabled:0", "cluster_enabled:1")))},
		{name: "CONFIG-disconnected", phase: "CONFIG1", retry: true},
		{name: "CONFIG-rejected", phase: "CONFIG1", reply: "-ERR forbidden\r\n"},
		{name: "WAL-not-sync", phase: "CONFIG1", reply: "*2\r\n$26\r\nrocksdb.write_options.sync\r\n$2\r\nno\r\n"},
		{name: "WAL-disabled", phase: "CONFIG2", reply: "-ERR WAL disabled\r\n"},
		{name: "CONFIG2-disconnected", phase: "CONFIG2", retry: true},
		{name: "valid", success: true, auth: true},
		{name: "business-write-disconnected-is-not-connect", success: true, business: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, done := syncTransportServer(t, func(c net.Conn, r *Reader) error {
				if tc.auth {
					if err := syncTransportExpect(r, "AUTH"); err != nil {
						return err
					}
					if tc.phase == "AUTH" {
						return writeAll(c, []byte(tc.reply))
					}
					if err := writeAll(c, []byte("+OK\r\n")); err != nil {
						return err
					}
				}
				if err := syncTransportExpect(r, "INFO"); err != nil {
					return err
				}
				if tc.phase == "INFO" {
					return writeAll(c, []byte(tc.reply))
				}
				if err := writeAll(c, syncTransportBulk(goodInfo)); err != nil {
					return err
				}
				for i, item := range []struct{ key, value string }{{"rocksdb.write_options.sync", "yes"}, {"rocksdb.write_options.disable_wal", "no"}} {
					if err := syncTransportExpect(r, "CONFIG"); err != nil {
						return err
					}
					if tc.phase == fmt.Sprintf("CONFIG%d", i+1) {
						return writeAll(c, []byte(tc.reply))
					}
					if err := writeAll(c, []byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(item.key), item.key, len(item.value), item.value))); err != nil {
						return err
					}
				}
				if tc.business {
					return syncTransportExpect(r, "SET")
				}
				return nil
			})
			target := syncColdTarget{address: address, timeout: time.Second}
			if tc.auth {
				target.passwordFile = syncTransportPassword(t)
			}
			client, closeClient, err := target.open(context.Background())
			if tc.success {
				if err != nil {
					t.Fatal("valid cold setup failed", err)
				}
				if tc.business {
					_, err := client.request("SET", "key", "value")
					if err == nil || errors.Is(err, errSyncColdConnect) {
						t.Fatal("business write acquired connect retry authority", err)
					}
				}
				closeClient()
			} else if err == nil || errors.Is(err, errSyncColdConnect) != tc.retry {
				t.Fatalf("retry=%v got=%v", tc.retry, err)
			}
			if err != nil && strings.Contains(err.Error(), "SECRET") {
				t.Fatal("cold setup exposed remote reply/password")
			}
			syncTransportAwait(t, done)
		})
	}
}

func TestSyncTransportConnectAndCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	source := syncSource{address: address, timeout: time.Second, maxSnapshot: 1 << 20}
	target := syncColdTarget{address: address, timeout: time.Second}
	j, _ := captureJournal(t, 1<<20)
	if err := source.capture(context.Background(), j); !errors.Is(err, errSyncSourceTransport) {
		t.Fatal("source dial not retryable", err)
	}
	if _, _, err := target.open(context.Background()); !errors.Is(err, errSyncColdConnect) {
		t.Fatal("cold dial not retryable", err)
	}
	for _, which := range []string{"source", "cold"} {
		t.Run(which, func(t *testing.T) {
			readStarted := make(chan struct{})
			address, done := syncTransportServer(t, func(c net.Conn, r *Reader) error {
				if _, _, err := r.ReadCommand(); err != nil {
					return err
				}
				close(readStarted)
				_, err := io.Copy(io.Discard, c)
				return err
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { <-readStarted; cancel() }()
			var err error
			if which == "source" {
				source.address = address
				err = source.capture(ctx, j)
			} else {
				target.address = address
				_, _, err = target.open(ctx)
			}
			if !errors.Is(err, context.Canceled) || errors.Is(err, errSyncSourceTransport) || errors.Is(err, errSyncColdConnect) {
				t.Fatal("cancellation was not terminal", err)
			}
			syncTransportAwait(t, done)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := source.capture(ctx, j); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := target.open(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
