package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func admissionTestOptions(t *testing.T) admissionRunOptions {
	t.Helper()
	id, intent, _ := evictionOperationFixtureData()
	return admissionRunOptions{confirmationPrivateDir(t), confirmationTestBinding(id, intent), true, 8, 1024, 8}
}

func requireDirtyResume(t *testing.T, o admissionRunOptions) *admissionRun {
	t.Helper()
	o.Create = false
	g, err := openAdmissionRun(o, nil)
	if err != nil {
		t.Fatal("dirty must not block ordinary access", err)
	}
	if !g.recoveredDirty {
		g.close()
		t.Fatal("dirty diagnostic lost")
	}
	testKeyRequest(t, g.barriers, "ordinary").settle(true)
	requireBarrierError(t, testKeyBarrier(t, g.barriers, "new-transition").cancel(), nil)
	return g
}

// Real Service listener after recovery: ordinary keys work, durable operation
// keys remain local rejections. No pending business command is replayed.
func assertRecoveredService(t *testing.T, s *Service, checks ...string) {
	t.Helper()
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
	joined := false
	t.Cleanup(func() {
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("recovered service leaked")
			}
		}
	})
	c, err := net.DialTimeout("tcp", data.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	reader, _ := NewReader(c, s.cfg.Limits)
	for i := 0; i < len(checks); i += 2 {
		if err := writeAll(c, encodeCommand("GET", checks[i])); err != nil {
			t.Fatal(err)
		}
		expect(t, reader, checks[i+1])
	}
	c.Close()
	cancel()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovered service stop timeout")
	}
}

func TestAdmissionRunCleanAndDirty(t *testing.T) {
	o := admissionTestOptions(t)
	for i := 0; i < 4; i++ {
		g, err := openAdmissionRun(o, nil)
		if err != nil {
			t.Fatal(err)
		}
		if duplicate, err := openAdmissionRun(o, nil); err == nil {
			duplicate.close()
			t.Fatal("second owner")
		}
		testKeyRequest(t, g.barriers, "k").settle(true)
		if err := g.finish(); err != nil {
			t.Fatal(err)
		}
		if err := g.finish(); err == nil {
			t.Fatal("repeated finish")
		}
		if _, err := g.barriers.enter("k"); err != errKeyBarrierClosed {
			t.Fatal(err)
		}
		o.Create = false
	}
	g, err := openAdmissionRun(o, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.close() // No clean marker, even though no requests were active.
	for i := 0; i < 3; i++ {
		requireDirtyResume(t, o).close()
	}
}

func TestAdmissionRunRetainedState(t *testing.T) {
	for _, kind := range []string{"active", "uncertain", "reserved", "closed"} {
		t.Run(kind, func(t *testing.T) {
			o := admissionTestOptions(t)
			g, err := openAdmissionRun(o, nil)
			if err != nil {
				t.Fatal(err)
			}
			var r *keyRequest
			switch kind {
			case "active":
				r = testKeyRequest(t, g.barriers, "k")
			case "uncertain":
				r = testKeyRequest(t, g.barriers, "k")
				testKeyBarrier(t, g.barriers, "k")
				r.settle(false)
			case "reserved":
				testKeyBarrier(t, g.barriers, "k")
			case "closed":
				g.barriers.close()
			}
			if err := g.finish(); err != nil {
				t.Fatal(err)
			}
			if r != nil {
				r.settle(true)
			} // Late completion cannot change the latch.
			o.Create = false
			requireDirtyResume(t, o).close()
		})
	}
}

func TestAdmissionRunPersistenceFailures(t *testing.T) {
	for _, stage := range []string{"before_write", "after_write", "after_sync", "after_directory_sync"} {
		for _, phase := range []string{"start", "finish"} {
			t.Run(phase+"_"+stage, func(t *testing.T) {
				o := admissionTestOptions(t)
				fault := func(s string) error {
					if s == phase+"_"+stage {
						return errors.New("injected")
					}
					return nil
				}
				g, err := openAdmissionRun(o, fault)
				if phase == "start" {
					if !errors.Is(err, errAdmissionRunIO) || g != nil {
						t.Fatalf("start: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if err := g.finish(); !errors.Is(err, errAdmissionRunIO) {
						t.Fatal(err)
					}
				}
				o.Create = false
				resumed, err := openAdmissionRun(o, nil)
				// A complete clean record may survive an error after the write.
				// This is safe only because finish had no outstanding state.
				wantResume := phase == "finish" || stage != "before_write"
				if (err == nil) != wantResume {
					t.Fatalf("resume: %v, want success %v", err, wantResume)
				}
				if resumed != nil {
					resumed.close()
				}
			})
		}
	}
}

func TestAdmissionRunRejectsUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"missing", "truncated", "trailing", "checksum", "binding", "mode", "symlink", "hardlink", "fifo", "directory", "nonempty_create"} {
		t.Run(kind, func(t *testing.T) {
			o := admissionTestOptions(t)
			path := filepath.Join(o.Dir, admissionRunFile)
			if kind == "nonempty_create" {
				if err := os.WriteFile(filepath.Join(o.Dir, "confirmation-operation.log"), []byte("retained"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if kind != "missing" && kind != "fifo" && kind != "directory" {
				g, err := openAdmissionRun(o, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := g.finish(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "truncated":
					err = os.WriteFile(path, data[:len(data)-1], 0600)
				case "trailing":
					err = os.WriteFile(path, append(data, 'x'), 0600)
				case "checksum":
					data[5] ^= 1
					err = os.WriteFile(path, data, 0600)
				case "binding":
					o.Binding.Epoch++
				case "mode":
					err = os.Chmod(path, 0644)
				case "symlink":
					err = os.Rename(path, path+".saved")
					if err == nil {
						err = os.Symlink(path+".saved", path)
					}
				case "hardlink":
					err = os.Link(path, path+".link")
				}
				if err != nil {
					t.Fatal(err)
				}
			} else if kind == "fifo" {
				if err := unix.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			} else if kind == "directory" {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			o.Create = kind == "nonempty_create"
			g, err := openAdmissionRun(o, nil)
			if err == nil {
				g.close()
				t.Fatal("unsafe file accepted")
			}
		})
	}
}

func TestAdmissionRunTornTransitions(t *testing.T) {
	o := admissionTestOptions(t)
	old := admissionRunRecord{1, evictionHash(o.Binding), strings.Repeat("a", 32), "clean"}
	newRecord := old
	newRecord.BootID = strings.Repeat("b", 32)
	newRecord.State = "dirty"
	for _, phase := range []string{"start", "finish"} {
		if phase == "finish" {
			old = newRecord
			newRecord.State = "clean"
		}
		before, _ := syncSegmentFrame(old)
		after, _ := syncSegmentFrame(newRecord)
		if len(before) != len(after) {
			t.Fatal("record length changed")
		}
		for prefix := 0; prefix <= len(after); prefix++ {
			mixed := append(bytes.Clone(after[:prefix]), before[prefix:]...)
			path := filepath.Join(o.Dir, admissionRunFile)
			if err := os.WriteFile(path, mixed, 0600); err != nil {
				t.Fatal(err)
			}
			o.Create = false
			g, err := openAdmissionRun(o, nil)
			allowed := bytes.Equal(mixed, before) || bytes.Equal(mixed, after)
			if (err == nil) != allowed {
				t.Fatalf("%s prefix %d: %v", phase, prefix, err)
			}
			if g != nil {
				g.close()
			}
		}
	}
}

func TestAdmissionRunProcessCrash(t *testing.T) {
	const childEnv = "REDIS_PILOT_TEST_ADMISSION_CHILD"
	if dir := os.Getenv(childEnv); dir != "" {
		id, intent, _ := evictionOperationFixtureData()
		o := admissionRunOptions{dir, confirmationTestBinding(id, intent), true, 8, 1024, 8}
		g, err := openAdmissionRun(o, nil)
		if err != nil {
			t.Fatal(err)
		}
		testKeyRequest(t, g.barriers, "inflight")
		fmt.Println("admission-ready")
		io.Copy(io.Discard, os.Stdin)
		t.Fatal("child should have been killed")
	}
	o := admissionTestOptions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAdmissionRunProcessCrash$")
	cmd.Env = append(os.Environ(), childEnv+"="+o.Dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || line != "admission-ready\n" {
		t.Fatalf("child readiness: %q %v", line, err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child exited cleanly")
	}
	o.Create = false
	requireDirtyResume(t, o).close()
}

func TestAdmissionRunServiceLifecycle(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint("unknown=", unknown), func(t *testing.T) {
			addr := fakeBackend(t, func(c net.Conn, a [][]byte) bool {
				if string(a[0]) == "INFO" {
					infoReply(c, "master")
					return true
				}
				if unknown {
					return false
				}
				return writeAll(c, []byte(":1\r\n")) == nil
			})
			o := admissionTestOptions(t)
			o.Binding.Source = addr
			cfg := passConfig(addr)
			cfg.GatewayID = o.Binding.GatewayID
			s, _ := NewService(cfg)
			s.admissionBoot = &o
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
			c, err := net.DialTimeout("tcp", data.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(2 * time.Second))
			if err := writeAll(c, encodeCommand("INCR", "k")); err != nil {
				t.Fatal(err)
			}
			r, _ := NewReader(c, cfg.Limits)
			if unknown {
				if _, err := r.ReadFrame(); err == nil {
					t.Fatal("fabricated reply")
				}
			} else {
				expect(t, r, ":1\r\n")
			}
			c.Close()
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("service did not join workers")
			}
			o.Create = false
			g, err := openAdmissionRun(o, nil)
			if err != nil {
				t.Fatal(err)
			}
			if g.recoveredDirty {
				t.Fatal("ordinary reply loss must not prevent a joined clean shutdown")
			}
			testKeyRequest(t, g.barriers, "k").settle(true)
			requireBarrierError(t, testKeyBarrier(t, g.barriers, "k").cancel(), nil)
			g.close()
		})
	}
}

func TestAdmissionRunServiceAcceptsDirtyStartup(t *testing.T) {
	o := admissionTestOptions(t)
	addr := fakeBackend(t, func(c net.Conn, args [][]byte) bool {
		if string(args[0]) == "INFO" {
			infoReply(c, "master")
			return true
		}
		return writeAll(c, []byte("$1\r\nv\r\n")) == nil
	})
	o.Binding.Source = addr
	g, err := openAdmissionRun(o, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.close()
	o.Create = false
	cfg := passConfig(o.Binding.Source)
	cfg.GatewayID = o.Binding.GatewayID
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admissionBoot = &o
	assertRecoveredService(t, s, "ordinary", "$1\r\nv\r\n")
}

func TestAdmissionRunServiceBindingMismatch(t *testing.T) {
	for _, mismatch := range []string{"source", "gateway"} {
		o := admissionTestOptions(t)
		cfg := passConfig(o.Binding.Source)
		cfg.GatewayID = o.Binding.GatewayID
		if mismatch == "source" {
			cfg.RedisAddress = "127.0.0.1:1"
		} else {
			cfg.GatewayID = "other-gateway"
		}
		s, err := NewService(cfg)
		if err != nil {
			t.Fatal(err)
		}
		s.admissionBoot = &o
		if err := s.serve(context.Background(), nil, nil); !errors.Is(err, errAdmissionRun) {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(o.Dir, admissionRunFile)); !os.IsNotExist(err) {
			t.Fatal("invalid binding created state")
		}
	}
}

func TestAdmissionRunResumeStartFailureBeforeWrite(t *testing.T) {
	o := admissionTestOptions(t)
	g, err := openAdmissionRun(o, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.finish(); err != nil {
		t.Fatal(err)
	}
	o.Create = false
	g, err = openAdmissionRun(o, func(s string) error {
		if s == "start_before_write" {
			return errors.New("injected")
		}
		return nil
	})
	if g != nil || !errors.Is(err, errAdmissionRunIO) {
		t.Fatal("failed startup admitted traffic", err)
	}
	// The old clean record remains valid; failed startup never published a set.
	g, err = openAdmissionRun(o, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.close()
}

func TestAdmissionRunCannotSkipExistingOperation(t *testing.T) {
	o := admissionTestOptions(t)
	g, err := openAdmissionRun(o, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.close()
	if err := os.WriteFile(filepath.Join(o.Dir, "confirmation-operation.log"), []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	o.Create = false
	cfg := passConfig(o.Binding.Source)
	cfg.GatewayID = o.Binding.GatewayID
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admissionBoot = &o
	if err := s.serve(context.Background(), nil, nil); !errors.Is(err, errAdmissionRun) {
		t.Fatal("missing controller bypassed operation recovery", err)
	}
}
