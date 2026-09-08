package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
)

const syncerProcessHelperEnv = "REDIS_PILOT_SYNCER_PROCESS_HELPER"

// The current test executable supplies the real CLI/signal boundary without a
// separate build. The parent owns and joins every child, including on failures.
func TestSyncerProcessHelper(t *testing.T) {
	if os.Getenv(syncerProcessHelperEnv) != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			os.Exit(mainWithSignals())
		}
	}
	os.Exit(2)
}

// This is a process lifecycle test, NOT a Redis/Kvrocks correctness or resume
// test. Fixtures never provide an RDB or accept a business write. Both starts
// reach PSYNC against the SAME empty journal; SIGTERM must close capture, exit
// successfully, and release the journal lock for the next process.
func TestSyncerProcessSIGTERMAndEmptyJournalRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	failures := make(chan error, 16)
	psync := make(chan struct{}, 4)
	sourceClosed := make(chan struct{}, 4)
	source := newSyncerProtocolFixture(t, failures, serveEmptySyncerSourceFixture(ctx, psync, sourceClosed))
	cold := newSyncerProtocolFixture(t, failures, serveEmptySyncerColdFixture)
	dir := t.TempDir()
	journal := filepath.Join(dir, "journal")
	config := filepath.Join(dir, "syncer.yaml")
	yaml := fmt.Sprintf("mode: isolated\nconfirm_exclusive_cold: true\nsource_address: %q\ncold_address: %q\nadmin_listen: 127.0.0.1:0\njournal_dir: %q\nallow_unauthenticated: true\nio_timeout: 30s\nshutdown_timeout: 3s\n", source.address(), cold.address(), journal)
	if err := os.WriteFile(config, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	var originalManifest []byte
	var originalLock os.FileInfo
	for attempt := 1; attempt <= 2; attempt++ {
		child := startSyncerProcess(t, ctx, config)
		select {
		case <-psync:
		case err := <-failures:
			t.Fatalf("attempt %d: protocol fixture: %v", attempt, err)
		case <-child.done:
			t.Fatalf("attempt %d: child exited before PSYNC: %v\n%s", attempt, child.err, child.output.String())
		case <-ctx.Done():
			t.Fatalf("attempt %d: did not reach PSYNC: %v\n%s", attempt, ctx.Err(), child.output.String())
		}
		if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("attempt %d: send SIGTERM: %v", attempt, err)
		}
		select {
		case <-child.done:
			if child.err != nil {
				t.Fatalf("attempt %d: SIGTERM exit: %v\n%s", attempt, child.err, child.output.String())
			}
		case err := <-failures:
			t.Fatalf("attempt %d: protocol fixture: %v", attempt, err)
		case <-ctx.Done():
			t.Fatalf("attempt %d: child failed to exit: %v\n%s", attempt, ctx.Err(), child.output.String())
		}
		if !strings.Contains(child.output.String(), `msg="isolated syncer stopped"`) {
			t.Fatalf("attempt %d: missing normal CLI shutdown log: %s", attempt, child.output.String())
		}
		select {
		case <-sourceClosed:
		case err := <-failures:
			t.Fatalf("attempt %d: protocol fixture: %v", attempt, err)
		case <-ctx.Done():
			t.Fatalf("attempt %d: source connection was not closed", attempt)
		}
		manifest, err := os.ReadFile(filepath.Join(journal, "manifest"))
		if err != nil {
			t.Fatal(err)
		}
		lock, err := os.Stat(filepath.Join(journal, "LOCK"))
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 1 {
			originalManifest, originalLock = manifest, lock
		} else if !bytes.Equal(originalManifest, manifest) || !os.SameFile(originalLock, lock) {
			t.Fatal("restart replaced the existing empty manifest or lock instead of reopening it")
		}
	}
	select {
	case err := <-failures:
		t.Fatalf("protocol fixture: %v", err)
	default:
	}
}

// Three independent streams must be alive in the same child before stopping.
// As in the legacy test, these fixtures deliberately provide no RDB or business
// writes: reopening three empty journals is not a data-resumption acceptance test.
func TestSyncerProcessThreeTasksSIGTERMAndEmptyJournalRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	failures := make(chan error, 32)
	dir := t.TempDir()
	config := filepath.Join(dir, "syncer.yaml")
	var yaml strings.Builder
	fmt.Fprintln(&yaml, "mode: isolated\nadmin_listen: 127.0.0.1:0\nio_timeout: 30s\nshutdown_timeout: 3s\ntasks:")
	type taskFixture struct {
		id, journal   string
		psync, closed chan struct{}
		manifest      []byte
		lock          os.FileInfo
	}
	tasks := make([]taskFixture, 3)
	for i := range tasks {
		task := &tasks[i]
		task.id = fmt.Sprintf("shard-%d", i+1)
		task.journal = filepath.Join(dir, task.id)
		task.psync, task.closed = make(chan struct{}, 4), make(chan struct{}, 4)
		source := newSyncerProtocolFixture(t, failures, serveEmptySyncerSourceFixture(ctx, task.psync, task.closed))
		cold := newSyncerProtocolFixture(t, failures, func(conn net.Conn) error {
			return serveEmptySyncerColdTaskFixture(conn, task.id)
		})
		fmt.Fprintf(&yaml, "  - task_id: %s\n    confirm_exclusive_cold: true\n    source_address: %q\n    cold_address: %q\n    journal_dir: %q\n    allow_unauthenticated: true\n", task.id, source.address(), cold.address(), task.journal)
	}
	if err := os.WriteFile(config, []byte(yaml.String()), 0600); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		child := startSyncerProcess(t, ctx, config)
		for _, task := range tasks {
			select {
			case <-task.psync:
			case err := <-failures:
				t.Fatalf("attempt %d: %s protocol fixture: %v", attempt, task.id, err)
			case <-child.done:
				t.Fatalf("attempt %d: child exited before all PSYNCs: %v\n%s", attempt, child.err, child.output.String())
			case <-ctx.Done():
				t.Fatalf("attempt %d: %s did not reach PSYNC: %v\n%s", attempt, task.id, ctx.Err(), child.output.String())
			}
		}
		if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("attempt %d: send SIGTERM: %v", attempt, err)
		}
		select {
		case <-child.done:
			if child.err != nil {
				t.Fatalf("attempt %d: SIGTERM exit: %v\n%s", attempt, child.err, child.output.String())
			}
		case err := <-failures:
			t.Fatalf("attempt %d: protocol fixture: %v", attempt, err)
		case <-ctx.Done():
			t.Fatalf("attempt %d: child failed to exit: %v\n%s", attempt, ctx.Err(), child.output.String())
		}
		output := child.output.String()
		if !strings.Contains(output, `msg="isolated syncer stopped"`) || strings.Count(output, `msg="isolated syncer task stopped"`) != len(tasks) {
			t.Fatalf("attempt %d: CLI did not summarize exactly three tasks and normal service exit: %s", attempt, output)
		}
		for i := range tasks {
			task := &tasks[i]
			if !strings.Contains(output, "task_id="+task.id+" ") {
				t.Fatalf("attempt %d: missing task %s final status: %s", attempt, task.id, output)
			}
			select {
			case <-task.closed:
			case err := <-failures:
				t.Fatalf("attempt %d: %s protocol fixture: %v", attempt, task.id, err)
			case <-ctx.Done():
				t.Fatalf("attempt %d: %s source connection was not closed", attempt, task.id)
			}
			manifest, err := os.ReadFile(filepath.Join(task.journal, "manifest"))
			if err != nil {
				t.Fatal(err)
			}
			lock, err := os.Stat(filepath.Join(task.journal, "LOCK"))
			if err != nil {
				t.Fatal(err)
			}
			if attempt == 1 {
				task.manifest, task.lock = manifest, lock
			} else if !bytes.Equal(task.manifest, manifest) || !os.SameFile(task.lock, lock) {
				t.Fatalf("%s restart replaced existing empty manifest or lock", task.id)
			}
		}
		if strings.Contains(output, dir) || strings.Contains(output, "127.0.0.1:") {
			t.Fatalf("attempt %d: CLI leaked a journal path or backend endpoint: %s", attempt, output)
		}
	}
	select {
	case err := <-failures:
		t.Fatalf("protocol fixture: %v", err)
	default:
	}
}

func serveEmptySyncerSourceFixture(ctx context.Context, psync, closed chan<- struct{}) func(net.Conn) error {
	return func(conn net.Conn) error {
		r, err := gateway.NewReader(conn, gateway.Limits{MaxBytes: 4096, MaxElements: 8, MaxDepth: 2})
		if err != nil {
			return err
		}
		for _, command := range [][]string{{"REPLCONF", "listening-port", "0"}, {"REPLCONF", "capa", "psync2"}} {
			if err := expectSyncerFixtureCommand(r, command); err != nil {
				return err
			}
			if _, err := io.WriteString(conn, "+OK\r\n"); err != nil {
				return err
			}
		}
		if err := expectSyncerFixtureCommand(r, []string{"PSYNC", "?", "-1"}); err != nil {
			return err
		}
		select {
		case psync <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		// Intentionally hold the initial PSYNC response: no snapshot is captured.
		var b [1]byte
		if n, err := conn.Read(b[:]); n != 0 || !errors.Is(err, io.EOF) {
			return fmt.Errorf("source expected peer EOF after SIGTERM; bytes=%d error=%v", n, err)
		}
		select {
		case closed <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
}

func serveEmptySyncerColdFixture(conn net.Conn) error {
	return serveEmptySyncerColdTaskFixture(conn, "")
}

func serveEmptySyncerColdTaskFixture(conn net.Conn, taskID string) error {
	r, err := gateway.NewReader(conn, gateway.Limits{MaxBytes: 4096, MaxElements: 8, MaxDepth: 2})
	if err != nil {
		return err
	}
	info := "# Server\r\nredis_version:7.0.0\r\nkvrocks_version:2.15.0\r\nrole:master\r\ncluster_enabled:0\r\n"
	type exchange struct {
		command []string
		reply   string
	}
	steps := []exchange{
		{[]string{"INFO", "all"}, syncerFixtureBulk(info)},
		{[]string{"CONFIG", "GET", "rocksdb.write_options.sync"}, "*2\r\n" + syncerFixtureBulk("rocksdb.write_options.sync") + syncerFixtureBulk("yes")},
		{[]string{"CONFIG", "GET", "rocksdb.write_options.disable_wal"}, "*2\r\n" + syncerFixtureBulk("rocksdb.write_options.disable_wal") + syncerFixtureBulk("no")},
	}
	tag := "isolated"
	if taskID != "" {
		tag = "task:" + taskID
		steps = append(steps, exchange{[]string{"SCAN", "0", "COUNT", "64", "MATCH", "__pilot_sync_v1__:*"}, "*2\r\n$1\r\n0\r\n*0\r\n"})
	}
	for _, suffix := range []string{"v2-checkpoint", "v2-anchor", "v2-pending"} {
		key := "__pilot_sync_v1__:{" + tag + "}:" + suffix
		steps = append(steps, exchange{[]string{"GET", key}, "$-1\r\n"}, exchange{[]string{"PTTL", key}, ":-2\r\n"})
	}
	steps = append(steps, exchange{[]string{"SCAN", "0", "COUNT", "64"}, "*2\r\n$1\r\n0\r\n*0\r\n"})
	for _, step := range steps {
		if err := expectSyncerFixtureCommand(r, step.command); err != nil {
			return err
		}
		if _, err := io.WriteString(conn, step.reply); err != nil {
			return err
		}
	}
	if _, _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("cold fixture expected EOF after read-only checks, got %v", err)
	}
	return nil
}

func syncerFixtureBulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

func expectSyncerFixtureCommand(r *gateway.Reader, expected []string) error {
	_, raw, err := r.ReadCommand()
	if err != nil {
		return err
	}
	actual := make([]string, len(raw))
	for i, arg := range raw {
		actual[i] = string(arg)
	}
	if !reflect.DeepEqual(expected, actual) {
		return fmt.Errorf("unexpected fixture command: got %q, want %q", actual, expected)
	}
	return nil
}

type syncerProtocolFixture struct {
	listener net.Listener
	accepted chan struct{}
	stopped  chan struct{}
	mu       sync.Mutex
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
}

func newSyncerProtocolFixture(t *testing.T, failures chan<- error, serve func(net.Conn) error) *syncerProtocolFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &syncerProtocolFixture{listener: listener, accepted: make(chan struct{}), stopped: make(chan struct{}), conns: make(map[net.Conn]bool)}
	go func() {
		defer close(f.accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns[conn] = true
			f.mu.Unlock()
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				defer conn.Close()
				defer func() { f.mu.Lock(); delete(f.conns, conn); f.mu.Unlock() }()
				conn.SetDeadline(time.Now().Add(30 * time.Second))
				if err := serve(conn); err != nil {
					select {
					case <-f.stopped:
					case failures <- err:
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		close(f.stopped)
		listener.Close()
		<-f.accepted // no further WaitGroup additions after the accept loop joins
		f.mu.Lock()
		for conn := range f.conns {
			conn.Close()
		}
		f.mu.Unlock()
		done := make(chan struct{})
		go func() { f.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("protocol fixture connections failed to exit")
		}
	})
	return f
}

func (f *syncerProtocolFixture) address() string { return f.listener.Addr().String() }

type syncerProcessOutput struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncerProcessOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncerProcessOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type syncerChildProcess struct {
	cmd    *exec.Cmd
	output syncerProcessOutput
	done   chan struct{}
	err    error // read only after done closes
}

func startSyncerProcess(t *testing.T, ctx context.Context, config string) *syncerChildProcess {
	t.Helper()
	child := &syncerChildProcess{done: make(chan struct{})}
	child.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSyncerProcessHelper$", "--", "-config", config)
	child.cmd.Env = append(os.Environ(), syncerProcessHelperEnv+"=1")
	child.cmd.Stdout, child.cmd.Stderr = &child.output, &child.output
	if err := child.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { child.err = child.cmd.Wait(); close(child.done) }()
	t.Cleanup(func() {
		select {
		case <-child.done:
			return
		default:
			child.cmd.Process.Kill()
		}
		select {
		case <-child.done:
		case <-time.After(5 * time.Second):
			t.Error("syncer child process failed to exit after kill")
		}
	})
	return child
}
