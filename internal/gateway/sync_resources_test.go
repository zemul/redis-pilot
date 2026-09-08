package gateway

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func syncResourceFixture(t *testing.T, policy syncResourcePolicy) *syncSegmentFixture {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "segments")
	s, err := openSyncSegmentsWithPolicy(dir, 1024, 1<<20, policy)
	if err != nil {
		t.Fatal(err)
	}
	f := &syncSegmentFixture{dir: dir, store: s}
	t.Cleanup(func() { f.store.close() })
	s.availableBytes = func() (uint64, error) { return 1 << 40, nil }
	snapshot := captureSnapshot("127.0.0.1:12345")
	snapshot.Data = syncTestRDB()
	if err := s.append(snapshot); err != nil {
		t.Fatal(err)
	}
	f.records = append(f.records, snapshot)
	f.points = append(f.points, nextPendingCheckpoint(syncApplyCheckpoint{}, snapshot))
	return f
}

func syncResourceFileSizes(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	sizes := make(map[string]int64, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		sizes[entry.Name()] = info.Size()
	}
	return sizes
}

func syncResourceStatusForTest(t *testing.T, s *syncSegments) syncResourceStatus {
	t.Helper()
	status, err := s.resourceStatus()
	if err != nil && !errors.Is(err, errSyncCapturePressure) {
		t.Fatal(err)
	}
	return status
}

func TestSyncResourcePolicy(t *testing.T) {
	p := defaultSyncResourcePolicy()
	if p.MinFreeBytes != 64<<20 || p.ResumeFreeBytes != 128<<20 || p.MaxUnconfirmedBytes != 64<<20 || p.ResumeUnconfirmedBytes != 32<<20 || p.MaxViews != 4 || p.MaxDirectoryEntries != 4096 {
		t.Fatalf("unexpected experimental defaults: %+v", p)
	}
	for name, mutate := range map[string]func(*syncResourcePolicy){
		"zero":             func(p *syncResourcePolicy) { *p = syncResourcePolicy{} },
		"free_order":       func(p *syncResourcePolicy) { p.ResumeFreeBytes = p.MinFreeBytes - 1 },
		"negative_backlog": func(p *syncResourcePolicy) { p.MaxUnconfirmedBytes = -1 },
		"negative_resume":  func(p *syncResourcePolicy) { p.ResumeUnconfirmedBytes = -1 },
		"backlog_order":    func(p *syncResourcePolicy) { p.ResumeUnconfirmedBytes = p.MaxUnconfirmedBytes + 1 },
		"no_views":         func(p *syncResourcePolicy) { p.MaxViews = 0 },
		"negative_views":   func(p *syncResourcePolicy) { p.MaxViews = -1 },
		"no_directory_cap": func(p *syncResourcePolicy) { p.MaxDirectoryEntries = 0 },
		"negative_dir_cap": func(p *syncResourcePolicy) { p.MaxDirectoryEntries = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			policy := defaultSyncResourcePolicy()
			mutate(&policy)
			dir := filepath.Join(t.TempDir(), "rejected")
			s, err := openSyncSegmentsWithPolicy(dir, 1024, 1<<20, policy)
			if err == nil {
				s.close()
				t.Fatal("invalid resource policy accepted")
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid policy changed filesystem: %v", err)
			}
		})
	}
}

func TestSyncResourceAvailabilityArithmetic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		blocks uint64
		unit   int64
		want   uint64
		bad    bool
	}{
		{name: "empty", unit: 4096},
		{name: "normal", blocks: 123, unit: 4096, want: 123 * 4096},
		{name: "uint64_limit", blocks: math.MaxUint64, unit: 1, want: math.MaxUint64},
		{name: "large_exact", blocks: math.MaxUint64 / 4096, unit: 4096, want: (math.MaxUint64 / 4096) * 4096},
		{name: "zero_unit", blocks: 1, unit: 0, bad: true},
		{name: "negative_unit", blocks: 1, unit: -1, bad: true},
		{name: "overflow", blocks: math.MaxUint64/4096 + 1, unit: 4096, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := syncBlockAvailable(tc.blocks, tc.unit)
			if (err != nil) != tc.bad || !tc.bad && got != tc.want {
				t.Fatalf("available(%d, %d) = %d, %v; want %d bad=%t", tc.blocks, tc.unit, got, err, tc.want, tc.bad)
			}
		})
	}
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	if _, err := syncAvailableBytes(int(f.store.dir.Fd())); err != nil {
		t.Fatal("real filesystem availability sample failed", err)
	}
	if _, err := syncAvailableBytes(-1); err == nil {
		t.Fatal("invalid filesystem descriptor was accepted")
	}
	for _, values := range [][2]int64{{-1, 0}, {0, -1}, {math.MaxInt64, 1}, {1, math.MaxInt64}} {
		if got := captureWriteBudget(values[0], values[1], true); got != math.MaxUint64 {
			t.Fatalf("invalid write budget %v did not fail closed: %d", values, got)
		}
	}
	if got, want := captureWriteBudget(100, 20, false), uint64(120)+syncAllocationSlack; got != want {
		t.Fatalf("ordinary write budget %d, want %d", got, want)
	}
	if got, want := captureWriteBudget(100, 20, true), uint64(120+syncManifestLimit)+2*syncAllocationSlack; got != want {
		t.Fatalf("publication write budget %d, want %d", got, want)
	}
}

func TestSyncResourceDiskPressureAndHysteresis(t *testing.T) {
	policy := defaultSyncResourcePolicy()
	policy.MinFreeBytes, policy.ResumeFreeBytes = 16<<20, 32<<20
	f := syncResourceFixture(t, policy)
	f.append(t, 300)
	position := f.store.durablePosition()
	generation := f.store.manifest.Generation
	files := syncResourceFileSizes(t, f.dir)
	r := f.command(len(f.records), 300) // This record needs a new segment.
	available := policy.MinFreeBytes / 2
	f.store.availableBytes = func() (uint64, error) { return available, nil }
	if err := f.store.append(r); !errors.Is(err, errSyncCapturePressure) {
		t.Fatalf("low disk did not stop capture: %v", err)
	}
	if f.store.poisoned || f.store.durablePosition() != position || f.store.manifest.Generation != generation || !reflect.DeepEqual(files, syncResourceFileSizes(t, f.dir)) {
		t.Fatal("pressure rejection changed journal or poisoned its handle")
	}
	status := syncResourceStatusForTest(t, f.store)
	if !status.DiskPaused || status.AvailableBytes != available {
		t.Fatalf("missing low disk status: %+v", status)
	}
	available = (policy.MinFreeBytes + policy.ResumeFreeBytes) / 2
	if err := f.store.append(r); !errors.Is(err, errSyncCapturePressure) {
		t.Fatalf("capture resumed inside hysteresis band: %v", err)
	}
	if !syncResourceStatusForTest(t, f.store).DiskPaused {
		t.Fatal("status cleared disk pause before resume threshold")
	}
	available = policy.ResumeFreeBytes * 2
	if syncResourceStatusForTest(t, f.store).DiskPaused {
		t.Fatal("status did not resample recovery above high watermark")
	}
	if err := f.store.append(r); err != nil {
		t.Fatal("same record could not be retried after pressure lifted", err)
	}
	if f.store.durablePosition() != r.Position {
		t.Fatal("successful retry did not advance capture once")
	}
}

func TestSyncResourceDiskSampleFailsClosed(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	f.append(t, 300)
	position := f.store.durablePosition()
	files := syncResourceFileSizes(t, f.dir)
	f.store.availableBytes = func() (uint64, error) { return 1 << 40, errors.New("statfs unavailable") }
	if err := f.store.append(f.command(len(f.records), 300)); err == nil {
		t.Fatal("failed disk sample permitted capture")
	}
	if _, err := f.store.resourceStatus(); err == nil {
		t.Fatal("status hid disk sampling error")
	}
	if f.store.poisoned || f.store.durablePosition() != position || !reflect.DeepEqual(files, syncResourceFileSizes(t, f.dir)) {
		t.Fatal("sampling failure changed durable state or poisoned journal")
	}
	f.store.availableBytes = func() (uint64, error) { return 1 << 40, nil }
	f.append(t, 300)
}

func TestSyncResourceWriteBudgetBoundaries(t *testing.T) {
	for _, kind := range []string{"snapshot", "rotation", "active_append"} {
		t.Run(kind, func(t *testing.T) {
			policy := defaultSyncResourcePolicy()
			dir := filepath.Join(t.TempDir(), "segments")
			s, err := openSyncSegmentsWithPolicy(dir, 16<<10, 1<<20, policy)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.close() })
			available := uint64(1 << 40)
			s.availableBytes = func() (uint64, error) { return available, nil }
			r := captureSnapshot("127.0.0.1:12345")
			r.Data = syncTestRDB()
			if kind != "snapshot" {
				if err := s.append(r); err != nil {
					t.Fatal(err)
				}
				if kind == "active_append" {
					raw := encodeCommand("SET", "budget-first", "v")
					position := s.durablePosition()
					position.Offset += int64(len(raw))
					if err := s.append(syncRecord{Kind: "command", Position: position, Data: raw}); err != nil {
						t.Fatal(err)
					}
				}
				raw := encodeCommand("SET", "budget-next", "v")
				position := s.durablePosition()
				position.Offset += int64(len(raw))
				r = syncRecord{Kind: "command", Position: position, Data: raw}
			}
			frame, err := syncSegmentFrame(r)
			if err != nil {
				t.Fatal(err)
			}
			headerSize := int64(0)
			if kind == "snapshot" {
				headerSize = int64(len(syncAnchorMagic))
			} else if kind == "rotation" {
				// Random IDs have a fixed encoded width; reproduce the exact
				// header length without inspecting a file that must not exist yet.
				h, err := syncSegmentFrame(syncSegmentHeader{1, "segment-" + strings.Repeat("0", 32), s.endLocked()})
				if err != nil {
					t.Fatal(err)
				}
				headerSize = int64(len(syncSegmentMagic) + len(h))
			}
			budget := captureWriteBudget(int64(len(frame)), headerSize, kind != "active_append")
			position := s.durablePosition()
			files := syncResourceFileSizes(t, dir)
			generation := s.manifest.Generation
			available = policy.MinFreeBytes + budget - 1
			if err := s.append(r); !errors.Is(err, errSyncCapturePressure) {
				t.Fatalf("write admitted one byte below reserve plus budget: %v", err)
			}
			if s.poisoned || s.durablePosition() != position || s.manifest.Generation != generation || !reflect.DeepEqual(files, syncResourceFileSizes(t, dir)) {
				t.Fatal("budget rejection left partial filesystem changes")
			}
			available = policy.ResumeFreeBytes + budget - 1
			if err := s.append(r); !errors.Is(err, errSyncCapturePressure) {
				t.Fatalf("paused write admitted one byte below resume plus budget: %v", err)
			}
			available = policy.ResumeFreeBytes + budget
			if err := s.append(r); err != nil {
				t.Fatal("exactly sufficient admission budget was rejected", err)
			}
			if s.durablePosition() != r.Position {
				t.Fatal("exact-budget append did not advance capture position")
			}
		})
	}
}

func TestSyncResourceBacklogUsesConfirmedSourceBytes(t *testing.T) {
	policy := defaultSyncResourcePolicy()
	rawBytes := int64(len(encodeCommand("SET", "segment-key-0001", strings.Repeat("v", 300))))
	policy.MaxUnconfirmedBytes = 3*rawBytes + rawBytes/2
	policy.ResumeUnconfirmedBytes = rawBytes + 1
	f := syncResourceFixture(t, policy)
	initial := syncResourceStatusForTest(t, f.store)
	if initial.UnconfirmedBytes != 0 || initial.LiveBytes == 0 {
		t.Fatalf("anchor counted as source backlog or disappeared from storage: %+v", initial)
	}
	for i := 0; i < 3; i++ {
		f.append(t, 300)
	}
	status := syncResourceStatusForTest(t, f.store)
	if status.UnconfirmedBytes != 3*rawBytes || status.LiveBytes <= status.UnconfirmedBytes {
		t.Fatalf("source offset lag was confused with framed file bytes: %+v", status)
	}
	position := f.store.durablePosition()
	r := f.command(len(f.records), 300)
	if err := f.store.append(r); !errors.Is(err, errSyncCapturePressure) {
		t.Fatalf("unconfirmed backlog did not pause capture: %v", err)
	}
	if f.store.poisoned || f.store.durablePosition() != position {
		t.Fatal("backlog limit advanced capture or poisoned journal")
	}
	if !syncResourceStatusForTest(t, f.store).BacklogPaused {
		t.Fatal("backlog pause did not persist below max but above resume")
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	if err := v.walkFrom(v.base, func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := syncResourceStatusForTest(t, f.store).UnconfirmedBytes; got != 3*rawBytes {
		t.Fatalf("reading the journal silently confirmed cold writes: %d", got)
	}
	// The fixture supplies the same proof object as the guarded cold protocol.
	// This is a local resource test, not a replacement for cold durability checks.
	if err := f.store.prune(v, f.meta(t, 2)); err != nil {
		t.Fatal(err)
	}
	status = syncResourceStatusForTest(t, f.store)
	if status.UnconfirmedBytes != rawBytes || status.BacklogPaused {
		t.Fatalf("confirmed prune did not lower backlog below resume: %+v", status)
	}
	if err := f.store.append(r); err != nil {
		t.Fatal("capture could not resume after confirmation", err)
	}
}

func TestSyncResourceBacklogZeroResumeNeedsNoStatusCall(t *testing.T) {
	policy := defaultSyncResourcePolicy()
	rawBytes := int64(len(encodeCommand("SET", "segment-key-0001", strings.Repeat("v", 300))))
	policy.MaxUnconfirmedBytes = rawBytes + rawBytes/2
	policy.ResumeUnconfirmedBytes = 0 // Resume only once cold confirmation catches up.
	f := syncResourceFixture(t, policy)
	f.append(t, 300)
	r := f.command(len(f.records), 300)
	if err := f.store.append(r); !errors.Is(err, errSyncCapturePressure) {
		t.Fatalf("test did not reach unconfirmed backlog limit: %v", err)
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	if err := f.store.prune(v, f.meta(t, len(f.records)-1)); err != nil {
		t.Fatal(err)
	}
	// No resourceStatus call between GC and retry: requesting diagnostics must
	// not be a prerequisite for continuing a now-safe replication stream.
	if err := f.store.append(r); err != nil {
		t.Fatal("fully confirmed journal cannot resume directly with zero resume watermark", err)
	}
	if f.store.durablePosition() != r.Position {
		t.Fatal("resumed record did not advance capture exactly once")
	}
}

func TestSyncResourcePruneCanUseCaptureReserve(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	for i := 0; i < 6; i++ {
		f.append(t, 300)
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	f.store.availableBytes = func() (uint64, error) { return 32 << 20, nil }
	if err := f.store.append(f.command(len(f.records), 300)); !errors.Is(err, errSyncCapturePressure) {
		t.Fatalf("capture did not preserve recovery reserve: %v", err)
	}
	before := len(segmentTestFiles(t, f.dir, "segment-"))
	if err := f.store.prune(v, f.meta(t, len(f.records)-1)); err != nil {
		t.Fatal("capture disk pause blocked safe GC with ample metadata space", err)
	}
	if after := len(segmentTestFiles(t, f.dir, "segment-")); after >= before {
		t.Fatal("reserve-backed prune did not retire old segments")
	}
	if !syncResourceStatusForTest(t, f.store).DiskPaused {
		t.Fatal("logical retirement was incorrectly treated as physical free space")
	}
}

func TestSyncResourceViewBudgetAndClose(t *testing.T) {
	policy := defaultSyncResourcePolicy()
	policy.MaxViews = 2
	f := syncResourceFixture(t, policy)
	for i := 0; i < 6; i++ {
		f.append(t, 300)
	}
	old, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer old.close()
	latest, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer latest.close()
	if extra, err := f.store.pendingView(); !errors.Is(err, errSyncViewLimit) {
		if extra != nil {
			extra.close()
		}
		t.Fatalf("view budget not enforced: %v", err)
	}
	if got := syncResourceStatusForTest(t, f.store).Views; got != 2 {
		t.Fatalf("rejected view leaked a budget slot: %d", got)
	}
	if err := f.store.prune(latest, f.meta(t, len(f.records)-1)); err != nil {
		t.Fatal(err)
	}
	if err := latest.close(); err != nil {
		t.Fatal(err)
	}
	if err := latest.close(); err != nil {
		t.Fatal("view close is not idempotent", err)
	}
	if got := syncResourceStatusForTest(t, f.store).Views; got != 1 {
		t.Fatalf("duplicate close changed view count twice: %d", got)
	}
	var got []syncRecord
	if err := old.walkFrom(old.base, func(r syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, r); return nil }); err != nil || !reflect.DeepEqual(got, f.records[1:]) {
		t.Fatal("GC invalidated a budgeted old view", err)
	}
	replacement, err := f.store.pendingView()
	if err != nil {
		t.Fatal("closing view did not restore budget", err)
	}
	replacement.close()
	old.close()
	if got := syncResourceStatusForTest(t, f.store).Views; got != 0 {
		t.Fatalf("closed views still charged: %d", got)
	}
}

func TestSyncResourceRedisPressureResume(t *testing.T) {
	address := syncTestRedis(t)
	conn, reader, err := authenticated(address, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	command := func(args ...string) string {
		t.Helper()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := writeAll(conn, encodeCommand(args...)); err != nil {
			t.Fatal(err)
		}
		raw, err := reader.ReadFrame()
		if err != nil || len(raw) == 0 || raw[0] == '-' {
			t.Fatalf("command %s: %q, %v", args[0], raw, err)
		}
		return string(raw)
	}
	command("CONFIG", "SET", "repl-diskless-sync", "no")
	command("CONFIG", "SET", "repl-backlog-size", "1048576")
	command("SET", "resource-snapshot-seed", "present")
	dir := filepath.Join(t.TempDir(), "segments")
	s, err := openSyncSegmentsWithPolicy(dir, 1024, 1<<20, defaultSyncResourcePolicy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })
	var available atomic.Uint64
	available.Store(1 << 40)
	s.availableBytes = func() (uint64, error) { return available.Load(), nil }
	source := syncSource{address: address, passwordFile: secretFile(t, "g4-backend-test"), timeout: 5 * time.Second, maxSnapshot: 1 << 20}
	start := func() (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- source.capture(ctx, s) }()
		t.Cleanup(cancel)
		return cancel, done
	}
	waitOffset := func(offset int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			position := s.durablePosition()
			if position.History != "" && position.Offset >= offset {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("capture stalled at %+v, expected %d", position, offset)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	offset := func() int64 {
		t.Helper()
		for _, line := range strings.Split(command("INFO", "replication"), "\r\n") {
			if strings.HasPrefix(line, "master_repl_offset:") {
				n, err := strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
		}
		t.Fatal("source replication offset absent")
		return 0
	}
	cancel, done := start()
	waitOffset(0)
	command("SET", "resource-before-pressure", "captured")
	waitOffset(offset())
	before := s.durablePosition()
	available.Store(0)
	command("SET", "resource-pressure-write", "must-resume")
	select {
	case err := <-done:
		if !errors.Is(err, errSyncCapturePressure) {
			t.Fatalf("pressure interrupted capture with wrong result: %v", err)
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("disk pressure did not stop capture")
	}
	cancel()
	if s.durablePosition() != before {
		t.Fatal("rejected source command advanced durable ACK position")
	}
	command("SET", "resource-offline-write", "retained-in-backlog")
	if s.durablePosition() != before {
		t.Fatal("stopped capture changed while source kept accepting writes")
	}
	available.Store(1 << 40)
	cancel, done = start() // Explicit reconnect; no hidden retry loop.
	waitOffset(offset())
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("resumed capture did not stop")
	}
	v, err := s.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	if !bytes.Contains(v.anchor.Data, []byte("resource-snapshot-seed")) {
		t.Fatal("resumption replaced initial snapshot")
	}
	var got [][]byte
	if err := v.walkFrom(v.base, func(r syncRecord, _, _ syncApplyCheckpoint) error {
		cmd, err := syncPendingCommand(r.Data)
		if err != nil {
			return err
		}
		if len(cmd) != 0 {
			got = append(got, r.Data)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := [][]byte{
		encodeCommand("SET", "resource-before-pressure", "captured"),
		encodeCommand("SET", "resource-pressure-write", "must-resume"),
		encodeCommand("SET", "resource-offline-write", "retained-in-backlog"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PSYNC resume omitted or duplicated source commands: got %q want %q", got, want)
	}
	t.Logf("pressure stopped ACK at %d; explicit PSYNC resumed through %d without losing the rejected record", before.Offset, v.end.Position.Offset)
}
