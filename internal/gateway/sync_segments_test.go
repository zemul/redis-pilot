package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These fixtures exercise only local durability. They do not assert that an
// arbitrary caller-supplied checkpoint proves cold data was committed.
type syncSegmentFixture struct {
	dir     string
	store   *syncSegments
	records []syncRecord
	points  []syncApplyCheckpoint
}

func newSyncSegmentFixture(t *testing.T, segmentLimit, totalLimit int64) *syncSegmentFixture {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "segments")
	s, err := openSyncSegments(dir, segmentLimit, totalLimit)
	if err != nil {
		t.Fatal(err)
	}
	f := &syncSegmentFixture{dir: dir, store: s}
	t.Cleanup(func() { f.store.close() })
	snapshot := captureSnapshot("127.0.0.1:12345")
	snapshot.Data = syncTestRDB()
	if err := s.append(snapshot); err != nil {
		t.Fatal(err)
	}
	f.records = append(f.records, snapshot)
	f.points = append(f.points, nextPendingCheckpoint(syncApplyCheckpoint{}, snapshot))
	return f
}

func (f *syncSegmentFixture) command(i, valueBytes int) syncRecord {
	raw := encodeCommand("SET", fmt.Sprintf("segment-key-%04d", i), strings.Repeat("v", valueBytes))
	pos := f.points[len(f.points)-1].Position
	pos.Offset += int64(len(raw))
	return syncRecord{Kind: "command", Position: pos, Data: raw}
}

func (f *syncSegmentFixture) append(t *testing.T, valueBytes int) {
	t.Helper()
	r := f.command(len(f.records), valueBytes)
	if err := f.store.append(r); err != nil {
		t.Fatal(err)
	}
	f.records = append(f.records, r)
	f.points = append(f.points, nextPendingCheckpoint(f.points[len(f.points)-1], r))
}

func (f *syncSegmentFixture) meta(t *testing.T, record int) syncPendingMeta {
	t.Helper()
	if record < 1 || record >= len(f.records) {
		t.Fatal("pending record outside fixture")
	}
	r := f.records[record]
	cmd, err := syncPendingCommand(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	before := syncPendingValue{}
	after, err := syncPendingAfter(cmd, before)
	if err != nil {
		t.Fatal(err)
	}
	p := &syncPending{Version: 1, Previous: f.points[record-1], Next: f.points[record], Record: r, Before: before, After: after}
	if err := validatePending(*p); err != nil {
		t.Fatal(err)
	}
	return syncPendingMeta{checkpoint: p.Next, anchor: f.points[0].encode(), pending: p}
}

func (f *syncSegmentFixture) reopen(t *testing.T, segmentLimit, totalLimit int64) {
	t.Helper()
	f.store.close()
	s, err := openSyncSegments(f.dir, segmentLimit, totalLimit)
	if err != nil {
		t.Fatal(err)
	}
	f.store = s
}

func segmentTestFiles(t *testing.T, dir, prefix string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}
	return paths
}

func TestSyncSegmentsRotateSeekAndReopen(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 12; i++ {
		f.append(t, 180)
	}
	if n := len(segmentTestFiles(t, f.dir, "segment-")); n < 3 {
		t.Fatalf("did not rotate: %d segments", n)
	}
	for round := 0; round < 2; round++ {
		v, err := f.store.pendingView()
		if err != nil {
			t.Fatal(err)
		}
		if v.base != f.points[0] || v.end != f.points[len(f.points)-1] {
			t.Fatal("view bounds differ from captured history")
		}
		for start := range f.points {
			var got []syncRecord
			err := v.walkFrom(f.points[start], func(r syncRecord, previous, next syncApplyCheckpoint) error {
				index := start + len(got) + 1
				if previous != f.points[index-1] || next != f.points[index] {
					t.Fatalf("wrong checkpoint chain at %d", index)
				}
				got = append(got, r)
				return nil
			})
			if err != nil || !reflect.DeepEqual(got, f.records[start+1:]) && !(len(got) == 0 && start == len(f.records)-1) {
				t.Fatalf("seek %d: got %d records, err %v", start, len(got), err)
			}
		}
		bad := f.points[4]
		bad.Chain = strings.Repeat("0", 64)
		called := false
		if err := v.walkFrom(bad, func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error { called = true; return nil }); err == nil || called {
			t.Fatal("unproved checkpoint accepted or emitted data")
		}
		v.close()
		f.reopen(t, 1024, 64<<10)
	}
}

func TestSyncSegmentsFixedViewSurvivesAppendPruneAndClose(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 8; i++ {
		f.append(t, 300)
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	fixedCount := len(f.records)
	for i := 0; i < 4; i++ {
		f.append(t, 300)
	}
	latest, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.prune(latest, f.meta(t, len(f.records)-1)); err != nil {
		t.Fatal(err)
	}
	latest.close()
	f.store.close()
	var got []syncRecord
	if err := v.walkFrom(f.points[0], func(r syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, f.records[1:fixedCount]) || v.end != f.points[fixedCount-1] {
		t.Fatal("pinned prefix changed after append/prune/close")
	}
}

func TestSyncSegmentsPruneRetainsPendingAndRejectsRollback(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 10; i++ {
		f.append(t, 300) // One framed record per segment at this limit.
	}
	beforeFiles := len(segmentTestFiles(t, f.dir, "segment-"))
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	meta := f.meta(t, 7)
	if err := f.store.prune(v, meta); err != nil {
		t.Fatal(err)
	}
	v.close()
	if n := len(segmentTestFiles(t, f.dir, "segment-")); n >= beforeFiles || n < 4 {
		t.Fatalf("GC did not preserve pending and future segments: before %d after %d", beforeFiles, n)
	}
	f.reopen(t, 1024, 64<<10)
	v, err = f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	if v.confirmed != meta.checkpoint || v.floor != meta.pending.Previous {
		t.Fatalf("prune bounds: confirmed %#v, floor %#v", v.confirmed, v.floor)
	}
	var got []syncRecord
	if err := v.walkFrom(meta.pending.Previous, func(r syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, r); return nil }); err != nil || !reflect.DeepEqual(got, f.records[7:]) {
		t.Fatal("pending record or later history was lost", err)
	}
	called := false
	if err := v.walkFrom(f.points[0], func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error { called = true; return nil }); err == nil || called {
		t.Fatal("GC silently resumed from missing history")
	}
	if err := f.store.prune(v, f.meta(t, 6)); err == nil {
		t.Fatal("confirmation watermark rolled back")
	}
}

func TestSyncSegmentsPruneRejectsUnprovedMetadata(t *testing.T) {
	for name, mutate := range map[string]func(*syncPendingMeta){
		"wrong_anchor":     func(m *syncPendingMeta) { m.anchor = "wrong" },
		"no_pending":       func(m *syncPendingMeta) { m.pending = nil },
		"not_committed":    func(m *syncPendingMeta) { m.checkpoint = m.pending.Previous },
		"wrong_chain":      func(m *syncPendingMeta) { m.checkpoint.Chain = strings.Repeat("0", 64) },
		"changed_record":   func(m *syncPendingMeta) { m.pending.Record.Data[0] ^= 1 },
		"changed_after":    func(m *syncPendingMeta) { m.pending.After.Data = []byte("tampered") },
		"checkpoint_ahead": func(m *syncPendingMeta) { m.checkpoint.Position.Offset++ },
	} {
		t.Run(name, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			for i := 0; i < 6; i++ {
				f.append(t, 300)
			}
			v, err := f.store.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			defer v.close()
			meta := f.meta(t, 5)
			meta.pending.Record.Data = bytes.Clone(meta.pending.Record.Data)
			mutate(&meta)
			before := segmentTestFiles(t, f.dir, "segment-")
			if err := f.store.prune(v, meta); err == nil {
				t.Fatal("unproved metadata accepted for destructive pruning")
			}
			if !reflect.DeepEqual(before, segmentTestFiles(t, f.dir, "segment-")) {
				t.Fatal("rejected metadata deleted a segment")
			}
		})
	}
}

func TestSyncSegmentsCapacityAndExclusiveLock(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 4096)
	if other, err := openSyncSegments(f.dir, 1024, 4096); err == nil {
		other.close()
		t.Fatal("second writer acquired journal lock")
	}
	for i := 0; ; i++ {
		if i > 32 {
			t.Fatal("total capacity was not enforced")
		}
		r := f.command(len(f.records), 300)
		err := f.store.append(r)
		if errors.Is(err, errSyncJournalFull) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		f.records = append(f.records, r)
		f.points = append(f.points, nextPendingCheckpoint(f.points[len(f.points)-1], r))
	}
	if f.store.durablePosition() != f.points[len(f.points)-1].Position {
		t.Fatal("rejected append advanced durable position")
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal("capacity error poisoned usable journal", err)
	}
	if err := f.store.prune(v, f.meta(t, len(f.records)-1)); err != nil {
		t.Fatal(err)
	}
	v.close()
	f.append(t, 300)
	f.reopen(t, 1024, 4096)
	if f.store.durablePosition() != f.points[len(f.points)-1].Position {
		t.Fatal("position changed across reopening after capacity reclamation")
	}
}

func TestSyncSegmentsMissingAndCorruptFilesFailClosed(t *testing.T) {
	for _, which := range []string{"manifest", "anchor", "segment"} {
		for _, damage := range []string{"missing", "truncated", "checksum", "symlink"} {
			t.Run(which+"_"+damage, func(t *testing.T) {
				f := newSyncSegmentFixture(t, 1024, 64<<10)
				for i := 0; i < 5; i++ {
					f.append(t, 300)
				}
				f.store.close()
				path := filepath.Join(f.dir, "manifest")
				if which != "manifest" {
					paths := segmentTestFiles(t, f.dir, which+"-")
					if len(paths) == 0 {
						t.Fatal("missing fixture file")
					}
					path = paths[0]
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				switch damage {
				case "missing":
					err = os.Remove(path)
				case "truncated":
					err = os.Truncate(path, int64(len(data)-1))
				case "checksum":
					data[len(data)-1] ^= 1
					err = os.WriteFile(path, data, 0600)
				case "symlink":
					outside := filepath.Join(t.TempDir(), "outside")
					if err = os.WriteFile(outside, data, 0600); err == nil {
						err = os.Remove(path)
					}
					if err == nil {
						err = os.Symlink(outside, path)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				s, err := openSyncSegments(f.dir, 1024, 64<<10)
				if err != nil {
					return // Startup validation can reject corruption before seek.
				}
				defer s.close()
				v, err := s.pendingView()
				if err != nil {
					return
				}
				defer v.close()
				if err := v.walkFrom(f.points[0], func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error { return nil }); err == nil {
					t.Fatal("damaged declared history was accepted")
				}
			})
		}
	}
}

func TestSyncSegmentsOrphansAreNeverAdopted(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 4; i++ {
		f.append(t, 300)
	}
	f.store.close()
	files := segmentTestFiles(t, f.dir, "segment-")
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"segment-" + strings.Repeat("f", 32), "anchor-" + strings.Repeat("e", 32), "manifest.unpublished"} {
		if err := os.WriteFile(filepath.Join(f.dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.reopen(t, 1024, 64<<10)
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	var got []syncRecord
	if err := v.walkFrom(f.points[0], func(r syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, r); return nil }); err != nil || !reflect.DeepEqual(got, f.records[1:]) {
		t.Fatal("orphan affected published history", err)
	}
	if f.store.durablePosition() != f.points[len(f.points)-1].Position {
		t.Fatal("orphan advanced durable capture")
	}
}

func TestSyncSegmentsRotationFaultRecovery(t *testing.T) {
	for _, stage := range []string{"segment_written", "segment_synced", "segment_dir_synced", "manifest_written", "manifest_synced", "manifest_renamed", "manifest_dir_synced"} {
		t.Run(stage, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			f.append(t, 300)
			old := f.points[len(f.points)-1]
			r := f.command(len(f.records), 300)
			next := nextPendingCheckpoint(old, r)
			fired := false
			f.store.fault = func(got string) error {
				if got == stage {
					fired = true
					return errors.New("injected rotation fault")
				}
				return nil
			}
			if err := f.store.append(r); err == nil || !fired {
				t.Fatalf("fault %s not reached: %v", stage, err)
			}
			if err := f.store.append(r); err == nil {
				t.Fatal("write continued after uncertain publication")
			}
			f.store.close()
			s, err := openSyncSegments(f.dir, 1024, 64<<10)
			if err != nil {
				t.Logf("uncertain publication rejected safely: %v", err)
				return
			}
			f.store = s
			position := s.durablePosition()
			if position != old.Position && position != next.Position {
				t.Fatal("recovery invented an intermediate history position")
			}
			v, err := s.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			defer v.close()
			var got []syncRecord
			if err := v.walkFrom(f.points[0], func(record syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, record); return nil }); err != nil {
				t.Fatal(err)
			}
			want := f.records[1:]
			if position == next.Position {
				want = append(append([]syncRecord{}, want...), r)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("recovery duplicated or lost part of acknowledged prefix")
			}
		})
	}
}

func TestSyncSegmentsGCFaultRecovery(t *testing.T) {
	for _, stage := range []string{"manifest_written", "manifest_synced", "manifest_renamed", "manifest_dir_synced", "gc_before_unlink", "gc_after_unlink", "gc_dir_synced"} {
		t.Run(stage, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			for i := 0; i < 8; i++ {
				f.append(t, 300)
			}
			v, err := f.store.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			meta := f.meta(t, 7)
			fired := false
			f.store.fault = func(got string) error {
				if got == stage {
					fired = true
					return errors.New("injected GC fault")
				}
				return nil
			}
			err = f.store.prune(v, meta)
			v.close()
			if err == nil || !fired {
				t.Fatalf("fault %s not reached: %v", stage, err)
			}
			f.store.close()
			s, err := openSyncSegments(f.dir, 1024, 64<<10)
			if err != nil {
				t.Logf("uncertain GC rejected safely: %v", err)
				return
			}
			f.store = s
			v, err = s.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			defer v.close()
			if v.end != f.points[len(f.points)-1] || v.floor != f.points[0] && v.floor != meta.pending.Previous {
				t.Fatal("GC recovered an unrelated boundary")
			}
			var got []syncRecord
			if err := v.walkFrom(meta.pending.Previous, func(r syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, r); return nil }); err != nil || !reflect.DeepEqual(got, f.records[7:]) {
				t.Fatal("GC fault lost pending proof or future records", err)
			}
			var all []syncRecord
			if err := v.walkFrom(v.floor, func(r syncRecord, _, _ syncApplyCheckpoint) error { all = append(all, r); return nil }); err != nil {
				t.Fatal("manifest references a deleted or incomplete segment", err)
			}
			want := f.records[1:]
			if v.floor == meta.pending.Previous {
				want = f.records[7:]
			}
			if !reflect.DeepEqual(all, want) {
				t.Fatal("GC recovery duplicated or skipped retained records")
			}
		})
	}
}

// Uses a disposable local Redis only when the existing test opt-in is set.
// The source's real byte offsets must survive segment rotation and PSYNC.
func TestSyncSegmentsRedisCapture(t *testing.T) {
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
	command("SET", "segment-snapshot-seed", "present")
	dir := filepath.Join(t.TempDir(), "segments")
	s, err := openSyncSegments(dir, 1024, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			s.close()
		}
	})
	source := syncSource{address: address, passwordFile: secretFile(t, "g4-backend-test"), timeout: 5 * time.Second, maxSnapshot: 1 << 20}
	start := func() (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func(log *syncSegments) { done <- source.capture(ctx, log) }(s)
		t.Cleanup(cancel)
		return cancel, done
	}
	stop := func(cancel context.CancelFunc, done <-chan error) {
		t.Helper()
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("capture did not stop")
		}
	}
	wait := func(offset int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			position := s.durablePosition()
			if position.History != "" && position.Offset >= offset {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("capture stalled at %#v, expected offset %d", position, offset)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	offset := func() int64 {
		t.Helper()
		for _, line := range strings.Split(command("INFO", "replication"), "\r\n") {
			if strings.HasPrefix(line, "master_repl_offset:") {
				value, err := strconv.ParseInt(strings.TrimPrefix(line, "master_repl_offset:"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
		}
		t.Fatal("master replication offset missing")
		return 0
	}
	cancel, done := start()
	wait(0)
	var expected [][]byte
	for i := 0; i < 8; i++ {
		args := []string{"SET", fmt.Sprintf("captured-segment-%d", i), strings.Repeat("v", 300)}
		command(args...)
		expected = append(expected, encodeCommand(args...))
	}
	wait(offset())
	stop(cancel, done)
	previous := s.durablePosition()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	command("SET", "segmented-offline-write", "retained")
	expected = append(expected, encodeCommand("SET", "segmented-offline-write", "retained"))
	s, err = openSyncSegments(dir, 1024, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if s.durablePosition() != previous {
		t.Fatal("reopen changed captured position")
	}
	cancel, done = start()
	wait(offset())
	stop(cancel, done)
	v, err := s.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	if !bytes.Contains(v.anchor.Data, []byte("segment-snapshot-seed")) || len(segmentTestFiles(t, dir, "segment-")) < 3 {
		t.Fatal("snapshot seed or segment rotation absent")
	}
	var got [][]byte
	if err := v.walkFrom(v.base, func(r syncRecord, _, _ syncApplyCheckpoint) error {
		cmd, err := syncPendingCommand(r.Data)
		if err != nil {
			return err
		}
		if len(cmd) > 0 {
			got = append(got, r.Data)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatal("PSYNC after reopen duplicated or omitted a successful source command")
	}
}

func TestSyncSegmentsIndexIsNotProof(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 6; i++ {
		f.append(t, 300)
	}
	f.store.close()
	path := filepath.Join(f.dir, "manifest")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest syncSegmentManifest
	err = readSyncSegmentObject(file, syncManifestMagic, &manifest, syncManifestLimit)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	forged := manifest.Segments[1].Start
	forged.Chain = strings.Repeat("0", 64)
	// Preserve all index-level neighboring equalities and use a valid CRC. The
	// record/segment proof, rather than the manifest's self-consistency, must win.
	manifest.Segments[0].End = forged
	manifest.Segments[1].Start = forged
	frame, err := syncSegmentFrame(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append([]byte(syncManifestMagic), frame...), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := openSyncSegments(f.dir, 1024, 64<<10)
	if err != nil {
		return
	}
	f.store = s
	v, err := s.pendingView()
	if err != nil {
		return
	}
	defer v.close()
	called := false
	if err := v.walkFrom(forged, func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error { called = true; return nil }); err == nil || called {
		t.Fatal("valid-CRC manifest index was trusted over segment proof")
	}
}

func TestSyncSegmentsChangedRecordWithValidCRCIsRejectedBeforeUse(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 5; i++ {
		f.append(t, 300)
	}
	entry := f.store.manifest.Segments[0]
	f.store.close()
	path := filepath.Join(f.dir, entry.ID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(data[len(syncSegmentMagic):])
	var header syncSegmentHeader
	var record syncRecord
	if err := readSyncSegmentFrame(reader, &header, syncMaxRecord); err != nil {
		t.Fatal(err)
	}
	if err := readSyncSegmentFrame(reader, &record, syncMaxRecord); err != nil || reader.Len() != 0 {
		t.Fatal("expected one-record segment", err)
	}
	record.Data[len(record.Data)-3] = 'w'
	headerFrame, err := syncSegmentFrame(header)
	if err != nil {
		t.Fatal(err)
	}
	recordFrame, err := syncSegmentFrame(record)
	if err != nil {
		t.Fatal(err)
	}
	changed := append(append([]byte(syncSegmentMagic), headerFrame...), recordFrame...)
	if len(changed) != len(data) {
		t.Fatal("mutation changed indexed file size")
	}
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := openSyncSegments(f.dir, 1024, 64<<10)
	if err != nil {
		return
	}
	f.store = s
	v, err := s.pendingView()
	if err != nil {
		return
	}
	defer v.close()
	called := false
	if err := v.walkFrom(f.points[0], func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error { called = true; return nil }); err == nil || called {
		t.Fatal("changed sealed segment was consumed before chain validation")
	}
}

func TestSyncSegmentsPruneRetainsWholeMultiRecordSegment(t *testing.T) {
	for _, edge := range []string{"first", "last"} {
		t.Run(edge, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			for i := 0; i < 18; i++ {
				f.append(t, 0)
			}
			entry := f.store.manifest.Segments[2]
			start, end := -1, -1
			for index, point := range f.points {
				if point == entry.Start {
					start = index
				}
				if point == entry.End {
					end = index
				}
			}
			if start < 0 || end-start < 2 {
				t.Fatalf("expected multi-record segment, got bounds %d..%d", start, end)
			}
			pendingIndex := start + 1
			if edge == "last" {
				pendingIndex = end
			}
			v, err := f.store.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.prune(v, f.meta(t, pendingIndex)); err != nil {
				t.Fatal(err)
			}
			v.close()
			f.reopen(t, 1024, 64<<10)
			v, err = f.store.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			defer v.close()
			if v.floor != entry.Start {
				t.Fatal("partial segment was pruned")
			}
			var got []syncRecord
			if err := v.walkFrom(v.floor, func(r syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, r); return nil }); err != nil || !reflect.DeepEqual(got, f.records[start+1:]) {
				t.Fatal("whole pending segment and suffix were not retained", err)
			}
		})
	}
}

func TestSyncSegmentsViewPinsExclusiveLock(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	f.append(t, 64)
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	f.store.close()
	if other, err := openSyncSegments(f.dir, 1024, 64<<10); err == nil {
		other.close()
		v.close()
		t.Fatal("new writer acquired lock while an old fixed view was alive")
	}
	v.close()
	f.reopen(t, 1024, 64<<10)
}

func TestSyncSegmentsOldViewCannotAuthorizeNewPending(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 5; i++ {
		f.append(t, 300)
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	f.append(t, 300)
	before := segmentTestFiles(t, f.dir, "segment-")
	if err := f.store.prune(v, f.meta(t, len(f.records)-1)); err == nil {
		t.Fatal("old view authorized pruning based on an absent pending record")
	}
	if !reflect.DeepEqual(before, segmentTestFiles(t, f.dir, "segment-")) {
		t.Fatal("unproved future pending deleted history")
	}
}

func TestSyncSegmentsActiveAppendFaultRecovery(t *testing.T) {
	for _, stage := range []string{"append_written", "append_synced"} {
		t.Run(stage, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			f.append(t, 0)
			old := f.points[len(f.points)-1]
			r := f.command(len(f.records), 0)
			fired := false
			f.store.fault = func(got string) error {
				if got == stage {
					fired = true
					return errors.New("injected active append fault")
				}
				return nil
			}
			if err := f.store.append(r); err == nil || !fired {
				t.Fatalf("active append stage not reached: %v", err)
			}
			if f.store.durablePosition() != old.Position {
				t.Fatal("uncertain append advanced advertised durable position")
			}
			f.store.close()
			s, err := openSyncSegments(f.dir, 1024, 64<<10)
			if err != nil {
				return
			}
			f.store = s
			v, err := s.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			defer v.close()
			var got []syncRecord
			if err := v.walkFrom(f.points[0], func(record syncRecord, _, _ syncApplyCheckpoint) error { got = append(got, record); return nil }); err != nil {
				t.Fatal(err)
			}
			want := append(append([]syncRecord{}, f.records[1:]...), r)
			if !reflect.DeepEqual(got, want) || v.end != nextPendingCheckpoint(old, r) {
				t.Fatal("complete surviving append was not durably recovered exactly once")
			}
		})
	}
}
