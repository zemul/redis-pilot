package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func syncOrphanTestFile(t *testing.T, dir, prefix string, id int) string {
	t.Helper()
	name := fmt.Sprintf("%s%032x", prefix, id)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("incomplete crash leftover"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func syncOrphanTestExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("protected path %s was lost: %v", path, err)
	}
}

func TestSyncOrphansGCInterruptionCleanupAndResume(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 8; i++ {
		f.append(t, 300)
	}
	old := append([]syncSegmentIndex(nil), f.store.manifest.Segments...)
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	meta := f.meta(t, 7)
	f.store.fault = func(stage string) error {
		if stage == "gc_before_unlink" {
			return errors.New("crash before first retired segment unlink")
		}
		return nil
	}
	err = f.store.prune(v, meta)
	v.close()
	if err == nil {
		t.Fatal("expected interrupted GC")
	}
	f.reopen(t, 1024, 64<<10)
	before := cloneSyncSegmentManifest(f.store.manifest)
	held, err := os.Open(filepath.Join(f.dir, old[0].ID))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	heldBefore, err := io.ReadAll(held)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.store.cleanupOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantRemoved := len(old) - len(before.Segments)
	if wantRemoved == 0 || result.RemovedFiles != wantRemoved || result.RemovedBytes <= 0 || result.SkippedFiles != 0 {
		t.Fatalf("unexpected cleanup: %+v, wanted %d files", result, wantRemoved)
	}
	if !reflect.DeepEqual(before, f.store.manifest) || f.store.durablePosition() != f.points[len(f.points)-1].Position {
		t.Fatal("cleanup changed capture or confirmation progress")
	}
	if _, err := held.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	heldAfter, err := io.ReadAll(held)
	if err != nil || !bytes.Equal(heldBefore, heldAfter) {
		t.Fatal("unlink invalidated an already open retired file", err)
	}
	f.reopen(t, 1024, 64<<10)
	v, err = f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	var got []syncRecord
	if err := v.walkFrom(meta.pending.Previous, func(r syncRecord, _, _ syncApplyCheckpoint) error {
		got = append(got, r)
		return nil
	}); err != nil || !reflect.DeepEqual(got, f.records[7:]) {
		t.Fatal("cleanup damaged pending proof or later records", err)
	}
}

func TestSyncOrphansPartialCleanupReopenRetry(t *testing.T) {
	for _, stage := range []string{"orphan_before_unlink", "orphan_after_unlink", "orphan_dir_synced"} {
		t.Run(stage, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			f.append(t, 300)
			for i, prefix := range []string{"anchor-", "segment-", "manifest-"} {
				syncOrphanTestFile(t, f.dir, prefix, i+1)
			}
			fired := false
			f.store.fault = func(got string) error {
				if got == stage {
					fired = true
					return errors.New("interrupted explicit orphan cleanup")
				}
				return nil
			}
			first, err := f.store.cleanupOrphans(context.Background())
			if err == nil || !fired || !f.store.poisoned {
				t.Fatal("cleanup fault was not fatal to the handle", err)
			}
			if _, err := f.store.cleanupOrphans(context.Background()); err == nil {
				t.Fatal("poisoned cleanup handle remained usable")
			}
			f.reopen(t, 1024, 64<<10)
			second, err := f.store.cleanupOrphans(context.Background())
			if err != nil || first.RemovedFiles+second.RemovedFiles != 3 || second.SkippedFiles != 0 {
				t.Fatalf("retry failed: first %+v, second %+v, err %v", first, second, err)
			}
			third, err := f.store.cleanupOrphans(context.Background())
			if err != nil || third != (syncOrphanCleanup{}) {
				t.Fatalf("cleanup was not idempotent: %+v %v", third, err)
			}
		})
	}
}

func TestSyncOrphansCancellationAfterUnlinkRequiresReopen(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	for i := 0; i < 3; i++ {
		syncOrphanTestFile(t, f.dir, "segment-", i+1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.store.fault = func(stage string) error {
		if stage == "orphan_after_unlink" {
			cancel()
		}
		return nil
	}
	first, err := f.store.cleanupOrphans(ctx)
	if !errors.Is(err, context.Canceled) || first.RemovedFiles != 1 || !f.store.poisoned {
		t.Fatalf("partial cancellation did not fail closed: %+v %v", first, err)
	}
	f.reopen(t, 1024, 64<<10)
	second, err := f.store.cleanupOrphans(context.Background())
	if err != nil || second.RemovedFiles != 2 {
		t.Fatalf("reopened cleanup could not complete cancellation remainder: %+v %v", second, err)
	}
}

func TestSyncOrphansPreserveUnsafeAndReferenced(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	f.append(t, 300)
	var preserved []string
	preserved = append(preserved, filepath.Join(f.dir, "LOCK"), filepath.Join(f.dir, "manifest"), filepath.Join(f.dir, f.store.manifest.Anchor))
	for _, idx := range f.store.manifest.Segments {
		preserved = append(preserved, filepath.Join(f.dir, idx.ID))
	}
	protectedCount := len(preserved)
	for _, name := range []string{"notes.txt", "segment-invalid", "anchor-ABCDEF0123456789ABCDEF0123456789"} {
		path := filepath.Join(f.dir, name)
		if err := os.WriteFile(path, []byte("user data"), 0600); err != nil {
			t.Fatal(err)
		}
		preserved = append(preserved, path)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("must survive"), 0600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(f.dir, fmt.Sprintf("segment-%032x", 100))
	if err := os.Symlink(outside, symlink); err != nil {
		t.Fatal(err)
	}
	preserved = append(preserved, symlink)
	hard := syncOrphanTestFile(t, f.dir, "segment-", 101)
	hardAlias := filepath.Join(f.dir, fmt.Sprintf("segment-%032x", 102))
	if err := os.Link(hard, hardAlias); err != nil {
		t.Fatal(err)
	}
	preserved = append(preserved, hard, hardAlias)
	mode := syncOrphanTestFile(t, f.dir, "anchor-", 103)
	if err := os.Chmod(mode, 0644); err != nil {
		t.Fatal(err)
	}
	preserved = append(preserved, mode)
	dir := filepath.Join(f.dir, fmt.Sprintf("manifest-%032x", 104))
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(dir, "inside")
	if err := os.WriteFile(inside, []byte("not recursively visited"), 0600); err != nil {
		t.Fatal(err)
	}
	preserved = append(preserved, dir)
	fifo := filepath.Join(f.dir, fmt.Sprintf("segment-%032x", 105))
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	preserved = append(preserved, fifo)
	removed := syncOrphanTestFile(t, f.dir, "manifest-", 106)
	result, err := f.store.cleanupOrphans(context.Background())
	if err != nil || result.RemovedFiles != 1 || result.SkippedFiles != len(preserved)-protectedCount {
		t.Fatalf("unexpected cleanup: %+v %v", result, err)
	}
	for _, path := range preserved {
		syncOrphanTestExists(t, path)
	}
	syncOrphanTestExists(t, outside)
	syncOrphanTestExists(t, inside)
	if _, err := os.Lstat(removed); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("eligible orphan still exists", err)
	}
}

func TestSyncOrphansPreflightLimitAndCancellationDeleteNothing(t *testing.T) {
	for _, kind := range []string{"limit", "cancelled", "cancelled_second_batch"} {
		t.Run(kind, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			var paths []string
			for i := 0; i < 70; i++ {
				paths = append(paths, syncOrphanTestFile(t, f.dir, "segment-", i+1))
			}
			ctx := context.Background()
			if kind == "limit" {
				f.store.policy.MaxDirectoryEntries = 65 // Exceed after a complete first batch.
			} else if kind == "cancelled" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			} else {
				cancelled, cancel := context.WithCancel(ctx)
				defer cancel()
				ctx = &syncOrphanCancelContext{Context: cancelled, remaining: 2, cancel: cancel}
			}
			result, err := f.store.cleanupOrphans(ctx)
			if err == nil || result.RemovedFiles != 0 || f.store.poisoned {
				t.Fatalf("preflight failed incorrectly: %+v %v poison=%v", result, err, f.store.poisoned)
			}
			for _, path := range paths {
				syncOrphanTestExists(t, path)
			}
		})
	}
}

// Cancel while scanning, after the first 64-entry batch has been checked. This
// exercises the no-deletion preflight guarantee independently of directory order.
type syncOrphanCancelContext struct {
	context.Context
	remaining int
	cancel    context.CancelFunc
}

func (c *syncOrphanCancelContext) Err() error {
	if c.remaining == 0 {
		c.cancel()
	} else {
		c.remaining--
	}
	return c.Context.Err()
}

func TestSyncOrphansManifestDamageDeletesNothing(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt", "generation", "floor", "active-prefix", "reference", "anchor-corrupt", "anchor-changed"} {
		t.Run(damage, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 2048, 64<<10)
			f.append(t, 40)
			f.append(t, 40) // Active in-memory tail is ahead of the published prefix.
			orphan := syncOrphanTestFile(t, f.dir, "segment-", 200)
			path := filepath.Join(f.dir, "manifest")
			if damage == "missing" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else if damage == "reference" {
				if err := os.Remove(filepath.Join(f.dir, f.store.manifest.Segments[0].ID)); err != nil {
					t.Fatal(err)
				}
			} else if damage == "anchor-corrupt" || damage == "anchor-changed" {
				anchorPath := filepath.Join(f.dir, f.store.manifest.Anchor)
				data, err := os.ReadFile(anchorPath)
				if err != nil {
					t.Fatal(err)
				}
				if damage == "anchor-corrupt" {
					data[len(data)-1] ^= 1
				} else {
					changed := f.store.anchor
					changed.Data = bytes.Clone(changed.Data)
					changed.Data[len(changed.Data)-1] ^= 1
					frame, err := syncSegmentFrame(changed)
					if err != nil {
						t.Fatal(err)
					}
					data = append([]byte(syncAnchorMagic), frame...)
				}
				if int64(len(data)) != f.store.manifest.AnchorSize {
					t.Fatal("damage must preserve anchor file size")
				}
				if err := os.WriteFile(anchorPath, data, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				mf, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				var m syncSegmentManifest
				err = readSyncSegmentObject(mf, syncManifestMagic, &m, syncManifestLimit)
				mf.Close()
				if err != nil {
					t.Fatal(err)
				}
				switch damage {
				case "generation":
					m.Generation++
				case "floor":
					m.Floor = f.points[1]
				case "active-prefix":
					m.Segments[0].End = f.points[1] // Header-only size cannot prove this end.
				}
				frame, err := syncSegmentFrame(m)
				if err != nil {
					t.Fatal(err)
				}
				if damage == "corrupt" {
					frame[len(frame)-1] ^= 1
				}
				if err := os.WriteFile(path, append([]byte(syncManifestMagic), frame...), 0600); err != nil {
					t.Fatal(err)
				}
			}
			result, err := f.store.cleanupOrphans(context.Background())
			if err == nil || result.RemovedFiles != 0 || !f.store.poisoned {
				t.Fatalf("unsafe cleanup accepted manifest damage: %+v %v", result, err)
			}
			syncOrphanTestExists(t, orphan)
		})
	}
}

func TestSyncOrphansLogicalByteTotalOverflow(t *testing.T) {
	candidates := []syncOrphanCandidate{
		{stat: unix.Stat_t{Size: math.MaxInt64 - 3}},
		{stat: unix.Stat_t{Size: 3}},
	}
	if total, err := syncOrphanTotalBytes(candidates); err != nil || total != math.MaxInt64 {
		t.Fatal("valid largest logical byte total rejected", total, err)
	}
	candidates[1].stat.Size = 4
	if _, err := syncOrphanTotalBytes(candidates); err == nil {
		t.Fatal("overflowing sparse-file lengths accepted")
	}
	candidates[1].stat.Size = -1
	if _, err := syncOrphanTotalBytes(candidates); err == nil {
		t.Fatal("negative file length accepted")
	}
}

func TestSyncOrphansActiveUnpublishedTailAndEmptyJournal(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			var s *syncSegments
			if empty {
				var err error
				s, err = openSyncSegments(filepath.Join(t.TempDir(), "segments"), 2048, 64<<10)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.close() })
			} else {
				f := newSyncSegmentFixture(t, 2048, 64<<10)
				for i := 0; i < 3; i++ {
					f.append(t, 20)
				}
				s = f.store
			}
			before := cloneSyncSegmentManifest(s.manifest)
			syncOrphanTestFile(t, s.dir.Name(), "anchor-", 1)
			result, err := s.cleanupOrphans(context.Background())
			if err != nil || result.RemovedFiles != 1 || !reflect.DeepEqual(before, s.manifest) {
				t.Fatalf("cleanup rejected a legitimate manifest prefix: %+v %v", result, err)
			}
		})
	}
}

func TestSyncOrphansReplacementBeforeUnlinkIsRejected(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	orphan := syncOrphanTestFile(t, f.dir, "segment-", 1)
	replacement := filepath.Join(t.TempDir(), "replacement")
	if err := os.WriteFile(replacement, []byte("new namespace occupant"), 0600); err != nil {
		t.Fatal(err)
	}
	f.store.fault = func(stage string) error {
		if stage == "orphan_before_unlink" {
			return os.Rename(replacement, orphan)
		}
		return nil
	}
	result, err := f.store.cleanupOrphans(context.Background())
	if err == nil || result.RemovedFiles != 0 || !f.store.poisoned {
		t.Fatal("replacement inode was accepted for deletion", result, err)
	}
	data, err := os.ReadFile(orphan)
	if err != nil || string(data) != "new namespace occupant" {
		t.Fatal("cleanup removed a replacement inode", err)
	}
}

func TestSyncOrphansRejectClosedHandleAndUnsafeOwner(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	f.store.close()
	if _, err := f.store.cleanupOrphans(context.Background()); err == nil {
		t.Fatal("closed handle accepted cleanup")
	}
	st := unix.Stat_t{Mode: unix.S_IFREG | 0600, Uid: uint32(os.Geteuid()), Nlink: 1, Size: 1}
	if !syncOrphanSafeFile(st) {
		t.Fatal("ordinary owned file rejected")
	}
	st.Uid++
	if syncOrphanSafeFile(st) {
		t.Fatal("another UID's file accepted")
	}
}
