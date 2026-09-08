package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func syncIdentityForTest(id string) *syncTaskIdentity {
	return &syncTaskIdentity{TaskID: id, Source: "127.0.0.1:12345", Cold: "127.0.0.1:12346"}
}

func syncIdentityJournalForTest(t *testing.T, identity *syncTaskIdentity) (*syncSegments, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "journal")
	j, err := openSyncSegmentsForTask(dir, 1024, 1<<20, defaultSyncResourcePolicy(), identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.close() })
	j.availableBytes = func() (uint64, error) { return 1 << 40, nil }
	return j, dir
}

func syncIdentitySnapshotForTest(t *testing.T, j *syncSegments) {
	t.Helper()
	record := captureSnapshot("127.0.0.1:12345")
	record.Data = syncTestRDB()
	if err := j.append(record); err != nil {
		t.Fatal(err)
	}
}

func syncIdentityEvidence(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	evidence := make(map[string][32]byte)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		evidence[entry.Name()] = sha256.Sum256(data)
	}
	return evidence
}

func TestSyncTaskIdentityPersistsAcrossAppendRotateAndReopen(t *testing.T) {
	identity := syncIdentityForTest("cluster-a-shard-0")
	j, dir := syncIdentityJournalForTest(t, identity)
	identity.TaskID = "caller-cannot-mutate-binding"
	if j.manifest.Version != 2 || j.manifest.Identity.TaskID != "cluster-a-shard-0" {
		t.Fatal("new journal lacks independent v2 identity")
	}
	syncIdentitySnapshotForTest(t, j)
	for i := 0; i < 8; i++ {
		if err := syncShutdownAppendForTest(j, strings.Repeat("k", 180)); err != nil {
			t.Fatal(err)
		}
	}
	if len(j.manifest.Segments) < 2 {
		t.Fatal("test did not rotate")
	}
	want := j.durablePosition()
	clone := cloneSyncSegmentManifest(j.manifest)
	clone.Identity.TaskID = "copy-cannot-mutate-binding"
	if j.manifest.Identity.TaskID != "cluster-a-shard-0" {
		t.Fatal("manifest clone aliases identity")
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openSyncSegmentsForTask(dir, 1024, 1<<20, defaultSyncResourcePolicy(), syncIdentityForTest("cluster-a-shard-0"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if reopened.durablePosition() != want || reopened.manifest.Version != 2 {
		t.Fatal("reopen lost history or identity version")
	}
}

func TestSyncTaskIdentityMismatchNeverChangesExistingEvidence(t *testing.T) {
	for _, captured := range []bool{false, true} {
		for _, mismatch := range []string{"id", "source", "cold", "legacy"} {
			t.Run(mismatch+map[bool]string{false: "_empty", true: "_captured"}[captured], func(t *testing.T) {
				j, dir := syncIdentityJournalForTest(t, syncIdentityForTest("task-a"))
				if captured {
					syncIdentitySnapshotForTest(t, j)
					if err := syncShutdownAppendForTest(j, "retained"); err != nil {
						t.Fatal(err)
					}
				}
				j.close()
				before := syncIdentityEvidence(t, dir)
				expected := syncIdentityForTest("task-a")
				switch mismatch {
				case "id":
					expected.TaskID = "task-b"
				case "source":
					expected.Source = "127.0.0.1:12347"
				case "cold":
					expected.Cold = "127.0.0.1:12348"
				case "legacy":
					expected = nil
				}
				other, err := openSyncSegmentsForTask(dir, 1024, 1<<20, defaultSyncResourcePolicy(), expected)
				if other != nil {
					other.close()
				}
				if !errors.Is(err, errSyncTaskIdentity) {
					t.Fatal("identity change accepted", err)
				}
				if !reflect.DeepEqual(before, syncIdentityEvidence(t, dir)) {
					t.Fatal("wrong identity changed evidence")
				}
			})
		}
	}
}

func TestSyncTaskIdentityDoesNotAdoptLegacyJournal(t *testing.T) {
	for _, captured := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "captured"}[captured], func(t *testing.T) {
			j, dir := syncIdentityJournalForTest(t, nil)
			if captured {
				syncIdentitySnapshotForTest(t, j)
			}
			if j.manifest.Version != 1 || j.manifest.Identity != nil {
				t.Fatal("legacy format changed")
			}
			j.close()
			before := syncIdentityEvidence(t, dir)
			other, err := openSyncSegmentsForTask(dir, 1024, 1<<20, defaultSyncResourcePolicy(), syncIdentityForTest("task-a"))
			if other != nil {
				other.close()
			}
			if !errors.Is(err, errSyncTaskIdentity) || !reflect.DeepEqual(before, syncIdentityEvidence(t, dir)) {
				t.Fatal("legacy log was adopted", err)
			}
			legacy, err := openSyncSegments(dir, 1024, 1<<20)
			if err != nil {
				t.Fatal("legacy reopen broken", err)
			}
			legacy.close()
		})
	}
}

func TestSyncTaskIdentityRejectsBadManifestAndAppend(t *testing.T) {
	j, dir := syncIdentityJournalForTest(t, syncIdentityForTest("task-a"))
	before := syncIdentityEvidence(t, dir)
	record := captureSnapshot("127.0.0.1:12349")
	record.Data = syncTestRDB()
	if err := j.append(record); !errors.Is(err, errSyncTaskIdentity) {
		t.Fatal("foreign source appended", err)
	}
	if !reflect.DeepEqual(before, syncIdentityEvidence(t, dir)) {
		t.Fatal("rejected append changed files")
	}
	j.close()
	for _, bad := range []syncSegmentManifest{
		{Version: 2, Generation: 1},
		{Version: 1, Generation: 1, Identity: syncIdentityForTest("task-a")},
		{Version: 2, Generation: 1, Identity: &syncTaskIdentity{TaskID: "../bad", Source: "127.0.0.1:12345", Cold: "127.0.0.1:12346"}},
	} {
		data, err := syncSegmentFrame(bad)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "manifest"), append([]byte(syncManifestMagic), data...), 0600); err != nil {
			t.Fatal(err)
		}
		opened, err := openSyncSegmentsForTask(dir, 1024, 1<<20, defaultSyncResourcePolicy(), syncIdentityForTest("task-a"))
		if opened != nil {
			opened.close()
		}
		if !errors.Is(err, errSyncJournalCorrupt) {
			t.Fatal("malformed binding accepted", err)
		}
	}
}

func TestSyncTaskIdentityOrphanCleanupVerifiesDiskBinding(t *testing.T) {
	j, dir := syncIdentityJournalForTest(t, syncIdentityForTest("task-a"))
	disk := cloneSyncSegmentManifest(j.manifest)
	disk.Identity.TaskID = "task-b"
	data, err := syncSegmentFrame(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest"), append([]byte(syncManifestMagic), data...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.cleanupOrphans(context.Background()); !errors.Is(err, errSyncJournalCorrupt) {
		t.Fatal("cleanup ignored changed disk binding", err)
	}
}

func TestSyncTaskIdentityTargetsCannotUseAnotherJournal(t *testing.T) {
	j, _ := syncIdentityJournalForTest(t, syncIdentityForTest("task-a"))
	for _, target := range []syncColdTarget{
		{address: "127.0.0.1:12346", timeout: time.Second},
		{address: "127.0.0.1:12346", timeout: time.Second, taskID: "task-b"},
		{address: "127.0.0.1:12348", timeout: time.Second, taskID: "task-a"},
	} {
		for _, op := range []func() error{
			func() error { _, err := target.drainSegments(context.Background(), j); return err },
			func() error { _, err := target.applySegments(context.Background(), j); return err },
			func() error { return target.pruneSegments(context.Background(), j) },
		} {
			if err := op(); !errors.Is(err, errSyncTaskIdentity) {
				t.Fatal("target contacted wrong journal", err)
			}
		}
	}
}

func TestIsolatedSyncNamedOptionsCanonicalizeIdentity(t *testing.T) {
	o := isolatedSyncOptionsForTest(t)
	o.TaskID = "cluster-a-0"
	o.SourceAddress = "[::ffff:127.0.0.1]:012345"
	o.ColdAddress = "[::ffff:127.0.0.1]:012346"
	task, err := NewIsolatedSyncTask(o)
	if err != nil {
		t.Fatal(err)
	}
	if task.options.SourceAddress != "127.0.0.1:12345" || task.options.ColdAddress != "127.0.0.1:12346" {
		t.Fatal("named identity is not canonical")
	}
	if task.runner.target.taskID != o.TaskID {
		t.Fatal("metadata task ID not connected")
	}
	o.TaskID = "../invalid"
	if _, err := NewIsolatedSyncTask(o); err == nil {
		t.Fatal("invalid task ID accepted")
	}
}

func TestIsolatedSyncNamedReuseFailsBeforeColdOrCapture(t *testing.T) {
	o := isolatedSyncOptionsForTest(t)
	o.TaskID = "task-a"
	j, err := openSyncSegmentsForTask(o.JournalDir, o.SegmentBytes, o.JournalBytes, defaultSyncResourcePolicy(), o.identity())
	if err != nil {
		t.Fatal(err)
	}
	j.close()
	o.TaskID = "task-b"
	task := isolatedSyncTaskForTest(t, o)
	task.runner.ops.capture = func(context.Context, *syncSegments) error { t.Error("captured with wrong identity"); return nil }
	task.runner.ops.drain = func(context.Context, *syncSegments) (syncPosition, error) {
		t.Error("drained with wrong identity")
		return syncPosition{}, nil
	}
	if err := task.Run(context.Background()); err == nil || task.Status().LastError != "task_identity" {
		t.Fatal("wrong task reused journal", err, task.Status())
	}
}

func TestSyncTaskIdentitySpecialManifestCannotBlockOpen(t *testing.T) {
	j, dir := syncIdentityJournalForTest(t, syncIdentityForTest("task-a"))
	j.close()
	manifest := filepath.Join(dir, "manifest")
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(manifest, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		other, err := openSyncSegmentsForTask(dir, 1024, 1<<20, defaultSyncResourcePolicy(), syncIdentityForTest("task-a"))
		if other != nil {
			other.close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errSyncJournalCorrupt) {
			t.Fatal("special journal file not rejected", err)
		}
	case <-time.After(time.Second):
		t.Fatal("one malformed journal blocked task startup")
	}
}
