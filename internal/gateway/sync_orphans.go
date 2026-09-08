package gateway

// Orphan cleanup is explicit: opening a journal never deletes crash evidence.
// It requires this owner-only directory to be exclusively used by this protocol
// and all cooperating writers to honor LOCK. An attacker with the same UID can
// race pathname replacement between the final stat and unlink; this mechanism
// is not a security boundary against such a writer.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

type syncOrphanCleanup struct {
	RemovedFiles int
	// RemovedBytes is the sum of logical file lengths, not physical space
	// released: older views may still hold open descriptors for these files.
	RemovedBytes int64
	// Protected manifest/LOCK/anchor/segments are not counted as skipped.
	SkippedFiles int
}

type syncOrphanCandidate struct {
	name string
	stat unix.Stat_t
}

func syncOrphanSafeFile(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Mode&07777 == 0600 &&
		st.Uid == uint32(os.Geteuid()) && st.Nlink == 1 && st.Size >= 0
}

func sameSyncOrphanFile(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Size == b.Size &&
		a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid && a.Nlink == b.Nlink
}

func (s *syncSegments) orphanStat(name string) (unix.Stat_t, error) {
	var st unix.Stat_t
	err := unix.Fstatat(int(s.dir.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW)
	return st, err
}

// Check the live manifest again before permitting deletion. In-memory active
// End/Size can advance without another manifest publication; every other field
// must match, and the older active prefix must still be proved by its records.
func (s *syncSegments) orphanManifestLocked() error {
	st, err := s.orphanStat("manifest")
	if err != nil || !syncOrphanSafeFile(st) {
		return errSyncJournalCorrupt
	}
	f, err := s.openFile("manifest", unix.O_RDONLY|unix.O_NONBLOCK)
	if err != nil {
		return err
	}
	defer f.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &opened); err != nil || !sameSyncOrphanFile(st, opened) {
		return errSyncJournalCorrupt
	}
	var disk syncSegmentManifest
	if err := readSyncSegmentObject(f, syncManifestMagic, &disk, syncManifestLimit); err != nil {
		return err
	}
	m := s.manifest
	if disk.Version != m.Version || disk.Generation != m.Generation || disk.Anchor != m.Anchor ||
		disk.AnchorSize != m.AnchorSize || disk.Base != m.Base || disk.Floor != m.Floor ||
		disk.Confirmed != m.Confirmed || len(disk.Segments) != len(m.Segments) ||
		len(s.files) != len(m.Segments) {
		return errSyncJournalCorrupt
	}
	for i, idx := range disk.Segments {
		live := m.Segments[i]
		if idx.Sealed {
			if idx != live {
				return errSyncJournalCorrupt
			}
		} else {
			if i != len(disk.Segments)-1 || idx.ID != live.ID || idx.Start != live.Start || live.Sealed ||
				idx.Size > live.Size || idx.End.Position.Offset > live.End.Position.Offset {
				return errSyncJournalCorrupt
			}
			if _, err := scanSyncSegment(s.files[i], idx, true, nil); err != nil {
				return err
			}
		}
		pathStat, err := s.orphanStat(idx.ID)
		var heldStat unix.Stat_t
		if err != nil || !syncOrphanSafeFile(pathStat) ||
			unix.Fstat(int(s.files[i].Fd()), &heldStat) != nil || !sameSyncOrphanFile(pathStat, heldStat) {
			return errSyncJournalCorrupt
		}
	}
	if m.Anchor != "" {
		anchorStat, err := s.orphanStat(m.Anchor)
		if err != nil || !syncOrphanSafeFile(anchorStat) || anchorStat.Size != m.AnchorSize {
			return errSyncJournalCorrupt
		}
		af, err := s.openFile(m.Anchor, unix.O_RDONLY|unix.O_NONBLOCK)
		if err != nil {
			return err
		}
		var openedAnchor unix.Stat_t
		statErr := unix.Fstat(int(af.Fd()), &openedAnchor)
		var anchor syncRecord
		readErr := readSyncSegmentObject(af, syncAnchorMagic, &anchor, syncMaxRecord+16)
		closeErr := af.Close()
		check := syncJournal{}
		if statErr != nil || readErr != nil || closeErr != nil || !sameSyncOrphanFile(anchorStat, openedAnchor) ||
			check.check(anchor) != nil || nextPendingCheckpoint(syncApplyCheckpoint{}, anchor) != m.Base {
			return errSyncJournalCorrupt
		}
		afterAnchor, err := s.orphanStat(m.Anchor)
		if err != nil || !sameSyncOrphanFile(openedAnchor, afterAnchor) {
			return errSyncJournalCorrupt
		}
	}
	after, err := s.orphanStat("manifest")
	if err != nil || !sameSyncOrphanFile(opened, after) {
		return errSyncJournalCorrupt
	}
	lockStat, err := s.orphanStat("LOCK")
	var heldLock unix.Stat_t
	if err != nil || !syncOrphanSafeFile(lockStat) || unix.Fstat(int(s.lock.Fd()), &heldLock) != nil || !sameSyncOrphanFile(lockStat, heldLock) {
		return errSyncJournalCorrupt
	}
	return nil
}

func (s *syncSegments) cleanupOrphans(ctx context.Context) (result syncOrphanCleanup, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.dir == nil || s.lock == nil {
		return result, errors.New("segmented replication journal unavailable")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := s.orphanManifestLocked(); err != nil {
		s.poisoned = true
		return result, fmt.Errorf("verify orphan cleanup manifest: %w", err)
	}
	limit := s.policy.MaxDirectoryEntries
	if limit <= 0 {
		return result, errors.New("invalid orphan cleanup directory entry limit")
	}
	protected := map[string]bool{"LOCK": true, "manifest": true}
	if s.manifest.Anchor != "" {
		protected[s.manifest.Anchor] = true
	}
	for _, idx := range s.manifest.Segments {
		protected[idx.ID] = true
	}
	// A fresh directory descriptor avoids depending on a prior ReadDir offset;
	// unlike dup, openat starts its own directory stream at the beginning.
	fd, err := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		s.poisoned = true
		return result, err
	}
	dir := os.NewFile(uintptr(fd), s.dir.Name())
	defer dir.Close()
	var candidates []syncOrphanCandidate
	entries := 0
	for {
		if err := ctx.Err(); err != nil {
			return result, err // Preflight cancellation never deletes anything.
		}
		batch, readErr := dir.ReadDir(64)
		entries += len(batch)
		if entries > limit {
			return result, errors.New("orphan cleanup directory entry limit exceeded")
		}
		for _, entry := range batch {
			name := entry.Name()
			if protected[name] {
				continue
			}
			if !validSyncSegmentName(name, "anchor-") && !validSyncSegmentName(name, "segment-") && !validSyncSegmentName(name, "manifest-") {
				result.SkippedFiles++
				continue
			}
			st, err := s.orphanStat(name)
			if err != nil {
				s.poisoned = true
				return result, err
			}
			if !syncOrphanSafeFile(st) {
				result.SkippedFiles++
				continue
			}
			opened, err := s.openFile(name, unix.O_RDONLY|unix.O_NONBLOCK)
			if err != nil {
				s.poisoned = true
				return result, err
			}
			var checked unix.Stat_t
			statErr := unix.Fstat(int(opened.Fd()), &checked)
			closeErr := opened.Close()
			if statErr != nil || closeErr != nil || !sameSyncOrphanFile(st, checked) {
				s.poisoned = true
				return result, errSyncJournalCorrupt
			}
			candidates = append(candidates, syncOrphanCandidate{name, st})
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			s.poisoned = true
			return result, readErr
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if _, err := syncOrphanTotalBytes(candidates); err != nil {
		return result, err // Overflow is rejected before deleting any file.
	}
	if len(candidates) == 0 {
		return result, nil
	}
	// From this point an interruption may leave a partially unlinked set. The
	// manifest already excludes all of them; close/reopen and retry is safe.
	s.poisoned = true
	defer func() {
		if err != nil && result.RemovedFiles > 0 {
			// Best effort makes partial progress durable even when an injected
			// boundary, cancellation or a later unlink fails. It never makes
			// the uncertain handle reusable; a failed Sync is retained too.
			err = errors.Join(err, s.dir.Sync())
		}
	}()
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := s.unlinkOrphan(candidate); err != nil {
			return result, err
		}
		result.RemovedFiles++
		result.RemovedBytes += candidate.stat.Size
		if err := s.stage("orphan_after_unlink"); err != nil {
			return result, err
		}
	}
	if err := s.dir.Sync(); err != nil {
		return result, err
	}
	if err := s.stage("orphan_dir_synced"); err != nil {
		return result, err
	}
	s.poisoned = false
	return result, nil
}

func syncOrphanTotalBytes(candidates []syncOrphanCandidate) (int64, error) {
	var total int64
	for _, candidate := range candidates {
		if candidate.stat.Size < 0 || candidate.stat.Size > math.MaxInt64-total {
			return 0, errors.New("orphan cleanup logical byte count overflow")
		}
		total += candidate.stat.Size
	}
	return total, nil
}

func (s *syncSegments) unlinkOrphan(candidate syncOrphanCandidate) error {
	f, err := s.openFile(candidate.name, unix.O_RDONLY|unix.O_NONBLOCK)
	if err != nil {
		return err
	}
	defer f.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &opened); err != nil || !syncOrphanSafeFile(opened) || !sameSyncOrphanFile(candidate.stat, opened) {
		return errSyncJournalCorrupt
	}
	if err := s.stage("orphan_before_unlink"); err != nil {
		return err
	}
	current, err := s.orphanStat(candidate.name)
	if err != nil || !syncOrphanSafeFile(current) || !sameSyncOrphanFile(opened, current) {
		return errSyncJournalCorrupt
	}
	return unix.Unlinkat(int(s.dir.Fd()), candidate.name, 0)
}
