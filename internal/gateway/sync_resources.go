package gateway

// G6a-7 is local admission control, not a service scheduler, filesystem quota,
// distributed owner lease, or a guarantee about the cold replica's actual lag.

import (
	"errors"
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

var (
	errSyncCapturePressure = errors.New("replication capture paused by resource pressure")
	errSyncViewLimit       = errors.New("replication journal view limit reached")
)

const syncAllocationSlack = uint64(64 << 10)

type syncResourcePolicy struct {
	MinFreeBytes           uint64
	ResumeFreeBytes        uint64
	MaxUnconfirmedBytes    int64
	ResumeUnconfirmedBytes int64
	MaxViews               int
	MaxDirectoryEntries    int
}

func defaultSyncResourcePolicy() syncResourcePolicy {
	return syncResourcePolicy{
		MinFreeBytes: 64 << 20, ResumeFreeBytes: 128 << 20,
		MaxUnconfirmedBytes: 64 << 20, ResumeUnconfirmedBytes: 32 << 20,
		MaxViews: 4, MaxDirectoryEntries: 4096,
	}
}

func (p syncResourcePolicy) validate() error {
	// Leave headroom for one GC manifest even when admission stops capture.
	// This is a safety margin, not a reserved extent: another process, quota,
	// inode exhaustion or filesystem metadata can still make a write fail.
	if p.MinFreeBytes < syncManifestLimit+syncAllocationSlack || p.ResumeFreeBytes <= p.MinFreeBytes || p.MaxUnconfirmedBytes <= 0 || p.ResumeUnconfirmedBytes < 0 || p.ResumeUnconfirmedBytes >= p.MaxUnconfirmedBytes || p.MaxViews < 1 || p.MaxViews > 16 || p.MaxDirectoryEntries < 1 || p.MaxDirectoryEntries > 16384 {
		return errors.New("invalid isolated replication resource policy")
	}
	return nil
}

type syncResourceStatus struct {
	AvailableBytes   uint64
	UnconfirmedBytes int64 // source bytes since last persisted GC confirmation, not exact cold lag
	LiveBytes        int64 // referenced anchor + segment file lengths, not physical disk use
	Views            int
	DiskPaused       bool
	BacklogPaused    bool
}

func syncAvailableBytes(fd int) (uint64, error) {
	var fs unix.Statfs_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return 0, err
	}
	// Bsize's signedness differs on Darwin/Linux; reject before conversion.
	if fs.Bsize <= 0 {
		return 0, errors.New("invalid filesystem allocation unit")
	}
	return syncBlockAvailable(uint64(fs.Bavail), int64(fs.Bsize))
}

func syncBlockAvailable(blocks uint64, blockBytes int64) (uint64, error) {
	if blockBytes <= 0 {
		return 0, errors.New("invalid filesystem allocation unit")
	}
	size := uint64(blockBytes)
	if blocks > math.MaxUint64/size {
		return 0, errors.New("filesystem available bytes overflow")
	}
	return blocks * size, nil
}

// Budget the write before touching the filesystem, including a new temporary
// manifest when bootstrap/rotation publishes a new set of files. Slack covers
// block rounding approximately; Fstatfs is not a reservation or quota check.
func captureWriteBudget(recordBytes, headerBytes int64, publish bool) uint64 {
	if recordBytes < 0 || headerBytes < 0 || recordBytes > math.MaxInt64-headerBytes {
		return math.MaxUint64
	}
	budget := uint64(recordBytes+headerBytes) + syncAllocationSlack
	if publish {
		budget += syncManifestLimit + syncAllocationSlack
	}
	return budget
}

func (s *syncSegments) resourceStatusLocked(rawBytes int64, writeBytes uint64) (syncResourceStatus, error) {
	var status syncResourceStatus
	if s.poisoned || s.dir == nil {
		return status, errors.New("segmented replication journal unavailable")
	}
	status.LiveBytes, status.Views = s.bytesLocked(), s.views
	if s.manifest.Anchor != "" {
		base := s.manifest.Base
		if s.manifest.Confirmed.Version != 0 {
			base = s.manifest.Confirmed
		}
		status.UnconfirmedBytes = s.endLocked().Position.Offset - base.Position.Offset
	}
	if rawBytes < 0 || status.UnconfirmedBytes < 0 || rawBytes > math.MaxInt64-status.UnconfirmedBytes {
		return status, errSyncJournalCorrupt
	}
	projected := status.UnconfirmedBytes + rawBytes
	// Recovery is based on the existing backlog, admission on the projected
	// backlog. Otherwise resume=0 could never admit even one new command after
	// GC drained to zero without an unrelated status() call first.
	if s.backlogPaused && status.UnconfirmedBytes <= s.policy.ResumeUnconfirmedBytes {
		s.backlogPaused = false
	}
	if !s.backlogPaused {
		s.backlogPaused = projected > s.policy.MaxUnconfirmedBytes
	}
	var err error
	if s.availableBytes != nil {
		status.AvailableBytes, err = s.availableBytes()
	} else {
		status.AvailableBytes, err = syncAvailableBytes(int(s.dir.Fd()))
	}
	if err != nil {
		s.diskPaused = true
		status.DiskPaused, status.BacklogPaused = true, s.backlogPaused
		return status, fmt.Errorf("%w: cannot sample filesystem availability", errSyncCapturePressure)
	}
	threshold := s.policy.MinFreeBytes
	if s.diskPaused {
		threshold = s.policy.ResumeFreeBytes
	}
	// Subtraction avoids overflow even for an extreme configured reserve.
	s.diskPaused = status.AvailableBytes < threshold || writeBytes > status.AvailableBytes-threshold
	status.DiskPaused, status.BacklogPaused = s.diskPaused, s.backlogPaused
	if status.DiskPaused || status.BacklogPaused {
		return status, fmt.Errorf("%w: disk=%t unconfirmed=%t", errSyncCapturePressure, status.DiskPaused, status.BacklogPaused)
	}
	return status, nil
}

func (s *syncSegments) resourceStatus() (syncResourceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resourceStatusLocked(0, 0)
}

func (s *syncSegments) admitCapture(rawBytes int64, writeBytes uint64) error {
	_, err := s.resourceStatusLocked(rawBytes, writeBytes)
	return err
}
