package gateway

// G6a-6 isolated segmented capture. The manifest is a durable locating index,
// not evidence that a cold write completed. Only the checked pending protocol
// may authorize prune. Sealed records are checked lazily when actually used.

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

const (
	syncSegmentMagic      = "RPGSEG01"
	syncAnchorMagic       = "RPGANC01"
	syncManifestMagic     = "RPGMAN01"
	syncManifestLimit     = 1 << 20
	syncSegmentCountLimit = 1024
)

type syncSegmentIndex struct {
	ID     string              `json:"id"`
	Start  syncApplyCheckpoint `json:"start"`
	End    syncApplyCheckpoint `json:"end"`
	Size   int64               `json:"size"`
	Sealed bool                `json:"sealed"`
}

type syncSegmentManifest struct {
	Version    int                 `json:"version"`
	Generation uint64              `json:"generation"`
	Anchor     string              `json:"anchor"`
	AnchorSize int64               `json:"anchor_size"`
	Base       syncApplyCheckpoint `json:"base"`
	Floor      syncApplyCheckpoint `json:"floor"`
	Confirmed  syncApplyCheckpoint `json:"confirmed"`
	Segments   []syncSegmentIndex  `json:"segments"`
}

type syncSegmentHeader struct {
	Version int                 `json:"version"`
	ID      string              `json:"id"`
	Start   syncApplyCheckpoint `json:"start"`
}

type syncSegments struct {
	mu       sync.Mutex
	applyMu  sync.Mutex // serializes the isolated applier and cold-confirmed GC
	dir      *os.File
	lock     *os.File
	files    []*os.File
	manifest syncSegmentManifest
	anchor   syncRecord
	// Logical capacity covers referenced anchor/segments only. Physical free
	// space is separately sampled; deleted files held by views still consume it.
	segmentLimit, totalLimit  int64
	poisoned                  bool
	policy                    syncResourcePolicy
	diskPaused, backlogPaused bool
	views                     int
	availableBytes            func() (uint64, error) // nil samples this directory's filesystem
	// Test-only injected I/O boundaries. A returned error poisons the handle;
	// close/reopen is mandatory, even if the last durable operation succeeded.
	fault func(stage string) error
}

func syncSegmentFrame(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 || len(b) > syncMaxRecord {
		return nil, errSyncJournalCorrupt
	}
	frame := make([]byte, 8+len(b))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	copy(frame[8:], b)
	return frame, nil
}

// Unlike the old capture format, metadata and segment records must have their
// exact canonical JSON encoding. Unknown/duplicate fields cannot be ignored.
func readSyncSegmentFrame(r io.Reader, v any, max int64) error {
	var h [8]byte
	n, err := io.ReadFull(r, h[:])
	if n == 0 && err == io.EOF {
		return io.EOF
	}
	if err != nil {
		return errSyncJournalCorrupt
	}
	nn := int64(binary.BigEndian.Uint32(h[:4]))
	if nn <= 0 || nn > max {
		return errSyncJournalCorrupt
	}
	b := make([]byte, int(nn))
	if _, err := io.ReadFull(r, b); err != nil {
		return errSyncJournalCorrupt
	}
	if crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)) != binary.BigEndian.Uint32(h[4:]) || json.Unmarshal(b, v) != nil {
		return errSyncJournalCorrupt
	}
	canonical, err := json.Marshal(v)
	if err != nil || !bytes.Equal(canonical, b) {
		return errSyncJournalCorrupt
	}
	return nil
}

func syncSegmentName(prefix string) (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(id[:]), nil
}

func validSyncSegmentName(name, prefix string) bool {
	if len(name) != len(prefix)+32 || name[:len(prefix)] != prefix {
		return false
	}
	b, err := hex.DecodeString(name[len(prefix):])
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == name[len(prefix):]
}

func validSyncCheckpoint(cp syncApplyCheckpoint) bool {
	chain, err := hex.DecodeString(cp.Chain)
	return cp.Version == 2 && validSyncHistory(cp.Position.History) && cp.Position.Source != "" && len(cp.Position.Source) <= 256 && cp.Position.Offset >= 0 && cp.Position.Offset < int64(^uint64(0)>>1) && err == nil && len(chain) == 32 && hex.EncodeToString(chain) == cp.Chain
}

func sameSyncLineage(a, b syncApplyCheckpoint) bool {
	return a.Position.Source == b.Position.Source && a.Position.History == b.Position.History
}

func (s *syncSegments) openFile(name string, flags int) (*os.File, error) {
	fd, err := unix.Openat(int(s.dir.Fd()), name, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(s.dir.Name(), name))
	st, err := f.Stat()
	var raw unix.Stat_t
	statErr := unix.Fstat(fd, &raw)
	if err != nil || statErr != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || raw.Uid != uint32(os.Geteuid()) || raw.Nlink != 1 {
		f.Close()
		return nil, errSyncJournalCorrupt
	}
	return f, nil
}

func (s *syncSegments) stage(name string) error {
	if s.fault != nil {
		return s.fault(name)
	}
	return nil
}

func (s *syncSegments) publish(m syncSegmentManifest) error {
	m.Generation = s.manifest.Generation + 1
	if m.Generation == 0 {
		return errSyncJournalCorrupt
	}
	b, err := syncSegmentFrame(m)
	if err != nil || len(b)+len(syncManifestMagic) > syncManifestLimit {
		return errSyncJournalFull
	}
	name, err := syncSegmentName("manifest-")
	if err != nil {
		return err
	}
	f, err := s.openFile(name, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(append([]byte(syncManifestMagic), b...)); err != nil {
		return err
	}
	if err = s.stage("manifest_written"); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = s.stage("manifest_synced"); err != nil {
		return err
	}
	if err = unix.Renameat(int(s.dir.Fd()), name, int(s.dir.Fd()), "manifest"); err != nil {
		return err
	}
	if err = s.stage("manifest_renamed"); err != nil {
		return err
	}
	if err = s.dir.Sync(); err != nil {
		return err
	}
	if err = s.stage("manifest_dir_synced"); err != nil {
		return err
	}
	s.manifest = m
	return nil
}

func readSyncSegmentObject(f *os.File, magic string, v any, limit int64) error {
	st, err := f.Stat()
	if err != nil || st.Size() > limit || st.Size() <= int64(len(magic)) {
		return errSyncJournalCorrupt
	}
	r := io.NewSectionReader(f, 0, st.Size())
	b := make([]byte, len(magic))
	if _, err := io.ReadFull(r, b); err != nil || string(b) != magic {
		return errSyncJournalCorrupt
	}
	if err := readSyncSegmentFrame(r, v, limit); err != nil {
		return errSyncJournalCorrupt
	}
	var tail [1]byte
	if n, err := r.Read(tail[:]); n != 0 || err != io.EOF {
		return errSyncJournalCorrupt
	}
	return nil
}

func cloneSyncSegmentManifest(m syncSegmentManifest) syncSegmentManifest {
	m.Segments = append([]syncSegmentIndex(nil), m.Segments...)
	return m
}

func openSyncSegments(dir string, segmentLimit, totalLimit int64) (*syncSegments, error) {
	return openSyncSegmentsWithPolicy(dir, segmentLimit, totalLimit, defaultSyncResourcePolicy())
}

func openSyncSegmentsWithPolicy(dir string, segmentLimit, totalLimit int64, policy syncResourcePolicy) (*syncSegments, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(dir) || segmentLimit < 1024 || segmentLimit > 16<<20 || totalLimit < segmentLimit || totalLimit > 1<<30 {
		return nil, errors.New("invalid segmented replication path or capacity")
	}
	created := false
	if err := os.Mkdir(dir, 0700); err == nil {
		created = true
	} else if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	s := &syncSegments{dir: os.NewFile(uintptr(fd), dir), segmentLimit: segmentLimit, totalLimit: totalLimit, policy: policy}
	ok := false
	defer func() {
		if !ok {
			s.close()
		}
	}()
	st, err := s.dir.Stat()
	var dirStat unix.Stat_t
	statErr := unix.Fstat(fd, &dirStat)
	if err != nil || statErr != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 || dirStat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("segmented replication directory must be owner-only")
	}
	if created {
		parent, err := os.Open(filepath.Dir(dir))
		if err != nil {
			return nil, err
		}
		err = parent.Sync()
		parent.Close()
		if err != nil {
			return nil, err
		}
	}
	s.lock, err = s.openFile("LOCK", unix.O_CREAT|unix.O_RDWR)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(s.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errors.New("segmented replication journal already in use")
	}
	if err = s.lock.Sync(); err != nil {
		return nil, err
	}
	if err = s.dir.Sync(); err != nil {
		return nil, err
	}
	mf, err := s.openFile("manifest", unix.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		// A missing index is not an empty journal when any prior capture or
		// temporary publication remains. Do not silently reset its lineage.
		entries, readErr := s.dir.ReadDir(2)
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		for _, entry := range entries {
			if entry.Name() != "LOCK" {
				return nil, errSyncJournalCorrupt
			}
		}
		if err = s.publish(syncSegmentManifest{Version: 1}); err != nil {
			return nil, err
		}
		ok = true
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	err = readSyncSegmentObject(mf, syncManifestMagic, &s.manifest, syncManifestLimit)
	mf.Close()
	if err != nil {
		return nil, err
	}
	m := &s.manifest
	if m.Version != 1 || m.Generation == 0 || len(m.Segments) > syncSegmentCountLimit {
		return nil, errSyncJournalCorrupt
	}
	zero := syncApplyCheckpoint{}
	if m.Anchor == "" {
		if m.AnchorSize != 0 || m.Base != zero || m.Floor != zero || m.Confirmed != zero || len(m.Segments) != 0 {
			return nil, errSyncJournalCorrupt
		}
		ok = true
		return s, nil
	}
	if !validSyncSegmentName(m.Anchor, "anchor-") || !validSyncCheckpoint(m.Base) || !validSyncCheckpoint(m.Floor) || m.AnchorSize < 1 || m.AnchorSize > syncMaxRecord+16 {
		return nil, errSyncJournalCorrupt
	}
	af, err := s.openFile(m.Anchor, unix.O_RDONLY)
	if err != nil {
		return nil, errSyncJournalCorrupt
	}
	ast, err := af.Stat()
	if err != nil || ast.Size() != m.AnchorSize {
		af.Close()
		return nil, errSyncJournalCorrupt
	}
	err = readSyncSegmentObject(af, syncAnchorMagic, &s.anchor, syncMaxRecord+16)
	af.Close()
	check := syncJournal{}
	if err != nil || check.check(s.anchor) != nil || nextPendingCheckpoint(zero, s.anchor) != m.Base {
		return nil, errSyncJournalCorrupt
	}
	if !sameSyncLineage(m.Base, m.Floor) || m.Floor.Position.Offset < m.Base.Position.Offset || (m.Floor.Position.Offset == m.Base.Position.Offset && m.Floor != m.Base) {
		return nil, errSyncJournalCorrupt
	}
	previous := m.Floor
	seen := make(map[string]bool)
	total := m.AnchorSize
	for i := range m.Segments {
		idx := &m.Segments[i]
		if !validSyncSegmentName(idx.ID, "segment-") || seen[idx.ID] || idx.Start != previous || !validSyncCheckpoint(idx.End) || !sameSyncLineage(idx.Start, idx.End) || idx.End.Position.Offset < idx.Start.Position.Offset || idx.Size < int64(len(syncSegmentMagic))+8 || idx.Size > segmentLimit || idx.Sealed != (i < len(m.Segments)-1) || (idx.Sealed && idx.End.Position.Offset == idx.Start.Position.Offset) {
			return nil, errSyncJournalCorrupt
		}
		if idx.End.Position.Offset == idx.Start.Position.Offset && idx.End != idx.Start {
			return nil, errSyncJournalCorrupt
		}
		seen[idx.ID] = true
		f, err := s.openFile(idx.ID, unix.O_RDWR)
		if err != nil {
			return nil, errSyncJournalCorrupt
		}
		s.files = append(s.files, f)
		st, err := f.Stat()
		if err != nil || st.Size() > segmentLimit || st.Size() < idx.Size || (idx.Sealed && st.Size() != idx.Size) {
			return nil, errSyncJournalCorrupt
		}
		if !idx.Sealed {
			old := *idx
			idx.Size = st.Size()
			end, err := scanSyncSegment(f, *idx, false, nil)
			if err != nil {
				return nil, err
			}
			// A manifest-published active prefix may not disappear even if the
			// later unindexed tail contains complete frames.
			prefix := old
			if _, err := scanSyncSegment(f, prefix, true, nil); err != nil {
				return nil, err
			}
			idx.End = end
			if err := f.Sync(); err != nil {
				return nil, err
			}
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				return nil, err
			}
		}
		total += idx.Size
		previous = idx.End
	}
	if total > totalLimit {
		return nil, errSyncJournalFull
	}
	if len(m.Segments) == 0 && m.Floor != m.Base {
		return nil, errSyncJournalCorrupt
	}
	if m.Confirmed != zero && (!validSyncCheckpoint(m.Confirmed) || !sameSyncLineage(m.Base, m.Confirmed) || m.Confirmed.Position.Offset < m.Floor.Position.Offset || m.Confirmed.Position.Offset > previous.Position.Offset || (m.Confirmed.Position.Offset == m.Floor.Position.Offset && m.Confirmed != m.Floor) || (m.Confirmed.Position.Offset == previous.Position.Offset && m.Confirmed != previous)) {
		return nil, errSyncJournalCorrupt
	}
	if m.Floor != m.Base && m.Confirmed == zero {
		return nil, errSyncJournalCorrupt
	}
	ok = true
	return s, nil
}

func (s *syncSegments) endLocked() syncApplyCheckpoint {
	if n := len(s.manifest.Segments); n != 0 {
		return s.manifest.Segments[n-1].End
	}
	return s.manifest.Base
}

func (s *syncSegments) bytesLocked() int64 {
	total := s.manifest.AnchorSize
	for _, idx := range s.manifest.Segments {
		total += idx.Size
	}
	return total
}

func (s *syncSegments) durablePosition() syncPosition {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endLocked().Position
}

func (s *syncSegments) append(r syncRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return errors.New("segmented replication journal unavailable")
	}
	check := syncJournal{position: s.endLocked().Position}
	if err := check.check(r); err != nil {
		return err
	}
	if !utf8.ValidString(r.Position.Source) {
		return errSyncJournalCorrupt
	}
	if len(r.Data) > 8<<20 {
		return errSyncJournalFull
	}
	b, err := syncSegmentFrame(r)
	if err != nil {
		return err
	}
	if s.manifest.Anchor == "" {
		if int64(len(syncAnchorMagic)+len(b)) > s.totalLimit {
			return errSyncJournalFull
		}
		if err := s.admitCapture(0, captureWriteBudget(int64(len(b)), int64(len(syncAnchorMagic)), true)); err != nil {
			return err
		}
		name, err := syncSegmentName("anchor-")
		if err != nil {
			return err
		}
		s.poisoned = true
		f, err := s.openFile(name, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err = f.Write(append([]byte(syncAnchorMagic), b...)); err != nil {
			return err
		}
		if err = s.stage("anchor_written"); err != nil {
			return err
		}
		if err = f.Sync(); err != nil {
			return err
		}
		if err = s.stage("anchor_synced"); err != nil {
			return err
		}
		if err = s.dir.Sync(); err != nil {
			return err
		}
		if err = s.stage("anchor_dir_synced"); err != nil {
			return err
		}
		m := cloneSyncSegmentManifest(s.manifest)
		m.Anchor, m.AnchorSize = name, int64(len(syncAnchorMagic)+len(b))
		m.Base = nextPendingCheckpoint(syncApplyCheckpoint{}, r)
		m.Floor = m.Base
		if err = s.publish(m); err != nil {
			return err
		}
		s.anchor = r
		s.anchor.Data = append([]byte(nil), r.Data...)
		s.poisoned = false
		return nil
	}
	n := len(s.manifest.Segments)
	rotate := n == 0 || s.manifest.Segments[n-1].Size+int64(len(b)) > s.segmentLimit
	if rotate {
		if n >= syncSegmentCountLimit {
			return errSyncJournalFull
		}
		name, err := syncSegmentName("segment-")
		if err != nil {
			return err
		}
		start := s.endLocked()
		h, err := syncSegmentFrame(syncSegmentHeader{1, name, start})
		if err != nil {
			return err
		}
		headerSize := int64(len(syncSegmentMagic) + len(h))
		if headerSize+int64(len(b)) > s.segmentLimit || s.bytesLocked()+headerSize+int64(len(b)) > s.totalLimit {
			return errSyncJournalFull
		}
		if err := s.admitCapture(int64(len(r.Data)), captureWriteBudget(int64(len(b)), headerSize, true)); err != nil {
			return err
		}
		s.poisoned = true
		f, err := s.openFile(name, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR)
		if err != nil {
			return err
		}
		keep := false
		defer func() {
			if !keep {
				f.Close()
			}
		}()
		if _, err = f.Write(append([]byte(syncSegmentMagic), h...)); err != nil {
			return err
		}
		if err = s.stage("segment_written"); err != nil {
			return err
		}
		if err = f.Sync(); err != nil {
			return err
		}
		if err = s.stage("segment_synced"); err != nil {
			return err
		}
		if err = s.dir.Sync(); err != nil {
			return err
		}
		if err = s.stage("segment_dir_synced"); err != nil {
			return err
		}
		m := cloneSyncSegmentManifest(s.manifest)
		if n > 0 {
			m.Segments[n-1].Sealed = true
		}
		m.Segments = append(m.Segments, syncSegmentIndex{name, start, start, headerSize, false})
		if err = s.publish(m); err != nil {
			return err
		}
		s.files = append(s.files, f)
		keep = true
		n++
	} else {
		if s.bytesLocked()+int64(len(b)) > s.totalLimit {
			return errSyncJournalFull
		}
		if err := s.admitCapture(int64(len(r.Data)), captureWriteBudget(int64(len(b)), 0, false)); err != nil {
			return err
		}
	}
	s.poisoned = true
	f := s.files[n-1]
	if _, err = f.Write(b); err != nil {
		return err
	}
	if err = s.stage("append_written"); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = s.stage("append_synced"); err != nil {
		return err
	}
	idx := &s.manifest.Segments[n-1]
	idx.Size += int64(len(b))
	idx.End = nextPendingCheckpoint(idx.End, r)
	s.poisoned = false
	return nil
}

func (s *syncSegments) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poisoned = true
	var errs []error
	for _, f := range s.files {
		errs = append(errs, f.Close())
	}
	s.files = nil
	if s.lock != nil {
		errs = append(errs, s.lock.Close())
		s.lock = nil
	}
	if s.dir != nil {
		errs = append(errs, s.dir.Close())
		s.dir = nil
	}
	return errors.Join(errs...)
}

type syncSegmentView struct {
	anchor                      syncRecord
	base, floor, confirmed, end syncApplyCheckpoint
	owner                       *syncSegments
	lock                        *os.File
	indexes                     []syncSegmentIndex
	files                       []*os.File
	registered                  bool
	closeMu                     sync.Mutex
}

func syncSegmentDup(f *os.File) (*os.File, error) {
	fd, err := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), f.Name()), nil
}

func (s *syncSegments) pendingView() (*syncSegmentView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || s.manifest.Anchor == "" {
		return nil, errSyncJournalCorrupt
	}
	if s.views >= s.policy.MaxViews {
		return nil, errSyncViewLimit
	}
	v := &syncSegmentView{anchor: s.anchor, base: s.manifest.Base, floor: s.manifest.Floor, confirmed: s.manifest.Confirmed, end: s.endLocked(), owner: s, indexes: append([]syncSegmentIndex(nil), s.manifest.Segments...)}
	v.anchor.Data = append([]byte(nil), s.anchor.Data...)
	var err error
	v.lock, err = syncSegmentDup(s.lock)
	if err != nil {
		return nil, err
	}
	for _, f := range s.files {
		dup, err := syncSegmentDup(f)
		if err != nil {
			v.close()
			return nil, err
		}
		v.files = append(v.files, dup)
	}
	s.views++
	v.registered = true
	return v, nil
}

func (v *syncSegmentView) close() error {
	v.closeMu.Lock()
	defer v.closeMu.Unlock()
	// Failed construction calls close while owner.mu is already held. Such a
	// view has not registered yet, so its partial descriptors need no callback.
	if v.registered {
		v.owner.mu.Lock()
		defer v.owner.mu.Unlock()
		v.owner.views--
		v.registered = false
	}
	var errs []error
	for _, f := range v.files {
		errs = append(errs, f.Close())
	}
	v.files = nil
	if v.lock != nil {
		errs = append(errs, v.lock.Close())
		v.lock = nil
	}
	return errors.Join(errs...)
}

// scanSyncSegment reads one fixed extent. Header and records are checksummed;
// the complete digest and final byte boundary must match a sealed/view index.
func scanSyncSegment(f *os.File, idx syncSegmentIndex, verifyEnd bool, fn func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error) (syncApplyCheckpoint, error) {
	r := io.NewSectionReader(f, 0, idx.Size)
	b := make([]byte, len(syncSegmentMagic))
	if _, err := io.ReadFull(r, b); err != nil || string(b) != syncSegmentMagic {
		return syncApplyCheckpoint{}, errSyncJournalCorrupt
	}
	var header syncSegmentHeader
	if err := readSyncSegmentFrame(r, &header, 2048); err != nil || header.Version != 1 || header.ID != idx.ID || header.Start != idx.Start {
		return syncApplyCheckpoint{}, errSyncJournalCorrupt
	}
	previous := idx.Start
	for {
		var record syncRecord
		err := readSyncSegmentFrame(r, &record, syncMaxRecord)
		if err == io.EOF {
			break
		}
		if err != nil {
			return syncApplyCheckpoint{}, err
		}
		check := syncJournal{position: previous.Position}
		if err := check.check(record); err != nil {
			return syncApplyCheckpoint{}, err
		}
		next := nextPendingCheckpoint(previous, record)
		if fn != nil {
			if err := fn(record, previous, next); err != nil {
				return syncApplyCheckpoint{}, err
			}
		}
		previous = next
	}
	if at, _ := r.Seek(0, io.SeekCurrent); at != idx.Size || (verifyEnd && previous != idx.End) {
		return syncApplyCheckpoint{}, errSyncJournalCorrupt
	}
	return previous, nil
}

// walkFrom locates by the persisted checkpoint range, then verifies the entire
// candidate segment before emitting anything from it. It never scans earlier
// sealed segments. Each later segment is likewise checked before its callbacks;
// therefore corruption in a later segment can follow earlier valid callbacks.
// Callers requiring all-or-nothing preflight must perform a read-only walk first.
func (v *syncSegmentView) walkFrom(cp syncApplyCheckpoint, fn func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error) error {
	if v.lock == nil || len(v.files) != len(v.indexes) || !validSyncCheckpoint(cp) || !sameSyncLineage(cp, v.base) || cp.Position.Offset < v.floor.Position.Offset || cp.Position.Offset > v.end.Position.Offset {
		return errSyncHistoryGap
	}
	if len(v.indexes) == 0 {
		if cp != v.base || v.floor != v.base || v.end != v.base {
			return errSyncHistoryGap
		}
		return nil
	}
	start := -1
	for i, idx := range v.indexes {
		// Prefer the preceding segment at a shared boundary: this verifies
		// the actual record yielding cp instead of trusting the next header.
		if cp.Position.Offset >= idx.Start.Position.Offset && cp.Position.Offset <= idx.End.Position.Offset {
			start = i
			break
		}
	}
	if start < 0 {
		return errSyncHistoryGap
	}
	found := cp == v.indexes[start].Start
	if found && cp != v.floor {
		// The only unproved segment-start checkpoint accepted is the durable
		// retained floor. Interior boundaries were selected above instead.
		return errSyncHistoryGap
	}
	for i := start; i < len(v.indexes); i++ {
		idx := v.indexes[i]
		if _, err := scanSyncSegment(v.files[i], idx, true, nil); err != nil {
			return err
		}
		_, err := scanSyncSegment(v.files[i], idx, true, func(record syncRecord, previous, next syncApplyCheckpoint) error {
			if !found {
				if next == cp {
					found = true
					return nil
				}
				if next.Position.Offset >= cp.Position.Offset {
					return errSyncHistoryGap
				}
				return nil
			}
			if fn != nil {
				return fn(record, previous, next)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !found {
			return errSyncHistoryGap
		}
	}
	if !found {
		return errSyncHistoryGap
	}
	return nil
}

// prune assumes its caller has serialized cold application, WATCHed/re-read
// metadata, and verified pending.Next plus the business after-state. This local
// proof deliberately cannot infer cold durability merely from capture offset.
func (s *syncSegments) prune(v *syncSegmentView, meta syncPendingMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned || v == nil || v.owner != s || v.lock == nil || v.base != s.manifest.Base || meta.anchor != v.base.encode() || meta.pending == nil || validatePending(*meta.pending) != nil || meta.checkpoint != meta.pending.Next {
		return errSyncTargetDrift
	}
	p := meta.pending
	if p.Previous.Position.Offset < s.manifest.Floor.Position.Offset || !sameSyncLineage(meta.checkpoint, s.manifest.Base) || meta.checkpoint.Position.Offset > v.end.Position.Offset {
		return errSyncHistoryGap
	}
	confirmed := s.manifest.Confirmed
	if confirmed != (syncApplyCheckpoint{}) && (meta.checkpoint.Position.Offset < confirmed.Position.Offset || (meta.checkpoint.Position.Offset == confirmed.Position.Offset && meta.checkpoint != confirmed)) {
		return errSyncHistoryGap
	}
	stop := errors.New("pending located")
	err := v.walkFrom(p.Previous, func(r syncRecord, previous, next syncApplyCheckpoint) error {
		if previous != p.Previous || next != p.Next || r.Kind != p.Record.Kind || r.Position != p.Record.Position || !bytes.Equal(r.Data, p.Record.Data) {
			return errSyncHistoryGap
		}
		return stop
	})
	if !errors.Is(err, stop) {
		if err == nil {
			return errSyncHistoryGap
		}
		return err
	}
	keepID := ""
	for _, idx := range v.indexes {
		if p.Next.Position.Offset > idx.Start.Position.Offset && p.Next.Position.Offset <= idx.End.Position.Offset {
			keepID = idx.ID
			break
		}
	}
	keep := -1
	for i, idx := range s.manifest.Segments {
		if idx.ID == keepID {
			keep = i
			break
		}
	}
	if keep < 0 {
		return errSyncHistoryGap
	}
	if keep == 0 && meta.checkpoint == confirmed {
		return nil
	}
	old := append([]syncSegmentIndex(nil), s.manifest.Segments[:keep]...)
	oldFiles := append([]*os.File(nil), s.files[:keep]...)
	m := cloneSyncSegmentManifest(s.manifest)
	m.Segments = append([]syncSegmentIndex(nil), m.Segments[keep:]...)
	m.Floor, m.Confirmed = m.Segments[0].Start, meta.checkpoint
	s.poisoned = true
	if err := s.publish(m); err != nil {
		return err
	}
	s.files = append([]*os.File(nil), s.files[keep:]...)
	// A durable manifest never refers to a deleted file. Open views keep
	// independent descriptors, so unlink cannot invalidate their proof.
	defer func() {
		for _, f := range oldFiles {
			f.Close()
		}
	}()
	for _, idx := range old {
		if err := s.stage("gc_before_unlink"); err != nil {
			return err
		}
		if err := unix.Unlinkat(int(s.dir.Fd()), idx.ID, 0); err != nil {
			return fmt.Errorf("remove retired replication segment: %w", err)
		}
		if err := s.stage("gc_after_unlink"); err != nil {
			return err
		}
	}
	if err := s.dir.Sync(); err != nil {
		return err
	}
	if err := s.stage("gc_dir_synced"); err != nil {
		return err
	}
	s.poisoned = false
	return nil
}
