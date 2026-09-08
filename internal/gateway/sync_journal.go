package gateway

// G6a bounded journal prototype. Not connected to the running Service.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	errSyncJournalFull    = errors.New("replication journal capacity reached")
	errSyncJournalCorrupt = errors.New("replication journal corrupt or incomplete; explicit recovery required")
	errSyncHistoryGap     = errors.New("replication history cannot resume; automatic full replacement is forbidden")
)

const syncJournalMagic = "RPGSYNC1"
const syncMaxRecord = 16 << 20

// Offset identifies the last byte inclusively. A journal position means
// durable capture only; cold applied progress is tracked in its own checkpoint.
type syncPosition struct {
	Source  string `json:"source"`
	History string `json:"history"`
	Offset  int64  `json:"offset"`
}

type syncRecord struct {
	Kind     string       `json:"kind"`
	Position syncPosition `json:"position"`
	Data     []byte       `json:"data"` // opaque RDB or one exact RESP command frame
}

// Single-process exclusive journal. Each framed record has length + CRC32C.
// Only fsync success advances the in-memory durable position. Recovery scans
// complete records; corruption/torn tails fail closed, never silently truncate.
type syncJournal struct {
	mu       sync.Mutex
	f        *os.File
	limit    int64
	size     int64
	position syncPosition
	poisoned bool
}

func openSyncJournal(path string, limit int64) (*syncJournal, error) {
	if !filepath.IsAbs(path) || limit < 1024 || limit > 1<<30 {
		return nil, errors.New("invalid replication journal path or capacity")
	}
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, errors.New("cannot open replication journal")
	}
	f := os.NewFile(uintptr(fd), path)
	ok := false
	defer func() {
		if !ok {
			f.Close()
		}
	}()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("replication journal must be regular and owner-only")
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errors.New("replication journal already in use")
	}
	j := &syncJournal{f: f, limit: limit, size: st.Size()}
	if j.size > limit {
		return nil, errSyncJournalFull
	}
	if j.size == 0 {
		if _, err = f.WriteString(syncJournalMagic); err != nil {
			return nil, err
		}
		if err = f.Sync(); err != nil {
			return nil, err
		}
		// Persist creation as well as contents on filesystems honoring directory fsync.
		dir, err := os.Open(filepath.Dir(path))
		if err != nil {
			return nil, err
		}
		err = dir.Sync()
		dir.Close()
		if err != nil {
			return nil, err
		}
		j.size = int64(len(syncJournalMagic))
	} else {
		magic := make([]byte, len(syncJournalMagic))
		if _, err = io.ReadFull(f, magic); err != nil || string(magic) != syncJournalMagic {
			return nil, errSyncJournalCorrupt
		}
		for {
			record, err := readSyncRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil || j.check(record) != nil {
				return nil, errSyncJournalCorrupt
			}
			j.position = record.Position
		}
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	// A complete record surviving an uncertain prior fsync becomes durable here.
	if err = f.Sync(); err != nil {
		return nil, err
	}
	ok = true
	return j, nil
}

func validSyncHistory(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 20
}

func (j *syncJournal) check(r syncRecord) error {
	if !validSyncHistory(r.Position.History) || r.Position.Offset < 0 || r.Position.Offset == math.MaxInt64 || r.Position.Source == "" || len(r.Position.Source) > 256 {
		return errSyncJournalCorrupt
	}
	if j.position.History == "" {
		if r.Kind != "snapshot" || len(r.Data) < 9 || !bytes.HasPrefix(r.Data, []byte("REDIS")) {
			return errSyncJournalCorrupt
		}
		return nil
	}
	if r.Kind != "command" || len(r.Data) == 0 || r.Position.Source != j.position.Source || r.Position.History != j.position.History || j.position.Offset > math.MaxInt64-int64(len(r.Data))-1 || r.Position.Offset != j.position.Offset+int64(len(r.Data)) {
		return errSyncJournalCorrupt
	}
	return nil
}

func readSyncRecord(r io.Reader) (syncRecord, error) {
	var v syncRecord
	var header [8]byte
	n, err := io.ReadFull(r, header[:])
	if err == io.EOF && n == 0 {
		return v, io.EOF
	}
	if err != nil {
		return v, errSyncJournalCorrupt
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length == 0 || length > syncMaxRecord {
		return v, errSyncJournalCorrupt
	}
	b := make([]byte, int(length))
	if _, err = io.ReadFull(r, b); err != nil {
		return v, errSyncJournalCorrupt
	}
	if crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)) != binary.BigEndian.Uint32(header[4:]) {
		return v, errSyncJournalCorrupt
	}
	if json.Unmarshal(b, &v) != nil {
		return v, errSyncJournalCorrupt
	}
	return v, nil
}

func (j *syncJournal) append(r syncRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.poisoned {
		return errors.New("replication journal unavailable")
	}
	if err := j.check(r); err != nil {
		return err
	}
	// Bound before JSON's base64 allocation.
	if len(r.Data) > 8<<20 {
		return errors.New("replication capture record exceeds prototype limit")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > syncMaxRecord || int64(len(b)+8) > j.limit-j.size {
		return errSyncJournalFull
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(header[4:], crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	j.poisoned = true // any partial write/fsync error requires close and explicit recovery
	if _, err = j.f.Write(header[:]); err != nil {
		return err
	}
	if _, err = j.f.Write(b); err != nil {
		return err
	}
	if err = j.f.Sync(); err != nil {
		return err
	}
	j.size += int64(len(b) + 8)
	j.position = r.Position
	j.poisoned = false
	return nil
}

func (j *syncJournal) durablePosition() syncPosition {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.position
}

func (j *syncJournal) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.poisoned = true
	return j.f.Close()
}

// Copy a bounded durable prefix for the isolated applier. Holding the mutex
// avoids a concurrent close/append; network operations happen after release.
// This deliberately scans/copies the small prototype log, not a scalable tailer.
func (j *syncJournal) applicationPrefix() ([]syncRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.poisoned || j.size > 16<<20 {
		return nil, errors.New("journal unavailable or above application prototype limit")
	}
	r := io.NewSectionReader(j.f, int64(len(syncJournalMagic)), j.size-int64(len(syncJournalMagic)))
	check := &syncJournal{}
	var records []syncRecord
	for {
		record, err := readSyncRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil || check.check(record) != nil {
			return nil, errSyncJournalCorrupt
		}
		if len(records) >= 4096 {
			return nil, errors.New("application prototype record limit exceeded")
		}
		records = append(records, record)
		check.position = record.Position
	}
	if len(records) == 0 || check.position != j.position {
		return nil, errSyncJournalCorrupt
	}
	return records, nil
}
