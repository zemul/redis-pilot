package gateway

// G6b-2b: isolated, bounded, single-operation write-ahead intent. Recovery
// restores knowledge, never permission to delete. A stored cold receipt must
// be revalidated online; no hot-deletion dispatch API exists.

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const evictionOperationMagic = "RPEVI001"
const evictionOperationFrameLimit = 256 << 10
const evictionOperationFileLimit = 512 << 10

var (
	errEvictionOperation       = errors.New("eviction operation journal invalid or identity mismatch")
	errEvictionOperationIO     = errors.New("eviction operation persistence uncertain; close and recover")
	errEvictionOperationClosed = errors.New("eviction operation journal closed")
)

type evictionPreparation struct {
	Before syncPosition `json:"before"`
	Record syncRecord   `json:"record"`
}

type evictionOperationState struct {
	Identity syncTaskIdentity     `json:"identity"`
	Intent   evictionIntent       `json:"intent"`
	Prepare  *evictionPreparation `json:"prepare,omitempty"`
	Receipt  *evictionColdReceipt `json:"receipt,omitempty"`
}

type evictionOperation struct {
	mu               sync.Mutex
	f, dir           *os.File
	state            evictionOperationState
	poisoned, closed bool
	// Isolated fault-injection seam, never a runtime configuration option.
	fault func(string) error
}

func newEvictionIntent(task string, epoch uint64, history string, key, value []byte) (evictionIntent, error) {
	if len(key) > 64<<10 || len(value) > syncPendingValueLimit {
		return evictionIntent{}, errEvictionOperation
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return evictionIntent{}, errEvictionOperationIO
	}
	h := sha256.Sum256(value)
	i := evictionIntent{Version: 1, TaskID: task, OperationID: hex.EncodeToString(id[:]), Epoch: epoch, History: history,
		Key: append([]byte{}, key...), ValueSHA256: hex.EncodeToString(h[:])}
	if !i.valid() {
		return evictionIntent{}, errEvictionOperation
	}
	return i, nil
}

func (i evictionIntent) prepareKey() string {
	return syncMetadataPrefix + "{g6b-probe}:prepare:" + i.TaskID + ":" + i.OperationID
}

func (i evictionIntent) prepareFrame() []byte {
	return encodeCommand("SET", i.prepareKey(), "prepare:"+i.encode())
}

func validEvictionPreparation(s evictionOperationState, p evictionPreparation) bool {
	if p.Before.Source != s.Identity.Source || p.Before.History != s.Intent.History || p.Before.Offset < 0 ||
		!bytes.Equal(p.Record.Data, s.Intent.prepareFrame()) {
		return false
	}
	check := syncJournal{position: p.Before}
	return check.check(p.Record) == nil
}

func copyEvictionOperationState(s evictionOperationState) evictionOperationState {
	s.Intent.Key = append([]byte{}, s.Intent.Key...)
	if s.Prepare != nil {
		p := *s.Prepare
		p.Record.Data = bytes.Clone(p.Record.Data)
		s.Prepare = &p
	}
	if s.Receipt != nil {
		r := *s.Receipt
		s.Receipt = &r
	}
	return s
}

// create != nil means create exclusively; nil means recover an existing file,
// never create a missing one. The parent must be a private, trusted directory.
// A fixed path per shard is a caller obligation, not distributed fencing.
func openEvictionOperation(path string, identity syncTaskIdentity, create *evictionIntent, fault func(string) error) (*evictionOperation, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || !validSyncTaskIdentity(&identity) ||
		(create != nil && (!create.valid() || create.TaskID != identity.TaskID)) {
		return nil, errEvictionOperation
	}
	dfd, err := unix.Open(filepath.Dir(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errEvictionOperation
	}
	g := &evictionOperation{dir: os.NewFile(uintptr(dfd), filepath.Dir(path)), fault: fault}
	ok := false
	defer func() {
		if !ok {
			g.close()
		}
	}()
	var ds unix.Stat_t
	if unix.Fstat(dfd, &ds) != nil || ds.Uid != uint32(os.Geteuid()) || ds.Mode&0077 != 0 {
		return nil, errEvictionOperation
	}
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if create != nil {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	fd, err := unix.Openat(dfd, filepath.Base(path), flags, 0600)
	if err != nil {
		return nil, errEvictionOperation
	}
	g.f = os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 ||
		st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 || st.Size > evictionOperationFileLimit {
		return nil, errEvictionOperation
	}
	if unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		return nil, errEvictionOperation
	}
	if create != nil {
		g.state = copyEvictionOperationState(evictionOperationState{Identity: identity, Intent: *create})
		frame, err := syncSegmentFrame(g.state)
		if err != nil || len(frame) > evictionOperationFrameLimit {
			return nil, errEvictionOperation
		}
		data := append([]byte(evictionOperationMagic), frame...)
		if err := g.persist(data, "create"); err != nil {
			return nil, err
		}
	} else {
		magic := make([]byte, len(evictionOperationMagic))
		if _, err := io.ReadFull(g.f, magic); err != nil || string(magic) != evictionOperationMagic {
			return nil, errEvictionOperation
		}
		if readSyncSegmentFrame(g.f, &g.state, evictionOperationFrameLimit) != nil || g.state.Prepare != nil || g.state.Receipt != nil ||
			g.state.Identity != identity || !g.state.Intent.valid() || g.state.Intent.TaskID != identity.TaskID {
			return nil, errEvictionOperation
		}
		var prepare evictionPreparation
		err := readSyncSegmentFrame(g.f, &prepare, evictionOperationFrameLimit)
		if err != io.EOF {
			if err != nil || !validEvictionPreparation(g.state, prepare) {
				return nil, errEvictionOperation
			}
			g.state.Prepare = &prepare
			var receipt evictionColdReceipt
			if err := readSyncSegmentFrame(g.f, &receipt, evictionOperationFrameLimit); err != io.EOF {
				if err != nil || !validEvictionReceipt(g.state, receipt) {
					return nil, errEvictionOperation
				}
				g.state.Receipt = &receipt
				var extra [1]byte
				if n, err := g.f.Read(extra[:]); n != 0 || err != io.EOF {
					return nil, errEvictionOperation
				}
			}
		}
		// Surviving complete bytes become confirmed local state only after fsync.
		if g.f.Sync() != nil {
			return nil, errEvictionOperationIO
		}
	}
	if g.dir.Sync() != nil || g.inject("after_directory_sync") != nil {
		return nil, errEvictionOperationIO
	}
	ok = true
	return g, nil
}

func (g *evictionOperation) inject(stage string) error {
	if g.fault != nil {
		return g.fault(stage)
	}
	return nil
}

func (g *evictionOperation) persist(data []byte, stage string) error {
	g.poisoned = true
	if g.inject(stage+"_before_write") != nil {
		return errEvictionOperationIO
	}
	n, err := g.f.Write(data)
	if err != nil || n != len(data) || g.inject(stage+"_after_write") != nil || g.f.Sync() != nil ||
		g.inject(stage+"_after_sync") != nil {
		return errEvictionOperationIO
	}
	g.poisoned = false
	return nil
}

// recordPreparation accepts source-capture evidence only, never a cold ACK.
// The caller must have read it from the durable, bound capture journal. Exact
// bytes alone cannot prove provenance; this component does not open that log.
func (g *evictionOperation) recordPreparation(p evictionPreparation) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errEvictionOperationClosed
	}
	if g.poisoned {
		return errEvictionOperationIO
	}
	if !validEvictionPreparation(g.state, p) {
		return errEvictionOperation
	}
	if old := g.state.Prepare; old != nil {
		if old.Before == p.Before && old.Record.Position == p.Record.Position && bytes.Equal(old.Record.Data, p.Record.Data) {
			return nil
		}
		return errEvictionOperation
	}
	frame, err := syncSegmentFrame(p)
	if err != nil || len(frame) > evictionOperationFrameLimit {
		return errEvictionOperation
	}
	st, err := g.f.Stat()
	if err != nil {
		g.poisoned = true
		return errEvictionOperationIO
	}
	if int64(len(frame)) > evictionOperationFileLimit-st.Size() {
		return errEvictionOperation
	}
	if err := g.persist(frame, "prepare"); err != nil {
		return err
	}
	p.Record.Data = bytes.Clone(p.Record.Data)
	g.state.Prepare = &p
	return nil
}

func (g *evictionOperation) snapshot() (evictionOperationState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return evictionOperationState{}, errEvictionOperationClosed
	}
	if g.poisoned {
		return evictionOperationState{}, errEvictionOperationIO
	}
	return copyEvictionOperationState(g.state), nil
}

func (g *evictionOperation) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	if g.f != nil {
		g.f.Close()
	}
	if g.dir != nil {
		g.dir.Close()
	}
}
