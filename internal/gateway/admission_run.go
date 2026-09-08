package gateway

// Durable local run diagnostics and identity lock. Dirty is not a business
// startup prohibition, a lease, or proof about Redis/cold data durability.

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const admissionRunFile = "gateway-admission.run"

var (
	errAdmissionRun   = errors.New("gateway admission state invalid, missing or already owned")
	errAdmissionRunIO = errors.New("gateway admission persistence uncertain; business startup blocked")
)

// Internal only: there is no runtime config switch until hot preparation and
// ownership validation are wired. Create is explicit fresh provisioning into
// an empty private directory; resume NEVER creates missing state.
type admissionRunOptions struct {
	Dir                          string
	Binding                      ConfirmationBinding
	Create                       bool
	MaxKeys, MaxBytes, MaxActive int
}

type admissionRunRecord struct {
	Version       int    `json:"version"`
	BindingSHA256 string `json:"binding_sha256"`
	BootID        string `json:"boot_id"`
	State         string `json:"state"` // dirty/clean have the same encoded length.
}

type admissionRun struct {
	mu             sync.Mutex
	f, dir         *os.File
	barriers       *keyBarrierSet
	record         admissionRunRecord
	closed         bool
	recoveredDirty bool
	// Tests only. Errors after I/O may leave a complete durable record.
	fault func(string) error
}

func openAdmissionRun(o admissionRunOptions, fault func(string) error) (*admissionRun, error) {
	if !syncAbsolutePath(o.Dir) || o.Binding.Validate() != nil {
		return nil, errAdmissionRun
	}
	barriers, err := newKeyBarrierSet(o.MaxKeys, o.MaxBytes, o.MaxActive)
	if err != nil {
		return nil, errAdmissionRun
	}
	dfd, err := unix.Open(o.Dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errAdmissionRun
	}
	g := &admissionRun{dir: os.NewFile(uintptr(dfd), o.Dir), barriers: barriers, fault: fault}
	ok := false
	defer func() {
		if !ok {
			g.close()
		}
	}()
	var st unix.Stat_t
	if unix.Fstat(dfd, &st) != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 {
		return nil, errAdmissionRun
	}
	if o.Create {
		// Do not adopt a directory with surviving operation/confirmation state.
		if names, err := g.dir.Readdirnames(1); len(names) != 0 || err != io.EOF {
			return nil, errAdmissionRun
		}
	}
	flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
	if o.Create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	fd, err := unix.Openat(dfd, admissionRunFile, flags, 0600)
	if err != nil {
		return nil, errAdmissionRun
	}
	g.f = os.NewFile(uintptr(fd), filepath.Join(o.Dir, admissionRunFile))
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 ||
		st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 || st.Size > 4096 || unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		return nil, errAdmissionRun
	}
	if !o.Create {
		if readSyncSegmentFrame(g.f, &g.record, 4096) != nil || g.record.Version != 1 ||
			g.record.BindingSHA256 != evictionHash(o.Binding) || !canonicalEvictionHex(g.record.BootID, 16) ||
			(g.record.State != "dirty" && g.record.State != "clean") {
			return nil, errAdmissionRun
		}
		var tail [1]byte
		if n, err := g.f.Read(tail[:]); n != 0 || err != io.EOF {
			return nil, errAdmissionRun
		}
		g.recoveredDirty = g.record.State == "dirty"
		// Diagnostic only. Neither ordinary reply loss nor a dirty boot is a
		// shard-wide transition gate. Recover actual operation intents separately.
	}
	boot, err := confirmationBoot()
	if err != nil {
		return nil, errAdmissionRunIO
	}
	g.record = admissionRunRecord{1, evictionHash(o.Binding), boot, "dirty"}
	// No request may be admitted until BOTH the file and directory are durable.
	if err := g.persist("start"); err != nil {
		return nil, err
	}
	ok = true
	return g, nil
}

func (g *admissionRun) inject(stage string) error {
	if g.fault != nil {
		return g.fault(stage)
	}
	return nil
}

// Fixed-length, checksummed overwrite keeps disk usage constant. A torn record
// fails recovery. On startup an old clean record surviving a failed overwrite
// is safe: this boot has not yet admitted requests. A surviving clean shutdown
// record is safe only because finish closes admission after workers have joined.
func (g *admissionRun) persist(stage string) error {
	frame, err := syncSegmentFrame(g.record)
	if err != nil || g.inject(stage+"_before_write") != nil {
		return errAdmissionRunIO
	}
	n, err := g.f.WriteAt(frame, 0)
	if err != nil || n != len(frame) || g.inject(stage+"_after_write") != nil ||
		g.f.Sync() != nil || g.inject(stage+"_after_sync") != nil ||
		g.dir.Sync() != nil || g.inject(stage+"_after_directory_sync") != nil {
		return errAdmissionRunIO
	}
	return nil
}

// finish is called ONLY after all business readers/writers and preparation
// workers have joined. Retained transitions/uncertainty keep the diagnostic
// dirty marker, but are not a shutdown failure or a business-start prohibition.
func (g *admissionRun) finish() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errAdmissionRun
	}
	defer g.closeLocked()
	if !g.barriers.closeQuiescent() {
		return nil
	}
	g.record.State = "clean"
	return g.persist("finish")
}

// close aborts without writing clean, including all startup failure paths.
func (g *admissionRun) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeLocked()
}

func (g *admissionRun) closeLocked() {
	if g.closed {
		return
	}
	g.closed = true
	g.barriers.close()
	if g.f != nil {
		g.f.Close()
	}
	if g.dir != nil {
		g.dir.Close()
	}
}
