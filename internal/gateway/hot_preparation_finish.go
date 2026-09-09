package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

const finishingFile = "confirmation-finishing.log"

func openFinishingFile(path string, create bool) (*os.File, bool, error) {
	flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
	if create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	fd, err := unix.Open(path, flags, 0600)
	created := create && err == nil
	if create && errors.Is(err, unix.EEXIST) {
		fd, err = unix.Open(path, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	}
	if err != nil {
		return nil, false, err
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 || st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 || st.Size > evictionOperationFrameLimit+8 {
		f.Close()
		return nil, false, errEvictionOperation
	}
	return f, created, nil
}

func (h *hotPreparation) restoreFinishing() error {
	path := filepath.Join(h.options.Confirmation.OperationDir, finishingFile)
	f, _, err := openFinishingFile(path, false)
	if errors.Is(err, os.ErrNotExist) {
		if h.confirmation != nil {
			state, err := h.confirmation.op.snapshot()
			if err != nil {
				return err
			}
			h.finishing = state.Finished
		}
		return nil
	}
	if err != nil || h.confirmation == nil {
		if f != nil {
			f.Close()
		}
		return errEvictionOperation
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, evictionOperationFrameLimit+9))
	if err != nil {
		return err
	}
	state, err := h.confirmation.op.snapshot()
	if err != nil {
		return err
	}
	expected, err := syncSegmentFrame(state.Intent)
	if err != nil || !bytes.Equal(data, expected) {
		return errEvictionOperation
	}
	if f.Sync() != nil || h.confirmation.op.dir.Sync() != nil {
		return errEvictionOperationIO
	}
	h.finishing = true
	return nil
}

// The fixed, bounded intent file is written BEFORE any finish RPC. A complete
// surviving write is revalidated/fsynced on retry; malformed bytes are not ignored.
func (h *hotPreparation) persistFinishing() error {
	state, err := h.confirmation.op.snapshot()
	if err != nil {
		return err
	}
	frame, err := syncSegmentFrame(state.Intent)
	if err != nil {
		return err
	}
	path := filepath.Join(h.options.Confirmation.OperationDir, finishingFile)
	f, created, err := openFinishingFile(path, true)
	if err != nil {
		return err
	}
	defer f.Close()
	if !created {
		data, readErr := io.ReadAll(io.LimitReader(f, evictionOperationFrameLimit+9))
		if readErr != nil || !bytes.Equal(data, frame) {
			return errEvictionOperation
		}
	} else {
		if n, writeErr := f.Write(frame); writeErr != nil || n != len(frame) {
			return errEvictionOperationIO
		}
	}
	if f.Sync() != nil || h.confirmation.op.dir.Sync() != nil {
		return errEvictionOperationIO
	}
	return nil
}

// Caller holds h.gate. Repeated requests for the immediately completed ID
// succeed locally; older requests during a new operation never touch its barrier.
func (h *hotPreparation) requestFinish(ctx context.Context, req hotPreparationRequest) error {
	if h.request == nil {
		op, err := openEvictionOperation(filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log"), h.options.Confirmation.Binding.identity(), nil, nil)
		if err != nil {
			return err
		}
		defer op.close()
		state, err := op.snapshot()
		if err != nil || !state.Finished || state.Intent.OperationID != req.OperationID || !bytes.Equal(state.Intent.Key, req.Key) {
			return errConfirmationRPC
		}
		return nil
	}
	if h.request.OperationID != req.OperationID || !bytes.Equal(h.request.Key, req.Key) || h.confirmation == nil || h.poisoned {
		return errConfirmationRPC
	}
	if err := h.persistFinishing(); err != nil {
		return err
	}
	h.finishing = true
	err := h.finishAttempt(ctx)
	if h.finishing {
		select {
		case h.wakeFinish <- struct{}{}:
		default:
		}
	}
	return err
}

func (h *hotPreparation) finishAttempt(ctx context.Context) error {
	if !h.finishing || h.confirmation == nil || h.poisoned {
		return errConfirmationRPC
	}
	state, err := h.confirmation.op.snapshot()
	if errors.Is(err, errEvictionOperationIO) || errors.Is(err, errEvictionOperationClosed) {
		// A failed fsync may still leave a complete frame. Reopen, validate
		// every frame and fsync again; never clear protection on corrupt bytes.
		h.confirmation.op.close()
		op, openErr := openEvictionOperation(filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log"), h.options.Confirmation.Binding.identity(), nil, nil)
		if openErr != nil {
			return openErr
		}
		h.confirmation.op = op
		state, err = op.snapshot()
	}
	if err != nil {
		return err
	}
	if h.request == nil || state.Intent.OperationID != h.request.OperationID || !bytes.Equal(state.Intent.Key, h.request.Key) {
		return errConfirmationRPC
	}
	if !state.Finished {
		// Finishing retains the hot value. Once the immutable source marker
		// is verified, delayed preparation scripts cannot append another SET;
		// ordinary traffic need not wait for cold acknowledgement.
		if h.barrier != nil {
			if err := h.verifyRetainedMarker(ctx, state.Intent); err != nil {
				if err := h.prepare(ctx, *h.request); err != nil {
					return err
				}
				if err := h.verifyRetainedMarker(ctx, state.Intent); err != nil {
					return err
				}
			}
			if err := h.releaseRetainedHot(); err != nil {
				return err
			}
		}
		if state.Receipt == nil {
			if err := h.confirmation.Confirm(ctx); err != nil {
				return err
			}
		}
		if err := h.confirmation.Finish(ctx); err != nil {
			return err
		}
	}
	// A persisted local finish is evidence that the remote completion was
	// checked. It authorizes only reopening access to retained HOT data.
	path := filepath.Join(h.options.Confirmation.OperationDir, finishingFile)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := h.confirmation.op.dir.Sync(); err != nil {
		return err
	}
	if err := h.releaseRetainedHot(); err != nil {
		return err
	}
	h.confirmation.Close()
	if h.autoFinishTimer != nil {
		h.autoFinishTimer.Stop()
		h.autoFinishTimer = nil
	}
	h.autoFinishAt = time.Time{}
	h.confirmation, h.barrier, h.request = nil, nil, nil
	h.finishing, h.prepared = false, false
	return nil
}

// Caller owns h.gate; all source markers are retained and Redis must be
// exclusively accessed through this Gateway with noeviction. Not a failover lease.
func (h *hotPreparation) verifyRetainedMarker(ctx context.Context, intent evictionIntent) error {
	c, r, err := h.service.openBackend(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		c.SetDeadline(deadline)
	}
	call := func(args ...string) ([]byte, error) {
		if err := writeAll(c, encodeCommand(args...)); err != nil {
			return nil, err
		}
		return r.ReadFrame()
	}
	check := func() bool {
		raw, err := call("INFO", "replication")
		if err != nil {
			return false
		}
		p, code := parseSyncSourceInfo(raw)
		return code == "none" && p.History == intent.History
	}
	if !check() {
		return errConfirmationRPC
	}
	raw, err := call("CONFIG", "GET", "maxmemory-policy")
	if err != nil || string(raw) != "*2\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n" {
		return errConfirmationRPC
	}
	raw, err = call("GET", intent.prepareKey())
	expected := "prepare:" + intent.encode()
	if err != nil || string(raw) != "$"+strconv.Itoa(len(expected))+"\r\n"+expected+"\r\n" {
		return errConfirmationRPC
	}
	raw, err = call("PTTL", intent.prepareKey())
	if err != nil || string(raw) != ":-1\r\n" || !check() {
		return errConfirmationRPC
	}
	return ctx.Err()
}

func (h *hotPreparation) releaseRetainedHot() error {
	if h.barrier == nil {
		return nil
	}
	b := h.barrier
	g := b.set
	g.mu.Lock()
	defer g.mu.Unlock()
	if b.entry.barrier != b || b.entry.active != 0 {
		return errKeyBarrierBusy
	}
	b.entry.barrier, b.entry.uncertain = nil, false
	b.signalLocked()
	g.removeIdleLocked(b.entry)
	h.barrier = nil
	h.confirmation.hotRetained = true
	return nil
}

func (h *hotPreparation) runFinishing(retry bool) {
	defer close(h.finishDone)
	delay := 250 * time.Millisecond
	for {
		var timer *time.Timer
		var tick <-chan time.Time
		if retry {
			timer = time.NewTimer(delay)
			tick = timer.C
		}
		select {
		case <-h.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-h.wakeFinish:
			if timer != nil {
				timer.Stop()
			}
		case <-tick:
		}
		select {
		case h.gate <- struct{}{}:
		case <-h.ctx.Done():
			return
		}
		var err error
		expired := h.confirmation != nil && !h.autoFinishAt.IsZero() && !time.Now().Before(h.autoFinishAt)
		if expired && !h.finishing && !h.closed && !h.poisoned {
			err = h.persistFinishing()
			if err == nil {
				h.finishing = true
			}
		}
		pending := (h.finishing || expired) && !h.closed
		if h.finishing && !h.closed && err == nil {
			ctx, cancel := context.WithTimeout(h.ctx, h.options.Confirmation.Timeout)
			err = h.finishAttempt(ctx)
			cancel()
		}
		retry = (h.finishing || (expired && h.confirmation != nil)) && !h.closed
		<-h.gate
		if pending && err != nil {
			delay = min(delay*2, 5*time.Second)
		} else {
			delay = 250 * time.Millisecond
		}
	}
}
