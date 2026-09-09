package gateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// Owned by the server gate; capture never acquires this gate. The first held
// barrier is released only after a durable hot-retained completion.
type confirmationLive struct {
	stopping                        atomic.Bool // visible without waiting behind an in-flight RPC
	runner                          *syncRunner
	started, active, ready, blocked bool
	cancel                          context.CancelFunc
	done                            chan struct{}
	held                            *evictionOperationState
	previous                        syncApplyCheckpoint
	verified                        bool
	verifiedAt                      time.Time
}

// Live mode starts from an existing bounded named capture, preserving the
// pinned history. Initial provisioning/FULLRESYNC replacement is not granted.
func NewLiveConfirmationServer(o IsolatedConfirmationOptions) (*IsolatedConfirmationServer, error) {
	s, err := NewIsolatedConfirmationServer(o)
	if err != nil {
		return nil, err
	}
	r := s.probe.runner
	r.confirmation = s
	s.live = &confirmationLive{runner: r, done: make(chan struct{})}
	s.target.evictionReceipt = true
	r.target.evictionReceipt = true
	// A held lane cannot reclaim its suffix. Use a conservative backlog budget
	// below the file cap; the existing hard cap still fails closed for unusual
	// frame overhead/oversized commands and is not a disk reservation.
	s.journal.policy.MaxUnconfirmedBytes = min(s.journal.policy.MaxUnconfirmedBytes, o.JournalBytes/8)
	s.journal.policy.ResumeUnconfirmedBytes = s.journal.policy.MaxUnconfirmedBytes / 2
	return s, nil
}

// Find the first barrier in the SAME immutable view that will be applied.
// A marker appended concurrently cannot slip into an unbounded second view.
func liveConfirmationBoundary(ctx context.Context, v *syncSegmentView, binding ConfirmationBinding) (*evictionOperationState, syncApplyCheckpoint, error) {
	return liveConfirmationBoundaryFrom(ctx, v, binding, v.floor)
}

func liveConfirmationBoundaryFrom(ctx context.Context, v *syncSegmentView, binding ConfirmationBinding, from syncApplyCheckpoint) (*evictionOperationState, syncApplyCheckpoint, error) {
	var held *evictionOperationState
	end := v.end
	found := errors.New("live preparation found")
	complete, err := v.walkResidency(ctx, from, v.end, func(r syncRecord, previous, _ syncApplyCheckpoint) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if residencyRecord(r.Data) {
			e, err := parseResidencyEnvelope(r.Data)
			if err != nil || !binding.accepts(e.intent) {
				return errSyncUnsupported
			}
			return nil
		}
		reader, _ := NewReader(bytes.NewReader(r.Data), DefaultConfig().Limits)
		_, argv, err := reader.ReadCommand()
		if err != nil {
			return errSyncUnsupported
		}
		if len(argv) == 3 && strings.EqualFold(string(argv[0]), "SET") && bytes.HasPrefix(argv[1], []byte(syncMetadataPrefix+"{g6b-probe}:prepare:")) {
			var intent evictionIntent
			payload, ok := strings.CutPrefix(string(argv[2]), "prepare:")
			if !ok || decodeConfirmation(strings.NewReader(payload), &intent) != nil || !binding.accepts(intent) || !bytes.Equal(r.Data, intent.prepareFrame()) {
				return errSyncUnsupported
			}
			p := evictionPreparation{Before: previous.Position, Record: r}
			s := evictionOperationState{Identity: binding.identity(), Intent: intent, Prepare: &p}
			if !validEvictionPreparation(s, p) {
				return errSyncHistoryGap
			}
			held, end = &s, previous
			return found
		}
		_, err = syncPendingCommand(r.Data)
		return err
	})
	if errors.Is(err, found) {
		err = nil
	} else if err == nil {
		end = complete
	}
	return held, end, err
}

func (s *IsolatedConfirmationServer) liveOperation(held *evictionOperationState) (*evictionOperation, error) {
	path := filepath.Join(s.options.OperationDir, "confirmation-operation.log")
	var create *evictionIntent
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		if held == nil {
			return nil, nil
		}
		create = &held.Intent
		if held.Intent.Version == 2 && held.Intent.PreviousOperationID != strings.Repeat("0", 32) {
			return nil, errSyncHistoryGap
		}
	} else if err != nil {
		return nil, errConfirmationRPC
	}
	op, err := openEvictionOperation(path, s.options.Binding.identity(), create, nil)
	if err != nil {
		return nil, err
	}
	state, err := op.snapshot()
	if err == nil && state.Finished {
		op.close()
		if held == nil {
			return nil, nil
		}
		if held.Intent.Version != state.Intent.Version || (held.Intent.Version == 2 && held.Intent.PreviousOperationID != state.Intent.OperationID) {
			return nil, errSyncHistoryGap
		}
		op, err = replaceFinishedOperation(path, s.options.Binding.identity(), held.Intent)
		if err != nil {
			return nil, err
		}
		state, err = op.snapshot()
	}
	if held == nil {
		op.close()
		return nil, errSyncHistoryGap
	}
	if err != nil || evictionHash(state.Intent) != evictionHash(held.Intent) {
		op.close()
		return nil, errConfirmationRPC
	}
	if err := op.recordPreparation(*held.Prepare); err != nil {
		op.close()
		return nil, err
	}
	return op, nil
}

// Invoked by the actual runner, not its test operations. Lock order is server
// gate -> journal applyMu. RPC uses the same gate and cannot advance this lane.
func (s *IsolatedConfirmationServer) drainLive(ctx context.Context, j *syncSegments) (position syncPosition, result error) {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return position, ctx.Err()
	}
	l := s.live
	if s.closed || l == nil || !l.active || l.blocked || j != s.journal {
		return position, errConfirmationRPC
	}
	defer func() {
		if result != nil && !errors.Is(result, errSyncColdConnect) {
			l.blocked = true
		}
	}()
	if l.ready && l.held != nil {
		return l.previous.Position, nil
	}
	if !l.ready {
		if err := s.checkSource(ctx, j.durablePosition().Offset); err != nil {
			return position, err
		}
	}
	var held *evictionOperationState
	var end syncApplyCheckpoint
	position, result = func() (syncPosition, error) {
		j.applyMu.Lock()
		defer j.applyMu.Unlock()
		v, err := j.pendingView()
		if err != nil {
			return syncPosition{}, err
		}
		defer v.close()
		c, closeClient, err := s.target.open(ctx)
		if err != nil {
			return syncPosition{}, err
		}
		meta, err := c.pendingMeta()
		var terminal string
		if err == nil {
			var exists bool
			terminal, exists, err = c.get(c.evictionReceiptKey())
			if err == nil && exists {
				err = c.expect(":-1\r\n", "PTTL", c.evictionReceiptKey())
			}
		}
		closeClient()
		if err != nil {
			return syncPosition{}, err
		}
		from := v.base
		if meta.checkpoint.Version != 0 {
			if err := v.provePending(meta); err != nil {
				return syncPosition{}, err
			}
			from = meta.checkpoint
		}
		// A cold completion can outlive the process that committed it. Restore
		// the local terminal frame only from the exact receipt and a proved
		// checkpoint; an ordinary prepared receipt is never completion.
		if ack, ok := parseFinishedColdReceipt(terminal); ok {
			path := filepath.Join(s.options.OperationDir, "confirmation-operation.log")
			op, err := openEvictionOperation(path, s.options.Binding.identity(), nil, nil)
			if err != nil {
				return syncPosition{}, err
			}
			state, err := op.snapshot()
			if err == nil && state.Receipt != nil && *state.Receipt == ack &&
				validEvictionReceipt(state, ack) && sameSyncLineage(meta.checkpoint, ack.Prepared) &&
				meta.checkpoint.Position.Offset >= ack.Prepared.Position.Offset {
				err = op.recordFinished()
			}
			op.close()
			if err != nil {
				return syncPosition{}, err
			}
		}
		held, end, err = liveConfirmationBoundaryFrom(ctx, v, s.options.Binding, from)
		if err != nil {
			return syncPosition{}, err
		}
		op, err := s.liveOperation(held)
		if err != nil {
			return syncPosition{}, err
		}
		target := s.target
		if op != nil {
			defer op.close()
			state, err := op.snapshot()
			if err != nil {
				return syncPosition{}, err
			}
			held = &state
			// Even a locally persisted receipt must be proved against this log;
			// online confirmation remains mandatory after process restart.
			ack, err := v.proveEvictionPreparation(state)
			if err != nil || ack.Previous != end || (state.Receipt != nil && *state.Receipt != ack) {
				return syncPosition{}, errSyncHistoryGap
			}
			target.evictionReceipt = true
		}
		return target.applySegmentView(ctx, v, end)
	}()
	if result != nil {
		return position, result
	}
	if held == nil {
		j.mu.Lock()
		base := j.manifest.Base.Position
		j.mu.Unlock()
		if position != base {
			if err := s.target.pruneSegments(ctx, j); err != nil {
				return position, err
			}
		}
	}
	l.held, l.previous, l.ready = held, end, true
	return position, nil
}

// Graceful shutdown with a held barrier is a persisted STOP, not a fully
// drained cold tail. No confirmation call or deletion is synthesized here.
func (s *IsolatedConfirmationServer) stopLive(ctx context.Context, j *syncSegments, target syncPosition) error {
	applied, err := s.drainLive(ctx, j)
	if err != nil {
		return err
	}
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	held := s.live.held != nil
	previous := s.live.previous.Position
	<-s.gate
	if j.durablePosition() != target || (!held && applied != target) || (held && (applied != previous || target.History != previous.History || target.Offset < previous.Offset)) {
		return errSyncShutdownIncomplete
	}
	resources, err := j.resourceStatus()
	j.mu.Lock()
	confirmed := j.manifest.Confirmed.Position
	j.mu.Unlock()
	s.live.runner.update(func(status *syncRunnerStatus) {
		status.Captured, status.Applied, status.Confirmed, status.Resources = target, applied, confirmed, resources
		status.ResourcesObserved, status.ResourceUpdatedAt = true, time.Now()
		if err == nil || errors.Is(err, errSyncCapturePressure) {
			recordSyncProgress(status, target, applied, confirmed, resources, status.ResourceUpdatedAt)
		}
	})
	if err != nil && !errors.Is(err, errSyncCapturePressure) {
		return err
	}
	return ctx.Err()
}

// RunLive owns the continuous runner until all capture/ACK workers join. It
// never releases the journal; the HTTP service closes it after request joins.
func (s *IsolatedConfirmationServer) RunLive(ctx context.Context) (result error) {
	if ctx == nil {
		return errConfirmationRPC
	}
	s.gate <- struct{}{}
	l := s.live
	if s.closed || l == nil || l.started {
		<-s.gate
		return errConfirmationRPC
	}
	runCtx, abort := context.WithCancel(context.Background())
	l.started, l.active, l.cancel = true, true, abort
	<-s.gate
	defer func() {
		l.stopping.Store(true)
		abort()
		s.gate <- struct{}{}
		l.active = false
		if result != nil {
			l.blocked = true
		}
		<-s.gate
		close(l.done)
	}()
	if ctx.Err() != nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { err := l.runner.run(runCtx, s.journal); l.stopping.Store(true); done <- err }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	l.stopping.Store(true)
	if abortSyncBeforePublication(l.runner, abort) {
		err := <-done
		if syncOnlyCancellation(err) {
			return nil
		}
		return err
	}
	grace, cancel := context.WithTimeout(context.Background(), s.options.Timeout)
	defer cancel()
	err := l.runner.shutdown(grace)
	if err != nil {
		abort()
	}
	workErr := <-done
	if err != nil {
		return err
	}
	return workErr
}

type confirmationLiveStatus struct {
	State                 string    `json:"state"`
	Barrier               string    `json:"barrier"`
	AcceptingConfirmation bool      `json:"accepting_confirmation"`
	CapturedOffset        int64     `json:"captured_offset"`
	AppliedOffset         int64     `json:"applied_offset"`
	CapturedValid         bool      `json:"captured_valid"`
	AppliedValid          bool      `json:"applied_valid"`
	SourceAttempts        int       `json:"source_attempts"`
	CapturePaused         bool      `json:"capture_paused"`
	VerifiedAt            time.Time `json:"verified_at"`
}

// Caller holds server gate. Status is observation only, never receipt proof.
func (s *IsolatedConfirmationServer) liveStatus() confirmationLiveStatus {
	l := s.live
	r := l.runner.snapshot()
	barrier := "none"
	if l.held != nil {
		barrier = "waiting_confirmation"
		if l.held.Receipt != nil {
			barrier = "recorded_requires_revalidation"
		}
		if l.verified {
			barrier = "confirmed_held"
		}
	}
	state := r.State
	if state == "" {
		state = "starting"
	}
	if l.stopping.Load() && state != "stopped" && state != "failed" {
		state = "stopping"
	}
	return confirmationLiveStatus{State: state, Barrier: barrier,
		AcceptingConfirmation: l.active && l.ready && !l.blocked && !l.stopping.Load() && l.held != nil,
		CapturedOffset:        r.Captured.Offset, AppliedOffset: r.Applied.Offset,
		CapturedValid: validSyncMetricPosition(r.Captured), AppliedValid: validSyncMetricPosition(r.Applied),
		SourceAttempts: r.SourceAttempts, CapturePaused: r.State == "pressure" || r.Resources.BacklogPaused || r.Resources.DiskPaused, VerifiedAt: l.verifiedAt}
}
