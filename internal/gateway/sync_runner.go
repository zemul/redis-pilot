package gateway

// G6a-8 explicitly invoked isolated loop, NOT a Service/config deployment mode.
// It owns a supplied open journal for the run but never closes/reopens it, never
// repairs a poisoned log and never retries a possibly executed cold write.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	errSyncRunnerBusy      = errors.New("replication loop already owns runner or journal")
	errSyncRetryExhausted  = errors.New("replication connection retry budget exhausted")
	errSyncPressureTimeout = errors.New("replication capture resource pause deadline exceeded")
)

type syncRunnerConfig struct {
	TickInterval, RetryMin, RetryMax, CleanupInterval, MaxPressure time.Duration
	MaxRetries                                                     int
}

func defaultSyncRunnerConfig() syncRunnerConfig {
	return syncRunnerConfig{100 * time.Millisecond, 100 * time.Millisecond, 5 * time.Second, 5 * time.Second, 5 * time.Minute, 8}
}

func (c syncRunnerConfig) validate() error {
	if c.TickInterval < 5*time.Millisecond || c.TickInterval > 10*time.Second || c.RetryMin < c.TickInterval || c.RetryMax < c.RetryMin || c.RetryMax > 30*time.Second || c.CleanupInterval < c.TickInterval || c.CleanupInterval > time.Hour || c.MaxPressure < c.TickInterval || c.MaxPressure > time.Hour || c.MaxRetries < 1 || c.MaxRetries > 100 {
		return errors.New("invalid isolated replication loop limits")
	}
	return nil
}

type syncRunnerOps struct {
	capture func(context.Context, *syncSegments) error
	drain   func(context.Context, *syncSegments) (syncPosition, error)
	cleanup func(context.Context, *syncSegments) error
}

type syncRunnerStatus struct {
	State                                      string
	Captured, Applied, Confirmed               syncPosition
	SourceAttempts, SourceRetries, ColdRetries int
	Resources                                  syncResourceStatus
	LastError                                  string // fixed category, never credentials or remote response
	StartedAt, UpdatedAt, FinishedAt           time.Time
	DrainTarget                                syncPosition
	StopRequested                              bool
	ResourcesObserved                          bool
	ResourceUpdatedAt                          time.Time
	Progress                                   syncProgressSnapshot
}

type syncRunner struct {
	confirmation *IsolatedConfirmationServer // explicit live-confirmation mode only
	source       syncSource
	target       syncColdTarget
	config       syncRunnerConfig
	ops          *syncRunnerOps // isolated tests only; nil uses the real bounded protocol
	runMu        sync.Mutex
	mu           sync.Mutex
	status       syncRunnerStatus
	control      *syncRunControl // only the published active/completed generation; guarded by mu
}

func (r *syncRunner) snapshot() syncRunnerStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

func (r *syncRunner) update(fn func(*syncRunnerStatus)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(&r.status)
	r.status.UpdatedAt = time.Now()
}

func syncRunnerReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errSyncRetryExhausted):
		return "retry_exhausted"
	case errors.Is(err, errSyncPressureTimeout):
		return "pressure_timeout"
	case errors.Is(err, errSyncHistoryGap):
		return "history_gap"
	case errors.Is(err, errSyncPendingUncertain), errors.Is(err, errSyncApplyUncertain):
		return "cold_outcome_uncertain"
	case errors.Is(err, errSyncTargetDrift):
		return "cold_state_mismatch"
	case errors.Is(err, errSyncJournalCorrupt):
		return "journal_corrupt"
	case errors.Is(err, errSyncJournalFull):
		return "journal_capacity"
	case errors.Is(err, errSyncCapturePressure):
		return "resource_pressure"
	case errors.Is(err, errSyncSourceTransport):
		return "source_transport"
	case errors.Is(err, errSyncColdConnect):
		return "cold_connection"
	case errors.Is(err, errSyncUnsupported):
		return "unsupported_record"
	case errors.Is(err, errSyncShutdownTimeout):
		return "shutdown_timeout"
	case errors.Is(err, errSyncShutdownAborted):
		return "shutdown_aborted"
	case errors.Is(err, errSyncShutdownIncomplete):
		return "shutdown_incomplete"
	case errors.Is(err, errSyncCredentialFile):
		return "credential_file"
	case errors.Is(err, errSyncTaskIdentity):
		return "task_identity"
	case syncOnlyCancellation(err):
		return "canceled"
	default:
		return "unclassified_failure"
	}
}

func syncExpectedCaptureStop(err error) bool {
	return !syncTerminalOutcome(err) && (err == nil || syncOnlyCancellation(err) || errors.Is(err, errSyncSourceTransport) || errors.Is(err, errSyncCapturePressure))
}

func (r *syncRunner) validate(j *syncSegments) error {
	if j == nil {
		return errors.New("replication loop requires an open journal")
	}
	j.mu.Lock()
	identity := cloneSyncTaskIdentity(j.manifest.Identity)
	j.mu.Unlock()
	if identity != nil {
		if identity.TaskID != r.target.taskID || identity.Source != r.source.address || identity.Cold != r.target.address {
			return errSyncTaskIdentity
		}
	} else if r.target.taskID != "" {
		return errSyncTaskIdentity
	}
	if err := r.config.validate(); err != nil {
		return err
	}
	if err := r.source.validate(); err != nil {
		return err
	}
	if _, err := syncReadPassword(r.source.passwordFile); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(r.target.address)
	p, pe := strconv.Atoi(port)
	ip := net.ParseIP(host)
	if err != nil || pe != nil || ip == nil || !ip.IsLoopback() || p < 1 || p > 65535 || r.target.timeout <= 0 || r.target.timeout > 30*time.Second {
		return errors.New("replication loop requires bounded loopback cold target")
	}
	sourceHost, sourcePort, _ := net.SplitHostPort(r.source.address)
	sourcePortNumber, _ := strconv.Atoi(sourcePort)
	if net.ParseIP(sourceHost).Equal(ip) && sourcePortNumber == p {
		return errors.New("replication source and cold target must differ")
	}
	pos := j.durablePosition()
	if pos.History != "" && pos.Source != r.source.address {
		return errSyncHistoryGap
	}
	if r.ops != nil && (r.ops.capture == nil || r.ops.drain == nil || r.ops.cleanup == nil) {
		return errors.New("incomplete isolated replication loop operations")
	}
	return nil
}

// Existing history must be reconciled before a new source connection. An empty
// journal has no safe history to apply: validate the target and require it empty
// before allowing first capture; bootstrap rechecks emptiness under WATCH later.
func (target syncColdTarget) drainSegments(ctx context.Context, j *syncSegments) (syncPosition, error) {
	if err := target.checkJournalIdentity(j); err != nil {
		return syncPosition{}, err
	}
	position := j.durablePosition()
	if position.History == "" {
		c, closeClient, err := target.open(ctx)
		if err != nil {
			return syncPosition{}, err
		}
		defer closeClient()
		meta, err := c.pendingMeta()
		if err != nil {
			return syncPosition{}, err
		}
		if meta.checkpoint.Version != 0 {
			return syncPosition{}, errSyncHistoryGap
		}
		keys, err := c.keys()
		if err != nil {
			return syncPosition{}, err
		}
		if len(keys) != 0 {
			return syncPosition{}, errSyncTargetDrift
		}
		return syncPosition{}, nil
	}
	applied, err := target.applySegments(ctx, j)
	if err != nil {
		return applied, err
	}
	j.mu.Lock()
	base := j.manifest.Base.Position
	j.mu.Unlock()
	// A snapshot-only bootstrap has no pending record to authorize pruning.
	if applied == base {
		return applied, nil
	}
	return applied, target.pruneSegments(ctx, j)
}

func syncRetryDelay(c syncRunnerConfig, failures int) time.Duration {
	delay := c.RetryMin
	for i := 1; i < failures && delay < c.RetryMax; i++ {
		if delay >= c.RetryMax/2 {
			return c.RetryMax
		}
		delay *= 2
	}
	return delay
}

func (r *syncRunner) run(ctx context.Context, j *syncSegments) (result error) {
	var control *syncRunControl
	// Registered first, so completion is published only AFTER both ownership
	// locks have been released and all capture/ACK work has joined.
	defer func() {
		if control != nil {
			control.result = result
			close(control.done)
		}
	}()
	if !r.runMu.TryLock() {
		return errSyncRunnerBusy
	}
	defer r.runMu.Unlock()
	r.mu.Lock()
	r.control = nil // shutdown during validation must not report a prior run's success
	started := time.Now()
	r.status = syncRunnerStatus{State: "starting", StartedAt: started, UpdatedAt: started}
	r.mu.Unlock()
	failStart := func(err error) error {
		r.update(func(s *syncRunnerStatus) {
			s.State, s.LastError, s.FinishedAt = "failed", syncRunnerReason(err), time.Now()
		})
		return err
	}
	if err := r.validate(j); err != nil {
		return failStart(err)
	}
	if !j.runMu.TryLock() {
		return failStart(errSyncRunnerBusy)
	}
	defer j.runMu.Unlock()
	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	captureRoot, stopRoot := context.WithCancel(workCtx)
	defer stopRoot()
	control = &syncRunControl{stop: make(chan struct{}), done: make(chan struct{}), stopCapture: stopRoot, force: cancel}
	r.mu.Lock()
	now := time.Now()
	r.control, r.status = control, syncRunnerStatus{State: "starting", StartedAt: started, UpdatedAt: now}
	r.mu.Unlock()
	ops := syncRunnerOps{
		capture: func(ctx context.Context, j *syncSegments) error { return r.source.capture(ctx, j) },
		drain:   r.target.drainSegments,
		cleanup: func(ctx context.Context, j *syncSegments) error { _, err := j.cleanupOrphans(ctx); return err },
	}
	if r.ops != nil {
		ops = *r.ops
	}
	if r.confirmation != nil {
		ops.drain = r.confirmation.drainLive
		// Normal pruning is done only by drainLive before it sees a barrier.
		// Retain source evidence and all suffix records while held.
		ops.cleanup = func(context.Context, *syncSegments) error { return nil }
	}
	var captureDone chan error
	var captureCancel context.CancelFunc
	var attemptStart syncPosition
	stopCapture := func() error {
		if captureDone == nil {
			return nil
		}
		captureCancel()
		err := <-captureDone // capture itself joins its ACK goroutine before return
		captureDone, captureCancel = nil, nil
		return err
	}
	defer func() {
		cancel(nil)
		err := stopCapture()
		if err != nil && !syncExpectedCaptureStop(err) && (result == nil || syncOnlyCancellation(result)) {
			result = err
		}
		if cause := context.Cause(workCtx); syncOnlyCancellation(result) && (errors.Is(cause, errSyncShutdownTimeout) || errors.Is(cause, errSyncShutdownAborted)) {
			result = cause
		}
		captured := j.durablePosition()
		r.update(func(s *syncRunnerStatus) {
			s.Captured, s.LastError = captured, syncRunnerReason(result)
			s.FinishedAt = time.Now()
			s.State = "failed"
			if result == nil {
				s.State = "drained"
				if r.confirmation != nil {
					s.State = "stopped"
				}
			} else if syncOnlyCancellation(result) {
				s.State = "stopped"
			}
		})
	}()
	ticker := time.NewTicker(r.config.TickInterval)
	defer ticker.Stop()
	var applied syncPosition
	var sourceRetryAt, coldRetryAt, cleanupAt, pressureSince time.Time
	var pressurePosition syncPosition
	var sourceFailures, coldFailures, pressureFailures int
	var ready, capturePressurePending bool
	finishGraceful := func() error {
		stopRoot()
		if err := stopCapture(); !syncExpectedCaptureStop(err) {
			return err
		}
		// Includes a final complete frame that may have fsynced while the
		// connection was being closed. Never freeze a stale status snapshot.
		target := j.durablePosition()
		r.update(func(s *syncRunnerStatus) { s.State, s.DrainTarget, s.Captured = "draining", target, target })
		if r.confirmation != nil {
			return r.confirmation.stopLive(workCtx, j, target)
		}
		return r.drainStopped(workCtx, j, ops, target)
	}
	markPressure := func(now time.Time, prospective bool) {
		if pressureSince.IsZero() {
			pressureSince, pressurePosition = now, j.durablePosition()
		}
		capturePressurePending = capturePressurePending || prospective
	}
	// A cancellation requested by this loop is expected; safety failures that
	// raced the cancellation still stop the run instead of being hidden.
	stopForPause := func(now time.Time) error {
		err := stopCapture()
		if !syncExpectedCaptureStop(err) {
			return err
		}
		if errors.Is(err, errSyncCapturePressure) {
			markPressure(now, true)
		}
		return nil
	}
	step := func(now time.Time) error {
		if err := workCtx.Err(); err != nil {
			return err
		}
		if control.requested() {
			return errSyncGraceRequested
		}
		if !pressureSince.IsZero() && now.Sub(pressureSince) >= r.config.MaxPressure {
			return errSyncPressureTimeout
		}
		if now.Before(coldRetryAt) {
			return nil
		}
		captured := j.durablePosition()
		if !ready || captured != applied {
			r.update(func(s *syncRunnerStatus) { s.State = "applying" })
			position, err := ops.drain(workCtx, j)
			err = syncDrainCancellation(workCtx, err)
			if position.History != "" {
				applied = position
				r.update(func(s *syncRunnerStatus) { s.Applied = applied })
			}
			if err != nil {
				if !errors.Is(err, errSyncColdConnect) || syncTerminalOutcome(err) {
					return err
				}
				if stopErr := stopForPause(now); stopErr != nil {
					return stopErr
				}
				ready = false
				coldFailures++
				if coldFailures > r.config.MaxRetries {
					return fmt.Errorf("%w: %w", errSyncRetryExhausted, err)
				}
				coldRetryAt = time.Now().Add(syncRetryDelay(r.config, coldFailures))
				r.update(func(s *syncRunnerStatus) { s.State, s.LastError = "retry_cold", "cold_connection"; s.ColdRetries++ })
				return nil
			}
			applied, ready, coldFailures = position, true, 0
			coldRetryAt = time.Time{}
		}
		if control.requested() {
			return errSyncGraceRequested
		}
		if !now.Before(cleanupAt) {
			if err := ops.cleanup(workCtx, j); err != nil {
				return err
			}
			cleanupAt = time.Now().Add(r.config.CleanupInterval)
		}
		resources, err := j.resourceStatus()
		j.mu.Lock()
		confirmed := j.manifest.Confirmed.Position
		j.mu.Unlock()
		captured = j.durablePosition()
		r.update(func(s *syncRunnerStatus) {
			s.Captured, s.Applied, s.Confirmed, s.Resources = captured, applied, confirmed, resources
			s.ResourcesObserved, s.ResourceUpdatedAt = true, time.Now()
			if err == nil || errors.Is(err, errSyncCapturePressure) {
				recordSyncProgress(s, captured, applied, confirmed, resources, s.ResourceUpdatedAt)
			}
		})
		if err != nil {
			if !errors.Is(err, errSyncCapturePressure) {
				return err
			}
			if stopErr := stopForPause(now); stopErr != nil {
				return stopErr
			}
			markPressure(now, false)
			r.update(func(s *syncRunnerStatus) { s.State, s.LastError = "pressure", "resource_pressure" })
			return nil
		}
		if !capturePressurePending || captured != pressurePosition {
			pressureSince, capturePressurePending, pressureFailures = time.Time{}, false, 0
		}
		if captureDone == nil && !now.Before(sourceRetryAt) {
			captureCtx, childCancel := context.WithCancel(captureRoot)
			captureCancel, captureDone, attemptStart = childCancel, make(chan error, 1), captured
			done := captureDone
			go func() { done <- ops.capture(captureCtx, j) }()
			r.update(func(s *syncRunnerStatus) { s.SourceAttempts++ })
		}
		state := "running"
		if capturePressurePending {
			state = "pressure"
		} else if captureDone == nil {
			state = "retry_source"
		}
		r.update(func(s *syncRunnerStatus) {
			s.State = state
			if state == "running" {
				s.LastError = ""
			}
		})
		return nil
	}
	if err := step(time.Now()); err != nil {
		if errors.Is(err, errSyncGraceRequested) {
			return finishGraceful()
		}
		return err
	}
	for {
		select {
		case <-workCtx.Done():
			return workCtx.Err()
		case <-control.stop:
			return finishGraceful()
		case <-ticker.C:
			if err := step(time.Now()); err != nil {
				if errors.Is(err, errSyncGraceRequested) {
					return finishGraceful()
				}
				return err
			}
		case err := <-captureDone:
			captureCancel()
			captureDone, captureCancel = nil, nil
			if syncTerminalOutcome(err) {
				return err
			}
			if control.requested() && syncExpectedCaptureStop(err) {
				return finishGraceful()
			}
			if workCtx.Err() != nil && syncExpectedCaptureStop(err) {
				return workCtx.Err()
			}
			now := time.Now()
			if j.durablePosition() != attemptStart {
				sourceFailures = 0
			}
			switch {
			case errors.Is(err, errSyncCapturePressure):
				markPressure(now, true)
				pressureFailures++
				sourceRetryAt = now.Add(syncRetryDelay(r.config, pressureFailures))
			case errors.Is(err, errSyncSourceTransport):
				sourceFailures++
				if sourceFailures > r.config.MaxRetries {
					return fmt.Errorf("%w: %w", errSyncRetryExhausted, err)
				}
				sourceRetryAt = now.Add(syncRetryDelay(r.config, sourceFailures))
				r.update(func(s *syncRunnerStatus) { s.SourceRetries++; s.LastError = "source_transport" })
			case err == nil:
				return errors.New("replication capture exited without a stop reason")
			default:
				return err
			}
			if err := step(now); err != nil {
				if errors.Is(err, errSyncGraceRequested) {
					return finishGraceful()
				}
				return err
			}
		}
	}
}
