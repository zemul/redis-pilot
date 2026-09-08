package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func syncShutdownRequestForTest(t *testing.T, r *syncRunner, timeout time.Duration) <-chan error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- r.shutdown(ctx) }()
	return done
}

func syncShutdownSignalForTest(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown test did not reach controlled boundary")
	}
}

// Fake drain operations must model the real applier's confirmation watermark,
// not merely return a position and pretend the cold commit was confirmed.
func syncShutdownConfirmForTest(s *syncSegments) syncPosition {
	s.mu.Lock()
	defer s.mu.Unlock()
	end := s.endLocked()
	if end != s.manifest.Base {
		s.manifest.Confirmed = end
	}
	return end.Position
}

func syncShutdownAppendForTest(s *syncSegments, key string) error {
	raw := encodeCommand("SET", key, "value")
	position := s.durablePosition()
	position.Offset += int64(len(raw))
	return s.append(syncRecord{Kind: "command", Position: position, Data: raw})
}

func TestSyncRunnerShutdownStopsCaptureDuringDrainThenFreezesJoinedTail(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	coldEntered := make(chan struct{})
	coldContexts := make(chan context.Context, 1)
	releaseCold := make(chan struct{})
	captureCanceled := make(chan struct{})
	releaseCapture := make(chan struct{})
	var releaseColdOnce, releaseCaptureOnce sync.Once
	unblockCold := func() { releaseColdOnce.Do(func() { close(releaseCold) }) }
	unblockCapture := func() { releaseCaptureOnce.Do(func() { close(releaseCapture) }) }
	var captureFinished atomic.Bool
	var drainCalls, cleanupCalls atomic.Int32
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		defer captureFinished.Store(true)
		if err := syncShutdownAppendForTest(s, "before-grace"); err != nil {
			return err
		}
		<-ctx.Done()
		// A final complete frame may become durable while the capture goroutine
		// is stopping. DrainTarget must be fixed only after this goroutine joins.
		if err := syncShutdownAppendForTest(s, "last-frame-before-join"); err != nil {
			return err
		}
		close(captureCanceled)
		<-releaseCapture
		return ctx.Err()
	}
	r.ops.drain = func(ctx context.Context, s *syncSegments) (syncPosition, error) {
		call := drainCalls.Add(1)
		if call == 2 {
			s.mu.Lock()
			fixed := s.endLocked()
			s.mu.Unlock()
			coldContexts <- ctx
			close(coldEntered)
			select {
			case <-releaseCold:
			case <-ctx.Done():
				return syncPosition{}, ctx.Err()
			}
			s.mu.Lock()
			s.manifest.Confirmed = fixed
			s.mu.Unlock()
			return fixed.Position, nil
		}
		return syncShutdownConfirmForTest(s), nil
	}
	r.ops.cleanup = func(context.Context, *syncSegments) error { cleanupCalls.Add(1); return nil }
	_, runDone := syncRunnerStartForTest(t, r, f.store)
	t.Cleanup(func() { unblockCold(); unblockCapture() })
	syncShutdownSignalForTest(t, coldEntered)
	coldCtx := <-coldContexts
	shutdownDone := syncShutdownRequestForTest(t, r, time.Second)
	syncShutdownSignalForTest(t, captureCanceled)
	if coldCtx.Err() != nil {
		t.Fatal("graceful shutdown canceled in-flight cold work")
	}
	unblockCold()
	select {
	case err := <-shutdownDone:
		t.Fatal("shutdown returned before capture joined", err)
	default:
	}
	unblockCapture()
	if err := syncRunnerResultForTest(t, shutdownDone); err != nil {
		t.Fatal("graceful shutdown did not drain the fixed tail", err)
	}
	if err := syncRunnerResultForTest(t, runDone); err != nil || !captureFinished.Load() {
		t.Fatal("run did not finish successfully after capture join", err)
	}
	status := r.snapshot()
	target := f.store.durablePosition()
	if status.State != "drained" || status.DrainTarget != target || status.Applied != target || status.Confirmed != target || status.Captured != target || drainCalls.Load() < 3 || cleanupCalls.Load() < 2 {
		t.Fatalf("final frame was not confirmed and cleaned: %+v drains=%d cleanups=%d", status, drainCalls.Load(), cleanupCalls.Load())
	}
}

func TestSyncRunnerShutdownInvalidAndInactiveRequestsHaveNoSideEffects(t *testing.T) {
	r := syncRunnerForTest()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.shutdown(ctx); !errors.Is(err, errSyncRunnerNotRunning) {
		t.Fatal("inactive runner accepted a shutdown request", err)
	}
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	var captureStarted, captureFinished atomic.Bool
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		captureStarted.Store(true)
		defer captureFinished.Store(true)
		<-ctx.Done()
		return ctx.Err()
	}
	stopRun, runDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, captureStarted.Load)
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	tooLong, cancelLong := context.WithTimeout(context.Background(), 31*time.Second)
	defer cancelLong()
	alreadyCanceled, cancelCanceled := context.WithTimeout(context.Background(), time.Second)
	cancelCanceled()
	for _, invalid := range []context.Context{context.Background(), expired, tooLong, alreadyCanceled} {
		if err := r.shutdown(invalid); err == nil {
			t.Fatal("invalid shutdown deadline was accepted")
		}
		if captureFinished.Load() {
			t.Fatal("invalid shutdown request affected the active run")
		}
	}
	stopRun()
	if err := syncRunnerResultForTest(t, runDone); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.shutdown(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatal("completed shutdown lookup did not return the prior run result", err)
	}
}

func TestSyncRunnerShutdownConcurrentRequestsShareOneDrain(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var started, ended, calls atomic.Int32
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		started.Add(1)
		defer ended.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
		calls.Add(1)
		return syncShutdownConfirmForTest(s), nil
	}
	_, runDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return started.Load() == 1 })
	deadline, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	const requestCount = 8
	results := make(chan error, requestCount)
	start := make(chan struct{})
	for i := 0; i < requestCount; i++ {
		go func() { <-start; results <- r.shutdown(deadline) }()
	}
	close(start)
	for i := 0; i < requestCount; i++ {
		if err := syncRunnerResultForTest(t, results); err != nil {
			t.Fatal("concurrent shutdown did not share the same success", err)
		}
	}
	if err := syncRunnerResultForTest(t, runDone); err != nil || started.Load() != 1 || ended.Load() != 1 || calls.Load() > 2 {
		t.Fatal("duplicate requests restarted capture or repeated final drain", err, started.Load(), ended.Load(), calls.Load())
	}
	if err := r.shutdown(context.Background()); err != nil {
		t.Fatal("completed successful run could not be queried without a new deadline", err)
	}
}

func TestSyncRunnerShutdownPublishesRequestBeforeCancelingSource(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var started, canceledBeforePublished atomic.Bool
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		started.Store(true)
		<-ctx.Done()
		return ctx.Err()
	}
	_, runDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, started.Load)
	// No shutdown caller exists yet. Intercept the control's source-cancel
	// boundary deterministically: a captureDone result must never observe
	// cancellation before the graceful request becomes visible to run().
	r.mu.Lock()
	control := r.control
	original := control.stopCapture
	control.stopCapture = func() {
		if !control.requested() {
			canceledBeforePublished.Store(true)
		}
		original()
	}
	r.mu.Unlock()
	if err := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := syncRunnerResultForTest(t, runDone); err != nil || canceledBeforePublished.Load() {
		t.Fatal("source cancellation was published before graceful control", err)
	}
}

func TestSyncRunnerShutdownRetriesColdConnectForFixedTail(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var started, sourceFinished atomic.Bool
	var retries atomic.Int32
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		started.Store(true)
		<-ctx.Done()
		if err := syncShutdownAppendForTest(s, "shutdown-tail"); err != nil {
			return err
		}
		sourceFinished.Store(true)
		return ctx.Err()
	}
	r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
		if sourceFinished.Load() && retries.Add(1) <= 2 {
			return syncPosition{}, errSyncColdConnect
		}
		return syncShutdownConfirmForTest(s), nil
	}
	_, runDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, started.Load)
	if err := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, time.Second)); err != nil {
		t.Fatal("cold connection retry could not drain fixed shutdown tail", err)
	}
	if err := syncRunnerResultForTest(t, runDone); err != nil {
		t.Fatal(err)
	}
	status := r.snapshot()
	if retries.Load() != 3 || status.ColdRetries != 2 || status.DrainTarget != status.Confirmed || status.Applied != status.DrainTarget || status.State != "drained" {
		t.Fatalf("fixed-tail cold retry result is incomplete: %+v attempts=%d", status, retries.Load())
	}
}

func TestSyncRunnerShutdownColdRetryExhaustionAndCleanupFailure(t *testing.T) {
	for _, phase := range []string{"cold_retry_exhaustion", "cleanup_failure"} {
		t.Run(phase, func(t *testing.T) {
			f := syncResourceFixture(t, defaultSyncResourcePolicy())
			r := syncRunnerForTest()
			var started, sourceFinished atomic.Bool
			var retryCalls atomic.Int32
			cleanupFailure := errors.New("final orphan cleanup failed")
			r.ops.capture = func(ctx context.Context, s *syncSegments) error {
				started.Store(true)
				<-ctx.Done()
				if err := syncShutdownAppendForTest(s, "failed-grace-tail"); err != nil {
					return err
				}
				sourceFinished.Store(true)
				return ctx.Err()
			}
			r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
				if sourceFinished.Load() && phase == "cold_retry_exhaustion" {
					retryCalls.Add(1)
					return syncPosition{}, errSyncColdConnect
				}
				return syncShutdownConfirmForTest(s), nil
			}
			r.ops.cleanup = func(context.Context, *syncSegments) error {
				if sourceFinished.Load() && phase == "cleanup_failure" {
					return cleanupFailure
				}
				return nil
			}
			_, runDone := syncRunnerStartForTest(t, r, f.store)
			syncRunnerWaitForTest(t, started.Load)
			want := cleanupFailure
			if phase == "cold_retry_exhaustion" {
				want = errSyncRetryExhausted
			}
			if err := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, time.Second)); !errors.Is(err, want) {
				t.Fatal("shutdown lost its terminal fixed-tail error", err)
			}
			if err := syncRunnerResultForTest(t, runDone); !errors.Is(err, want) || !sourceFinished.Load() || r.snapshot().State == "drained" {
				t.Fatal("run claimed a successful drain despite final failure", err)
			}
			if phase == "cold_retry_exhaustion" && retryCalls.Load() != int32(r.config.MaxRetries+1) {
				t.Fatal("fixed-tail retry budget was not bounded", retryCalls.Load())
			}
		})
	}
}

func TestSyncRunnerShutdownTimeoutJoinsAndPreservesFatalCause(t *testing.T) {
	for name, coldFailure := range map[string]error{
		"deadline":  nil,
		"uncertain": errSyncPendingUncertain,
		"corrupt":   errSyncJournalCorrupt,
	} {
		t.Run(name, func(t *testing.T) {
			f := syncResourceFixture(t, defaultSyncResourcePolicy())
			r := syncRunnerForTest()
			entered := make(chan struct{})
			var captureFinished atomic.Bool
			var calls atomic.Int32
			r.ops.capture = func(ctx context.Context, s *syncSegments) error {
				defer captureFinished.Store(true)
				if err := syncShutdownAppendForTest(s, "timeout-tail"); err != nil {
					return err
				}
				<-ctx.Done()
				return ctx.Err()
			}
			r.ops.drain = func(ctx context.Context, s *syncSegments) (syncPosition, error) {
				if calls.Add(1) == 2 {
					close(entered)
					<-ctx.Done()
					if coldFailure != nil {
						return syncPosition{}, coldFailure
					}
					return syncPosition{}, ctx.Err()
				}
				return syncShutdownConfirmForTest(s), nil
			}
			_, runDone := syncRunnerStartForTest(t, r, f.store)
			syncShutdownSignalForTest(t, entered)
			shutdownErr := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, 30*time.Millisecond))
			// Check immediately after shutdown returns, before synchronizing on
			// runDone; otherwise run's later return could hide early done publication.
			if !captureFinished.Load() {
				t.Fatal("shutdown returned before capture joined")
			}
			if !r.runMu.TryLock() {
				t.Fatal("shutdown returned before runner ownership was released")
			}
			r.runMu.Unlock()
			if !f.store.runMu.TryLock() {
				t.Fatal("shutdown returned before journal ownership was released")
			}
			f.store.runMu.Unlock()
			runErr := syncRunnerResultForTest(t, runDone)
			want := coldFailure
			if want == nil {
				want = errSyncShutdownTimeout
			}
			if !errors.Is(shutdownErr, want) || !errors.Is(runErr, want) || !captureFinished.Load() {
				t.Fatal("shutdown timeout masked a fatal cause or did not join", shutdownErr, runErr)
			}
		})
	}
}

func TestSyncRunnerShutdownRequiresConfirmedIncrementalTarget(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var started atomic.Bool
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		started.Store(true)
		<-ctx.Done()
		if err := syncShutdownAppendForTest(s, "unconfirmed-shutdown-tail"); err != nil {
			return err
		}
		return ctx.Err()
	}
	// Deliberately return Applied without setting Confirmed. Ordinary capture
	// may keep going, but a successful shutdown must not make a durability claim.
	_, runDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, started.Load)
	if err := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, time.Second)); !errors.Is(err, errSyncShutdownIncomplete) {
		t.Fatal("unconfirmed incremental tail was reported drained", err)
	}
	if err := syncRunnerResultForTest(t, runDone); !errors.Is(err, errSyncShutdownIncomplete) {
		t.Fatal("run did not retain shutdown confirmation failure", err)
	}
}

func TestSyncRunnerShutdownRejectsDifferentAppliedOrConfirmedPosition(t *testing.T) {
	for _, mismatch := range []string{"confirmed_behind", "confirmed_other_source", "applied_behind"} {
		t.Run(mismatch, func(t *testing.T) {
			f := syncResourceFixture(t, defaultSyncResourcePolicy())
			r := syncRunnerForTest()
			var started, finished atomic.Bool
			r.ops.capture = func(ctx context.Context, s *syncSegments) error {
				started.Store(true)
				<-ctx.Done()
				if err := syncShutdownAppendForTest(s, "mismatched-shutdown-tail"); err != nil {
					return err
				}
				finished.Store(true)
				return ctx.Err()
			}
			r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
				if !finished.Load() {
					return s.durablePosition(), nil
				}
				s.mu.Lock()
				defer s.mu.Unlock()
				end := s.endLocked()
				s.manifest.Confirmed = end
				switch mismatch {
				case "confirmed_behind":
					s.manifest.Confirmed = s.manifest.Base
				case "confirmed_other_source":
					s.manifest.Confirmed.Position.Source = "127.0.0.1:12347"
				case "applied_behind":
					return s.manifest.Base.Position, nil
				}
				return end.Position, nil
			}
			_, runDone := syncRunnerStartForTest(t, r, f.store)
			syncRunnerWaitForTest(t, started.Load)
			if err := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, time.Second)); !errors.Is(err, errSyncShutdownIncomplete) {
				t.Fatal("a different position was accepted as the shutdown target", err)
			}
			if err := syncRunnerResultForTest(t, runDone); !errors.Is(err, errSyncShutdownIncomplete) {
				t.Fatal(err)
			}
		})
	}
}

func TestSyncRunnerShutdownEmptyAndSnapshotOnlyExceptions(t *testing.T) {
	for _, empty := range []bool{true, false} {
		name := "snapshot"
		if empty {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			var s *syncSegments
			if empty {
				var err error
				s, err = openSyncSegments(filepath.Join(t.TempDir(), "segments"), 1024, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				s.availableBytes = func() (uint64, error) { return 1 << 40, nil }
				t.Cleanup(func() { s.close() })
			} else {
				s = syncResourceFixture(t, defaultSyncResourcePolicy()).store
			}
			r := syncRunnerForTest()
			var started atomic.Bool
			r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
				started.Store(true)
				<-ctx.Done()
				return ctx.Err()
			}
			_, runDone := syncRunnerStartForTest(t, r, s)
			syncRunnerWaitForTest(t, started.Load)
			if err := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, time.Second)); err != nil {
				t.Fatal("empty or snapshot-only journal required a nonexistent pending confirmation", err)
			}
			if err := syncRunnerResultForTest(t, runDone); err != nil {
				t.Fatal(err)
			}
			status := r.snapshot()
			if status.State != "drained" || status.DrainTarget != s.durablePosition() || status.Applied != status.DrainTarget || status.Confirmed != (syncPosition{}) {
				t.Fatalf("invalid snapshot-only completion status: %+v", status)
			}
		})
	}
}

func TestSyncRunnerShutdownParentCancellationRemainsImmediate(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	entered := make(chan struct{})
	var calls atomic.Int32
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		if err := syncShutdownAppendForTest(s, "parent-cancel-tail"); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	r.ops.drain = func(ctx context.Context, s *syncSegments) (syncPosition, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-ctx.Done()
			return syncPosition{}, ctx.Err()
		}
		return syncShutdownConfirmForTest(s), nil
	}
	cancelRun, runDone := syncRunnerStartForTest(t, r, f.store)
	syncShutdownSignalForTest(t, entered)
	shutdownDone := syncShutdownRequestForTest(t, r, time.Second)
	cancelRun()
	if err := syncRunnerResultForTest(t, shutdownDone); !errors.Is(err, context.Canceled) {
		t.Fatal("graceful request overrode immediate parent cancellation", err)
	}
	if err := syncRunnerResultForTest(t, runDone); !errors.Is(err, context.Canceled) {
		t.Fatal("parent cancellation was transformed into successful drain", err)
	}
}

func TestSyncRunnerShutdownOldDeadlineCannotStopReusedRun(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var starts atomic.Int32
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		starts.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	_, firstDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return starts.Load() == 1 })
	oldDeadline, cancelOld := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelOld()
	if err := r.shutdown(oldDeadline); err != nil {
		t.Fatal(err)
	}
	if err := syncRunnerResultForTest(t, firstDone); err != nil {
		t.Fatal(err)
	}
	stopSecond, secondDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return starts.Load() == 2 })
	<-oldDeadline.Done()
	// Once a previously registered deadline has fired, synchronize with a
	// later tick so stale force callbacks have had a chance to run.
	tick := time.NewTimer(2 * r.config.TickInterval)
	<-tick.C
	select {
	case err := <-secondDone:
		t.Fatal("an old shutdown generation stopped a new run", err)
	default:
	}
	if r.snapshot().State == "drained" {
		t.Fatal("new run inherited the previous drain request")
	}
	stopSecond()
	if err := syncRunnerResultForTest(t, secondDone); !errors.Is(err, context.Canceled) {
		t.Fatal("reused run could not cancel independently", err)
	}
}

func TestSyncRunnerShutdownJoinedTerminalAndCancellationRemainFatal(t *testing.T) {
	for _, phase := range []string{"inflight_drain", "final_drain", "capture"} {
		for name, terminal := range map[string]error{
			"uncertain": errSyncPendingUncertain,
			"full":      errSyncJournalFull,
			"corrupt":   errSyncJournalCorrupt,
		} {
			t.Run(phase+"_"+name, func(t *testing.T) {
				f := syncResourceFixture(t, defaultSyncResourcePolicy())
				r := syncRunnerForTest()
				entered := make(chan struct{})
				captureRelease := make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(captureRelease) }) }
				var started, captureFinished atomic.Bool
				var calls atomic.Int32
				r.ops.capture = func(ctx context.Context, s *syncSegments) error {
					started.Store(true)
					defer captureFinished.Store(true)
					if phase != "final_drain" {
						if err := syncShutdownAppendForTest(s, "joined-terminal-tail"); err != nil {
							return err
						}
					}
					<-ctx.Done()
					if phase == "final_drain" {
						if err := syncShutdownAppendForTest(s, "joined-final-drain-tail"); err != nil {
							return err
						}
					}
					if phase == "capture" {
						// Wait until cold work actually observes the forced deadline,
						// then race the terminal capture result with final cancellation.
						<-captureRelease
						return errors.Join(terminal, ctx.Err())
					}
					return ctx.Err()
				}
				r.ops.drain = func(ctx context.Context, s *syncSegments) (syncPosition, error) {
					if calls.Add(1) == 2 {
						close(entered)
						<-ctx.Done()
						release()
						if phase == "capture" {
							return syncPosition{}, ctx.Err()
						}
						return syncPosition{}, errors.Join(terminal, ctx.Err())
					}
					return syncShutdownConfirmForTest(s), nil
				}
				_, runDone := syncRunnerStartForTest(t, r, f.store)
				t.Cleanup(release)
				syncRunnerWaitForTest(t, started.Load)
				if phase != "final_drain" {
					syncShutdownSignalForTest(t, entered)
				}
				shutdownDone := syncShutdownRequestForTest(t, r, time.Second)
				if phase == "final_drain" {
					syncShutdownSignalForTest(t, entered)
				}
				syncRunnerWaitForTest(t, func() bool { return r.snapshot().StopRequested })
				// Exercise the timeout force boundary only after the intended cold
				// operation is in flight; a very short wall-clock deadline could
				// otherwise expire during the capture's final local fsync instead.
				r.mu.Lock()
				force := r.control.force
				r.mu.Unlock()
				force(errSyncShutdownTimeout)
				shutdownErr := syncRunnerResultForTest(t, shutdownDone)
				runErr := syncRunnerResultForTest(t, runDone)
				for _, err := range []error{shutdownErr, runErr} {
					if !errors.Is(err, terminal) || !errors.Is(err, context.Canceled) || errors.Is(err, errSyncShutdownTimeout) {
						t.Fatal("forced shutdown replaced a joined terminal outcome", err)
					}
				}
				if !captureFinished.Load() || r.snapshot().State != "failed" || r.snapshot().LastError == "shutdown_timeout" || r.snapshot().LastError == "canceled" {
					t.Fatalf("joined terminal outcome was reported as normal cancellation: %+v", r.snapshot())
				}
			})
		}
	}
}

func TestSyncRunnerShutdownExplicitCancelIsAbortedNotDeadline(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	entered := make(chan struct{})
	var calls atomic.Int32
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		if err := syncShutdownAppendForTest(s, "explicit-abort-tail"); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	r.ops.drain = func(ctx context.Context, s *syncSegments) (syncPosition, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-ctx.Done()
			return syncPosition{}, ctx.Err()
		}
		return syncShutdownConfirmForTest(s), nil
	}
	_, runDone := syncRunnerStartForTest(t, r, f.store)
	syncShutdownSignalForTest(t, entered)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- r.shutdown(shutdownCtx) }()
	syncRunnerWaitForTest(t, func() bool { return r.snapshot().StopRequested })
	cancelShutdown() // Accepted request, canceled manually before its deadline.
	shutdownErr := syncRunnerResultForTest(t, shutdownDone)
	runErr := syncRunnerResultForTest(t, runDone)
	for _, err := range []error{shutdownErr, runErr} {
		if !errors.Is(err, errSyncShutdownAborted) || errors.Is(err, errSyncShutdownTimeout) {
			t.Fatal("explicit shutdown cancellation was confused with a deadline", err)
		}
	}
	if status := r.snapshot(); status.State != "failed" || status.LastError != "shutdown_aborted" {
		t.Fatalf("explicit abort has the wrong status: %+v", status)
	}
}

func TestSyncRunnerShutdownInvalidRestartClearsPriorDrainedStatus(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var started atomic.Bool
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		started.Store(true)
		<-ctx.Done()
		return ctx.Err()
	}
	_, firstDone := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, started.Load)
	if err := syncRunnerResultForTest(t, syncShutdownRequestForTest(t, r, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := syncRunnerResultForTest(t, firstDone); err != nil || r.snapshot().State != "drained" {
		t.Fatal("initial run did not drain successfully", err)
	}
	r.config.TickInterval = -time.Second
	if err := r.run(context.Background(), f.store); err == nil {
		t.Fatal("invalid config restart was accepted")
	}
	status := r.snapshot()
	if status.State != "failed" || status.DrainTarget != (syncPosition{}) || status.Applied != (syncPosition{}) || status.Confirmed != (syncPosition{}) || status.Captured != (syncPosition{}) || status.StopRequested || status.LastError == "" {
		t.Fatalf("failed restart retained stale successful drain metrics: %+v", status)
	}
	if err := r.shutdown(context.Background()); !errors.Is(err, errSyncRunnerNotRunning) {
		t.Fatal("failed restart exposed an earlier generation's successful shutdown", err)
	}
}
