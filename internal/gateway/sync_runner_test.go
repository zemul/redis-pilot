package gateway

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func syncRunnerForTest() *syncRunner {
	config := defaultSyncRunnerConfig()
	config.TickInterval = 5 * time.Millisecond
	config.RetryMin = 10 * time.Millisecond
	config.RetryMax = 20 * time.Millisecond
	config.CleanupInterval = 10 * time.Millisecond
	config.MaxPressure = 100 * time.Millisecond
	config.MaxRetries = 2
	return &syncRunner{
		source: syncSource{address: "127.0.0.1:12345", timeout: time.Second, maxSnapshot: 1 << 20},
		target: syncColdTarget{address: "127.0.0.1:12346", timeout: time.Second},
		config: config,
		ops: &syncRunnerOps{
			capture: func(ctx context.Context, _ *syncSegments) error { <-ctx.Done(); return ctx.Err() },
			drain:   func(_ context.Context, s *syncSegments) (syncPosition, error) { return s.durablePosition(), nil },
			cleanup: func(context.Context, *syncSegments) error { return nil },
		},
	}
}

func syncRunnerStartForTest(t *testing.T, r *syncRunner, s *syncSegments) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		done <- r.run(ctx, s)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("runner did not cancel and join before journal cleanup")
		}
	})
	return cancel, done
}

func syncRunnerResultForTest(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not return")
		return nil
	}
}

func syncRunnerWaitForTest(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !predicate() {
		select {
		case <-deadline.C:
			t.Fatal("runner condition did not become true")
		case <-tick.C:
		}
	}
}

func TestSyncRunnerInvalidConfigDoesNotTouchJournal(t *testing.T) {
	for name, mutate := range map[string]func(*syncRunner){
		"negative_tick":     func(r *syncRunner) { r.config.TickInterval = -time.Second },
		"negative_retry":    func(r *syncRunner) { r.config.RetryMin = -time.Second },
		"reversed_retry":    func(r *syncRunner) { r.config.RetryMax = r.config.RetryMin / 2 },
		"negative_cleanup":  func(r *syncRunner) { r.config.CleanupInterval = -time.Second },
		"negative_pressure": func(r *syncRunner) { r.config.MaxPressure = -time.Second },
		"negative_retries":  func(r *syncRunner) { r.config.MaxRetries = -1 },
		"source_not_local":  func(r *syncRunner) { r.source.address = "192.0.2.10:12345" },
		"target_not_local":  func(r *syncRunner) { r.target.address = "192.0.2.11:12346" },
		"same_target":       func(r *syncRunner) { r.target.address = r.source.address },
		"source_no_timeout": func(r *syncRunner) { r.source.timeout = 0 },
		"target_no_timeout": func(r *syncRunner) { r.target.timeout = 0 },
		"bad_snapshot_cap":  func(r *syncRunner) { r.source.maxSnapshot = 0 },
		"partial_ops":       func(r *syncRunner) { r.ops.cleanup = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f := syncResourceFixture(t, defaultSyncResourcePolicy())
			r := syncRunnerForTest()
			var calls atomic.Int32
			r.ops.capture = func(context.Context, *syncSegments) error { calls.Add(1); return nil }
			r.ops.drain = func(context.Context, *syncSegments) (syncPosition, error) { calls.Add(1); return syncPosition{}, nil }
			r.ops.cleanup = func(context.Context, *syncSegments) error { calls.Add(1); return nil }
			mutate(r)
			before := syncResourceFileSizes(t, f.dir)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := r.run(ctx, f.store); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("invalid runner config was not rejected before starting", err)
			}
			if calls.Load() != 0 || f.store.poisoned || !reflect.DeepEqual(before, syncResourceFileSizes(t, f.dir)) {
				t.Fatal("config validation invoked operations or changed the journal")
			}
		})
	}
}

func TestSyncRunnerStartupDrainAndCleanupBeforeCapture(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var drained, cleaned, captured atomic.Int32
	var badOrder atomic.Bool
	r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
		drained.Add(1)
		return s.durablePosition(), nil
	}
	r.ops.cleanup = func(context.Context, *syncSegments) error {
		if drained.Load() == 0 {
			badOrder.Store(true)
		}
		cleaned.Add(1)
		return nil
	}
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		if drained.Load() == 0 || cleaned.Load() == 0 {
			badOrder.Store(true)
		}
		captured.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	cancel, done := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return captured.Load() == 1 })
	cancel()
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, context.Canceled) || badOrder.Load() {
		t.Fatal("startup did not repair and clean before capture", err)
	}
	status := r.snapshot()
	if status.Captured != f.store.durablePosition() || status.Applied != f.store.durablePosition() || status.SourceAttempts != 1 {
		t.Fatalf("runner status did not record durable progress: %+v", status)
	}
}

func TestSyncRunnerStartupFailureNeverStartsCapture(t *testing.T) {
	for _, phase := range []string{"drain", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			f := syncResourceFixture(t, defaultSyncResourcePolicy())
			r := syncRunnerForTest()
			fatal := errors.New("startup failure")
			var captured atomic.Int32
			r.ops.capture = func(context.Context, *syncSegments) error { captured.Add(1); return nil }
			if phase == "drain" {
				r.ops.drain = func(context.Context, *syncSegments) (syncPosition, error) { return syncPosition{}, fatal }
			} else {
				r.ops.cleanup = func(context.Context, *syncSegments) error { return fatal }
			}
			_, done := syncRunnerStartForTest(t, r, f.store)
			if err := syncRunnerResultForTest(t, done); !errors.Is(err, fatal) || captured.Load() != 0 {
				t.Fatal("startup failure was masked or capture ran first", err)
			}
		})
	}
}

func TestSyncRunnerRejectsSameJournalAndSameRunnerReentry(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	other := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var capturing atomic.Bool
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		capturing.Store(true)
		<-ctx.Done()
		return ctx.Err()
	}
	cancel, done := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, capturing.Load)
	for name, contender := range map[string]struct {
		r *syncRunner
		s *syncSegments
	}{
		"same_journal": {syncRunnerForTest(), f.store},
		"same_runner":  {r, other.store},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := contender.r.run(ctx, contender.s); !errors.Is(err, errSyncRunnerBusy) {
				t.Fatal("concurrent runner was not refused", err)
			}
		})
	}
	cancel()
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSyncRunnerSourceRetryBudgetAndBackoff(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	r.config.MaxRetries = 3
	var calls atomic.Int32
	times := make(chan time.Time, 8)
	r.ops.capture = func(context.Context, *syncSegments) error {
		calls.Add(1)
		times <- time.Now()
		return fmt.Errorf("source retry test: %w", errSyncSourceTransport)
	}
	_, done := syncRunnerStartForTest(t, r, f.store)
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, errSyncRetryExhausted) {
		t.Fatal("continuous source transport failures did not exhaust retries", err)
	}
	if calls.Load() != int32(r.config.MaxRetries+1) || r.snapshot().SourceAttempts != r.config.MaxRetries+1 {
		t.Fatalf("incorrect retry attempts: calls=%d status=%+v", calls.Load(), r.snapshot())
	}
	previous := <-times
	for i := 1; i <= r.config.MaxRetries; i++ {
		current := <-times
		minimum := r.config.RetryMin
		if i > 1 {
			minimum = r.config.RetryMax
		}
		if elapsed := current.Sub(previous); elapsed < minimum*3/4 {
			t.Fatalf("retry %d skipped exponential backoff: %v", i, elapsed)
		}
		previous = current
	}
}

func TestSyncRunnerDurableSourceProgressResetsRetryBudget(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	fatal := errors.New("fourth attempt reached")
	var calls atomic.Int32
	r.ops.capture = func(_ context.Context, s *syncSegments) error {
		switch calls.Add(1) {
		case 3:
			if err := s.append(f.command(1, 10)); err != nil {
				return err
			}
		case 4:
			return fatal
		}
		return errSyncSourceTransport
	}
	_, done := syncRunnerStartForTest(t, r, f.store)
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, fatal) || calls.Load() != 4 {
		t.Fatal("real durable progress did not reset source retry failures", err, calls.Load())
	}
	if r.snapshot().Captured != f.store.durablePosition() {
		t.Fatal("source durable progress missing from status")
	}
}

func TestSyncRunnerCancellationInterruptsBackoff(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	r.config.RetryMin, r.config.RetryMax = time.Second, time.Second
	var calls atomic.Int32
	r.ops.capture = func(context.Context, *syncSegments) error { calls.Add(1); return errSyncSourceTransport }
	cancel, done := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return calls.Load() == 1 })
	start := time.Now()
	cancel()
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, context.Canceled) || time.Since(start) >= 750*time.Millisecond || calls.Load() != 1 {
		t.Fatal("cancellation waited for retry delay or retried anyway", err)
	}
}

func TestSyncRunnerSourceFatalErrorsNeverRetry(t *testing.T) {
	for name, fatal := range map[string]error{
		"history_gap": errSyncHistoryGap,
		"corrupt":     errSyncJournalCorrupt,
		"full":        errSyncJournalFull,
		"unsupported": errSyncUnsupported,
		"unknown":     errors.New("unknown capture failure"),
	} {
		t.Run(name, func(t *testing.T) {
			f := syncResourceFixture(t, defaultSyncResourcePolicy())
			r := syncRunnerForTest()
			var calls atomic.Int32
			r.ops.capture = func(context.Context, *syncSegments) error { calls.Add(1); return fatal }
			_, done := syncRunnerStartForTest(t, r, f.store)
			if err := syncRunnerResultForTest(t, done); !errors.Is(err, fatal) || calls.Load() != 1 {
				t.Fatal("fatal source error was retried or masked", err, calls.Load())
			}
		})
	}
}

func TestSyncRunnerColdConnectStopsCaptureBeforeRetry(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var captureCalls, drainCalls atomic.Int32
	var captureLive, retryWhileCaptureLive atomic.Bool
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		captureLive.Store(true)
		call := captureCalls.Add(1)
		defer captureLive.Store(false)
		if call == 1 {
			if err := s.append(f.command(1, 10)); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return ctx.Err()
	}
	r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
		call := drainCalls.Add(1)
		if call == 2 {
			return syncPosition{}, errSyncColdConnect
		}
		if call == 3 && captureLive.Load() {
			retryWhileCaptureLive.Store(true)
		}
		return s.durablePosition(), nil
	}
	cancel, done := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return captureCalls.Load() >= 2 })
	cancel()
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, context.Canceled) || retryWhileCaptureLive.Load() || captureLive.Load() {
		t.Fatal("cold retry did not pause and join source capture", err)
	}
	if status := r.snapshot(); status.ColdRetries < 1 {
		t.Fatalf("cold retry was absent from status: %+v", status)
	}
}

func TestSyncRunnerColdRetryBudgetWithoutStartingCapture(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var drained, captured atomic.Int32
	r.ops.drain = func(context.Context, *syncSegments) (syncPosition, error) {
		drained.Add(1)
		return syncPosition{}, errSyncColdConnect
	}
	r.ops.capture = func(context.Context, *syncSegments) error { captured.Add(1); return nil }
	_, done := syncRunnerStartForTest(t, r, f.store)
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, errSyncRetryExhausted) || drained.Load() != int32(r.config.MaxRetries+1) || captured.Load() != 0 {
		t.Fatal("startup cold retry budget or capture gate failed", err, drained.Load(), captured.Load())
	}
}

func TestSyncRunnerPressurePauseThenResume(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	r.config.MaxPressure = time.Second
	var available atomic.Uint64
	available.Store(1 << 40)
	f.store.availableBytes = func() (uint64, error) { return available.Load(), nil }
	var captured, cleaned atomic.Int32
	r.ops.capture = func(ctx context.Context, _ *syncSegments) error {
		if captured.Add(1) == 1 {
			available.Store(0)
			return errSyncCapturePressure
		}
		<-ctx.Done()
		return ctx.Err()
	}
	r.ops.cleanup = func(context.Context, *syncSegments) error { cleaned.Add(1); return nil }
	cancel, done := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return cleaned.Load() >= 3 && r.snapshot().Resources.DiskPaused })
	if captured.Load() != 1 {
		t.Fatal("source reconnected while disk pressure remained active")
	}
	available.Store(1 << 40)
	syncRunnerWaitForTest(t, func() bool { return captured.Load() == 2 })
	cancel()
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, context.Canceled) || f.store.poisoned {
		t.Fatal("pressure resume poisoned the retained journal handle", err)
	}
}

func TestSyncRunnerPressureTimeoutDoesNotBusyReconnect(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	r.config.MaxPressure = 40 * time.Millisecond
	var available atomic.Uint64
	available.Store(1 << 40)
	f.store.availableBytes = func() (uint64, error) { return available.Load(), nil }
	var captures atomic.Int32
	r.ops.capture = func(context.Context, *syncSegments) error {
		captures.Add(1)
		available.Store(0)
		return errSyncCapturePressure
	}
	_, done := syncRunnerStartForTest(t, r, f.store)
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, errSyncPressureTimeout) || captures.Load() != 1 || f.store.poisoned {
		t.Fatal("pressure was retried without relief or did not time out", err, captures.Load())
	}
}

func TestSyncRunnerFatalDrainAndCleanupCancelJoinCapture(t *testing.T) {
	for name, fatal := range map[string]error{
		"uncertain": errSyncPendingUncertain,
		"drift":     errSyncTargetDrift,
		"unknown":   errors.New("unexpected cold error"),
		"cleanup":   errors.New("cleanup I/O failure"),
	} {
		t.Run(name, func(t *testing.T) {
			f := syncResourceFixture(t, defaultSyncResourcePolicy())
			r := syncRunnerForTest()
			var started, finished atomic.Bool
			r.ops.capture = func(ctx context.Context, s *syncSegments) error {
				started.Store(true)
				defer finished.Store(true)
				if err := s.append(f.command(1, 10)); err != nil {
					return err
				}
				<-ctx.Done()
				return ctx.Err()
			}
			if name == "cleanup" {
				r.ops.cleanup = func(context.Context, *syncSegments) error {
					if started.Load() {
						return fatal
					}
					return nil
				}
			} else {
				r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
					if started.Load() {
						return syncPosition{}, fatal
					}
					return s.durablePosition(), nil
				}
			}
			_, done := syncRunnerStartForTest(t, r, f.store)
			if err := syncRunnerResultForTest(t, done); !errors.Is(err, fatal) || !started.Load() || !finished.Load() {
				t.Fatal("fatal error was masked by cancellation or capture was not joined", err)
			}
		})
	}
}

func TestSyncRunnerCancellationReleasesViewsAndAllowsReuse(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var started atomic.Int32
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		view, err := s.pendingView()
		if err != nil {
			return err
		}
		defer view.close()
		started.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	for attempt := int32(1); attempt <= 2; attempt++ {
		cancel, done := syncRunnerStartForTest(t, r, f.store)
		syncRunnerWaitForTest(t, func() bool { return started.Load() == attempt })
		if status := syncResourceStatusForTest(t, f.store); status.Views != 1 {
			t.Fatal("capture view was not held during operation")
		}
		cancel()
		if err := syncRunnerResultForTest(t, done); !errors.Is(err, context.Canceled) {
			t.Fatal("reused runner did not cancel normally", err)
		}
		if status := syncResourceStatusForTest(t, f.store); status.Views != 0 {
			t.Fatal("runner returned before capture released its journal view")
		}
	}
}

func TestSyncRunnerProspectivePressureRetainsEpisodeDeadline(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	r.config.MaxPressure = 70 * time.Millisecond
	var captured atomic.Int32
	// Current resourceStatus remains healthy, but the next append's projected
	// write budget cannot fit. Bounded delayed attempts must not reset the
	// no-progress pressure deadline or reconnect in a hot loop.
	r.ops.capture = func(context.Context, *syncSegments) error {
		captured.Add(1)
		return errSyncCapturePressure
	}
	_, done := syncRunnerStartForTest(t, r, f.store)
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, errSyncPressureTimeout) {
		t.Fatal("projected pressure forgot its pause deadline", err)
	}
	if attempts := captured.Load(); attempts < 2 || attempts > 10 {
		t.Fatalf("projected pressure did not use bounded delayed attempts: %d", attempts)
	}
}

func TestSyncRunnerOperationSerializationAndSingleCapture(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	var serialActive, captureActive, drainCalls, cleanupCalls atomic.Int32
	var overlap atomic.Bool
	enterSerial := func() func() {
		if serialActive.Add(1) != 1 {
			overlap.Store(true)
		}
		return func() { serialActive.Add(-1) }
	}
	r.ops.drain = func(_ context.Context, s *syncSegments) (syncPosition, error) {
		defer enterSerial()()
		drainCalls.Add(1)
		return s.durablePosition(), nil
	}
	r.ops.cleanup = func(context.Context, *syncSegments) error {
		defer enterSerial()()
		cleanupCalls.Add(1)
		return nil
	}
	r.ops.capture = func(ctx context.Context, s *syncSegments) error {
		if captureActive.Add(1) != 1 {
			overlap.Store(true)
		}
		defer captureActive.Add(-1)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
				raw := encodeCommand("SET", fmt.Sprintf("runner-%d", i), "value")
				position := s.durablePosition()
				position.Offset += int64(len(raw))
				if err := s.append(syncRecord{Kind: "command", Position: position, Data: raw}); err != nil {
					return err
				}
			}
		}
	}
	cancel, done := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return drainCalls.Load() >= 3 && cleanupCalls.Load() >= 3 })
	cancel()
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, context.Canceled) || overlap.Load() || captureActive.Load() != 0 || serialActive.Load() != 0 {
		t.Fatal("runner operations overlapped or outlived cancellation", err)
	}
}

func TestSyncRunnerRetryDelaySaturatesAtConfiguredMaximum(t *testing.T) {
	config := defaultSyncRunnerConfig()
	config.RetryMin, config.RetryMax = 10*time.Millisecond, 25*time.Millisecond
	for failures, want := range map[int]time.Duration{
		1:          10 * time.Millisecond,
		2:          20 * time.Millisecond,
		3:          25 * time.Millisecond,
		1000000000: 25 * time.Millisecond,
	} {
		if got := syncRetryDelay(config, failures); got != want {
			t.Fatalf("retry delay %d = %s, wanted %s", failures, got, want)
		}
	}
}

func TestSyncRunnerStatusDoesNotExposeUnderlyingErrorText(t *testing.T) {
	f := syncResourceFixture(t, defaultSyncResourcePolicy())
	r := syncRunnerForTest()
	const sensitive = "backend-secret-response-must-not-leak"
	fatal := errors.New(sensitive)
	r.ops.capture = func(context.Context, *syncSegments) error { return fatal }
	_, done := syncRunnerStartForTest(t, r, f.store)
	if err := syncRunnerResultForTest(t, done); !errors.Is(err, fatal) {
		t.Fatal("runner changed the inspectable original error", err)
	}
	status := r.snapshot()
	if status.State != "failed" || status.LastError == "" || strings.Contains(status.LastError, sensitive) {
		t.Fatalf("status did not expose only a fixed failure category: %+v", status)
	}
}
