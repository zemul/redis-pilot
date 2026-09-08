package gateway

// G6a-9: explicit, bounded shutdown for the isolated runner only. This does not
// stop Redis writes or promise that every source write has reached this journal.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	errSyncRunnerNotRunning   = errors.New("replication loop has no published run")
	errSyncShutdownTimeout    = errors.New("replication graceful shutdown deadline exceeded")
	errSyncShutdownAborted    = errors.New("replication graceful shutdown explicitly canceled")
	errSyncShutdownIncomplete = errors.New("replication shutdown did not confirm the frozen journal tail")
	errSyncGraceRequested     = errors.New("replication graceful shutdown requested") // internal control flow only
)

type syncRunControl struct {
	stop, done  chan struct{}
	once        sync.Once
	stopCapture context.CancelFunc
	force       context.CancelCauseFunc
	result      error // written before closing done; only read after receiving done
}

func (c *syncRunControl) requested() bool {
	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

// Every accepted caller supplies a deadline, at most 30s away. The earliest
// caller cancellation may force this generation to stop. Even then we join
// before returning: a filesystem operation need not stop exactly at a deadline.
func (r *syncRunner) shutdown(ctx context.Context) error {
	r.mu.Lock()
	c := r.control
	if c == nil {
		r.mu.Unlock()
		return errSyncRunnerNotRunning
	}
	select {
	case <-c.done:
		r.mu.Unlock()
		return c.result
	default:
	}
	deadline, ok := ctx.Deadline()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 30*time.Second {
		r.mu.Unlock()
		return errors.New("replication shutdown requires a future deadline within 30 seconds")
	}
	c.once.Do(func() {
		r.status.StopRequested, r.status.UpdatedAt = true, time.Now()
		// Separate source root is canceled even if the main loop is blocked
		// on cold I/O. It cannot authorize a new source connection afterward.
		close(c.stop)
		c.stopCapture()
	})
	r.mu.Unlock()
	stopForce := context.AfterFunc(ctx, func() {
		cause := errSyncShutdownAborted
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cause = errSyncShutdownTimeout
		}
		c.force(cause)
	})
	defer stopForce()
	<-c.done
	return c.result
}

func (r *syncRunner) drainStopped(ctx context.Context, j *syncSegments, ops syncRunnerOps, target syncPosition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// No snapshot was durably captured: nothing to apply. This is not cold
	// bootstrap confirmation and the position-valid metrics remain false.
	if target.History == "" {
		return nil
	}
	var applied syncPosition
	for failures := 0; ; {
		position, err := ops.drain(ctx, j)
		err = syncDrainCancellation(ctx, err)
		if position.History != "" {
			applied = position
			r.update(func(s *syncRunnerStatus) { s.Applied = applied })
		}
		if err == nil {
			applied = position
			break
		}
		if !errors.Is(err, errSyncColdConnect) || syncTerminalOutcome(err) || ctx.Err() != nil {
			return err
		}
		failures++
		if failures > r.config.MaxRetries {
			return fmt.Errorf("%w: %w", errSyncRetryExhausted, err)
		}
		r.update(func(s *syncRunnerStatus) { s.ColdRetries++; s.LastError = "cold_connection" })
		timer := time.NewTimer(syncRetryDelay(r.config, failures))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	j.mu.Lock()
	base, confirmed := j.manifest.Base.Position, j.manifest.Confirmed.Position
	j.mu.Unlock()
	if applied != target || j.durablePosition() != target || (target != base && confirmed != target) {
		return errSyncShutdownIncomplete
	}
	if err := ops.cleanup(ctx, j); err != nil {
		return err
	}
	resources, resourceErr := j.resourceStatus()
	r.update(func(s *syncRunnerStatus) {
		s.Captured, s.Applied, s.Confirmed, s.Resources = target, applied, confirmed, resources
		s.ResourcesObserved, s.ResourceUpdatedAt = true, time.Now()
	})
	if resourceErr != nil && !errors.Is(resourceErr, errSyncCapturePressure) {
		return resourceErr
	}
	return ctx.Err()
}
