package gateway

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

// Internal opt-in only, set before service startup. Ready scans are not a
// final topology publication or permission to delete metadata/remove nodes.
type migrationWorkerOptions struct {
	ScanCount                       int
	KeysPerSecond, BytesPerSecond   int64
	StepTimeout, RetryMin, RetryMax time.Duration
	Finalize                        bool // internal opt-in; requires the same backend preflight as migration
}

func defaultMigrationWorkerOptions() migrationWorkerOptions {
	return migrationWorkerOptions{ScanCount: 128, KeysPerSecond: 100, BytesPerSecond: 16 << 20, StepTimeout: 30 * time.Second, RetryMin: 500 * time.Millisecond, RetryMax: 30 * time.Second}
}
func (o migrationWorkerOptions) validate() error {
	if o.ScanCount < 1 || o.ScanCount > 1000 || o.KeysPerSecond < 1 || o.KeysPerSecond > 1e9 || o.BytesPerSecond < 1 || o.BytesPerSecond > 1<<40 || o.StepTimeout <= 0 || o.StepTimeout > time.Hour || o.RetryMin <= 0 || o.RetryMax < o.RetryMin || o.RetryMax > time.Minute {
		return errors.New("invalid migration worker limits")
	}
	return nil
}

type migrationPacer struct {
	mu        sync.Mutex
	next      time.Time
	perSecond int64
}

func (p *migrationPacer) wait(ctx context.Context, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n <= 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	start := p.next
	if start.Before(now) {
		start = now
	}
	if err := migrationWait(ctx, time.Until(start)); err != nil {
		// A cancelled reservation must not accumulate debt on each retry.
		return err
	}
	duration := time.Duration(float64(n) / float64(p.perSecond) * float64(time.Second))
	p.next = time.Now().Add(duration)
	return nil
}
func migrationWait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type migrationRateKey struct{}
type migrationRateCaller struct {
	raw   migrationCaller
	pacer *migrationPacer
}

func (c migrationRateCaller) migrationFrameBudget() int {
	if b, ok := c.raw.(interface{ migrationFrameBudget() int }); ok {
		return b.migrationFrameBudget()
	}
	return 0
}

func (c migrationRateCaller) call(ctx context.Context, args ...string) ([]byte, error) {
	if err := c.pacer.wait(ctx, int64(len(encodeCommand(args...)))); err != nil {
		return nil, err
	}
	b, err := c.raw.call(ctx, args...)
	if err != nil {
		return b, err
	}
	if err = c.pacer.wait(ctx, int64(len(b))); err != nil {
		return nil, err
	}
	return b, nil
}
func migrationLimitedCaller(ctx context.Context, c migrationCaller) migrationCaller {
	if p, ok := ctx.Value(migrationRateKey{}).(*migrationPacer); ok {
		return migrationRateCaller{c, p}
	}
	return c
}

// The service owns this goroutine's context and waits for it during shutdown.
// Transient journal/backend failures retry with bounded backoff; ordinary
// traffic never waits for the whole scan, only for a key's actual handoff.
func (s *cacheShards) runMigrationWorker(ctx context.Context, o migrationWorkerOptions) error {
	if err := o.validate(); err != nil {
		return err
	}
	if !s.workerRunning.CompareAndSwap(false, true) {
		return errors.New("migration worker already running")
	}
	defer s.workerRunning.Store(false)
	backoff := o.RetryMin
	keyPacer, bytePacer := &migrationPacer{perSecond: o.KeysPerSecond}, &migrationPacer{perSecond: o.BytesPerSecond}
	for ctx.Err() == nil {
		s.mu.RLock()
		j, change := s.journal, s.change
		s.mu.RUnlock()
		if j == nil {
			if err := migrationWait(ctx, o.RetryMin); err != nil {
				return err
			}
			continue
		}
		phase, err := j.FinalPhase(ctx)
		if err == nil {
			s.mu.Lock()
			if s.journal == j && s.publicationPending {
				err = s.reconcileFinalLocked(ctx, j)
			}
			s.mu.Unlock()
		}
		finish := phase != sharding.FinalActive
		if err == nil && o.Finalize && !finish {
			var p sharding.WorkerProgress
			p, err = j.WorkerProgress(ctx)
			finish = p.Phase == sharding.WorkerAwaitFinalize
			if err == nil && finish {
				var pending bool
				pending, err = j.HasPending(ctx)
				finish = !pending
			}
		}
		if err == nil && finish {
			err = s.finalizeMigration(ctx, j, o, keyPacer, bytePacer)
		} else if err == nil && change != nil {
			var p sharding.WorkerProgress
			p, err = j.BeginWorker(ctx)
			if err == nil {
				cycle := migrationWorkerCycle{s: s, j: j, change: change, options: o, progress: p, keys: keyPacer, bytes: bytePacer}
				err = cycle.run(ctx)
			}
		} else if err == nil {
			err = errors.New("active task has no transition")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			s.r.events.WithLabelValues("migration_worker_retry").Inc()
			if err := migrationWait(ctx, backoff); err != nil {
				return err
			}
			backoff *= 2
			if backoff > o.RetryMax {
				backoff = o.RetryMax
			}
			continue
		}
		backoff = o.RetryMin
		if finish || o.Finalize {
			continue
		}
		// Keep serving the old+new transition. New failed on-demand handoffs
		// after this snapshot must wake recovery; awaiting_finalize isn't done.
		for ctx.Err() == nil {
			if err := migrationWait(ctx, o.RetryMin); err != nil {
				return err
			}
			pending, e := j.HasPending(ctx)
			s.mu.RLock()
			changed := s.journal != j || s.change != change
			s.mu.RUnlock()
			if e != nil || pending || changed {
				break
			}
		}
	}
	return ctx.Err()
}

type migrationWorkerCycle struct {
	s           *cacheShards
	j           *sharding.Journal
	change      *sharding.Transition
	options     migrationWorkerOptions
	progress    sharding.WorkerProgress
	keys, bytes *migrationPacer
	failed      bool
}

func (w *migrationWorkerCycle) save(ctx context.Context) error {
	return w.j.SaveWorker(ctx, w.progress)
}
func (w *migrationWorkerCycle) failure(ctx context.Context, code string) error {
	w.failed = true
	w.progress.Failures++
	w.progress.LastError = code
	return w.save(ctx)
}
func (w *migrationWorkerCycle) move(ctx context.Context, key []byte) error {
	if err := w.keys.wait(ctx, 1); err != nil {
		return err
	}
	step, cancel := context.WithTimeout(ctx, w.options.StepTimeout)
	defer cancel()
	step = context.WithValue(step, migrationRateKey{}, w.bytes)
	w.s.mu.RLock()
	defer w.s.mu.RUnlock()
	if w.s.journal != w.j || w.s.change != w.change {
		return errors.New("migration task changed")
	}
	release, err := w.s.r.acquire(step, key)
	if err != nil {
		return err
	}
	defer release()
	_, err = w.s.moveLocked(step, key)
	w.progress.Attempts++
	if err == nil && w.progress.Phase == sharding.WorkerVerify {
		n := w.s.node(w.change.DesiredPlacement(key).Source.ID)
		token, e := w.s.ensure(step, n)
		if e != nil {
			return e
		}
		c, e := w.s.raw(step, n)
		if e != nil {
			return e
		}
		defer c.Close()
		b, e := c.call(step, "EVAL", workerSourceAbsentScript, "2", w.s.r.guard, string(key), token)
		if e != nil || string(b) != ":0\r\n" {
			return errors.New("source still contains a migrated key")
		}
	}
	return err
}
func (w *migrationWorkerCycle) run(ctx context.Context) error {
	high, err := w.j.Highwater(ctx)
	if err != nil {
		return err
	}
	after := int64(0)
	for after < high {
		page, err := w.j.PendingPage(ctx, after, high, 100)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		for _, h := range page {
			if err := w.move(ctx, h.Key); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := w.failure(ctx, "handoff_recovery_failed"); err != nil {
					return err
				}
			}
			after = h.Sequence
		}
		if err := w.save(ctx); err != nil {
			return err
		}
	}
	for _, phase := range []sharding.WorkerPhase{sharding.WorkerCopy, sharding.WorkerVerify} {
		w.progress.Phase = phase
		for i, node := range w.change.ScanSources() {
			w.progress.SourceIndex = i
			w.progress.Cursor = "0"
			if err := w.save(ctx); err != nil {
				return err
			}
			if err := w.scanSource(ctx, node); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := w.failure(ctx, "source_scan_failed"); err != nil {
					return err
				}
			}
		}
	}
	if w.failed {
		return errors.New("migration cycle has unresolved failures")
	}
	// Brief publication barrier only: no network/copying while holding this
	// write lock. All admitted requests finish before pending/highwater checks.
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if w.s.journal != w.j || w.s.change != w.change {
		return errors.New("migration task changed")
	}
	pending, err := w.j.HasPending(ctx)
	if err != nil {
		return err
	}
	if pending {
		if err := w.failure(ctx, "handoffs_pending_after_scan"); err != nil {
			return err
		}
		return errors.New("handoffs still pending")
	}
	w.progress.VerifiedSequence, err = w.j.Highwater(ctx)
	if err != nil {
		return err
	}
	w.progress.Phase = sharding.WorkerAwaitFinalize
	w.progress.SourceIndex = len(w.change.ScanSources())
	w.progress.Cursor = "0"
	w.progress.LastError = ""
	return w.save(ctx)
}

const workerSourceAbsentScript = `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cold owner changed') end
return redis.call('EXISTS',KEYS[2])`

func (w *migrationWorkerCycle) scanSource(ctx context.Context, node sharding.Node) error {
	n := w.s.node(node.ID)
	setup, cancel := context.WithTimeout(ctx, w.options.StepTimeout)
	token, err := w.s.ensure(setup, n)
	if err != nil {
		cancel()
		return err
	}
	c, err := w.s.raw(setup, n)
	cancel()
	if err != nil {
		return err
	}
	defer c.Close()
	cursor := "0"
	for {
		step, cancel := context.WithTimeout(ctx, w.options.StepTimeout)
		b, err := migrationLimitedCaller(context.WithValue(step, migrationRateKey{}, w.bytes), c).call(step, "EVAL", shardGlobalScanScript, "1", w.s.r.guard, token, cursor, "COUNT", strconv.Itoa(w.options.ScanCount))
		cancel()
		if err != nil {
			return err
		}
		parts, err := frameParts(b)
		if err != nil || len(parts) != 2 {
			return errors.New("invalid source scan reply")
		}
		var ok bool
		cursor, ok = frameString(parts[0])
		if !ok || cursor == "" || len(cursor) > 1024 {
			return errors.New("invalid source cursor")
		}
		items, err := frameParts(parts[1])
		if err != nil {
			return err
		}
		for _, item := range items {
			key, ok := frameString(item)
			if !ok {
				return errors.New("invalid source key")
			}
			if !cacheBusinessKey([]byte(key)) {
				continue
			}
			w.progress.Visited++
			placement := w.change.DesiredPlacement([]byte(key))
			if !placement.Moves() || placement.Source != node {
				continue
			}
			if err := w.move(ctx, []byte(key)); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := w.failure(ctx, "key_handoff_failed"); err != nil {
					return err
				}
			}
		}
		w.progress.Pages++
		w.progress.Cursor = cursor
		if err := w.save(ctx); err != nil {
			return err
		}
		if cursor == "0" {
			return nil
		}
	}
}
