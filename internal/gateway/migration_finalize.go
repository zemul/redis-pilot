package gateway

import (
	"context"
	"crypto/rand"
	"errors"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

// Caller holds the topology write lease, including during restart assembly.
func (s *cacheShards) reconcileFinalLocked(ctx context.Context, j *sharding.Journal) error {
	p, err := j.FinalPhase(ctx)
	if err != nil {
		s.publicationPending = true
		return err
	}
	s.publicationPending = false
	if p == sharding.FinalActive {
		return nil
	}
	if s.change != nil {
		s.retiring, s.change, s.topology = s.change, nil, j.Target()
		s.cursors.mu.Lock()
		s.cursors.entries = nil // transition and stable traversal have different node lists
		s.cursors.mu.Unlock()
	}
	if p == sharding.FinalComplete {
		s.retiring, s.journal = nil, nil
	}
	return nil
}

// Publish under a bounded global admission barrier; cleanup runs per key after
// publication. No user data is deleted here, only exact task-bound markers.
func (s *cacheShards) publishMigration(ctx context.Context, j *sharding.Journal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.journal != j {
		return errors.New("migration task changed")
	}
	if err := s.reconcileFinalLocked(ctx, j); err != nil {
		return err
	}
	if s.retiring != nil || s.journal == nil {
		return nil
	}
	p, err := j.WorkerProgress(ctx)
	if err != nil {
		return err
	}
	pending, err := j.HasPending(ctx)
	if err != nil {
		return err
	}
	if p.Phase != sharding.WorkerAwaitFinalize || pending {
		return errors.New("migration scans or handoffs incomplete")
	}
	// Invalidate every pre-publication migration/ordinary script before any
	// marker can disappear. Retrying a partial rotation is safe; restart claims
	// fresh owners before admitting traffic. Never simply DEL the owner key.
	if err := s.rotateMigrationOwnersLocked(ctx, s.change); err != nil {
		return err
	}
	s.publicationPending = true
	err = j.Publish(ctx)
	// Even a cancelled/unknown commit must be reconciled before old routing is
	// admitted again. On failure the worker retries; never infer a rollback.
	if e := s.reconcileFinalLocked(ctx, j); e != nil {
		return e
	}
	return err
}

func (s *cacheShards) rotateMigrationOwnersLocked(ctx context.Context, change *sharding.Transition) error {
	seen := map[string]bool{}
	for _, node := range change.TraversalNodes() {
		if seen[node.ID] {
			continue
		}
		seen[node.ID] = true
		n := s.node(node.ID)
		if _, err := s.ensure(ctx, n); err != nil {
			return err
		}
		n.mu.Lock()
		n.previous, n.token, n.settled = n.token, rand.Text(), false
		n.mu.Unlock()
		if _, err := s.ensure(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

const migrationRetireScript = `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN retirement owner changed') end
if ARGV[3]=='cleaned' and redis.call('EXISTS',KEYS[3])~=0 then return redis.error_reply('TRYAGAIN source data remains') end
if redis.call('EXISTS',KEYS[2])==0 then return 1 end
if redis.call('HGET',KEYS[2],'binding')~=ARGV[2] or redis.call('HGET',KEYS[2],'key')~=KEYS[3] or redis.call('HGET',KEYS[2],'phase')~=ARGV[3] then return redis.error_reply('TRYAGAIN retirement binding changed') end
redis.call('DEL',KEYS[2]);return 1`

func (s *cacheShards) retireMigrationKey(ctx context.Context, j *sharding.Journal, h sharding.Handoff) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.journal != j || s.retiring == nil || s.publicationPending {
		return errors.New("task not published")
	}
	release, err := s.r.acquire(ctx, h.Key)
	if err != nil {
		return err
	}
	defer release()
	for i, node := range []sharding.Node{h.Placement.Source, h.Placement.Target} {
		n := s.node(node.ID)
		token, err := s.ensure(ctx, n)
		if err != nil {
			return err
		}
		c, err := s.raw(ctx, n)
		if err != nil {
			return err
		}
		f, err := newMigrationFence(migrationLimitedCaller(ctx, c), node, s.r.guard, token, h)
		if err == nil {
			phase := "cleaned"
			if i == 1 {
				phase = "serving"
			}
			var b []byte
			b, err = f.c.call(ctx, "EVAL", migrationRetireScript, "3", f.ownerKey, f.marker, f.key, f.owner, f.binding, phase)
			if err == nil && string(b) != ":1\r\n" {
				err = errors.New("migration marker retirement not confirmed")
			}
		}
		c.Close()
		if err != nil {
			return err
		}
	}
	return j.Forget(ctx, h)
}

func (s *cacheShards) finalizeMigration(ctx context.Context, j *sharding.Journal, o migrationWorkerOptions, keys, wire *migrationPacer) error {
	s.finalizeMu.Lock()
	defer s.finalizeMu.Unlock()
	step, cancel := context.WithTimeout(ctx, o.StepTimeout)
	err := s.publishMigration(step, j)
	cancel()
	if err != nil {
		return err
	}
	s.mu.RLock()
	done := s.journal != j
	s.mu.RUnlock()
	if done {
		return nil
	}
	after := int64(0)
	var failed bool
	for {
		page, err := j.Page(ctx, after, 100)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		for _, h := range page {
			if err := keys.wait(ctx, 1); err != nil {
				return err
			}
			step, cancel := context.WithTimeout(ctx, o.StepTimeout)
			err := s.retireMigrationKey(context.WithValue(step, migrationRateKey{}, wire), j, h)
			cancel()
			if err != nil {
				failed = true
				s.r.events.WithLabelValues("migration_retirement_pending").Inc()
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			after = h.Sequence
		}
	}
	if failed {
		return errors.New("migration metadata retirement pending")
	}
	ctx, cancel = context.WithTimeout(ctx, o.StepTimeout)
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.journal != j {
		return errors.New("migration task changed")
	}
	// Fence delayed cleanup packets too, before the next task can reuse a
	// marker name. Publication fencing alone does not isolate cleanup itself.
	if err := s.rotateMigrationOwnersLocked(ctx, s.retiring); err != nil {
		return err
	}
	err = j.Complete(ctx)
	if e := s.reconcileFinalLocked(ctx, j); e != nil {
		return e
	}
	return err
}
