package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

var errShardWatchChanged = errors.New("WATCH topology changed")

func (s *cacheShards) revisionLocked() uint64 {
	if s.change != nil {
		_, to := s.change.Revisions()
		return to + s.primaryEpoch
	}
	return s.topology.Revision() + s.primaryEpoch
}

// admit is also used by WATCH/EXEC. Its release retains the topology read lease
// through native WATCH/EXEC and cache publication, not merely through planning.
func (s *cacheShards) admit(ctx context.Context, keys [][]byte, watchRevision uint64) (context.Context, func(), error) {
	s.mu.RLock()
	if watchRevision != 0 && watchRevision != s.revisionLocked() {
		s.mu.RUnlock()
		return nil, nil, errShardWatchChanged
	}
	next, release, err := s.admitLocked(ctx, keys)
	if err != nil {
		s.mu.RUnlock()
		return nil, nil, err
	}
	return next, func() { release(); s.mu.RUnlock() }, nil
}

func (s *cacheShards) admitLocked(ctx context.Context, keys [][]byte) (context.Context, func(), error) {
	if s.publicationPending {
		return nil, nil, errors.New("topology publication requires reconciliation")
	}
	if len(keys) == 0 {
		if s.node(s.topology.Config().Nodes[0].ID).blocked.Load() {
			return nil, nil, sharding.ErrPrimaryPending
		}
		a := &shardAdmission{runtime: s.r, node: s.node(s.topology.Config().Nodes[0].ID), revision: s.revisionLocked()}
		a.live.Store(true)
		return context.WithValue(ctx, shardAdmissionKey{}, a), func() { a.live.Store(false) }, nil
	}
	p, err := planCacheKeys(s.topology, s.change, keys, false)
	if err != nil {
		return nil, nil, err
	}
	for _, g := range p.groups {
		if s.node(g.node.ID).blocked.Load() {
			return nil, nil, sharding.ErrPrimaryPending
		}
	}
	release, err := s.r.acquireKeys(ctx, keys)
	if err != nil {
		return nil, nil, err
	}
	a := &shardAdmission{runtime: s.r, node: s.node(p.groups[0].node.ID), revision: s.revisionLocked()}
	a.retiring = s.retiring != nil
	seen := map[string]bool{}
	for _, k := range p.keys {
		if seen[k.key] {
			continue
		}
		seen[k.key] = true
		g := shardKeyGuard{key: k.key}
		if s.retiring != nil {
			h, found, e := s.journal.Lookup(ctx, []byte(k.key))
			if e != nil {
				release()
				return nil, nil, e
			}
			if found {
				if h.State != sharding.Cleaned || h.Placement.Target != a.node.node {
					release()
					return nil, nil, errors.New("invalid published handoff")
				}
				g.binding = h.TaskID + ":" + strconv.FormatInt(h.Sequence, 10)
			}
		}
		if k.placement.Moves() {
			h, found, e := s.journal.Lookup(ctx, []byte(k.key))
			if e != nil {
				release()
				return nil, nil, e
			}
			if !found || h.State == sharding.Prepared {
				h, e = s.moveLocked(ctx, []byte(k.key))
				if e != nil {
					release()
					return nil, nil, e
				}
			} else if h.State == sharding.Committed {
				// Cleanup may be pending on an unavailable old node. Do not
				// make serving the committed target depend on reaching it.
				if e := s.activateCommitted(ctx, h); e != nil {
					release()
					return nil, nil, e
				}
			}
			if h.Placement != k.placement || h.State == sharding.Prepared {
				release()
				return nil, nil, errors.New("handoff does not match request")
			}
			g.binding = h.TaskID + ":" + strconv.FormatInt(h.Sequence, 10)
		}
		a.keys = append(a.keys, g)
	}
	a.live.Store(true)
	return context.WithValue(ctx, shardAdmissionKey{}, a), func() { a.live.Store(false); release() }, nil
}

func (s *cacheShards) activateCommitted(ctx context.Context, h sharding.Handoff) error {
	n := s.node(h.Placement.Target.ID)
	token, err := s.ensure(ctx, n)
	if err != nil {
		return err
	}
	c, err := s.raw(ctx, n)
	if err != nil {
		return err
	}
	defer c.Close()
	f, err := newMigrationFence(c, n.node, s.r.guard, token, h)
	if err != nil {
		return err
	}
	phase, attempt, err := f.state(ctx)
	if err != nil {
		return err
	}
	if phase != "ready" && phase != "serving" {
		return errors.New("committed target not ready")
	}
	return f.activate(ctx, attempt)
}

// migrate uses the same locks as ordinary requests and keeps a still-valid hot
// object intact. The background worker uses the same admission around moveLocked.
func (s *cacheShards) migrate(ctx context.Context, key []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.change == nil || !cacheBusinessKey(key) || !s.change.DesiredPlacement(key).Moves() {
		return errors.New("key has no pending placement change")
	}
	release, err := s.r.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer release()
	_, err = s.moveLocked(ctx, key)
	return err
}

func (s *cacheShards) moveLocked(ctx context.Context, key []byte) (sharding.Handoff, error) {
	h, err := s.journal.Prepare(ctx, key)
	if err != nil {
		return h, err
	}
	if h.State == sharding.Cleaned {
		return h, nil
	}
	sourceNode, targetNode := s.node(h.Placement.Source.ID), s.node(h.Placement.Target.ID)
	sourceToken, err := s.ensure(ctx, sourceNode)
	if err != nil {
		return h, err
	}
	targetToken, err := s.ensure(ctx, targetNode)
	if err != nil {
		return h, err
	}
	if h.State == sharding.Prepared {
		// Holding the business key locks prevents new registrations. An
		// acknowledged CLIENT KILL alone is not a backend drain proof.
		if err := s.drainBlocking(ctx, sourceNode, key, false); err != nil {
			return h, err
		}
		if err := s.drainBlocking(ctx, targetNode, key, false); err != nil {
			return h, err
		}
	}
	source, err := s.raw(ctx, sourceNode)
	if err != nil {
		return h, err
	}
	defer source.Close()
	target, err := s.raw(ctx, targetNode)
	if err != nil {
		return h, err
	}
	defer target.Close()
	f, err := newMigrationFence(migrationLimitedCaller(ctx, source), sourceNode.node, s.r.guard, sourceToken, h)
	if err != nil {
		return h, err
	}
	t, err := newMigrationFence(migrationLimitedCaller(ctx, target), targetNode.node, s.r.guard, targetToken, h)
	if err != nil {
		return h, err
	}
	f.address, t.address = sourceNode.backendAddress(), targetNode.backendAddress()
	f.replicas, t.replicas = sourceNode.replicas, targetNode.replicas
	var attempt string
	if h.State == sharding.Prepared {
		attempt, err = copyMigrationKey(ctx, s.journal, h, f, t)
	} else {
		_, attempt, err = t.state(ctx)
	}
	if err == nil {
		err = finishMigrationKey(ctx, s.journal, h, f, t, attempt)
	}
	if err != nil {
		return h, err
	}
	h, _, err = s.journal.Lookup(ctx, key)
	return h, err
}

func shardRuntimeError(err error) []byte {
	if errors.Is(err, errShardCrossNode) {
		return []byte("-CROSSNODE command requires keys on one cold node\r\n")
	}
	if errors.Is(err, errShardCommand) || errors.Is(err, errShardGlobal) {
		return []byte("-ERR unsupported sharded command or cursor; command not executed\r\n")
	}
	return []byte("-TRYAGAIN shard admission unavailable; business command not executed\r\n")
}

func shardCursorOrBlocking(a [][]byte) bool {
	if len(a) == 0 {
		return false
	}
	switch strings.ToUpper(string(a[0])) {
	case "SCAN", "KEYS", "HSCAN", "SSCAN", "ZSCAN", "BLPOP", "BRPOP":
		return true
	}
	return false
}

func (s *cacheShards) execute(parent context.Context, args [][]byte) []byte {
	if len(args) == 0 {
		return shardRuntimeError(errShardCommand)
	}
	name := strings.ToUpper(string(args[0]))
	if name == "BLPOP" || name == "BRPOP" {
		return s.block(parent, args)
	}
	ctx, cancel := context.WithTimeout(parent, s.r.s.cfg.RequestTimeout)
	defer cancel()
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Connection-local commands and scripts without KEYS cannot access data
	if s.publicationPending {
		return shardRuntimeError(errors.New("topology publication requires reconciliation"))
	}
	// outside the existing Lua sandbox. Pick one node only for compilation.
	p, valid := s.r.prepareCache(args)
	keyless := valid && len(p.keys) == 0 && (p.kind == "local" || p.kind == "script" || p.kind == "noscript")
	if name == "SCRIPT" || keyless {
		n := s.node(s.topology.Config().Nodes[0].ID)
		a := &shardAdmission{runtime: s.r, node: n, revision: s.revisionLocked()}
		a.live.Store(true)
		defer a.live.Store(false)
		return s.r.execute(context.WithValue(ctx, shardAdmissionKey{}, a), args)
	}
	if name == "SCAN" || name == "HSCAN" || name == "SSCAN" || name == "ZSCAN" {
		return s.scanLocked(ctx, args)
	}
	if shardCursorOrBlocking(args) {
		return shardRuntimeError(errShardGlobal)
	}
	plan, err := s.r.planCacheCommand(s.topology, s.change, args)
	if err != nil {
		return shardRuntimeError(err)
	}
	if len(plan.groups) == 1 {
		routed, release, err := s.admitLocked(ctx, p.keys)
		if err != nil {
			return shardRuntimeError(err)
		}
		defer release()
		return s.r.execute(routed, args)
	}
	// All operands/options were validated before any group is executed. Keep
	// node grouping deterministic and preserve duplicate operand positions.
	var count int64
	replyBytes := 0
	values := make([][]byte, len(plan.keys))
	for _, group := range plan.groups {
		part := [][]byte{[]byte(name)}
		keys := make([][]byte, 0, len(group.positions))
		for _, pos := range group.positions {
			key := []byte(plan.keys[pos].key)
			keys = append(keys, key)
			part = append(part, key)
			if name == "MSET" {
				part = append(part, args[2+2*pos])
			}
		}
		routed, release, err := s.admitLocked(ctx, keys)
		if err != nil {
			return shardFanoutError(name)
		}
		b := s.r.execute(routed, part)
		release()
		if len(b) == 0 || b[0] == '-' {
			return shardFanoutError(name)
		}
		replyBytes += len(b)
		if replyBytes > s.r.s.cfg.Limits.MaxBytes {
			return shardFanoutError(name)
		}
		switch name {
		case "MGET":
			parts, e := frameParts(b)
			if e != nil || len(parts) != len(group.positions) {
				return shardFanoutError(name)
			}
			for i, pos := range group.positions {
				values[pos] = parts[i]
			}
		case "MSET":
			if string(b) != "+OK\r\n" {
				return shardFanoutError(name)
			}
		default:
			v, ok := frameString(b)
			n, e := strconv.ParseInt(v, 10, 64)
			if !ok || e != nil || n < 0 || n > int64(len(group.positions)) {
				return shardFanoutError(name)
			}
			count += n
		}
	}
	if name == "MGET" {
		return cacheArray(values)
	}
	if name == "MSET" {
		return []byte("+OK\r\n")
	}
	return []byte(fmt.Sprintf(":%d\r\n", count))
}

func shardFanoutError(name string) []byte {
	if name == "MGET" || name == "EXISTS" {
		return []byte("-ERR cross-node read failed; no partial result returned\r\n")
	}
	return []byte("-ERR cross-node write failed; earlier groups may have succeeded; command was not retried\r\n")
}
