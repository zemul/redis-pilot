package gateway

import (
	"context"
	"encoding/json"
	"errors"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (r *cacheRuntime) restorePrimaries(ctx context.Context) error {
	if r.shardState.primaries == nil {
		return nil
	}
	states, err := r.shardState.primaries.Snapshot(ctx)
	if err != nil {
		return err
	}
	byID := map[string]sharding.PrimaryState{}
	for _, v := range states {
		byID[v.Current.ShardID] = v
	}
	for _, n := range r.shards.nodeSnapshot() {
		v, ok := byID[n.node.ID]
		if !ok {
			return errors.New("durable primary missing for routing node")
		}
		if !r.cfg.allowsBackend(v.Current.Address) || (v.Pending != nil && !r.cfg.allowsBackend(v.Pending.Address)) {
			return errors.New("durable primary outside backend allowlist")
		}
		n.address = v.Current.Address
		n.replicas = v.Current.Replicas
		n.blocked.Store(v.Pending != nil)
	}
	return nil
}

// Called only by the control polling goroutine. The topology lease drains
// admitted requests; individual blocked nodes remain unavailable across retries.
func (s *Service) reconcilePrimaries(ctx context.Context, c apitypes.ColdCluster) error {
	r := s.cache
	if r == nil || r.shards == nil || r.shardState == nil {
		return errors.New("primary runtime unavailable")
	}
	disk, shards := r.shardState, r.shards
	if len(c.Primaries) == 0 {
		if disk.primaries != nil {
			return errors.New("primary-aware control assignment required")
		}
		return nil // legacy Server compatibility before tracking is initialized
	}
	if len(c.Primaries) > 2*sharding.MaxNodes {
		return sharding.ErrPrimaryConflict
	}
	desired := map[string]sharding.Primary{}
	for _, v := range c.Primaries {
		p := sharding.Primary(v)
		if sharding.ValidatePrimary(p) != nil || !r.cfg.allowsBackend(p.Address) || (r.cfg.enabled() && p.Address == s.cfg.RedisAddress) {
			return sharding.ErrPrimaryConflict
		}
		if _, ok := desired[p.ShardID]; ok {
			return sharding.ErrPrimaryConflict
		}
		desired[p.ShardID] = p
	}
	shards.mu.Lock()
	defer shards.mu.Unlock()
	needed := map[string]sharding.Node{}
	for _, n := range shards.topology.Config().Nodes {
		needed[n.ID] = n
	}
	if shards.change != nil {
		for _, n := range shards.change.TraversalNodes() {
			needed[n.ID] = n
		}
	}
	if shards.retiring != nil {
		for _, n := range shards.retiring.TraversalNodes() {
			needed[n.ID] = n
		}
	}
	// Persist newly assigned members before acceptColdTask can bind its durable
	// journal. Otherwise a crash between task acceptance and the next poll would
	// leave a routing node without its primary record during recovery.
	if c.Task != nil && c.Task.Phase != "complete" {
		for _, n := range c.Task.Target {
			if _, ok := needed[n.ID]; !ok {
				needed[n.ID] = sharding.Node{ID: n.ID, Address: n.Address}
			}
		}
	}
	var wanted []sharding.Primary
	known := map[string]sharding.PrimaryState{}
	if disk.primaries != nil {
		states, err := disk.primaries.Snapshot(ctx)
		if err != nil {
			return err
		}
		for _, v := range states {
			known[v.Current.ShardID] = v
		}
	}
	for id := range needed {
		p, ok := desired[id]
		if !ok {
			return sharding.ErrPrimaryConflict
		}
		if _, knownAlready := known[id]; !knownAlready {
			address := needed[id].Address
			if n := shards.node(id); n != nil {
				address = n.backendAddress()
			}
			if address != p.Address && (shards.change != nil || shards.retiring != nil || shards.journal != nil) {
				return errors.New("initial primary snapshot must match durable endpoint")
			}
		}
		wanted = append(wanted, p)
	}
	if disk.primaries == nil {
		// A crash after table creation but before the flag is persisted may leave
		// an unreferenced initial table, never a completed switch. Reopen it.
		var tables int
		if err := disk.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='shard_primaries'`).Scan(&tables); err != nil {
			return err
		}
		var store *sharding.PrimaryStore
		var err error
		if tables == 0 {
			store, err = sharding.CreatePrimaryStore(ctx, disk.db, wanted)
		} else {
			store, err = sharding.OpenPrimaryStore(ctx, disk.db)
		}
		if err != nil {
			return err
		}
		initial, err := store.Snapshot(ctx)
		if err != nil {
			return err
		}
		for _, v := range initial {
			p, wanted := desired[v.Current.ShardID]
			if v.Pending != nil || (wanted && (p.Group != v.Current.Group || p.Generation < v.Current.Generation || (p.Generation == v.Current.Generation && p != v.Current))) {
				return sharding.ErrPrimaryConflict
			}
		}
		m := disk.manifest
		m.PrimaryTracking = true
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if _, err = disk.db.ExecContext(ctx, `UPDATE runtime SET manifest=? WHERE id=1`, b); err != nil {
			return err
		}
		disk.manifest, disk.primaries = m, store
	}
	if err := disk.primaries.ReconcileMembers(ctx, wanted); err != nil {
		return err
	}
	states, err := disk.primaries.Snapshot(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, v := range states {
		if shards.node(v.Current.ShardID) == nil {
			continue
		}
		if err := s.reconcilePrimaryLocked(ctx, v, desired[v.Current.ShardID]); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (s *Service) reconcilePrimaryLocked(ctx context.Context, state sharding.PrimaryState, next sharding.Primary) error {
	r, shards, disk := s.cache, s.cache.shards, s.cache.shardState
	old := shards.node(next.ShardID)
	if old == nil || next.Group != state.Current.Group || next.Generation < state.Current.Generation {
		return sharding.ErrPrimaryConflict
	}
	if next.Generation == state.Current.Generation && next != state.Current {
		return sharding.ErrPrimaryConflict
	}
	if state.Pending != nil && next.Generation < state.Pending.Generation {
		return sharding.ErrPrimaryConflict
	}
	if state.Pending == nil && state.Current == next && !old.blocked.Load() && old.backendAddress() == next.Address && old.replicas == next.Replicas {
		return nil
	}
	if shards.revisionLocked() == ^uint64(0) {
		return errors.New("runtime epoch exhausted")
	}
	old.blocked.Store(true)
	r.hotFailed()
	shards.blocking.interrupt(next.ShardID)
	if state.Pending != nil && *state.Pending != next {
		if err := disk.primaries.Retarget(ctx, *state.Pending, next); err != nil {
			return err
		}
	} else if state.Pending == nil && state.Current != next {
		if err := disk.primaries.Prepare(ctx, state.Current, next); err != nil {
			return err
		}
	}
	// Only fixed replicated groups use the handoff replication barriers. Never
	// reinterpret a standalone task or changed membership as a safe replacement.
	if (shards.change != nil || shards.retiring != nil || shards.journal != nil) && (old.replicas == 0 || next.Replicas != old.replicas) {
		return errors.New("primary switch requires migration reconciliation")
	}
	candidate := &cacheShardNode{node: old.node, address: next.Address, password: old.password, replicas: next.Replicas}
	if err := shards.preflightCold(ctx, candidate, false); err != nil {
		return err
	}
	if err := shards.startNode(ctx, candidate); err != nil {
		return err
	}
	// Native blocking records may have been replicated. Only retire on the new
	// server after checking the unique connection name as well as its numeric ID.
	if err := shards.drainBlocking(ctx, candidate, nil, false); err != nil {
		return err
	}
	if err := disk.primaries.Complete(ctx, next); err != nil {
		return err
	}
	r.hotFailed() // cache recovery must happen after old admissions drained
	shards.nodesMu.Lock()
	shards.nodes[next.ShardID] = candidate
	shards.nodesMu.Unlock()
	shards.primaryEpoch++
	r.events.WithLabelValues("primary_switch_completed").Inc()
	return nil
}
