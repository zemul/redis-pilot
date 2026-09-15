package gateway

import (
	"errors"
	"strings"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

var (
	errShardCommand   = errors.New("unsupported sharded command, options or key")
	errShardCrossNode = errors.New("CROSSNODE command requires keys on one cold node")
	errShardGlobal    = errors.New("command requires dedicated cluster-wide or keyless handling")
)

// A plan is only desired placement. In particular, moves requires resolving
// durable handoffs under key coordination; it is NOT permission to send to To.
// The live single-backend configuration does not activate this planner yet.
type cacheShardKey struct {
	key       string
	placement sharding.Placement
}

type cacheShardGroup struct {
	node sharding.Node
	// Positions refer to key operands, not argv indexes. Keep duplicates for
	// MGET/EXISTS and preserve MSET order; never concurrently write one key.
	positions []int
}

type cacheShardPlan struct {
	keys                     []cacheShardKey
	groups                   []cacheShardGroup
	fromRevision, toRevision uint64
	moves                    bool
}

// planCacheCommand reuses the existing command/KEYS validation. No backend is
// contacted, no script is executed and no cache is altered by planning.
func (r *cacheRuntime) planCacheCommand(topology *sharding.Topology, change *sharding.Transition, a [][]byte) (cacheShardPlan, error) {
	if len(a) == 0 {
		return cacheShardPlan{}, errShardCommand
	}
	p, ok := r.prepareCache(a)
	if !ok {
		return cacheShardPlan{}, errShardCommand
	}
	name := strings.ToUpper(string(a[0]))
	split := name == "MGET" || name == "MSET" || name == "DEL" || name == "UNLINK" || name == "EXISTS"
	return planCacheKeys(topology, change, p.keys, split)
}

// planCacheTransaction must be rerun before EXEC under coordination. It does
// not relax MSET into fan-out inside a transaction, and includes WATCH keys.
func (r *cacheRuntime) planCacheTransaction(topology *sharding.Topology, change *sharding.Transition, queue [][][]byte, watched [][]byte) (cacheShardPlan, error) {
	keys := append([][]byte(nil), watched...)
	for _, a := range queue {
		if len(a) == 0 {
			return cacheShardPlan{}, errShardCommand
		}
		if len(a) == 1 && strings.EqualFold(string(a[0]), "UNWATCH") {
			continue
		}
		p, ok := r.prepareCache(a)
		if !ok {
			return cacheShardPlan{}, errShardCommand
		}
		// SCAN/KEYS cannot masquerade as keyless, node-local operations.
		name := strings.ToUpper(string(a[0]))
		if name == "SCAN" || name == "KEYS" {
			return cacheShardPlan{}, errShardGlobal
		}
		keys = append(keys, p.keys...)
	}
	return planCacheKeys(topology, change, keys, false)
}

func planCacheKeys(topology *sharding.Topology, change *sharding.Transition, keys [][]byte, split bool) (cacheShardPlan, error) {
	var p cacheShardPlan
	if topology == nil || topology.Revision() == 0 {
		return p, errors.New("missing validated topology")
	}
	p.fromRevision = topology.Revision()
	p.toRevision = p.fromRevision
	if change != nil {
		// Compare the actual source snapshot too: equal revision alone is
		// not enough to trust configurations supplied by different callers.
		if !change.MatchesSource(topology) {
			return cacheShardPlan{}, errors.New("transition source differs from current topology")
		}
		_, to := change.Revisions()
		p.toRevision = to
	}
	if len(keys) == 0 {
		return cacheShardPlan{}, errShardGlobal
	}
	for _, key := range keys {
		if !cacheBusinessKey(key) {
			return cacheShardPlan{}, errShardCommand
		}
	}
	groups := map[string]int{}
	for i, key := range keys {
		var placement sharding.Placement
		if change != nil {
			placement = change.DesiredPlacement(key)
		} else {
			node := topology.Owner(key)
			placement = sharding.Placement{Source: node, Target: node}
		}
		p.keys = append(p.keys, cacheShardKey{key: string(key), placement: placement})
		p.moves = p.moves || placement.Moves()
		g, exists := groups[placement.Target.ID]
		if !exists {
			if !split && len(p.groups) != 0 {
				return cacheShardPlan{}, errShardCrossNode
			}
			g = len(p.groups)
			groups[placement.Target.ID] = g
			p.groups = append(p.groups, cacheShardGroup{node: placement.Target})
		}
		p.groups[g].positions = append(p.groups[g].positions, i)
	}
	return p, nil
}
