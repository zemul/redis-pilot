package gateway

import (
	"errors"
	"net/netip"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func (s *cacheShards) node(id string) *cacheShardNode {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	return s.nodes[id]
}

func (s *cacheShards) nodeSnapshot() []*cacheShardNode {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	out := make([]*cacheShardNode, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	return out
}

// Caller holds the topology lock with no active transition/retirement.
// Admission topology does not change here. Retired identities are not kept
// forever: their addresses may be reused only after completed retirement.
func (s *cacheShards) registerManaged(t *sharding.Topology) error {
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()
	if s.change != nil || s.retiring != nil || s.journal != nil {
		return errors.New("endpoint registration requires stable topology")
	}
	next := map[string]*cacheShardNode{}
	for _, n := range s.topology.Config().Nodes {
		next[n.ID] = s.nodes[n.ID]
	}
	hot, _ := netip.ParseAddrPort(s.r.s.cfg.RedisAddress)
	hot = netip.AddrPortFrom(hot.Addr().Unmap(), hot.Port())
	for _, n := range t.Config().Nodes {
		a, err := netip.ParseAddrPort(n.Address)
		if err != nil || !a.Addr().IsLoopback() || a == hot {
			return errors.New("invalid managed endpoint")
		}
		if old := next[n.ID]; old != nil && (old.node != n || old.password != "") {
			return errors.New("endpoint identity changed")
		}
		for id, old := range next {
			if old.node.Address == n.Address && id != n.ID {
				return errors.New("endpoint reused with another identity")
			}
		}
	}
	for _, n := range t.Config().Nodes {
		if next[n.ID] == nil {
			next[n.ID] = &cacheShardNode{node: n}
		}
	}
	s.nodes = next
	return nil
}
