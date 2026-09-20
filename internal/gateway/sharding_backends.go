package gateway

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

// Manual credentials are separate from durable routing snapshots. Managed
// passwordless endpoints may be added by validated control-plane tasks.
type cacheShardEndpoint struct {
	node     sharding.Node
	password string
}
type cacheShardNode struct {
	node            sharding.Node
	address         string // immutable physical primary override; node retains migration identity
	blocked         atomic.Bool
	replicas        int // immutable, managed group membership is fixed
	password        string
	mu              sync.Mutex
	token, previous string
	settled         bool
}

type cacheShards struct {
	r                  *cacheRuntime
	mu                 sync.RWMutex // topology changes drain all admitted operations
	topology           *sharding.Topology
	change             *sharding.Transition
	journal            *sharding.Journal
	nodesMu            sync.RWMutex
	nodes              map[string]*cacheShardNode // current/task endpoints; node objects retain immutable identities
	cursors            shardCursorStore
	blocking           shardBlockingActive
	workerConfig       *migrationWorkerOptions // immutable after service startup
	workerRunning      atomic.Bool
	finalizeMu         sync.Mutex
	retiring           *sharding.Transition
	publicationPending bool   // a publish ACK was lost; reconcile before admission
	primaryEpoch       uint64 // invalidates WATCH/SCAN on physical primary replacement
}

func newCacheShards(r *cacheRuntime, t *sharding.Topology, endpoints []cacheShardEndpoint) (*cacheShards, error) {
	if r == nil || t == nil || t.Revision() == 0 {
		return nil, errors.New("validated sharded runtime required")
	}
	s := &cacheShards{r: r, topology: t, nodes: map[string]*cacheShardNode{}}
	canonical := map[string]sharding.Node{}
	hotAddress := ""
	if r.cfg.enabled() {
		hotAddr, err := netip.ParseAddrPort(r.s.cfg.RedisAddress)
		if err != nil {
			return nil, err
		}
		hotAddress = netip.AddrPortFrom(hotAddr.Addr().Unmap(), hotAddr.Port()).String()
	}
	addresses := map[string]bool{}
	// A transition can require the union of two valid topologies, up to twice
	// the routing node limit. The registry is not itself a routing topology.
	for _, e := range endpoints {
		one, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 1, Nodes: []sharding.Node{e.node}})
		if err != nil {
			return nil, err
		}
		n := one.Config().Nodes[0]
		if _, ok := canonical[n.ID]; ok || addresses[n.Address] {
			return nil, errors.New("duplicate backend identity/address")
		}
		canonical[n.ID], addresses[n.Address] = n, true
	}
	for _, e := range endpoints {
		n := canonical[e.node.ID]
		if n.Address == hotAddress {
			return nil, errors.New("cold node cannot reuse hot backend")
		}
		s.nodes[n.ID] = &cacheShardNode{node: n, password: e.password}
	}
	if err := s.validateTopology(t); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *cacheShards) validateTopology(t *sharding.Topology) error {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	for _, n := range t.Config().Nodes {
		if s.nodes[n.ID] == nil || s.nodes[n.ID].node != n {
			return errors.New("topology has no matching backend credentials/address")
		}
	}
	return nil
}

func (s *cacheShards) raw(ctx context.Context, n *cacheShardNode) (*cacheConn, error) {
	if n.blocked.Load() {
		return nil, sharding.ErrPrimaryPending
	}
	return s.r.connectAddress(ctx, false, n.backendAddress(), n.password)
}

func (n *cacheShardNode) backendAddress() string {
	if n.address != "" {
		return n.address
	}
	return n.node.Address
}

func (s *cacheShards) start(ctx context.Context) error {
	if s.workerConfig != nil {
		if err := s.workerConfig.validate(); err != nil {
			return err
		}
	}
	for _, n := range s.requiredNodes() {
		if n.blocked.Load() {
			continue
		}
		var err error
		if s.r.cfg.coldEngine() == "redis" || (s.r.s.cfg.ControlPlane.ManageShards && s.journal != nil) {
			err = s.preflightCold(ctx, n, false)
		}
		if err == nil {
			err = s.startNode(ctx, n)
		}
		if err != nil {
			if s.r.s.cfg.ControlPlane.ManageShards && s.r.shardState != nil {
				n.blocked.Store(true)
				continue // start control polling even when the former master is down
			}
			return err
		}
	}
	return nil
}

// Snapshot only nodes needed for routing or unfinished retirement. An already
// removed node must not keep a completed cluster unready or block restart.
func (s *cacheShards) requiredNodes() []*cacheShardNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodes := s.topology.Config().Nodes
	if s.change != nil {
		nodes = append(nodes, s.change.TraversalNodes()...)
	}
	if s.retiring != nil {
		nodes = append(nodes, s.retiring.TraversalNodes()...)
	}
	byID := map[string]*cacheShardNode{}
	for _, n := range nodes {
		byID[n.ID] = s.node(n.ID)
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*cacheShardNode, 0, len(ids))
	for _, id := range ids {
		out = append(out, byID[id])
	}
	return out
}

func (s *cacheShards) startNode(ctx context.Context, n *cacheShardNode) error {
	prefixes := append([]string{}, s.r.cfg.ColdOnlyPrefixes...)
	sort.Strings(prefixes)
	policy, _ := json.Marshal(prefixes)
	c, err := s.raw(ctx, n)
	if err != nil {
		return err
	}
	b, err := c.call(ctx, "SET", cachePrefix+"cold-only-policy", string(policy), "NX")
	if err == nil && len(b) > 0 && b[0] != '-' {
		b, err = c.call(ctx, "GET", cachePrefix+"cold-only-policy")
	} else {
		c.Close()
		return errors.New("cold shard policy unavailable")
	}
	stored, ok := frameString(b)
	if err != nil || !ok || stored != string(policy) {
		c.Close()
		return errors.New("cold shard policy mismatch")
	}
	b, err = c.call(ctx, "GET", s.r.guard)
	c.Close()
	previous, ok := frameString(b)
	if err != nil || (!ok && string(b) != "$-1\r\n") {
		return errors.New("cold shard ownership unavailable")
	}
	n.mu.Lock()
	n.previous, n.token, n.settled = previous, rand.Text(), false
	n.mu.Unlock()
	if _, err := s.ensure(ctx, n); err != nil {
		return err
	}
	return nil
}

func (s *cacheShards) ensure(ctx context.Context, n *cacheShardNode) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.settled {
		return n.token, nil
	}
	if n.token == "" {
		return "", errors.New("cold shard was not initialized")
	}
	c, err := s.raw(ctx, n)
	if err != nil {
		return "", err
	}
	defer c.Close()
	b, err := c.call(ctx, "EVAL", cacheClaimScript, "1", s.r.guard, n.previous, n.token)
	if err != nil || string(b) != ":1\r\n" {
		if err == nil && string(b) == ":0\r\n" {
			if current, e := c.call(ctx, "GET", s.r.guard); e == nil && string(current) == "$-1\r\n" {
				n.previous = ""
			}
		}
		return "", errors.New("cold shard ownership not confirmed")
	}
	n.settled = true
	return n.token, nil
}

func (s *cacheShards) epoch(ctx context.Context) (string, error) {
	a := shardAdmissionFrom(ctx)
	if a == nil || a.runtime != s.r || !a.live.Load() {
		return "", errors.New("cold shard admission required")
	}
	return s.ensure(ctx, a.node)
}

func (s *cacheShards) uncertain(token string) {
	for _, n := range s.nodeSnapshot() {
		n.mu.Lock()
		if token == n.token && n.settled {
			n.previous, n.token, n.settled = n.token, rand.Text(), false
			n.mu.Unlock()
			s.r.hotFailed()
			s.r.events.WithLabelValues("cold_result_unknown").Inc()
			return
		}
		n.mu.Unlock()
	}
}

func (s *cacheShards) probe(ctx context.Context) {
	for {
		ready := true
		for _, n := range s.requiredNodes() {
			token, err := s.ensure(ctx, n)
			if err != nil {
				ready = false
				continue
			}
			c, err := s.raw(ctx, n)
			if err != nil {
				ready = false
				continue
			}
			b, err := c.call(ctx, "GET", s.r.guard)
			c.Close()
			v, ok := frameString(b)
			if err != nil || !ok || v != token {
				ready = false
				s.uncertain(token)
			}
			// Failed/orphaned blocking sessions must not stop ordinary cache
			// traffic. Retain their durable records until backend drain succeeds.
			cleanup, cancel := context.WithTimeout(ctx, s.r.s.cfg.RequestTimeout)
			if err := s.drainBlocking(cleanup, n, nil, true); err != nil {
				s.r.events.WithLabelValues("blocking_drain_pending").Inc()
			}
			cancel()
		}
		s.r.s.backendReady.Store(ready)
		if ready {
			s.r.recoverHot(ctx)
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// beginTransition is internal and requires the controller to have
// verified durability, clocks and legacy unregistered-writer prerequisites. It never opens
// a new/empty journal in place of an existing task. No second task can overlap.
func (s *cacheShards) beginTransition(to *sharding.Topology, j *sharding.Journal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beginTransitionLocked(to, j)
}

func (s *cacheShards) beginTransitionLocked(to *sharding.Topology, j *sharding.Journal) error {
	if s.change != nil || s.retiring != nil || s.journal != nil {
		return errors.New("a topology transition is already active")
	}
	if to == nil {
		return errors.New("missing target topology")
	}
	if err := s.validateTopology(to); err != nil {
		return err
	}
	c, err := sharding.NewTransition(s.topology, to)
	if err != nil {
		return err
	}
	if !j.Matches(c) {
		return errors.New("journal does not match transition")
	}
	// Prove the journal is readable, not just an in-memory snapshot match.
	if _, err := j.Page(context.Background(), 0, 1); err != nil {
		return err
	}
	if s.r.s.serving.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), s.r.s.cfg.RequestTimeout)
		defer cancel()
		current := map[string]bool{}
		for _, node := range s.topology.Config().Nodes {
			current[node.ID] = true
		}
		for _, node := range to.Config().Nodes {
			n := s.node(node.ID)
			n.mu.Lock()
			uninitialized := n.token == ""
			n.mu.Unlock()
			// A previously removed endpoint was no longer probed and may have
			// restarted. Revalidate its policy and claim a fresh owner on re-add.
			if uninitialized || !current[node.ID] {
				if err := s.startNode(ctx, n); err != nil {
					return err
				}
			}
		}
	}
	s.change, s.journal = c, j
	return s.reconcileFinalLocked(context.Background(), j)
}
