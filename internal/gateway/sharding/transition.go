package sharding

import "errors"

// Placement compares configurations, NOT the current data location. Moves
// means the executor must consult/complete the handoff; it never means copied.
type Placement struct {
	Source Node
	Target Node
}

func (p Placement) Moves() bool { return p.Source.ID != p.Target.ID }

type Transition struct {
	from, to *Topology
	scan     []Node
}

// NewTransition defines one membership change. Address changes for retained
// IDs need a separate controlled reconnect/failover path, not a scan migration.
func NewTransition(from, to *Topology) (*Transition, error) {
	if from == nil || to == nil || len(from.nodes) == 0 || len(to.nodes) == 0 || to.revision <= from.revision {
		return nil, errors.New("transition requires validated snapshots with increasing revisions")
	}
	old := map[string]Node{}
	addresses := map[string]string{}
	for _, n := range from.nodes {
		old[n.node.ID] = n.node
		addresses[n.node.Address] = n.node.ID
	}
	added := false
	for _, n := range to.nodes {
		previous, exists := old[n.node.ID]
		if owner, reused := addresses[n.node.Address]; reused && owner != n.node.ID {
			return nil, errors.New("migration cannot reuse a source address under another node ID")
		}
		if exists && previous.Address != n.node.Address {
			return nil, errors.New("retained node address changed; reconnect separately before membership migration")
		}
		added = added || !exists
		delete(old, n.node.ID)
	}
	if !added && len(old) == 0 {
		return nil, errors.New("transition does not change membership")
	}
	t := &Transition{from: from, to: to}
	for _, n := range from.nodes {
		// Additions can win keys from any source. With removals only, all
		// retained winners remain unchanged, so scan just removed nodes.
		if _, removed := old[n.node.ID]; added || removed {
			t.scan = append(t.scan, n.node)
		}
	}
	return t, nil
}

func (t *Transition) DesiredPlacement(key []byte) Placement {
	return Placement{Source: t.from.Owner(key), Target: t.to.Owner(key)}
}

func (t *Transition) ScanSources() []Node { return append([]Node(nil), t.scan...) }

// TraversalNodes is for user SCAN, not the migration worker. Scan every old
// node before scanning every target node, including retained nodes twice.
// A continuously present key removed during the old pass is then already on
// its target before the target pass starts. Duplicates are permitted by SCAN.
func (t *Transition) TraversalNodes() []Node {
	return append(t.from.Config().Nodes, t.to.Config().Nodes...)
}

func (t *Transition) Revisions() (uint64, uint64) { return t.from.revision, t.to.revision }

func (t *Transition) MatchesSource(source *Topology) bool { return t.from.Equal(source) }
