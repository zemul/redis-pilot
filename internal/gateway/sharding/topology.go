// Package sharding computes desired placement, not migration completion.
// A caller must resolve durable handoffs before executing against a new owner.
package sharding

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"sort"
)

// Algorithm fixes the score encoding and tag behavior as part of the storage
// format. Changing these rules requires an explicit migration, not an upgrade.
const Algorithm = "hrw-sha256-v1"

const MaxNodes = 1024
const maxSnapshotBytes = 1 << 20

type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// Config contains no secrets. Credentials are resolved separately by the
// runtime, and revisions are never included in a key's score.
type Config struct {
	Algorithm string `json:"algorithm"`
	Revision  uint64 `json:"revision"`
	Nodes     []Node `json:"nodes"`
}

type candidate struct {
	node   Node
	digest [32]byte
}

// Topology is immutable after validation; callers cannot mutate its slices.
type Topology struct {
	revision uint64
	nodes    []candidate
}

func New(c Config) (*Topology, error) {
	if c.Algorithm != Algorithm || c.Revision == 0 {
		return nil, errors.New("explicit supported algorithm and positive revision required")
	}
	if len(c.Nodes) == 0 || len(c.Nodes) > MaxNodes {
		return nil, fmt.Errorf("topology requires 1..%d nodes", MaxNodes)
	}
	t := &Topology{revision: c.Revision}
	ids, addresses := map[string]bool{}, map[string]bool{}
	for _, n := range c.Nodes {
		if !validID(n.ID) || ids[n.ID] {
			return nil, errors.New("node IDs must be unique, nonempty ASCII identifiers of at most 128 bytes")
		}
		addr, err := netip.ParseAddrPort(n.Address)
		ip := addr.Addr().Unmap()
		if err != nil || addr.Port() == 0 || ip.Zone() != "" || !(ip.IsGlobalUnicast() || ip.IsLoopback()) {
			return nil, errors.New("node address must be a literal unicast IP and nonzero port")
		}
		n.Address = netip.AddrPortFrom(ip, addr.Port()).String()
		if addresses[n.Address] {
			return nil, errors.New("different node IDs cannot share one backend address")
		}
		ids[n.ID], addresses[n.Address] = true, true
		t.nodes = append(t.nodes, candidate{n, sha256.Sum256([]byte(Algorithm + "/node\x00" + n.ID))})
	}
	sort.Slice(t.nodes, func(i, j int) bool { return t.nodes[i].node.ID < t.nodes[j].node.ID })
	return t, nil
}

func validID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, b := range []byte(s) {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' || b == '_' || b == '.') {
			return false
		}
	}
	return true
}

// RoutingKey uses the first opening brace and its first closing brace. An
// empty or unclosed first tag falls back to the full binary key; it does not
// search for another tag. It never changes the stored business key.
func RoutingKey(key []byte) []byte {
	if start := bytes.IndexByte(key, '{'); start >= 0 {
		if end := bytes.IndexByte(key[start+1:], '}'); end > 0 {
			return key[start+1 : start+1+end]
		}
	}
	return key
}

// Owner requires a Topology returned by New or Decode. Work is local O(N),
// with no network requests, per-key index or allocations in the lookup loop.
func (t *Topology) Owner(key []byte) Node {
	h := sha256.New()
	h.Write([]byte(Algorithm + "/key\x00"))
	h.Write(RoutingKey(key))
	var input [64]byte
	h.Sum(input[:0])
	var highest [32]byte
	best := 0
	for i, n := range t.nodes {
		copy(input[32:], n.digest[:])
		score := sha256.Sum256(input[:])
		// The sorted node order breaks score ties by the smallest stable ID.
		if i == 0 || bytes.Compare(score[:], highest[:]) > 0 {
			best, highest = i, score
		}
	}
	return t.nodes[best].node
}

func (t *Topology) Config() Config {
	c := Config{Algorithm: Algorithm, Revision: t.revision, Nodes: make([]Node, len(t.nodes))}
	for i, n := range t.nodes {
		c.Nodes[i] = n.node
	}
	return c
}

func (t *Topology) Revision() uint64 { return t.revision }

func (t *Topology) Equal(other *Topology) bool {
	if t == nil || other == nil || t.revision != other.revision || len(t.nodes) != len(other.nodes) {
		return false
	}
	for i := range t.nodes {
		if t.nodes[i].node != other.nodes[i].node {
			return false
		}
	}
	return true
}

// MarshalJSON returns a canonical snapshot, independent of input node order.
func (t *Topology) MarshalJSON() ([]byte, error) { return json.Marshal(t.Config()) }

// Decode validates a bounded snapshot. It does not persist or activate it.
func Decode(r io.Reader) (*Topology, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxSnapshotBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxSnapshotBytes {
		return nil, errors.New("topology snapshot exceeds 1MiB")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var c Config
	if err := d.Decode(&c); err != nil {
		return nil, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("expected one topology snapshot")
	}
	return New(c)
}
