package sharding

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
)

func testTopology(t testing.TB, count int, revision uint64) *Topology {
	t.Helper()
	c := Config{Algorithm: Algorithm, Revision: revision}
	for i := 0; i < count; i++ {
		c.Nodes = append(c.Nodes, Node{ID: fmt.Sprintf("node-%03d", i), Address: fmt.Sprintf("127.0.0.1:%d", 16000+i)})
	}
	x, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func TestRoutingKey(t *testing.T) {
	for _, c := range []struct{ key, want string }{
		{"user:{100}:a", "100"}, {"user:{100}:b", "100"}, {"plain", "plain"}, {"", ""},
		{"{}:{other}", "{}:{other}"}, {"a{unfinished", "a{unfinished"}, {"a{{x}}", "{x"},
		{"x}a{b}c{d}", "b"}, {"a{\x00\xff}b", "\x00\xff"},
	} {
		if got := string(RoutingKey([]byte(c.key))); got != c.want {
			t.Fatalf("%q: got %q, want %q", c.key, got, c.want)
		}
	}
}

func TestTopologyValidation(t *testing.T) {
	base := testTopology(t, 3, 1).Config()
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Algorithm = "" },
		func(c *Config) { c.Algorithm = "hrw-v2" },
		func(c *Config) { c.Revision = 0 },
		func(c *Config) { c.Nodes = nil },
		func(c *Config) { c.Nodes = make([]Node, MaxNodes+1) },
		func(c *Config) { c.Nodes[0].ID = "" },
		func(c *Config) { c.Nodes[0].ID = "a\x00b" },
		func(c *Config) { c.Nodes[0].ID = strings.Repeat("x", 129) },
		func(c *Config) { c.Nodes[0].ID = c.Nodes[1].ID },
		func(c *Config) { c.Nodes[0].Address = c.Nodes[1].Address },
		func(c *Config) { c.Nodes[0].Address = "[::ffff:127.0.0.1]:16001" },
		func(c *Config) { c.Nodes[0].Address = "localhost:6379" },
		func(c *Config) { c.Nodes[0].Address = "0.0.0.0:6379" },
		func(c *Config) { c.Nodes[0].Address = "[::]:6379" },
		func(c *Config) { c.Nodes[0].Address = "224.0.0.1:6379" },
		func(c *Config) { c.Nodes[0].Address = "255.255.255.255:6379" },
		func(c *Config) { c.Nodes[0].Address = "[::ffff:224.0.0.1]:6379" },
		func(c *Config) { c.Nodes[0].Address = "[fe80::1%en0]:6379" },
		func(c *Config) { c.Nodes[0].Address = "127.0.0.1:0" },
		func(c *Config) { c.Nodes[0].Address = "127.0.0.1:65536" },
	} {
		c := base
		c.Nodes = append([]Node(nil), base.Nodes...)
		mutate(&c)
		if _, err := New(c); err == nil {
			t.Fatalf("invalid configuration accepted: %+v", c)
		}
	}
	for _, addr := range []string{"10.0.0.1:6379", "[2001:db8::1]:6379", "[::1]:6379"} {
		if _, err := New(Config{Algorithm, 1, []Node{{"node", addr}}}); err != nil {
			t.Fatalf("valid metadata address %q: %v", addr, err)
		}
	}
}

func TestTopologyImmutableAndRoundTrip(t *testing.T) {
	original := testTopology(t, 5, 1)
	c := original.Config()
	for i, j := 0, len(c.Nodes)-1; i < j; i, j = i+1, j-1 {
		c.Nodes[i], c.Nodes[j] = c.Nodes[j], c.Nodes[i]
	}
	reordered, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Nodes[0].ID = "mutated-input"
	x := reordered.Config()
	x.Nodes[0].ID = "mutated-output"
	a, _ := json.Marshal(original)
	b, _ := json.Marshal(reordered)
	if !bytes.Equal(a, b) || !original.Equal(reordered) {
		t.Fatal("node order or caller mutation changed snapshot")
	}
	restored, err := Decode(bytes.NewReader(b))
	if err != nil || !original.Equal(restored) {
		t.Fatalf("snapshot restore: %v", err)
	}
	c = original.Config()
	c.Revision = 99
	for i := range c.Nodes {
		c.Nodes[i].Address = fmt.Sprintf("10.0.0.1:%d", 16000+i)
	}
	relocated, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		want := original.Owner(key).ID
		if restored.Owner(key).ID != want || reordered.Owner(key).ID != want || relocated.Owner(key).ID != want {
			t.Fatal("revision, address, order or restore changed key placement")
		}
	}
	for _, s := range []string{string(a) + " {}", string(a) + "garbage", "null", "{}", strings.Replace(string(a), `"revision":1`, `"unknown":1`, 1), strings.Repeat(" ", maxSnapshotBytes+1)} {
		if _, err := Decode(strings.NewReader(s)); err == nil {
			t.Fatal("invalid snapshot accepted")
		}
	}
}

func TestHRWReference(t *testing.T) {
	x := testTopology(t, 4, 1)
	for _, s := range []string{"", "user:100", "a{100}:one", "b{100}:two", "{}:{100}", "\x00\xffkey"} {
		// Independent, allocation-heavy reference specifies the score bytes.
		kd := sha256.Sum256(append([]byte("hrw-sha256-v1/key\x00"), RoutingKey([]byte(s))...))
		best, high := "", ""
		for _, n := range x.Config().Nodes {
			nd := sha256.Sum256([]byte("hrw-sha256-v1/node\x00" + n.ID))
			score := sha256.Sum256(append(append([]byte{}, kd[:]...), nd[:]...))
			value := hex.EncodeToString(score[:])
			if best == "" || value > high {
				best, high = n.ID, value
			}
		}
		if got := x.Owner([]byte(s)).ID; got != best {
			t.Fatalf("%q: %s != reference %s", s, got, best)
		}
	}
	if x.Owner([]byte("a{100}:one")) != x.Owner([]byte("b{100}:two")) {
		t.Fatal("same tag did not colocate")
	}
}

func TestHRWExpansionAndRemoval(t *testing.T) {
	for _, counts := range [][2]int{{3, 4}, {3, 6}, {10, 11}, {10, 20}} {
		t.Run(fmt.Sprintf("%d-to-%d", counts[0], counts[1]), func(t *testing.T) {
			from, to := testTopology(t, counts[0], 1), testTopology(t, counts[1], 2)
			change, err := NewTransition(from, to)
			if err != nil {
				t.Fatal(err)
			}
			const total = 20000
			moved := 0
			perNode := map[string]int{}
			old := map[string]bool{}
			for _, n := range from.Config().Nodes {
				old[n.ID] = true
			}
			for i := 0; i < total; i++ {
				p := change.DesiredPlacement([]byte(fmt.Sprintf("user:%d", i)))
				perNode[p.Target.ID]++
				if p.Moves() {
					moved++
					if old[p.Target.ID] {
						t.Fatal("unnecessary old-to-old movement")
					}
				}
			}
			want := float64(counts[1]-counts[0]) / float64(counts[1])
			if math.Abs(float64(moved)/total-want) > .02 {
				t.Fatalf("moved %d/%d; expected about %.4f", moved, total, want)
			}
			for _, n := range to.Config().Nodes {
				if math.Abs(float64(perNode[n.ID])-float64(total)/float64(counts[1])) > .15*float64(total)/float64(counts[1]) {
					t.Fatalf("unbalanced deterministic sample: %v", perNode)
				}
			}
		})
	}
	from := testTopology(t, 5, 8)
	c := from.Config()
	c.Revision++
	c.Nodes = append(c.Nodes[:1], c.Nodes[2:]...) // Remove a middle ID, not the last.
	to, _ := New(c)
	change, err := NewTransition(from, to)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		p := change.DesiredPlacement([]byte(fmt.Sprint(i)))
		if p.Moves() != (p.Source.ID == "node-001") {
			t.Fatal("removal changed a retained winner")
		}
	}
	sources := change.ScanSources()
	if len(sources) != 1 || sources[0].ID != "node-001" {
		t.Fatal(sources)
	}
	sources[0].ID = "mutated"
	if change.ScanSources()[0].ID != "node-001" {
		t.Fatal("mutable scan sources")
	}
}

func TestTransitionValidation(t *testing.T) {
	from := testTopology(t, 3, 1)
	for _, c := range []Config{
		from.Config(),
		testTopology(t, 4, 1).Config(),
		testTopology(t, 3, 2).Config(),
	} {
		to, _ := New(c)
		if _, err := NewTransition(from, to); err == nil {
			t.Fatal("invalid transition accepted")
		}
	}
	c := testTopology(t, 4, 2).Config()
	c.Nodes[0].Address = "10.0.0.1:9999"
	to, _ := New(c)
	if _, err := NewTransition(from, to); err == nil {
		t.Fatal("address change accepted as migration")
	}
	if _, err := NewTransition(nil, from); err == nil {
		t.Fatal("nil source accepted")
	}
	if _, err := NewTransition(&Topology{}, from); err == nil {
		t.Fatal("zero source accepted")
	}
	c = from.Config()
	c.Revision++
	c.Nodes[1].ID = "replacement-with-same-data"
	to, _ = New(c)
	if _, err := NewTransition(from, to); err == nil {
		t.Fatal("source reused as a supposedly new target")
	}
	c = testTopology(t, 4, 2).Config()
	c.Nodes = c.Nodes[1:] // mixed addition/removal
	to, _ = New(c)
	change, err := NewTransition(from, to)
	if err != nil || len(change.ScanSources()) != 3 {
		t.Fatalf("mixed change: %v", err)
	}
}

func TestTopologyConcurrentAndAllocations(t *testing.T) {
	x := testTopology(t, 10, 1)
	key := []byte("user:{100}:profile")
	want := x.Owner(key)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if x.Owner(key) != want {
					t.Error("unstable lookup")
				}
			}
		}()
	}
	wg.Wait()
	if n := testing.AllocsPerRun(100, func() { x.Owner(key) }); n != 0 {
		t.Fatalf("lookup allocated %.0f objects", n)
	}
}

func BenchmarkHRWOwner(b *testing.B) {
	for _, count := range []int{3, 10, 32, 100} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			x := testTopology(b, count, 1)
			key := []byte("user:{100}:profile")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Owner(key)
			}
		})
	}
}
