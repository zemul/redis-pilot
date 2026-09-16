package gateway

import (
	"fmt"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func TestShardRegistryTransitionUnion(t *testing.T) {
	c := shardConfigForTest(t)
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	config := sharding.Config{Algorithm: sharding.Algorithm, Revision: 1}
	var endpoints []cacheShardEndpoint
	for i := 0; i < 2*sharding.MaxNodes; i++ {
		n := sharding.Node{ID: fmt.Sprintf("n%d", i), Address: fmt.Sprintf("127.0.0.1:%d", 30000+i)}
		if i < sharding.MaxNodes {
			config.Nodes = append(config.Nodes, n)
		}
		endpoints = append(endpoints, cacheShardEndpoint{node: n})
	}
	from, err := sharding.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newCacheShards(s.cache, from, endpoints); err != nil {
		t.Fatal("valid transition union rejected", err)
	}
	endpoints = append(endpoints, endpoints[0])
	if _, err := newCacheShards(s.cache, from, endpoints); err == nil {
		t.Fatal("duplicate endpoint accepted")
	}
}

func TestShardManagedRegistry(t *testing.T) {
	c := shardConfigForTest(t)
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	from, _ := c.Cache.Sharding.registry()
	ep, err := s.cache.shardEndpoints()
	if err != nil {
		t.Fatal(err)
	}
	r, err := newCacheShards(s.cache, from, ep)
	if err != nil {
		t.Fatal(err)
	}
	to, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 2, Nodes: append(from.Config().Nodes, sharding.Node{ID: "new", Address: "127.0.0.1:29101"})})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.node("new")
				r.nodeSnapshot()
				r.requiredNodes()
			}
		}()
	}
	if err := r.registerManaged(to); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !r.topology.Equal(from) || r.node("new") == nil {
		t.Fatal("registry changed placement")
	}
	retained := from.Config().Nodes[0]
	for _, n := range []sharding.Node{{ID: retained.ID, Address: "127.0.0.1:29102"}, {ID: "reuse", Address: retained.Address}, {ID: "remote", Address: "10.0.0.1:6379"}, {ID: "hot", Address: c.RedisAddress}} {
		x, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 3, Nodes: []sharding.Node{n}})
		if err != nil {
			t.Fatal(err)
		}
		if r.registerManaged(x) == nil {
			t.Fatalf("accepted invalid endpoint %+v", n)
		}
	}
	reused, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 3, Nodes: append(from.Config().Nodes, sharding.Node{ID: "replacement", Address: "127.0.0.1:29101"})})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.registerManaged(reused); err != nil {
		t.Fatal(err)
	}
	if r.node("new") != nil || r.node("replacement") == nil {
		t.Fatal("unused registry blocked port reuse")
	}
}
