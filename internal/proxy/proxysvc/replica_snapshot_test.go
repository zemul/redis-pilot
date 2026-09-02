package proxysvc

import (
	"sync"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/backend"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/discovery"
)

// newTestGroupPools 造一个带若干 replica 池的 groupPools 并登记到 p.pools。
// backend.NewPool 不主动拨号（仅起 evictor goroutine），测试内不涉及网络。
func newTestGroupPools(p *Proxy, group string, addrs []string) *groupPools {
	pools := &groupPools{
		master:   backend.NewPool("10.0.0.1:6379", "", time.Second, 1, time.Minute),
		replicas: make(map[string]*backend.Pool),
	}
	for _, a := range addrs {
		pools.replicas[a] = backend.NewPool(a, "", time.Second, 1, time.Minute)
	}
	p.poolsMu.Lock()
	p.pools[group] = pools
	p.poolsMu.Unlock()
	return pools
}

// TestSnapshotReplicasIsolation 是核心契约：session 拿到的必须是独立副本，
// 而不是与 updateReplicaPools 共享同一 map。若有人把 snapshotReplicas 改回
// 直接返回 pools.replicas（共享），本测试立即失败。
func TestSnapshotReplicasIsolation(t *testing.T) {
	p := New(Config{})
	pools := newTestGroupPools(p, "g", []string{"10.0.0.2:6379", "10.0.0.3:6379"})

	snap := p.snapshotReplicas(pools)
	if len(snap) != 2 {
		t.Fatalf("snapshot size = %d, want 2", len(snap))
	}

	// 必须是不同的 map 容器（否则并发读写共享 map 会 fatal）。
	// 拓扑更新后：共享 map 变，已取的快照不变。
	p.updateReplicaPools("g", &discovery.GroupTopology{ReplicaAddrs: []string{"10.0.0.2:6379"}})

	p.poolsMu.Lock()
	sharedLen := len(pools.replicas)
	p.poolsMu.Unlock()
	if sharedLen != 1 {
		t.Fatalf("after update shared replicas = %d, want 1", sharedLen)
	}
	if len(snap) != 2 {
		t.Fatalf("snapshot mutated by topology update: len = %d, want 2 (must be an independent copy)", len(snap))
	}
	if _, ok := snap["10.0.0.3:6379"]; !ok {
		t.Fatal("snapshot lost removed replica; it must be an independent copy taken at session creation")
	}
}

// TestConcurrentSnapshotAndSessionReads 复刻 session 生命周期：连接时取一次快照，
// 之后整个生命周期持续读它（selectFromReadPool），与拓扑churn（updateReplicaPools
// 的 delete/写）并发。-race 下必须干净——这正是原 fatal（concurrent map read and
// map write）的高发场景（加减从库/failover）。
func TestConcurrentSnapshotAndSessionReads(t *testing.T) {
	p := New(Config{})
	base := []string{"10.0.0.2:6379", "10.0.0.3:6379", "10.0.0.4:6379"}
	pools := newTestGroupPools(p, "g", base)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 写者：不断增删 replica，模拟拓扑变化。
	wg.Add(1)
	go func() {
		defer wg.Done()
		toggle := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			var addrs []string
			if toggle%2 == 0 {
				addrs = []string{"10.0.0.2:6379"}
			} else {
				addrs = base
			}
			p.updateReplicaPools("g", &discovery.GroupTopology{ReplicaAddrs: addrs})
			toggle++
		}
	}()

	// 多个 session：各自取一次快照，然后反复读，直到收到 stop。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap := p.snapshotReplicas(pools) // session.New 时取一次
			for {
				select {
				case <-stop:
					return
				default:
				}
				for addr := range snap {
					_ = snap[addr] // selectFromReadPool 的读路径，不持锁
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}
