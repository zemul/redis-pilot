package gateway

import (
	"context"
	"net"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func attachNativeReplica(t *testing.T, cfg Config, primary, replica string) {
	t.Helper()
	if cfg.Cache == nil || cfg.Cache.coldEngine() == "redis" {
		configuredBackendCall(t, cfg, primary, "CONFIG", "SET", "repl-diskless-sync-delay", "0")
	}
	if b := configuredBackendCall(t, cfg, replica, "CONFIG", "SET", "masterauth", "g4-backend-test"); string(b) != "+OK\r\n" {
		t.Fatal("masterauth", string(b))
	}
	host, port, _ := net.SplitHostPort(primary)
	for deadline := time.Now().Add(10 * time.Second); ; {
		b := configuredBackendCall(t, cfg, replica, "SLAVEOF", host, port)
		if string(b) == "+OK\r\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica setup", string(b))
		}
		time.Sleep(50 * time.Millisecond)
	}
	conn, reader, err := authenticated(primary, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &cacheConn{Conn: conn, reader: reader, cfg: cfg}
	for deadline := time.Now().Add(10 * time.Second); ; {
		// Establish a write offset on the SAME connection used by WAIT.
		cacheCall(t, c, "SET", cachePrefix+"replica-test", "ready")
		if waitMigrationReplicas(context.Background(), c, 1) == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("replica did not synchronize", string(configuredBackendCall(t, cfg, replica, "INFO", "replication")), string(configuredBackendCall(t, cfg, primary, "INFO", "replication")))
		}
	}
}

func TestMigrationReplicaAckBeforeSourceRemoval(t *testing.T) {
	m := newMigrationFixture(t)
	h, a, b := m.key(t, "replica-barrier")
	cacheCall(t, m.a, "HSET", string(h.Key), "field", "value")
	b.replicas = 1
	attempt, err := copyMigrationKey(context.Background(), m.j, h, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if err = finishMigrationKey(context.Background(), m.j, h, a, b, attempt); err == nil {
		t.Fatal("missing replica accepted")
	}
	current, _, err := m.j.Lookup(context.Background(), h.Key)
	if err != nil || current.State != sharding.Prepared {
		t.Fatal("handoff committed without replica", current, err)
	}
	if got := cacheCall(t, m.a, "HGET", string(h.Key), "field"); string(got) != "$5\r\nvalue\r\n" {
		t.Fatal("source removed", string(got))
	}
	replica := isolatedTestRedis(t, true)
	attachNativeReplica(t, m.b.cfg, h.Placement.Target.Address, replica)
	if err = finishMigrationKey(context.Background(), m.j, h, a, b, attempt); err != nil {
		t.Fatal(err)
	}
	current, _, err = m.j.Lookup(context.Background(), h.Key)
	if err != nil || current.State != sharding.Cleaned {
		t.Fatal(current, err)
	}
	if got := configuredBackendCall(t, m.b.cfg, replica, "HGET", string(h.Key), "field"); string(got) != "$5\r\nvalue\r\n" {
		t.Fatal("replica missing complete object", string(got))
	}
}

func TestMigrationKvrocksReplicaBarrier(t *testing.T) {
	replica, _, _ := isolatedKvrocks(t)
	m := newMigrationFixture(t)
	h, a, b := m.key(t, "kvrocks-replica-barrier")
	cacheCall(t, m.a, "HSET", string(h.Key), "field", "value")
	b.replicas = 1
	attempt, err := copyMigrationKey(context.Background(), m.j, h, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if err = finishMigrationKey(context.Background(), m.j, h, a, b, attempt); err == nil {
		t.Fatal("missing Kvrocks replica accepted")
	}
	configuredBackendCall(t, m.b.cfg, replica, "CONFIG", "SET", "masterauth", "g4-backend-test")
	host, port, _ := net.SplitHostPort(h.Placement.Target.Address)
	configuredBackendCall(t, m.b.cfg, replica, "SLAVEOF", host, port)
	for deadline := time.Now().Add(20 * time.Second); ; {
		err = finishMigrationKey(context.Background(), m.j, h, a, b, attempt)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Kvrocks replication confirmation failed", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := configuredBackendCall(t, m.b.cfg, replica, "HGET", string(h.Key), "field"); string(got) != "$5\r\nvalue\r\n" {
		t.Fatal(string(got))
	}
}

func TestShardPrimaryDuringMigrationAndRestart(t *testing.T) {
	for _, stage := range []string{"prepared", "committed", "cleaned"} {
		t.Run(stage, func(t *testing.T) { testPrimaryMigrationAndRestart(t, "redis", stage) })
	}
}

func TestShardPrimaryKvrocksDuringMigrationAndRestart(t *testing.T) {
	for _, stage := range []string{"prepared", "committed", "cleaned"} {
		t.Run(stage, func(t *testing.T) { testPrimaryMigrationAndRestart(t, "kvrocks", stage) })
	}
}

func testPrimaryMigrationAndRestart(t *testing.T, engine, stage string) {
	t.Helper()
	ctx := context.Background()
	s, assignment, aReplica := primaryEngineTestRuntime(t, engine)
	bReplica := primaryTestBackend(t, engine)
	for i, replica := range []string{aReplica, bReplica} {
		attachNativeReplica(t, s.cfg, assignment.Primaries[i].Address, replica)
		assignment.Primaries[i].Replicas = 1
		assignment.Primaries[i].Generation++
	}
	if err := s.reconcilePrimaries(ctx, assignment); err != nil {
		t.Fatal(err)
	}
	key := primaryKey(t, s, "a")
	if b := s.cache.shards.execute(ctx, cacheArgs("HSET", key, "field", "before")); string(b) != ":1\r\n" {
		t.Fatal(string(b))
	}
	from := s.cache.shards.topology
	to, err := sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 2, Nodes: []sharding.Node{from.Config().Nodes[1]}})
	if err != nil {
		t.Fatal(err)
	}
	path, err := s.cache.shardState.taskPath("ha-shrink")
	if err != nil {
		t.Fatal(err)
	}
	j, err := sharding.CreateJournal(ctx, path, "ha-shrink", from, to)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	if err = s.cache.shardState.bindTask(ctx, "ha-shrink", to); err != nil {
		t.Fatal(err)
	}
	if err = s.cache.shards.beginTransition(to, s.cache.shardState.journal); err != nil {
		t.Fatal(err)
	}
	if stage == "cleaned" {
		if err = s.cache.shards.migrate(ctx, []byte(key)); err != nil {
			t.Fatal(err)
		}
	} else {
		stagePrimaryHandoff(t, s, key, stage == "committed")
	}
	old := assignment.Primaries[1].Address
	configuredBackendCall(t, s.cfg, bReplica, "REPLICAOF", "NO", "ONE")
	assignment.Primaries[1].Address, assignment.Primaries[1].Instance = bReplica, "b-promoted"
	assignment.Primaries[1].Generation++
	if err = s.reconcilePrimaries(ctx, assignment); err != nil {
		t.Fatal("migration switch refused", err)
	}
	if stage != "cleaned" {
		attachNativeReplica(t, s.cfg, bReplica, old)
	}
	if b := s.cache.shards.execute(ctx, cacheArgs("HSET", key, "field", "after")); string(b) != ":0\r\n" {
		t.Fatal(string(b))
	}
	if stage == "cleaned" {
		attachNativeReplica(t, s.cfg, bReplica, old)
	}
	s.cache.close()
	r, err := NewService(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.cache.start(ctx); err != nil {
		t.Fatal(err)
	}
	defer r.cache.close()
	if b := r.cache.shards.execute(ctx, cacheArgs("HGET", key, "field")); string(b) != "$5\r\nafter\r\n" {
		t.Fatal("restart lost routing", string(b))
	}
	options := fastWorkerOptions()
	options.Finalize = true
	stop := startTestMigrationWorker(t, &shardRuntimeFixture{s: r}, options)
	defer stop()
	for deadline := time.Now().Add(15 * time.Second); ; {
		phase, err := r.cache.shardState.journal.FinalPhase(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if phase == sharding.FinalComplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration failed to finish after election", phase)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if b := r.cache.shards.execute(ctx, cacheArgs("HGET", key, "field")); string(b) != "$5\r\nafter\r\n" {
		t.Fatal(string(b))
	}
}

func stagePrimaryHandoff(t *testing.T, s *Service, key string, commit bool) {
	t.Helper()
	ctx := context.Background()
	shards, j := s.cache.shards, s.cache.shardState.journal
	h, err := j.Prepare(ctx, []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	nodes := []*cacheShardNode{shards.node(h.Placement.Source.ID), shards.node(h.Placement.Target.ID)}
	var fences []*migrationFence
	for _, n := range nodes {
		token, err := shards.ensure(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		c, err := shards.raw(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		f, err := newMigrationFence(c, n.node, s.cache.guard, token, h)
		if err != nil {
			t.Fatal(err)
		}
		f.address, f.replicas = n.backendAddress(), n.replicas
		fences = append(fences, f)
	}
	attempt, err := copyMigrationKey(ctx, j, h, fences[0], fences[1])
	if err != nil {
		t.Fatal(err)
	}
	if commit {
		if err = fences[0].replicated(ctx, "frozen", ""); err != nil {
			t.Fatal(err)
		}
		if err = fences[1].replicated(ctx, "ready", attempt); err != nil {
			t.Fatal(err)
		}
		if err = j.Commit(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
}
