package store

import (
	"errors"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func primaryGeneration(t *testing.T, d *DB, group string) uint64 {
	t.Helper()
	var n uint64
	if err := d.sql.QueryRow(`SELECT generation FROM group_primary_generations WHERE grp=?`, group).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReservedNodeHealthPreservesPrimaryGeneration(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	if _, err := d.CreateColdCluster(spec); err != nil {
		t.Fatal(err)
	}
	before := primaryGeneration(t, d, "a")
	if err := d.WithNode(func(n *apitypes.NodeState) error {
		n.Servers["node"].Status = "healthy"
		n.Servers["node"].LastHeartbeat = "2026-09-20T12:00:00Z"
		return nil
	}); err != nil {
		t.Fatal("reserved node health blocked", err)
	}
	n, err := d.GetServer("node")
	if err != nil || n.Status != "healthy" || n.LastHeartbeat == "" {
		t.Fatal(n, err)
	}
	if after := primaryGeneration(t, d, "a"); after != before {
		t.Fatal("health changed primary generation", before, after)
	}
	if err := d.WithServers(func(n map[string]*apitypes.NodeServer) error {
		delete(n, "node")
		return nil
	}); err == nil {
		t.Fatal("reserved node deletion allowed")
	}
}

func TestPrimaryGenerationTransactional(t *testing.T) {
	d := openTestDB(t)
	g := &apitypes.InstanceGroupState{Type: "replication", Engine: "redis", CurrentMaster: "a"}
	if err := d.UpsertGroup("g", g); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n != 1 {
		t.Fatal(n)
	}
	for i := 0; i < 2; i++ {
		if err := d.UpsertGroup("g", g); err != nil {
			t.Fatal(err)
		}
	}
	if n := primaryGeneration(t, d, "g"); n != 1 {
		t.Fatal("heartbeat changed generation", n)
	}
	if err := d.WithInstances(func(s *apitypes.InstancesState) error { s.Groups["g"].CurrentMaster = "b"; return nil }); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n != 2 {
		t.Fatal(n)
	}
	tx, err := d.sql.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE instance_groups SET current_master='a' WHERE name='g'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n != 2 {
		t.Fatal("rollback advanced generation", n)
	}
	if err := d.UpsertGroup("g", g); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n != 3 {
		t.Fatal("old master return reused generation", n)
	}
	if err := d.DeleteGroup("g"); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertGroup("g", g); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n <= 3 {
		t.Fatal("group reuse lost tombstone", n)
	}
}

func TestPrimaryGenerationRestartAndEndpoint(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "10.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	if err = d.UpsertGroup("g", &apitypes.InstanceGroupState{CurrentMaster: "a"}); err != nil {
		t.Fatal(err)
	}
	i := &apitypes.Instance{Group: "g", Server: "node", Port: 6379}
	if err = d.UpsertInstance("a", i); err != nil {
		t.Fatal(err)
	}
	i.Port++
	if err = d.UpsertInstance("a", i); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n != 2 {
		t.Fatal(n)
	}
	if err = d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "10.0.0.2"}); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n != 3 {
		t.Fatal(n)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if n := primaryGeneration(t, d, "g"); n != 3 {
		t.Fatal("restart reset generation", n)
	}
}

func TestColdAssignmentPrimaries(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	if _, err := d.CreateColdCluster(spec); err != nil {
		t.Fatal(err)
	}
	c, err := d.ColdAssignment("gateway", "cluster")
	if err != nil || len(c.Primaries) != 2 {
		t.Fatal(c, err)
	}
	for _, p := range c.Primaries {
		if p.Generation != 1 || p.Group != p.Instance || p.ShardID != p.Instance {
			t.Fatal(p)
		}
	}
	if _, err := d.ColdAssignment("other", "cluster"); !errors.Is(err, ErrColdConflict) {
		t.Fatal(err)
	}
	if _, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "expand", ExpectedRevision: 1, Shards: append(spec.Shards, apitypes.ColdShard{ID: "c", Instance: "c"})}); err != nil {
		t.Fatal(err)
	}
	c, err = d.ColdAssignment("gateway", "cluster")
	if err != nil || len(c.Primaries) != 3 {
		t.Fatal("target primary absent", c, err)
	}
	// Read-only control projections never persist as another source of truth.
	v, err := d.GetColdCluster("cluster")
	if err != nil || len(v.Primaries) != 0 {
		t.Fatal(v, err)
	}
}

func TestPrimaryGenerationDeletionAndOverflow(t *testing.T) {
	d := openTestDB(t)
	g := &apitypes.InstanceGroupState{Type: "replication", Engine: "redis", CurrentMaster: "a"}
	if err := d.UpsertGroup("g", g); err != nil {
		t.Fatal(err)
	}
	i := &apitypes.Instance{Group: "g", Server: "node", Port: 6379}
	if err := d.UpsertInstance("a", i); err != nil {
		t.Fatal(err)
	}
	before := primaryGeneration(t, d, "g")
	if err := d.DeleteInstance("a"); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertInstance("a", i); err != nil {
		t.Fatal(err)
	}
	if n := primaryGeneration(t, d, "g"); n <= before {
		t.Fatal("instance recreation reused version", n)
	}
	if _, err := d.sql.Exec(`UPDATE group_primary_generations SET generation=9223372036854775807 WHERE grp='g'`); err != nil {
		t.Fatal(err)
	}
	g.CurrentMaster = "b"
	if err := d.UpsertGroup("g", g); err == nil {
		t.Fatal("version overflow accepted")
	}
	master, version, err := d.GroupPrimaryVersion("g")
	if err != nil || master != "a" || version != 9223372036854775807 {
		t.Fatal("overflow changed topology", master, version, err)
	}
}
