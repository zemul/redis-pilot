package store

import (
	"errors"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func coldFixture(t *testing.T, d *DB) apitypes.ColdClusterSpec {
	t.Helper()
	if err := d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"hot", "a", "b", "c"} {
		engine := "kvrocks"
		if name == "hot" {
			engine = "redis"
		}
		if err := d.UpsertGroup(name, &apitypes.InstanceGroupState{Engine: engine, Type: "standalone", CurrentMaster: name}); err != nil {
			t.Fatal(err)
		}
		if err := d.UpsertInstance(name, &apitypes.Instance{Group: name, Server: "node", Role: "master", Port: 17000 + i, DesiredState: "running", ObservedStatus: "running"}); err != nil {
			t.Fatal(err)
		}
	}
	return apitypes.ColdClusterSpec{ID: "cluster", GatewayID: "gateway", NodeID: "node", HotInstance: "hot", Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}, {ID: "b", Instance: "b"}}}
}

func TestColdClusterTaskAndRemovalBarrier(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	c, err := d.CreateColdCluster(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.Revision != 1 || c.Shards[0].Address != "127.0.0.1:17001" {
		t.Fatalf("bad registration %+v", c)
	}
	if _, err := d.CreateColdCluster(spec); !errors.Is(err, ErrColdConflict) {
		t.Fatal(err)
	}
	if err := d.DeleteInstance("b"); err == nil {
		t.Fatal("deleted active shard")
	}
	if err := d.WithInstances(func(st *apitypes.InstancesState) error {
		st.Instances["b"].Lock = &apitypes.Lock{HeldBy: "user", Operation: "stop"}
		return nil
	}); err == nil {
		t.Fatal("stop reached agent barrier")
	}
	if err := d.WithInstances(func(st *apitypes.InstancesState) error { st.Instances["b"].ObservedStatus = "unhealthy"; return nil }); err != nil {
		t.Fatal("health observation blocked", err)
	}
	req := apitypes.ColdTaskRequest{ID: "shrink", ExpectedRevision: 1, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}}
	task, err := d.CreateColdTask("cluster", req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.CreateColdTask("cluster", req); err != nil {
		t.Fatal("retry failed", err)
	}
	other := req
	other.ID = "other"
	if _, err := d.CreateColdTask("cluster", other); !errors.Is(err, ErrColdConflict) {
		t.Fatal("overlap", err)
	}
	r := apitypes.ColdTaskReport{TaskID: task.ID, ToRevision: 2, Phase: "published", Pages: 3, Attempts: 4}
	if err := d.ReportColdTask("other", "cluster", r); !errors.Is(err, ErrColdConflict) {
		t.Fatal("wrong Gateway", err)
	}
	if err := d.ReportColdTask("gateway", "cluster", r); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteInstance("b"); err == nil {
		t.Fatal("publication released source before cleanup")
	}
	r.Phase = "active"
	if err := d.ReportColdTask("gateway", "cluster", r); err != nil {
		t.Fatal(err)
	}
	c, _ = d.GetColdCluster("cluster")
	if c.Task.Phase != "published" {
		t.Fatal("late report rolled back phase")
	}
	r.Phase = "complete"
	if err := d.ReportColdTask("gateway", "cluster", r); err != nil {
		t.Fatal(err)
	}
	if err := d.ReportColdTask("gateway", "cluster", r); err != nil {
		t.Fatal("completion retry", err)
	}
	if err := d.DeleteInstance("b"); err != nil {
		t.Fatal("completed source retained", err)
	}
	if _, err := d.CreateColdTask("cluster", req); err != nil {
		t.Fatal("historical retry after source removal", err)
	}
	c, _ = d.GetColdCluster("cluster")
	if c.Revision != 2 || len(c.Shards) != 1 {
		t.Fatalf("not committed %+v", c)
	}
	if err := d.DeleteInstance("a"); err == nil {
		t.Fatal("current cold lost reservation")
	}
	if err := d.DeleteInstance("hot"); err == nil {
		t.Fatal("hot lost reservation")
	}
	if _, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: "expand", ExpectedRevision: 2, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}, {ID: "c", Instance: "c"}}}); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteInstance("c"); err == nil {
		t.Fatal("pending target not reserved")
	}
}

func TestColdClusterGuards(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	if _, err := d.CreateColdCluster(spec); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*apitypes.InstancesState){
		"delete": func(s *apitypes.InstancesState) { delete(s.Instances, "a") },
		"port":   func(s *apitypes.InstancesState) { s.Instances["a"].Port++ },
		"role":   func(s *apitypes.InstancesState) { s.Instances["a"].Role = "replica" },
		"stop":   func(s *apitypes.InstancesState) { s.Instances["a"].DesiredState = "stopped" },
		"config": func(s *apitypes.InstancesState) {
			s.Instances["a"].ConfigOverrides = map[string]string{"rocksdb.write_options.sync": "no"}
		},
		"proxy":   func(s *apitypes.InstancesState) { s.Groups["a"].Proxy = &apitypes.ProxyConfig{MasterPort: 18000} },
		"replica": func(s *apitypes.InstancesState) { s.Instances["new"] = &apitypes.Instance{Group: "a", Role: "replica"} },
		"join":    func(s *apitypes.InstancesState) { s.Instances["c"].Group = "a" },
	} {
		t.Run(name, func(t *testing.T) {
			if err := d.WithInstances(func(st *apitypes.InstancesState) error { mutate(st); return nil }); err == nil {
				t.Fatal("protected mutation accepted")
			}
		})
	}
	if err := d.DeleteGroup("a"); err == nil {
		t.Fatal("group deleted")
	}
	if err := d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "10.0.0.1"}); err == nil {
		t.Fatal("host changed")
	}
	spec.ID = "other"
	spec.GatewayID = "other"
	if _, err := d.CreateColdCluster(spec); err == nil {
		t.Fatal("backend reused")
	}
}

func TestColdClusterConcurrentTasks(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	if _, err := d.CreateColdCluster(spec); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	out := make(chan error, 2)
	for _, id := range []string{"one", "two"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, err := d.CreateColdTask("cluster", apitypes.ColdTaskRequest{ID: id, ExpectedRevision: 1, Shards: []apitypes.ColdShard{{ID: "a", Instance: "a"}}})
			out <- err
		}(id)
	}
	wg.Wait()
	close(out)
	success, conflict := 0, 0
	for err := range out {
		if err == nil {
			success++
		} else if errors.Is(err, ErrColdConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("%d %d", success, conflict)
	}
}

func TestColdClusterBadReferences(t *testing.T) {
	for _, kind := range []string{"missing", "hot-as-cold", "duplicate", "address", "remote", "direct-proxy", "replica"} {
		t.Run(kind, func(t *testing.T) {
			d := openTestDB(t)
			spec := coldFixture(t, d)
			switch kind {
			case "missing":
				spec.Shards[0].Instance = "missing"
			case "hot-as-cold":
				spec.Shards[0].Instance = "hot"
			case "duplicate":
				spec.Shards[1] = spec.Shards[0]
			case "address":
				spec.Shards[0].Address = "127.0.0.1:1"
			case "remote":
				spec.NodeID = "elsewhere"
			case "direct-proxy":
				d.WithInstances(func(s *apitypes.InstancesState) error {
					s.Groups["a"].Proxy = &apitypes.ProxyConfig{MasterPort: 19000}
					return nil
				})
			case "replica":
				d.WithInstances(func(s *apitypes.InstancesState) error { s.Groups["a"].Type = "replication"; return nil })
			}
			if _, err := d.CreateColdCluster(spec); !errors.Is(err, ErrColdInvalid) {
				t.Fatal(err)
			}
		})
	}
}
