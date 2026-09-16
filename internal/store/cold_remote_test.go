package store

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestColdRemotePlacement(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	for name, ip := range map[string]string{"hot-host": "10.70.100.63", "cold-host": "10.70.100.68"} {
		if err := d.UpsertServer(name, &apitypes.NodeServer{Endpoint: ip}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.WithInstances(func(s *apitypes.InstancesState) error {
		s.Instances["hot"].Server = "hot-host"
		for _, n := range []string{"a", "b", "c"} {
			s.Instances[n].Server = "cold-host"
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c, err := d.CreateColdCluster(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.HotAddress != "10.70.100.63:17000" || c.Shards[0].Address != "10.70.100.68:17001" {
		t.Fatalf("wrong placement %+v", c)
	}
	for _, name := range []string{"node", "hot-host", "cold-host"} {
		if d.DeleteServer(name) == nil {
			t.Fatal("deleted reserved node", name)
		}
		if d.UpsertServer(name, &apitypes.NodeServer{Endpoint: "10.70.100.99"}) == nil {
			t.Fatal("changed reserved node", name)
		}
	}
	task, err := d.CreateColdTask(c.ID, apitypes.ColdTaskRequest{ID: "expand", ExpectedRevision: 1, Shards: append(spec.Shards, apitypes.ColdShard{ID: "c", Instance: "c"})})
	if err != nil || task.Target[2].Address != "10.70.100.68:17003" {
		t.Fatalf("remote expansion: %+v %v", task, err)
	}
}

func TestColdRejectUnsafeRemoteEndpoints(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "0.0.0.0", "public.example", "8.8.8.8", "fe80::1%eth0"} {
		t.Run(ip, func(t *testing.T) {
			d := openTestDB(t)
			spec := coldFixture(t, d)
			if err := d.UpsertServer("remote", &apitypes.NodeServer{Endpoint: ip}); err != nil {
				t.Fatal(err)
			}
			if err := d.WithInstances(func(s *apitypes.InstancesState) error { s.Instances["a"].Server = "remote"; return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := d.CreateColdCluster(spec); err == nil {
				t.Fatal("accepted remote", ip)
			}
		})
	}
}
