package discovery

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestParseSnapshot_SingleGroup(t *testing.T) {
	snap := &apitypes.ProxySnapshot{
		Version: "v1",
		Listeners: []apitypes.ProxyListener{
			{
				Name:    "grp1-master",
				Group:   "grp1",
				Mode:    "master",
				Port:    16500,
				Cluster: "grp1-master-cluster",
			},
			{
				Name:        "grp1-auto",
				Group:       "grp1",
				Mode:        "auto",
				Port:        16379,
				Cluster:     "grp1-master-cluster",
				ReadCluster: "grp1-replica-cluster",
				Password:    "secret123",
			},
		},
		Clusters: []apitypes.ProxyCluster{
			{
				Name:     "grp1-master-cluster",
				Password: "secret123",
				Endpoints: []apitypes.ProxyEndpoint{
					{Address: "10.0.0.1", Port: 6379},
				},
			},
			{
				Name: "grp1-replica-cluster",
				Endpoints: []apitypes.ProxyEndpoint{
					{Address: "10.0.0.2", Port: 6379},
					{Address: "10.0.0.3", Port: 6379},
				},
			},
		},
	}

	topo := ParseSnapshot(snap)

	if len(topo.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(topo.Groups))
	}

	g := topo.Groups["grp1"]
	if g == nil {
		t.Fatal("group grp1 not found")
	}

	if g.MasterAddr != "10.0.0.1:6379" {
		t.Errorf("MasterAddr = %q, want 10.0.0.1:6379", g.MasterAddr)
	}
	if len(g.ReplicaAddrs) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(g.ReplicaAddrs))
	}
	if g.ReplicaAddrs[0] != "10.0.0.2:6379" {
		t.Errorf("ReplicaAddrs[0] = %q, want 10.0.0.2:6379", g.ReplicaAddrs[0])
	}
	if g.ReplicaAddrs[1] != "10.0.0.3:6379" {
		t.Errorf("ReplicaAddrs[1] = %q, want 10.0.0.3:6379", g.ReplicaAddrs[1])
	}
	if g.Password != "secret123" {
		t.Errorf("Password = %q, want secret123", g.Password)
	}
	if g.AutoPort != 16379 {
		t.Errorf("AutoPort = %d, want 16379", g.AutoPort)
	}
	if g.MasterPort != 16500 {
		t.Errorf("MasterPort = %d, want 16500", g.MasterPort)
	}
}

func TestManagerApplySnapshot_AppliesValidEmptySnapshot(t *testing.T) {
	m := NewManager(ServerConfig{}, PollConfig{})
	m.topo = &Topology{Version: "v1", Groups: map[string]*GroupTopology{"old": {Name: "old"}}}
	m.version = "v1"
	m.applySnapshot(&apitypes.ProxySnapshot{
		Version: "v2", Listeners: []apitypes.ProxyListener{}, Clusters: []apitypes.ProxyCluster{},
	})
	if len(m.Topology().Groups) != 0 || m.Topology().Version != "v2" {
		t.Fatalf("valid empty snapshot must clear old topology, got %#v", m.Topology())
	}
}

func TestParseSnapshot_MultiGroup(t *testing.T) {
	snap := &apitypes.ProxySnapshot{
		Version: "v2",
		Listeners: []apitypes.ProxyListener{
			{
				Group:   "g1",
				Mode:    "auto",
				Port:    16379,
				Cluster: "g1-master",
			},
			{
				Group:   "g2",
				Mode:    "auto",
				Port:    16380,
				Cluster: "g2-master",
			},
		},
		Clusters: []apitypes.ProxyCluster{
			{
				Name:     "g1-master",
				Password: "pw1",
				Endpoints: []apitypes.ProxyEndpoint{
					{Address: "10.0.0.1", Port: 6379},
				},
			},
			{
				Name: "g2-master",
				Endpoints: []apitypes.ProxyEndpoint{
					{Address: "10.0.0.2", Port: 6379},
				},
			},
		},
	}

	topo := ParseSnapshot(snap)

	if len(topo.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(topo.Groups))
	}

	g1 := topo.Groups["g1"]
	if g1.MasterAddr != "10.0.0.1:6379" {
		t.Errorf("g1 MasterAddr = %q", g1.MasterAddr)
	}
	if g1.Password != "pw1" {
		t.Errorf("g1 Password = %q, want pw1", g1.Password)
	}

	g2 := topo.Groups["g2"]
	if g2.MasterAddr != "10.0.0.2:6379" {
		t.Errorf("g2 MasterAddr = %q", g2.MasterAddr)
	}
	if g2.Password != "" {
		t.Errorf("g2 Password = %q, want empty", g2.Password)
	}
}

func TestParseSnapshot_NoReplicas(t *testing.T) {
	snap := &apitypes.ProxySnapshot{
		Version: "v3",
		Listeners: []apitypes.ProxyListener{
			{
				Group:   "solo",
				Mode:    "master",
				Port:    16500,
				Cluster: "solo-cluster",
			},
		},
		Clusters: []apitypes.ProxyCluster{
			{
				Name: "solo-cluster",
				Endpoints: []apitypes.ProxyEndpoint{
					{Address: "10.0.0.1", Port: 6379},
				},
			},
		},
	}

	topo := ParseSnapshot(snap)

	g := topo.Groups["solo"]
	if g == nil {
		t.Fatal("group solo not found")
	}
	if g.MasterAddr != "10.0.0.1:6379" {
		t.Errorf("MasterAddr = %q", g.MasterAddr)
	}
	if len(g.ReplicaAddrs) != 0 {
		t.Errorf("expected 0 replicas, got %d", len(g.ReplicaAddrs))
	}
	if g.MasterPort != 16500 {
		t.Errorf("MasterPort = %d, want 16500", g.MasterPort)
	}
	if g.AutoPort != 0 {
		t.Errorf("AutoPort = %d, want 0", g.AutoPort)
	}
}

func TestMasterChanged(t *testing.T) {
	old := &GroupTopology{MasterAddr: "10.0.0.1:6379"}
	new := &GroupTopology{MasterAddr: "10.0.0.2:6379"}

	if !new.MasterChanged(old) {
		t.Error("expected master changed")
	}

	same := &GroupTopology{MasterAddr: "10.0.0.1:6379"}
	if same.MasterChanged(old) {
		t.Error("expected master unchanged")
	}

	// nil old = new group
	if !new.MasterChanged(nil) {
		t.Error("expected master changed when old is nil (new group)")
	}
}
