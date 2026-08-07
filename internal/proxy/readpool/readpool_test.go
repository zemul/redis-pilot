package readpool

import (
	"testing"
)

func TestSelect_WeightDistribution(t *testing.T) {
	p := New([]Entry{
		{Addr: "master", Weight: 0, Role: RoleMaster},
		{Addr: "r1", Weight: 1, Role: RoleReplica},
		{Addr: "r2", Weight: 1, Role: RoleReplica},
	})

	counts := map[string]int{}
	const N = 30000
	for i := 0; i < N; i++ {
		e := p.Select()
		counts[e.Addr]++
	}

	// 两个 replica weight 相同，各约 50%
	for _, addr := range []string{"r1", "r2"} {
		c := counts[addr]
		if c < N*40/100 || c > N*60/100 {
			t.Errorf("addr %s count %d, expected ~%d (±10%%)", addr, c, N/2)
		}
	}
	// master weight=0，不应被选中
	if counts["master"] > 0 {
		t.Errorf("master with weight=0 was selected %d times", counts["master"])
	}
}

func TestSelect_MasterWeight(t *testing.T) {
	p := New([]Entry{
		{Addr: "master", Weight: 1, Role: RoleMaster},
		{Addr: "r1", Weight: 1, Role: RoleReplica},
	})

	counts := map[string]int{}
	const N = 20000
	for i := 0; i < N; i++ {
		e := p.Select()
		counts[e.Addr]++
	}

	// master:r1 = 1:1，各约 50%
	for _, addr := range []string{"master", "r1"} {
		c := counts[addr]
		if c < N*40/100 || c > N*60/100 {
			t.Errorf("addr %s count %d, expected ~%d", addr, c, N/2)
		}
	}
}

func TestMarkDown_FallbackToMaster(t *testing.T) {
	p := New([]Entry{
		{Addr: "master", Weight: 0, Role: RoleMaster},
		{Addr: "r1", Weight: 1, Role: RoleReplica},
	})

	// 正常情况选择 r1
	e := p.Select()
	if e.Addr != "r1" {
		t.Fatalf("expected r1, got %s", e.Addr)
	}

	// 标记 r1 down，应该回落 master
	p.MarkDown("r1")
	e = p.Select()
	if e.Addr != "master" {
		t.Fatalf("expected master fallback, got %s", e.Addr)
	}

	// HasAvailableReplica 应为 false
	if p.HasAvailableReplica() {
		t.Fatal("expected HasAvailableReplica=false after all replicas down")
	}
}

func TestMarkDown_PartialDown(t *testing.T) {
	p := New([]Entry{
		{Addr: "master", Weight: 0, Role: RoleMaster},
		{Addr: "r1", Weight: 1, Role: RoleReplica},
		{Addr: "r2", Weight: 1, Role: RoleReplica},
	})

	p.MarkDown("r1")

	counts := map[string]int{}
	const N = 10000
	for i := 0; i < N; i++ {
		e := p.Select()
		counts[e.Addr]++
	}

	// r1 不应被选中
	if counts["r1"] > 0 {
		t.Errorf("down replica r1 was selected %d times", counts["r1"])
	}
	// r2 应被选中
	if counts["r2"] == 0 {
		t.Error("r2 was never selected")
	}
	// master weight=0，不应被选中
	if counts["master"] > 0 {
		t.Errorf("master with weight=0 was selected %d times", counts["master"])
	}
}

func TestSelect_AllWeightZero(t *testing.T) {
	p := New([]Entry{
		{Addr: "master", Weight: 0, Role: RoleMaster},
		{Addr: "r1", Weight: 0, Role: RoleReplica},
	})

	e := p.Select()
	if e.Addr != "master" {
		t.Fatalf("expected master fallback when all weights zero, got %s", e.Addr)
	}
}

func TestSelect_NoReplicas(t *testing.T) {
	// 只有 master，没有 replica
	p := New([]Entry{
		{Addr: "master", Weight: 0, Role: RoleMaster},
	})

	e := p.Select()
	if e.Addr != "master" {
		t.Fatalf("expected master, got %s", e.Addr)
	}
	if p.HasAvailableReplica() {
		t.Fatal("expected HasAvailableReplica=false with no replicas")
	}
}

func TestSelect_UnevenWeights(t *testing.T) {
	p := New([]Entry{
		{Addr: "master", Weight: 0, Role: RoleMaster},
		{Addr: "r1", Weight: 3, Role: RoleReplica},
		{Addr: "r2", Weight: 1, Role: RoleReplica},
	})

	counts := map[string]int{}
	const N = 40000
	for i := 0; i < N; i++ {
		e := p.Select()
		counts[e.Addr]++
	}

	// r1:r2 = 3:1，r1 约 75%，r2 约 25%
	r1Ratio := float64(counts["r1"]) / float64(N)
	r2Ratio := float64(counts["r2"]) / float64(N)
	if r1Ratio < 0.70 || r1Ratio > 0.80 {
		t.Errorf("r1 ratio %.3f, expected ~0.75", r1Ratio)
	}
	if r2Ratio < 0.20 || r2Ratio > 0.30 {
		t.Errorf("r2 ratio %.3f, expected ~0.25", r2Ratio)
	}
}
