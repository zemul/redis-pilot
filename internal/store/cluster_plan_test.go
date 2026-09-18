package store

import (
	"errors"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestClusterPlanRejectsStaleCapacity(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	node := &apitypes.NodeServer{Endpoint: "127.0.0.1", AgentPort: 8400, Capacity: apitypes.ResourceSpec{Memory: "32Gi"}}
	if err := d.UpsertServer("node", node); err != nil {
		t.Fatal(err)
	}
	snapshot, err := d.ReadNode()
	if err != nil {
		t.Fatal(err)
	}
	node.Capacity.Memory = "1Gi"
	if err := d.UpsertServer("node", node); err != nil {
		t.Fatal(err)
	}
	plan := &ClusterPlan{Nodes: snapshot, Spec: apitypes.ColdClusterSpec{NodeID: "node"}}
	err = d.ReserveClusterPlan(plan, func(st *apitypes.InstancesState) error {
		st.Groups["tentative"] = &apitypes.InstanceGroupState{Engine: "redis", Type: "standalone"}
		return nil
	})
	if !errors.Is(err, ErrColdConflict) || !strings.Contains(err.Error(), "node node changed") {
		t.Fatalf("stale scheduling inputs accepted: %v", err)
	}
	st, err := d.ReadInstances()
	if err != nil || len(st.Groups) != 0 {
		t.Fatalf("reservation was not rolled back: %+v, %v", st, err)
	}
}
