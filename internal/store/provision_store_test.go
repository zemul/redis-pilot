package store

import (
	"fmt"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestProvisionReservationAtomicityAndGuards(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "127.0.0.1", AgentPort: 8400}); err != nil {
		t.Fatal(err)
	}
	task := Task{ID: "provision-1", Kind: "profile.create", Status: "pending", InstanceName: "i", GroupName: "g", ServerName: "node", Payload: `{"agent_endpoint":"127.0.0.1","agent_port":8400}`}
	reserve := func(st *apitypes.InstancesState) error {
		st.Groups["g"] = &apitypes.InstanceGroupState{Engine: "redis", Type: "standalone", Category: "cache"}
		st.Instances["i"] = &apitypes.Instance{Group: "g", Server: "node", Profile: "redis-cache", Role: "master", DesiredState: "running", Lock: &apitypes.Lock{HeldBy: task.ID, Operation: "profile.create"}}
		return nil
	}
	if err := d.WithInstancesAndTask(&task, func(st *apitypes.InstancesState) error { reserve(st); return fmt.Errorf("injected failure") }); err == nil {
		t.Fatal("expected abort")
	}
	st, _ := d.ReadInstances()
	if len(st.Instances) != 0 {
		t.Fatal("partial reservation")
	}
	if _, err := d.GetTask(task.ID); err == nil {
		t.Fatal("task survived rolled back reservation")
	}
	if err := d.WithInstancesAndTask(&task, reserve); err != nil {
		t.Fatal(err)
	}
	if err := d.WithInstancesAndTask(&task, func(st *apitypes.InstancesState) error {
		st.Instances["extra"] = &apitypes.Instance{Group: "other"}
		return nil
	}); err == nil {
		t.Fatal("duplicate task accepted")
	}
	st, _ = d.ReadInstances()
	if st.Instances["extra"] != nil {
		t.Fatal("duplicate-task error did not roll back resources")
	}
	for _, change := range []func(*apitypes.InstancesState){
		func(st *apitypes.InstancesState) { delete(st.Instances, "i") },
		func(st *apitypes.InstancesState) { st.Instances["i"].Lock = nil },
		func(st *apitypes.InstancesState) { st.Instances["i"].Port = 7000 },
		func(st *apitypes.InstancesState) { st.Instances["i"].DesiredState = "stopped" },
	} {
		if err := d.WithInstances(func(st *apitypes.InstancesState) error { change(st); return nil }); err == nil {
			t.Fatal("mutation bypassed durable reservation")
		}
	}
	if err := d.UpsertServer("node", &apitypes.NodeServer{Endpoint: "127.0.0.2", AgentPort: 8400}); err == nil {
		t.Fatal("Agent endpoint changed")
	}
	if _, err := d.sql.Exec(`CREATE TRIGGER inject_complete_failure BEFORE UPDATE ON instances WHEN NEW.observed_status='running' BEGIN SELECT RAISE(ABORT,'injected completion failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteProvisionTask(task.ID, "container"); err == nil {
		t.Fatal("expected completion failure")
	}
	before, _ := d.GetTask(task.ID)
	st, _ = d.ReadInstances()
	if before.Status != "pending" || st.Instances["i"].Lock == nil {
		t.Fatal("completion failure partially committed")
	}
	if _, err := d.sql.Exec(`DROP TRIGGER inject_complete_failure`); err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteProvisionTask(task.ID, "container"); err != nil {
		t.Fatal(err)
	}
	if err := d.CompleteProvisionTask(task.ID, "container"); err != nil {
		t.Fatal(err)
	}
	taskGot, _ := d.GetTask(task.ID)
	st, _ = d.ReadInstances()
	if taskGot.Status != "success" || st.Instances["i"].Lock != nil {
		t.Fatal("completion was not atomic")
	}
}
