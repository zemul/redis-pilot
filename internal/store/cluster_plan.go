package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type ClusterPlan struct {
	Spec     apitypes.ColdClusterSpec
	Parent   Task
	Children []*Task
	Request  apitypes.ClusterCreateRequest
	Digest   string
	Nodes    *apitypes.NodeState
}

// ReserveClusterPlan commits all allocations, child tasks, parent ownership and
// the replay receipt in one transaction, before any Agent can observe a task.
func (d *DB) ReserveClusterPlan(plan *ClusterPlan, fn func(*apitypes.InstancesState) error) error {
	return d.withInstancesAndTasks(&plan.Children, func(tx *sql.Tx) error {
		// Scheduling used a read snapshot. Reject changed placement inputs before
		// committing reservations; heartbeat timestamps alone are irrelevant.
		if plan.Nodes != nil {
			used := map[string]bool{plan.Spec.NodeID: true}
			for _, child := range plan.Children {
				used[child.ServerName] = true
			}
			for name := range used {
				old := plan.Nodes.Servers[name]
				var endpoint, status, labels, capacity string
				var port int
				err := tx.QueryRow(`SELECT endpoint,agent_port,status,labels,capacity FROM servers WHERE name=?`, name).Scan(&endpoint, &port, &status, &labels, &capacity)
				var gotLabels map[string]string
				var gotCapacity apitypes.ResourceSpec
				if err != nil || old == nil || json.Unmarshal([]byte(labels), &gotLabels) != nil || json.Unmarshal([]byte(capacity), &gotCapacity) != nil || endpoint != old.Endpoint || port != old.AgentPort || status != old.Status || !reflect.DeepEqual(gotLabels, old.Labels) || !reflect.DeepEqual(gotCapacity, old.Capacity) {
					return fmt.Errorf("%w: deployment node %s changed; retry creation", ErrColdConflict, name)
				}
			}
		}
		ids := make([]string, len(plan.Children))
		for i, t := range plan.Children {
			ids[i] = t.ID
		}
		if err := createClusterBuildTx(tx, plan.Spec, ids, plan.Parent); err != nil {
			return err
		}
		raw, err := json.Marshal(plan.Request)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO cluster_create_requests(cluster_id,digest,task_id,request) VALUES(?,?,?,?)`, plan.Spec.ID, plan.Digest, plan.Parent.ID, string(raw))
		return err
	}, fn)
}

func (d *DB) ClusterPlanReplay(id, digest string) (string, error) {
	var got, task string
	err := d.sql.QueryRow(`SELECT digest,task_id FROM cluster_create_requests WHERE cluster_id=?`, id).Scan(&got, &task)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if got != digest {
		return "", ErrColdConflict
	}
	return task, nil
}
