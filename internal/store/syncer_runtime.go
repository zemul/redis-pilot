package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var ErrSyncerHeartbeatConflict = errors.New("syncer heartbeat identity, sequence or binding conflict")

// SaveSyncerHeartbeat atomically records sticky, self-declared local task
// bindings and a process observation. It never assigns or transfers execution.
// Client clocks and duplicate retries cannot extend freshness.
func (d *DB) SaveSyncerHeartbeat(v apitypes.SyncerHeartbeat) error {
	received := time.Now().UTC() // include database/mutex queueing in observation age
	if err := v.Validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw string
	err = tx.QueryRow(`SELECT report FROM syncer_observations WHERE syncer_id=? AND boot_id=?`, v.SyncerID, v.BootID).Scan(&raw)
	newBoot := errors.Is(err, sql.ErrNoRows)
	if err != nil && !newBoot {
		return err
	}
	if !newBoot {
		var old apitypes.SyncerHeartbeat
		if err := json.Unmarshal([]byte(raw), &old); err != nil {
			return err
		}
		if v.Sequence <= old.Sequence {
			// Compare wire values: time.Time monotonic/location internals are
			// intentionally not persisted and must not break identical retries.
			wire, err := json.Marshal(v)
			if err == nil && string(wire) == raw {
				return nil
			}
			return ErrSyncerHeartbeatConflict
		}
		if (old.State == "stopped" || old.State == "failed") && (old.State != v.State || v.Running) {
			return ErrSyncerHeartbeatConflict
		}
		if len(old.Tasks) != len(v.Tasks) {
			return ErrSyncerHeartbeatConflict
		}
		// Membership, build and process placement are immutable within one boot.
		old.Sequence, old.State, old.LastError, old.Running = v.Sequence, v.State, v.LastError, v.Running
		for i := range old.Tasks {
			old.Tasks[i].Progress = v.Tasks[i].Progress
			old.Tasks[i].State, old.Tasks[i].LastError, old.Tasks[i].Running = v.Tasks[i].State, v.Tasks[i].LastError, v.Tasks[i].Running
		}
		if !reflect.DeepEqual(old, v) {
			return ErrSyncerHeartbeatConflict
		}
	}
	now := received
	for _, task := range v.Tasks {
		var owner, node, digest string
		err := tx.QueryRow(`SELECT syncer_id,node_id,identity_digest FROM syncer_task_bindings WHERE task_id=?`, task.TaskID).Scan(&owner, &node, &digest)
		if err == nil {
			if owner != v.SyncerID || node != v.NodeID || digest != task.IdentityDigest {
				return ErrSyncerHeartbeatConflict
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var count int
		if err := tx.QueryRow(`SELECT count(*) FROM syncer_task_bindings WHERE syncer_id=?`, v.SyncerID).Scan(&count); err != nil {
			return err
		}
		// Keep bounded history without silently deleting an offline task's owner.
		if count >= 64 {
			return ErrSyncerHeartbeatConflict
		}
		if _, err := tx.Exec(`INSERT INTO syncer_task_bindings(task_id,syncer_id,node_id,identity_digest,registered_ms) VALUES(?,?,?,?,?)`, task.TaskID, v.SyncerID, v.NodeID, task.IdentityDigest, now.UnixMilli()); err != nil {
			return err
		}
	}
	if newBoot {
		var count int
		if err := tx.QueryRow(`SELECT count(*) FROM syncer_observations WHERE syncer_id=?`, v.SyncerID).Scan(&count); err != nil {
			return err
		}
		if count >= 8 {
			res, err := tx.Exec(`DELETE FROM syncer_observations WHERE syncer_id=? AND boot_id=(SELECT boot_id FROM syncer_observations WHERE syncer_id=? AND received_ms<? ORDER BY received_ms,boot_id LIMIT 1)`, v.SyncerID, v.SyncerID, now.Add(-apitypes.SyncerStaleAfter).UnixMilli())
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n != 1 {
				return ErrSyncerHeartbeatConflict
			}
		}
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO syncer_observations(syncer_id,boot_id,report,received_ms) VALUES(?,?,?,?) ON CONFLICT(syncer_id,boot_id) DO UPDATE SET report=excluded.report,received_ms=excluded.received_ms`, v.SyncerID, v.BootID, string(data), now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit() // Observations must not bump Proxy topology generation.
}

func (d *DB) ListSyncerRuntime() (apitypes.SyncerRuntime, error) {
	result := apitypes.SyncerRuntime{Bindings: []apitypes.SyncTaskBinding{}, Observations: []apitypes.SyncerObservation{}, StaleAfterSeconds: int(apitypes.SyncerStaleAfter.Seconds())}
	tx, err := d.sql.Begin()
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT task_id,syncer_id,node_id,identity_digest,registered_ms FROM syncer_task_bindings ORDER BY task_id`)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var binding apitypes.SyncTaskBinding
		var ms int64
		if err := rows.Scan(&binding.TaskID, &binding.SyncerID, &binding.NodeID, &binding.IdentityDigest, &ms); err != nil {
			rows.Close()
			return result, err
		}
		binding.RegisteredAt = time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
		result.Bindings = append(result.Bindings, binding)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	rows, err = tx.Query(`SELECT report,received_ms FROM syncer_observations ORDER BY syncer_id,boot_id`)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	now, live := time.Now(), make(map[string]int)
	for rows.Next() {
		var raw string
		var ms int64
		if err := rows.Scan(&raw, &ms); err != nil {
			return result, err
		}
		v := apitypes.SyncerObservation{ConfigSource: "local_file"}
		if err := json.Unmarshal([]byte(raw), &v.SyncerHeartbeat); err != nil {
			return result, err
		}
		at := time.UnixMilli(ms)
		v.ReceivedAt, v.Status = at.UTC().Format(time.RFC3339Nano), v.State
		if at.After(now) || now.Sub(at) >= apitypes.SyncerStaleAfter {
			v.Status = "stale"
		} else if v.State != "stopped" && v.State != "failed" {
			// Include stopping boots: draining can still write to the cold DB.
			live[v.SyncerID]++
		}
		result.Observations = append(result.Observations, v)
	}
	for i := range result.Observations {
		v := &result.Observations[i]
		if v.Status != "stale" && v.State != "stopped" && v.State != "failed" && live[v.SyncerID] > 1 {
			v.Status = "conflict"
		}
		v.TaskProgress = make(map[string]apitypes.SyncProgressView, len(v.Tasks))
		received, _ := time.Parse(time.RFC3339Nano, v.ReceivedAt)
		for _, task := range v.Tasks {
			elapsed := now.Sub(received)
			if elapsed >= 0 {
				elapsed += apitypes.SyncProgressReportAllowance
			}
			view := apitypes.DeriveSyncProgress(task.Progress, task.State, task.LastError, task.Running, elapsed)
			if v.Status == "stale" || v.Status == "conflict" || v.State == "stopping" {
				view = apitypes.SyncProgressView{Condition: v.Status}
			}
			v.TaskProgress[task.TaskID] = view
		}
	}
	return result, rows.Err()
}
