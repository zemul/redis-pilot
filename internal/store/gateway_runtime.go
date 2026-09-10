package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var ErrGatewayHeartbeatConflict = errors.New("gateway heartbeat sequence or identity conflict")

// SaveGatewayHeartbeat uses Server receive time, never a client wall clock.
// A retry with the same sequence and content succeeds but does not extend freshness.
func (d *DB) SaveGatewayHeartbeat(v apitypes.GatewayHeartbeat) error {
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
	var oldRaw string
	err = tx.QueryRow(`SELECT report FROM gateway_observations WHERE gateway_id=? AND boot_id=?`, v.GatewayID, v.BootID).Scan(&oldRaw)
	if err == nil {
		var old apitypes.GatewayHeartbeat
		if err := json.Unmarshal([]byte(oldRaw), &old); err != nil {
			return err
		}
		if v.Sequence <= old.Sequence {
			if reflect.DeepEqual(v, old) {
				return nil
			}
			return ErrGatewayHeartbeatConflict
		}
		// Immutable process identity: restart to change placement/config/version.
		if old.Draining && !v.Draining {
			return ErrGatewayHeartbeatConflict
		}
		identity := old
		identity.Sequence, identity.Serving, identity.BackendReady, identity.Draining = v.Sequence, v.Serving, v.BackendReady, v.Draining
		if !reflect.DeepEqual(identity, v) {
			return ErrGatewayHeartbeatConflict
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	now := time.Now().UTC()
	if errors.Is(err, sql.ErrNoRows) {
		// Bound process history per configured identity, without replacing a live boot.
		var count int
		if err := tx.QueryRow(`SELECT count(*) FROM gateway_observations WHERE gateway_id=?`, v.GatewayID).Scan(&count); err != nil {
			return err
		}
		if count >= 8 {
			res, err := tx.Exec(`DELETE FROM gateway_observations WHERE gateway_id=? AND boot_id=(SELECT boot_id FROM gateway_observations WHERE gateway_id=? AND received_ms<? ORDER BY received_ms LIMIT 1)`, v.GatewayID, v.GatewayID, now.Add(-apitypes.GatewayStaleAfter).UnixMilli())
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n == 0 {
				return ErrGatewayHeartbeatConflict
			}
		}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO gateway_observations(gateway_id,boot_id,sequence,report,received_ms) VALUES(?,?,?,?,?) ON CONFLICT(gateway_id,boot_id) DO UPDATE SET sequence=excluded.sequence,report=excluded.report,received_ms=excluded.received_ms`, v.GatewayID, v.BootID, v.Sequence, string(raw), now.UnixMilli())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) ListGatewayObservations() ([]apitypes.GatewayObservation, error) {
	rows, err := d.sql.Query(`SELECT report,received_ms FROM gateway_observations ORDER BY gateway_id,boot_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now()
	result := make([]apitypes.GatewayObservation, 0)
	live := make(map[string]int)
	for rows.Next() {
		var raw string
		var ms int64
		if err := rows.Scan(&raw, &ms); err != nil {
			return nil, err
		}
		v := apitypes.GatewayObservation{ConfigSource: "local_file"}
		if err := json.Unmarshal([]byte(raw), &v.GatewayHeartbeat); err != nil {
			return nil, err
		}
		at := time.UnixMilli(ms)
		v.ReceivedAt = at.UTC().Format(time.RFC3339Nano)
		switch {
		case now.Sub(at) >= apitypes.GatewayStaleAfter || at.After(now):
			v.Status = "stale"
		case v.Draining:
			v.Status = "draining"
		case (v.Mode == "passthrough" || v.Mode == "cache") && v.Serving && v.BackendReady:
			v.Status = "ready"
		default:
			v.Status = "unready"
		}
		if v.Status != "stale" && !v.Draining {
			live[v.GatewayID]++
		}
		result = append(result, v)
	}
	for i := range result {
		if result[i].Status != "stale" && !result[i].Draining && live[result[i].GatewayID] > 1 {
			result[i].Status = "conflict"
		}
	}
	return result, rows.Err()
}
