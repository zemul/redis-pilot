package store

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func heartbeatFixture() apitypes.GatewayHeartbeat {
	return apitypes.GatewayHeartbeat{GatewayID: "gw", BootID: "boot1", Sequence: 1, NodeID: "node", ClusterID: "cluster", ShardIDs: []string{"shard0", "shard1"}, BuildVersion: "test", LocalConfigDigest: strings.Repeat("a", 64), Mode: "passthrough", Serving: true, BackendReady: true}
}

func TestCacheGatewayObservationReady(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	v := heartbeatFixture()
	v.Mode = "cache"
	if err := d.SaveGatewayHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	rows, err := d.ListGatewayObservations()
	if err != nil || len(rows) != 1 || rows[0].Status != "ready" {
		t.Fatalf("cache observation %+v %v", rows, err)
	}
}

func TestGatewayHeartbeatPersistenceSequenceAndFreshness(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	gen := d.Generation()
	v := heartbeatFixture()
	if err := d.SaveGatewayHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	age := time.Now().Add(-time.Minute).UnixMilli()
	if _, err := d.sql.Exec(`UPDATE gateway_observations SET received_ms=?`, age); err != nil {
		t.Fatal(err)
	}
	if err := d.SaveGatewayHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	rows, err := d.ListGatewayObservations()
	if err != nil || len(rows) != 1 || rows[0].Status != "stale" {
		t.Fatalf("duplicate renewed freshness: %+v %v", rows, err)
	}
	changed := v
	changed.BackendReady = false
	if err := d.SaveGatewayHeartbeat(changed); !errors.Is(err, ErrGatewayHeartbeatConflict) {
		t.Fatalf("same seq changed: %v", err)
	}
	v.Sequence = 2
	if err := d.SaveGatewayHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	changed = v
	changed.Sequence = 1
	if err := d.SaveGatewayHeartbeat(changed); !errors.Is(err, ErrGatewayHeartbeatConflict) {
		t.Fatalf("old seq: %v", err)
	}
	changed = v
	changed.Sequence = 3
	changed.ClusterID = "other"
	if err := d.SaveGatewayHeartbeat(changed); !errors.Is(err, ErrGatewayHeartbeatConflict) {
		t.Fatalf("identity mutation: %v", err)
	}
	if d.Generation() != gen {
		t.Fatal("heartbeat changed topology generation")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = d.ListGatewayObservations()
	if err != nil || len(rows) != 1 || rows[0].Sequence != 2 || rows[0].Status != "ready" || rows[0].AppliedRevision != "" || rows[0].ConfigSource != "local_file" {
		t.Fatalf("reopen: %+v %v", rows, err)
	}
}

func TestGatewayHeartbeatConflictsAndBound(t *testing.T) {
	d := openTestDB(t)
	v := heartbeatFixture()
	for i := 0; i < 8; i++ {
		v.BootID = fmt.Sprintf("boot%d", i)
		if err := d.SaveGatewayHeartbeat(v); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := d.ListGatewayObservations()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Status != "conflict" {
			t.Fatalf("not conflict: %+v", row)
		}
	}
	v.BootID = "boot8"
	if err := d.SaveGatewayHeartbeat(v); !errors.Is(err, ErrGatewayHeartbeatConflict) {
		t.Fatalf("unbounded boots: %v", err)
	}
	if _, err := d.sql.Exec(`UPDATE gateway_observations SET received_ms=? WHERE boot_id='boot0'`, time.Now().Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := d.SaveGatewayHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	rows, err = d.ListGatewayObservations()
	if err != nil || len(rows) != 8 {
		t.Fatalf("retention: %d %v", len(rows), err)
	}
	// Draining observations do not hide the replacement's health.
	for i := 1; i < 8; i++ {
		v.BootID = fmt.Sprintf("boot%d", i)
		v.Sequence = 2
		v.Draining = true
		v.Serving = false
		if err := d.SaveGatewayHeartbeat(v); err != nil {
			t.Fatal(err)
		}
	}
	rows, err = d.ListGatewayObservations()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		want := "draining"
		if row.BootID == "boot8" {
			want = "ready"
		}
		if row.Status != want {
			t.Fatalf("%s: %s", row.BootID, row.Status)
		}
	}
	v.BootID = "boot1"
	v.Sequence = 3
	v.Draining = false
	if err := d.SaveGatewayHeartbeat(v); !errors.Is(err, ErrGatewayHeartbeatConflict) {
		t.Fatal("draining boot resurrected", err)
	}
}

func TestGatewayHeartbeatConcurrentSequence(t *testing.T) {
	d := openTestDB(t)
	var wg sync.WaitGroup
	for i := 1; i <= 32; i++ {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			v := heartbeatFixture()
			v.Sequence = seq
			err := d.SaveGatewayHeartbeat(v)
			if err != nil && !errors.Is(err, ErrGatewayHeartbeatConflict) {
				t.Error(err)
			}
		}(int64(i))
	}
	wg.Wait()
	rows, err := d.ListGatewayObservations()
	if err != nil || len(rows) != 1 || rows[0].Sequence != 32 {
		t.Fatalf("out of order: %+v %v", rows, err)
	}
}
