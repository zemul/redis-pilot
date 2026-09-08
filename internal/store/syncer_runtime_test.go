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

func syncerHeartbeatFixture() apitypes.SyncerHeartbeat {
	return apitypes.SyncerHeartbeat{SyncerID: "syncer-1", NodeID: "node-1", BootID: "boot-1", Sequence: 1, BuildVersion: "test", State: "running", LastError: "none", Running: true,
		Tasks: []apitypes.SyncTaskReport{{TaskID: "task-1", IdentityDigest: strings.Repeat("a", 64), State: "running", LastError: "none", Running: true}}}
}

func TestSyncerRuntimePersistentBindingAndFreshness(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	v := syncerHeartbeatFixture()
	gen := d.Generation()
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	if _, err := d.sql.Exec(`UPDATE syncer_observations SET received_ms=?`, time.Now().Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	runtime, err := d.ListSyncerRuntime()
	if err != nil || len(runtime.Bindings) != 1 || len(runtime.Observations) != 1 || runtime.Observations[0].Status != "stale" {
		t.Fatalf("retry refreshed observation: %+v %v", runtime, err)
	}
	registered := runtime.Bindings[0].RegisteredAt
	v.Sequence++
	v.State, v.LastError, v.Tasks[0].State, v.Tasks[0].LastError = "degraded", "task_failure", "failed", "history_gap"
	v.Tasks[0].Running = false
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	if d.Generation() != gen {
		t.Fatal("observation changed topology generation")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err = d.ListSyncerRuntime()
	if err != nil || runtime.Observations[0].Status != "degraded" || runtime.Observations[0].Tasks[0].LastError != "history_gap" {
		t.Fatalf("reopen: %+v %v", runtime, err)
	}
	v.BootID, v.Sequence = "restarted", 1
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	runtime, err = d.ListSyncerRuntime()
	if err != nil || len(runtime.Bindings) != 1 || runtime.Bindings[0].RegisteredAt != registered || runtime.Bindings[0].SyncerID != v.SyncerID {
		t.Fatalf("restart changed owner: %+v %v", runtime, err)
	}
}

func TestSyncerRuntimeRejectsMutationAtomically(t *testing.T) {
	d := openTestDB(t)
	base := syncerHeartbeatFixture()
	if err := d.SaveSyncerHeartbeat(base); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*apitypes.SyncerHeartbeat){
		"same-sequence": func(v *apitypes.SyncerHeartbeat) { v.State = "degraded" },
		"old-sequence":  func(v *apitypes.SyncerHeartbeat) { v.Sequence = 0 },
		"membership":    func(v *apitypes.SyncerHeartbeat) { v.Sequence++; v.Tasks[0].TaskID = "new-task" },
		"version":       func(v *apitypes.SyncerHeartbeat) { v.Sequence++; v.BuildVersion = "changed" },
		"owner":         func(v *apitypes.SyncerHeartbeat) { v.SyncerID = "other" },
		"node":          func(v *apitypes.SyncerHeartbeat) { v.BootID = "new"; v.NodeID = "other" },
		"endpoint-identity": func(v *apitypes.SyncerHeartbeat) {
			v.BootID = "new"
			v.Tasks[0].IdentityDigest = strings.Repeat("b", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := syncerHeartbeatFixture()
			mutate(&v)
			if err := d.SaveSyncerHeartbeat(v); err == nil {
				t.Fatal("accepted conflicting identity")
			}
		})
	}
	// A new task before a conflicting existing one cannot partially register.
	v := syncerHeartbeatFixture()
	v.SyncerID, v.BootID = "other", "new"
	newTask := v.Tasks[0]
	newTask.TaskID = "aaa-new"
	v.Tasks = append([]apitypes.SyncTaskReport{newTask}, v.Tasks...)
	if err := d.SaveSyncerHeartbeat(v); !errors.Is(err, ErrSyncerHeartbeatConflict) {
		t.Fatal(err)
	}
	runtime, err := d.ListSyncerRuntime()
	if err != nil || len(runtime.Bindings) != 1 || len(runtime.Observations) != 1 {
		t.Fatalf("partial mutation: %+v %v", runtime, err)
	}
	// Offline does not make a binding available to another syncer.
	if _, err := d.sql.Exec(`UPDATE syncer_observations SET received_ms=1`); err != nil {
		t.Fatal(err)
	}
	if err := d.SaveSyncerHeartbeat(v); !errors.Is(err, ErrSyncerHeartbeatConflict) {
		t.Fatal("stale task reassigned", err)
	}
}

func TestSyncerRuntimeBootConflictsRetentionAndTerminal(t *testing.T) {
	d := openTestDB(t)
	for i := 0; i < 8; i++ {
		v := syncerHeartbeatFixture()
		v.BootID = fmt.Sprintf("boot-%d", i)
		if i == 0 {
			v.State = "stopping"
		}
		if err := d.SaveSyncerHeartbeat(v); err != nil {
			t.Fatal(err)
		}
	}
	runtime, err := d.ListSyncerRuntime()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range runtime.Observations {
		if v.Status != "conflict" {
			t.Fatal("draining/running overlap hidden", v.Status)
		}
	}
	v := syncerHeartbeatFixture()
	v.BootID = "boot-9"
	if err := d.SaveSyncerHeartbeat(v); !errors.Is(err, ErrSyncerHeartbeatConflict) {
		t.Fatal("unbounded boot history", err)
	}
	if _, err := d.sql.Exec(`UPDATE syncer_observations SET received_ms=1 WHERE boot_id='boot-0'`); err != nil {
		t.Fatal(err)
	}
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	runtime, err = d.ListSyncerRuntime()
	if err != nil || len(runtime.Observations) != 8 || len(runtime.Bindings) != 1 {
		t.Fatal("bad retention", err)
	}
	v.Sequence, v.State, v.Running = 2, "stopped", false
	v.Tasks[0].Running = false
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	v.Sequence, v.State, v.Running = 3, "running", true
	if err := d.SaveSyncerHeartbeat(v); !errors.Is(err, ErrSyncerHeartbeatConflict) {
		t.Fatal("terminal boot resurrected", err)
	}
}

func TestSyncerRuntimeConcurrentAndBindingBound(t *testing.T) {
	d := openTestDB(t)
	var wg sync.WaitGroup
	for i := 1; i <= 24; i++ {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			v := syncerHeartbeatFixture()
			v.Sequence = seq
			if err := d.SaveSyncerHeartbeat(v); err != nil && !errors.Is(err, ErrSyncerHeartbeatConflict) {
				t.Error(err)
			}
		}(int64(i))
	}
	wg.Wait()
	runtime, err := d.ListSyncerRuntime()
	if err != nil || len(runtime.Bindings) != 1 || runtime.Observations[0].Sequence != 24 {
		t.Fatalf("concurrent: %+v %v", runtime, err)
	}
	for i := 1; i < 64; i++ {
		if _, err := d.sql.Exec(`INSERT INTO syncer_task_bindings(task_id,syncer_id,node_id,identity_digest,registered_ms) VALUES(?,?,?,?,?)`, fmt.Sprintf("historical-%d", i), "syncer-1", "node-1", strings.Repeat("b", 64), 1); err != nil {
			t.Fatal(err)
		}
	}
	v := syncerHeartbeatFixture()
	v.BootID = "new"
	v.Tasks[0].TaskID = "new-task"
	if err := d.SaveSyncerHeartbeat(v); !errors.Is(err, ErrSyncerHeartbeatConflict) {
		t.Fatal("unbounded sticky history", err)
	}
	runtime, err = d.ListSyncerRuntime()
	if err != nil || len(runtime.Bindings) != 64 || len(runtime.Observations) != 1 {
		t.Fatal("partial bound mutation", err)
	}
}
