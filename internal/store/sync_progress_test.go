package store

import (
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func storeProgressSample() *apitypes.SyncProgress {
	pos := func(n int64) *apitypes.SyncProgressPosition {
		return &apitypes.SyncProgressPosition{History: strings.Repeat("a", 40), Offset: n}
	}
	now := time.Now()
	return (&apitypes.SyncProgress{Local: &apitypes.SyncLocalProgress{SampledAt: now, Captured: pos(100), Applied: pos(100), GCConfirmed: pos(90), JournalLimitBytes: 1000}, Source: apitypes.SyncSourceProgress{SampledAt: now, Position: pos(100), Error: "none"}}).At(now)
}

func TestSyncProgressStoreReceiveAgeAndConflictOverride(t *testing.T) {
	d := openTestDB(t)
	v := syncerHeartbeatFixture()
	v.Tasks[0].Progress = storeProgressSample()
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	read := func() apitypes.SyncProgressView {
		r, err := d.ListSyncerRuntime()
		if err != nil {
			t.Fatal(err)
		}
		return r.Observations[0].TaskProgress["task-1"]
	}
	if read().Condition != "sample_aligned" {
		t.Fatal("valid sample lost")
	}
	if _, err := d.sql.Exec(`UPDATE syncer_observations SET received_ms=?`, time.Now().Add(-44*time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if view := read(); view.Condition != "observation_stale" || view.SourceCaptureBytes != nil {
		t.Fatal("live heartbeat hid stale progress", view)
	}
	// Retry does not renew the server receive time or progress freshness.
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	if read().Condition != "observation_stale" {
		t.Fatal("duplicate renewed sample")
	}
	v.Sequence++
	v.Tasks[0].Progress = storeProgressSample()
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal("progress update must not change identity", err)
	}
	v.BootID = "other-boot"
	v.Sequence = 1
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	runtime, err := d.ListSyncerRuntime()
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range runtime.Observations {
		view := o.TaskProgress["task-1"]
		if view.Condition != "conflict" || view.SourceCaptureBytes != nil {
			t.Fatal("conflicting owner advertised aligned sample", view)
		}
	}
	if _, err := d.sql.Exec(`UPDATE syncer_observations SET received_ms=1`); err != nil {
		t.Fatal(err)
	}
	if view := read(); view.Condition != "stale" || view.SourceCaptureBytes != nil {
		t.Fatal("offline progress not suppressed", view)
	}
}

func TestSyncProgressStoreValidationAndReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	v := syncerHeartbeatFixture()
	v.Tasks[0].Progress = storeProgressSample()
	if err := d.SaveSyncerHeartbeat(v); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := d.ListSyncerRuntime()
	if err != nil || runtime.Observations[0].Tasks[0].Progress.Local.Applied.Offset != 100 {
		t.Fatal("progress persistence", err)
	}
	v.Sequence++
	v.Tasks[0].Progress.Source.Error = "SENSITIVE"
	if err := d.SaveSyncerHeartbeat(v); err == nil || strings.Contains(err.Error(), "SENSITIVE") {
		t.Fatal("unsafe raw progress error", err)
	}
	runtime, err = d.ListSyncerRuntime()
	if err != nil || runtime.Observations[0].Sequence != 1 {
		t.Fatal("invalid report mutated state")
	}
}
