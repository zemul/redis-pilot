package apitypes

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func progressFixture() *SyncProgress {
	at := time.Now().Add(-time.Second)
	position := func(n int64) *SyncProgressPosition {
		return &SyncProgressPosition{History: strings.Repeat("a", 40), Offset: n}
	}
	return (&SyncProgress{Local: &SyncLocalProgress{SampledAt: at, Captured: position(900), Applied: position(800), GCConfirmed: position(700), LiveJournalBytes: 100, JournalLimitBytes: 1000}, Source: SyncSourceProgress{SampledAt: at.Add(100 * time.Millisecond), Position: position(1000), Error: "none"}}).At(time.Now())
}

func TestSyncProgressDerivedGapsAndUnknowns(t *testing.T) {
	v := DeriveSyncProgress(progressFixture(), "running", "none", true, 0)
	if v.Condition != "catching_up" || *v.SourceCaptureBytes != 100 || *v.CapturedAppliedBytes != 100 || *v.CapturedGCBytes != 200 {
		t.Fatalf("wrong byte meanings %+v", v)
	}
	for name, tc := range map[string]struct {
		change              func(*SyncProgress)
		state, reason, want string
		running             bool
		elapsed             time.Duration
	}{
		"failed":                {state: "failed", reason: "history_gap", want: "needs_attention"},
		"uncertain":             {state: "running", reason: "cold_outcome_uncertain", want: "needs_attention", running: true},
		"unknown-error":         {state: "running", reason: "unknown", want: "needs_attention", running: true},
		"cold":                  {state: "retry_cold", reason: "cold_connection", want: "cold_unavailable", running: true},
		"source":                {state: "retry_source", reason: "source_transport", want: "source_unavailable", running: true},
		"pressure":              {state: "pressure", reason: "resource_pressure", want: "resource_pressure", running: true},
		"stopped":               {state: "stopped", reason: "none", want: "stopped"},
		"draining":              {state: "draining", reason: "none", want: "stopping", running: true},
		"local-stale":           {change: func(p *SyncProgress) { n := SyncProgressStaleAfter.Milliseconds(); p.Local.AgeMillis = &n }, want: "observation_stale", running: true},
		"source-stale":          {change: func(p *SyncProgress) { n := SyncProgressStaleAfter.Milliseconds(); p.Source.AgeMillis = &n }, want: "observation_stale", running: true},
		"unknown-age":           {change: func(p *SyncProgress) { p.Source.AgeMillis = nil }, want: "observation_stale", running: true},
		"server-elapsed":        {want: "observation_stale", running: true, elapsed: SyncProgressStaleAfter},
		"server-clock-backward": {want: "observation_stale", running: true, elapsed: -time.Second},
		"history-change":        {change: func(p *SyncProgress) { p.Source.Position.History = strings.Repeat("b", 40) }, want: "history_mismatch", running: true},
		"gc-history":            {change: func(p *SyncProgress) { p.Local.GCConfirmed.History = strings.Repeat("b", 40) }, want: "history_mismatch", running: true},
		"older-source":          {change: func(p *SyncProgress) { p.Source.Position.Offset = 899 }, want: "samples_not_aligned", running: true},
		"applied-ahead":         {change: func(p *SyncProgress) { p.Local.Applied.Offset = 901 }, want: "samples_not_aligned", running: true},
		"gc-ahead":              {change: func(p *SyncProgress) { p.Local.GCConfirmed.Offset = 801 }, want: "samples_not_aligned", running: true},
		"probe-error":           {change: func(p *SyncProgress) { p.Source.Position = nil; p.Source.Error = "source_unavailable" }, want: "source_unavailable", running: true},
		"replica":               {change: func(p *SyncProgress) { p.Source.Position = nil; p.Source.Error = "not_master" }, want: "source_observation_error", running: true},
		"local-missing":         {change: func(p *SyncProgress) { p.Local = nil }, want: "initializing", running: true},
		"position-missing":      {change: func(p *SyncProgress) { p.Local.Applied = nil }, want: "initializing", running: true},
		"disk-paused":           {change: func(p *SyncProgress) { p.Local.DiskPaused = true }, want: "resource_pressure", running: true},
		"invalid":               {change: func(p *SyncProgress) { p.Source.Position.Offset = -1 }, want: "invalid_observation", running: true},
	} {
		t.Run(name, func(t *testing.T) {
			p := progressFixture()
			if tc.change != nil {
				tc.change(p)
			}
			state, reason := tc.state, tc.reason
			if state == "" {
				state = "running"
			}
			if reason == "" {
				reason = "none"
			}
			view := DeriveSyncProgress(p, state, reason, tc.running, tc.elapsed)
			if view.Condition != tc.want || view.SourceCaptureBytes != nil || view.CapturedAppliedBytes != nil || view.CapturedGCBytes != nil {
				t.Fatalf("unsafe classification: %+v", view)
			}
		})
	}
}

func TestSyncProgressAlignedGCUnknownAndJournalWarning(t *testing.T) {
	p := progressFixture()
	p.Local.Captured.Offset = 1000
	p.Local.Applied.Offset = 1000
	p.Local.GCConfirmed = nil
	v := DeriveSyncProgress(p, "running", "none", true, 0)
	if v.Condition != "sample_aligned" || *v.SourceCaptureBytes != 0 || *v.CapturedAppliedBytes != 0 || v.CapturedGCBytes != nil {
		t.Fatal("GC unknown fabricated zero", v)
	}
	p.Local.LiveJournalBytes = 800
	v = DeriveSyncProgress(p, "running", "none", true, 0)
	if v.Condition != "journal_budget_warning" || !v.JournalBudgetWarning {
		t.Fatal("80% budget warning missing", v)
	}
	p.Local.LiveJournalBytes = 799
	if DeriveSyncProgress(p, "running", "none", true, 0).JournalBudgetWarning {
		t.Fatal("premature budget warning")
	}
}

func TestSyncProgressExactJSONValidationAndAgeClone(t *testing.T) {
	p := progressFixture()
	p.Source.Position.Offset = math.MaxInt64 - 1
	p.Local.Captured.Offset = math.MaxInt64 - 2
	p.Local.Applied.Offset = math.MaxInt64 - 3
	raw, err := json.Marshal(p)
	if err != nil || !strings.Contains(string(raw), `"offset":"9223372036854775806"`) {
		t.Fatal("offset lost precision", string(raw), err)
	}
	var decoded SyncProgress
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.Validate() != nil || decoded.Source.Position.Offset != math.MaxInt64-1 {
		t.Fatal("exact offset round trip", err)
	}
	if *DeriveSyncProgress(&decoded, "running", "none", true, 0).SourceCaptureBytes != 1 {
		t.Fatal("large offset subtraction")
	}
	clone := p.At(time.Now())
	clone.Local.Captured.Offset = 1
	clone.Source.Position.History = strings.Repeat("b", 40)
	if p.Local.Captured.Offset == 1 || p.Source.Position.History == clone.Source.Position.History {
		t.Fatal("clone shares position pointers")
	}
	future := p.At(p.Local.SampledAt.Add(-time.Second))
	if future.Local.AgeMillis != nil || future.Source.AgeMillis != nil {
		t.Fatal("future timestamps became fresh")
	}
	for _, mutate := range []func(*SyncProgress){
		func(p *SyncProgress) { p.Source.Position.History = "SECRET" }, func(p *SyncProgress) { p.Source.Error = "SECRET" }, func(p *SyncProgress) { p.Source.Error = "source_auth" }, func(p *SyncProgress) { p.Source.Position = nil }, func(p *SyncProgress) { p.Local.JournalLimitBytes = 0 }, func(p *SyncProgress) { p.Local.LiveJournalBytes = -1 }, func(p *SyncProgress) { p.Source.Position.Offset = math.MaxInt64 }, func(p *SyncProgress) { n := int64(-1); p.Source.AgeMillis = &n },
	} {
		bad := progressFixture()
		mutate(bad)
		if err := bad.Validate(); err == nil || strings.Contains(err.Error(), "SECRET") {
			t.Fatal("bad progress accepted or leaked", err)
		}
	}
}

func TestSyncProgressHeartbeatBudgetAndLegacy(t *testing.T) {
	v := validSyncerReport()
	v.SyncerID = strings.Repeat("s", 64)
	v.NodeID = strings.Repeat("n", 64)
	v.BootID = strings.Repeat("b", 64)
	v.BuildVersion = strings.Repeat("v", 128)
	v.Tasks = nil
	for i := 0; i < 8; i++ {
		p := progressFixture()
		n := uint64(math.MaxUint64)
		p.Local.AvailableBytes = &n
		v.Tasks = append(v.Tasks, SyncTaskReport{TaskID: strings.Repeat(string(rune('a'+i)), 64), IdentityDigest: strings.Repeat("a", 64), State: "running", LastError: "none", Running: true, Progress: p})
	}
	if err := v.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(v)
	if err != nil || len(raw) > 16384 {
		t.Fatal("bounded 8-task report no longer fits Server", len(raw), err)
	}
	legacy := validSyncerReport()
	if err := legacy.Validate(); err != nil {
		t.Fatal("legacy report without progress rejected", err)
	}
}
