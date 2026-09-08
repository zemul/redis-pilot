package apitypes

import (
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"time"
)

// Covers the bounded source-sampling and reporting cadences. Independent from
// heartbeat freshness: repeated reports do not refresh a cached sample's age.
const SyncProgressStaleAfter = 45 * time.Second
const SyncProgressReportAllowance = 3 * time.Second

// Byte positions are exact decimal strings on JSON APIs, never JS floats.
// Histories identify replication streams, not task ownership or write leases.
type SyncProgressPosition struct {
	History string `json:"history"`
	Offset  int64  `json:"offset,string"`
}

type SyncLocalProgress struct {
	SampledAt         time.Time             `json:"sampled_at"`
	AgeMillis         *int64                `json:"age_ms,omitempty"`
	Captured          *SyncProgressPosition `json:"captured,omitempty"`
	Applied           *SyncProgressPosition `json:"applied,omitempty"`
	GCConfirmed       *SyncProgressPosition `json:"gc_confirmed,omitempty"`
	LiveJournalBytes  int64                 `json:"live_journal_bytes,string"`
	JournalLimitBytes int64                 `json:"journal_limit_bytes,string"`
	AvailableBytes    *uint64               `json:"available_bytes,string,omitempty"`
	DiskPaused        bool                  `json:"disk_paused"`
	BacklogPaused     bool                  `json:"backlog_paused"`
}

type SyncSourceProgress struct {
	SampledAt time.Time             `json:"sampled_at"`
	AgeMillis *int64                `json:"age_ms,omitempty"`
	Position  *SyncProgressPosition `json:"position,omitempty"`
	Error     string                `json:"error"` // fixed probe category, never a Redis response
}

// Local is snapshotted BEFORE sampling Source to avoid subtracting a new local
// position from an older source head. It is an observation, not an atomic barrier.
type SyncProgress struct {
	Local  *SyncLocalProgress `json:"local,omitempty"`
	Source SyncSourceProgress `json:"source"`
}

type SyncProgressView struct {
	Condition            string `json:"condition"`
	SourceCaptureBytes   *int64 `json:"source_capture_bytes,string,omitempty"`
	CapturedAppliedBytes *int64 `json:"captured_applied_bytes,string,omitempty"`
	CapturedGCBytes      *int64 `json:"captured_gc_bytes,string,omitempty"`
	JournalBudgetWarning bool   `json:"journal_budget_warning"`
}

func validProgressPosition(p *SyncProgressPosition) bool {
	if p == nil {
		return true
	}
	b, err := hex.DecodeString(p.History)
	return err == nil && len(b) == 20 && strings.ToLower(p.History) == p.History && p.Offset >= 0 && p.Offset < math.MaxInt64
}

func validProgressAge(age *int64) bool { return age == nil || *age >= 0 }

func (p *SyncProgress) Validate() error {
	if p == nil {
		return nil
	}
	invalid := errors.New("invalid sync progress observation")
	if p.Source.SampledAt.IsZero() || !validProgressAge(p.Source.AgeMillis) || !validProgressPosition(p.Source.Position) {
		return invalid
	}
	switch p.Source.Error {
	case "none":
		if p.Source.Position == nil {
			return invalid
		}
	case "source_unavailable", "source_auth", "invalid_response", "not_master", "credential_file", "canceled":
		if p.Source.Position != nil {
			return invalid
		}
	default:
		return invalid
	}
	if l := p.Local; l != nil {
		if l.SampledAt.IsZero() || !validProgressAge(l.AgeMillis) || !validProgressPosition(l.Captured) || !validProgressPosition(l.Applied) || !validProgressPosition(l.GCConfirmed) || l.LiveJournalBytes < 0 || l.JournalLimitBytes < 1 || l.JournalLimitBytes > 1<<30 {
			return invalid
		}
	}
	return nil
}

func progressAge(now, at time.Time) *int64 {
	if at.IsZero() || at.After(now) {
		return nil
	}
	ms := now.Sub(at).Milliseconds()
	return &ms
}

// Clone and refresh ages on the reporting worker, preserving monotonic time
// where available. Server must add elapsed time since receipt, not compare clocks.
func (p *SyncProgress) At(now time.Time) *SyncProgress {
	if p == nil {
		return nil
	}
	copy := *p
	copy.Source.AgeMillis = progressAge(now, p.Source.SampledAt)
	if p.Source.Position != nil {
		v := *p.Source.Position
		copy.Source.Position = &v
	}
	if p.Local != nil {
		l := *p.Local
		l.AgeMillis = progressAge(now, l.SampledAt)
		if l.Captured != nil {
			v := *l.Captured
			l.Captured = &v
		}
		if l.Applied != nil {
			v := *l.Applied
			l.Applied = &v
		}
		if l.GCConfirmed != nil {
			v := *l.GCConfirmed
			l.GCConfirmed = &v
		}
		if l.AvailableBytes != nil {
			v := *l.AvailableBytes
			l.AvailableBytes = &v
		}
		copy.Local = &l
	}
	return &copy
}

func freshProgress(age *int64, elapsed time.Duration) bool {
	limit := SyncProgressStaleAfter.Milliseconds()
	return age != nil && *age >= 0 && *age < limit && elapsed >= 0 && elapsed < SyncProgressStaleAfter && *age < limit-elapsed.Milliseconds()
}

// DeriveSyncProgress is deliberately conservative: stale, failed, uncertain or
// incomparable observations omit lag, never manufacture a zero. Equal positions
// mean only equality in this sample, not current cold health or eviction safety.
func DeriveSyncProgress(p *SyncProgress, state, reason string, running bool, elapsed time.Duration) SyncProgressView {
	v := SyncProgressView{Condition: "unknown"}
	if !running {
		if state == "failed" {
			v.Condition = "needs_attention"
		} else {
			v.Condition = "stopped"
		}
		return v
	}
	switch reason {
	case "history_gap", "cold_outcome_uncertain", "cold_state_mismatch", "journal_corrupt", "task_identity", "unsupported_record":
		v.Condition = "needs_attention"
		return v
	}
	if state == "failed" {
		v.Condition = "needs_attention"
		return v
	}
	if state == "retry_cold" || reason == "cold_connection" {
		v.Condition = "cold_unavailable"
		return v
	}
	if state == "retry_source" || reason == "source_transport" {
		v.Condition = "source_unavailable"
		return v
	}
	if state == "pressure" || reason == "journal_capacity" || reason == "resource_pressure" {
		v.Condition = "resource_pressure"
		return v
	}
	if state == "draining" {
		v.Condition = "stopping"
		return v
	}
	if reason != "" && reason != "none" {
		v.Condition = "needs_attention"
		return v
	}
	if state == "unknown" {
		return v
	}
	if p == nil {
		return v
	}
	if p.Validate() != nil {
		v.Condition = "invalid_observation"
		return v
	}
	if !freshProgress(p.Source.AgeMillis, elapsed) || (p.Local != nil && !freshProgress(p.Local.AgeMillis, elapsed)) {
		v.Condition = "observation_stale"
		return v
	}
	if p.Source.Error != "none" {
		v.Condition = "source_observation_error"
		if p.Source.Error == "source_unavailable" || p.Source.Error == "canceled" {
			v.Condition = "source_unavailable"
		}
		return v
	}
	l := p.Local
	if l == nil || l.Captured == nil || l.Applied == nil {
		v.Condition = "initializing"
		return v
	}
	v.JournalBudgetWarning = l.LiveJournalBytes >= l.JournalLimitBytes-l.JournalLimitBytes/5
	if l.DiskPaused || l.BacklogPaused {
		v.Condition = "resource_pressure"
		return v
	}
	a, b, c := p.Source.Position, l.Captured, l.Applied
	if a.History != b.History || b.History != c.History || (l.GCConfirmed != nil && l.GCConfirmed.History != b.History) {
		v.Condition = "history_mismatch"
		return v
	}
	if a.Offset < b.Offset || b.Offset < c.Offset || (l.GCConfirmed != nil && l.GCConfirmed.Offset > c.Offset) {
		v.Condition = "samples_not_aligned"
		return v
	}
	x, y := a.Offset-b.Offset, b.Offset-c.Offset
	v.SourceCaptureBytes, v.CapturedAppliedBytes = &x, &y
	if l.GCConfirmed != nil {
		z := b.Offset - l.GCConfirmed.Offset
		v.CapturedGCBytes = &z
	}
	switch {
	case v.JournalBudgetWarning:
		v.Condition = "journal_budget_warning"
	case x > 0 || y > 0:
		v.Condition = "catching_up"
	default:
		v.Condition = "sample_aligned"
	}
	return v
}
