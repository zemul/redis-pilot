package gateway

// Isolated observer only: no default registration, server, goroutine, journal
// access or network I/O. The caller owns both the runner and its registry.

import (
	"math"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
)

const syncRunnerMetricPrefix = "redis_pilot_gateway_sync_"

var syncRunnerMetricStates = []string{
	"idle", "starting", "applying", "running", "retry_source", "retry_cold",
	"pressure", "draining", "drained", "stopped", "failed", "unknown",
}

var syncRunnerMetricErrors = []string{
	"none", "retry_exhausted", "pressure_timeout", "history_gap",
	"cold_outcome_uncertain", "cold_state_mismatch", "journal_corrupt",
	"journal_capacity", "resource_pressure", "source_transport", "cold_connection",
	"unsupported_record", "canceled", "unclassified_failure", "shutdown_timeout",
	"shutdown_incomplete", "shutdown_aborted", "unknown",
	"credential_file", "journal_open", "journal_close",
	"task_identity",
}

type syncRunnerCollector struct {
	runner                                                          *syncRunner
	descriptors                                                     []*prometheus.Desc
	present, state, lastError, timestamp, timestampValid            *prometheus.Desc
	stopRequested, position, positionValid, lag, lagValid           *prometheus.Desc
	resourcesObserved, availableValid, resourceBytes, views, paused *prometheus.Desc
	sourceAttempts, retries                                         *prometheus.Desc
}

// A nil runner is deliberately observable as runner_present=0, rather than a
// panic or a fabricated healthy/caught-up state. Registration is always explicit.
func newSyncRunnerCollector(r *syncRunner) prometheus.Collector {
	c := &syncRunnerCollector{runner: r}
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		d := prometheus.NewDesc(syncRunnerMetricPrefix+name, help, labels, nil)
		c.descriptors = append(c.descriptors, d)
		return d
	}
	c.present = desc("runner_present", "Whether this collector was given a runner; zero is not a healthy or caught-up runner.")
	c.state = desc("state", "One-hot cached runner state; empty before first run is idle, unrecognized values are unknown.", "state")
	c.lastError = desc("last_error", "One-hot fixed cached error category; unrecognized text is unknown and is never exposed as a label.", "reason")
	c.timestamp = desc("timestamp_seconds", "Unix seconds of a recorded lifecycle/resource observation event; omitted when unknown.", "event")
	c.timestampValid = desc("timestamp_valid", "Whether the matching event timestamp was recorded; missing timestamps are not epoch zero.", "event")
	c.stopRequested = desc("stop_requested", "Whether graceful stop was requested for the current or most recent run.")
	c.position = desc("position_offset_bytes", "Recorded inclusive source byte offset, emitted only for a valid position; offsets are Prometheus floating-point samples, not recovery checkpoints.", "position")
	c.positionValid = desc("position_valid", "Whether the cached position has a valid identity and offset; identity strings are never labels.", "position")
	c.lag = desc("lag_bytes", "Difference between same-lineage ordered positions; omitted when unknown or inconsistent. captured_confirmed is persisted GC confirmation lag, not exact cold apply lag.", "kind")
	c.lagValid = desc("lag_valid", "Whether both endpoints are valid, share source/history and have ordered offsets; zero does not mean caught up.", "kind")
	c.resourcesObserved = desc("resources_observed", "Whether a resource observation has been recorded; this collector never samples disk or journal state.")
	c.availableValid = desc("available_bytes_valid", "Whether the recorded filesystem available-byte sample succeeded; failed/unperformed samples are not zero free space.")
	c.resourceBytes = desc("resource_bytes", "Cached resource quantities: available is sampled filesystem space, live is referenced file lengths, unconfirmed is source backlog since persisted confirmation; not exact physical journal use or cold apply lag.", "kind")
	c.views = desc("open_views", "Open journal views in the last recorded resource observation, not a live filesystem query.")
	c.paused = desc("capture_paused", "Cached capture-admission pause flags by fixed resource reason, emitted only after a resource observation.", "reason")
	c.sourceAttempts = desc("source_attempts", "Source connection attempts in this run; a gauge because each new run resets it.")
	c.retries = desc("connection_retries", "Connection retries in this run by side; gauges because each new run resets them.", "side")
	return c
}

func (c *syncRunnerCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descriptors {
		ch <- desc
	}
}

func syncRunnerFixedMetricLabel(value, empty string, allowed []string) string {
	if value == "" {
		return empty
	}
	for _, label := range allowed {
		if value == label {
			return label
		}
	}
	return "unknown"
}

func validSyncMetricPosition(p syncPosition) bool {
	return p.Source != "" && len(p.Source) <= 256 && utf8.ValidString(p.Source) && validSyncHistory(p.History) && p.Offset >= 0 && p.Offset < math.MaxInt64
}

func (c *syncRunnerCollector) Collect(ch chan<- prometheus.Metric) {
	// Exactly one locked copy per scrape. All metrics below are derived from
	// that immutable copy, including when another goroutine resets the runner.
	var status syncRunnerStatus
	present := c.runner != nil
	if present {
		status = c.runner.snapshot()
	}
	metric := func(desc *prometheus.Desc, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
	}
	boolean := func(desc *prometheus.Desc, value bool, labels ...string) {
		n := float64(0)
		if value {
			n = 1
		}
		metric(desc, n, labels...)
	}
	boolean(c.present, present)
	state := syncRunnerFixedMetricLabel(status.State, "idle", syncRunnerMetricStates)
	if !present {
		state = "unknown"
	}
	for _, label := range syncRunnerMetricStates {
		boolean(c.state, label == state, label)
	}
	lastError := syncRunnerFixedMetricLabel(status.LastError, "none", syncRunnerMetricErrors)
	for _, label := range syncRunnerMetricErrors {
		boolean(c.lastError, label == lastError, label)
	}
	for _, item := range []struct {
		name     string
		at       time.Time
		observed bool
	}{
		{"started", status.StartedAt, present}, {"updated", status.UpdatedAt, present},
		{"finished", status.FinishedAt, present}, {"resources", status.ResourceUpdatedAt, present && status.ResourcesObserved},
	} {
		valid := item.observed && !item.at.IsZero()
		boolean(c.timestampValid, valid, item.name)
		if valid {
			metric(c.timestamp, float64(item.at.Unix())+float64(item.at.Nanosecond())/1e9, item.name)
		}
	}
	boolean(c.stopRequested, present && status.StopRequested)
	for _, item := range []struct {
		name     string
		position syncPosition
	}{
		{"captured", status.Captured}, {"applied", status.Applied}, {"confirmed", status.Confirmed}, {"drain_target", status.DrainTarget},
	} {
		valid := present && validSyncMetricPosition(item.position)
		boolean(c.positionValid, valid, item.name)
		if valid {
			metric(c.position, float64(item.position.Offset), item.name)
		}
	}
	for _, item := range []struct {
		name         string
		newer, older syncPosition
	}{
		{"captured_applied", status.Captured, status.Applied},
		{"captured_confirmed", status.Captured, status.Confirmed},
		{"drain_target_applied", status.DrainTarget, status.Applied},
	} {
		valid := present && validSyncMetricPosition(item.newer) && validSyncMetricPosition(item.older) &&
			item.newer.Source == item.older.Source && item.newer.History == item.older.History && item.newer.Offset >= item.older.Offset
		boolean(c.lagValid, valid, item.name)
		if valid {
			metric(c.lag, float64(item.newer.Offset-item.older.Offset), item.name)
		}
	}
	observed := present && status.ResourcesObserved
	boolean(c.resourcesObserved, observed)
	boolean(c.availableValid, observed && status.Resources.AvailableKnown)
	if observed {
		if status.Resources.AvailableKnown {
			metric(c.resourceBytes, float64(status.Resources.AvailableBytes), "available")
		}
		metric(c.resourceBytes, float64(status.Resources.LiveBytes), "live")
		metric(c.resourceBytes, float64(status.Resources.UnconfirmedBytes), "unconfirmed")
		metric(c.views, float64(status.Resources.Views))
		boolean(c.paused, status.Resources.DiskPaused, "disk")
		boolean(c.paused, status.Resources.BacklogPaused, "backlog")
	}
	metric(c.sourceAttempts, float64(status.SourceAttempts))
	metric(c.retries, float64(status.SourceRetries), "source")
	metric(c.retries, float64(status.ColdRetries), "cold")
}
