package gateway

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func syncRunnerMetricsGather(registry *prometheus.Registry) (map[string]float64, error) {
	families, err := registry.Gather()
	if err != nil {
		return nil, err
	}
	result := make(map[string]float64)
	for _, family := range families {
		if family.GetType() != dto.MetricType_GAUGE {
			return nil, fmt.Errorf("run-reset metric %s is not a gauge", family.GetName())
		}
		for _, sample := range family.Metric {
			var labels []string
			for _, label := range sample.Label {
				labels = append(labels, label.GetName()+"="+label.GetValue())
			}
			sort.Strings(labels)
			key := family.GetName() + "{" + strings.Join(labels, ",") + "}"
			if _, exists := result[key]; exists {
				return nil, errors.New("duplicate metric sample")
			}
			result[key] = sample.GetGauge().GetValue()
		}
	}
	return result, nil
}

func syncRunnerMetricKey(name string, labels ...string) string {
	sort.Strings(labels)
	return syncRunnerMetricPrefix + name + "{" + strings.Join(labels, ",") + "}"
}

func syncRunnerMetricsForTest(t *testing.T, runner *syncRunner) *prometheus.Registry {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(newSyncRunnerCollector(runner)); err != nil {
		t.Fatal(err)
	}
	return registry
}

func syncRunnerMetricsRead(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()
	samples, err := syncRunnerMetricsGather(registry)
	if err != nil {
		t.Fatal(err)
	}
	return samples
}

func requireSyncRunnerMetric(t *testing.T, samples map[string]float64, name string, value float64, labels ...string) {
	t.Helper()
	key := syncRunnerMetricKey(name, labels...)
	if got, ok := samples[key]; !ok || got != value {
		t.Fatalf("metric %s got=%v present=%v want=%v", key, got, ok, value)
	}
}

func absentSyncRunnerMetric(t *testing.T, samples map[string]float64, name string, labels ...string) {
	t.Helper()
	if _, ok := samples[syncRunnerMetricKey(name, labels...)]; ok {
		t.Fatal("unknown observation was exported as a numeric metric", name, labels)
	}
}

func syncRunnerMetricPosition(offset int64) syncPosition {
	return syncPosition{Source: "SECRET-SOURCE:12345", History: strings.Repeat("ac", 20), Offset: offset}
}

func TestSyncRunnerMetricsReadOnlySnapshot(t *testing.T) {
	started := time.Unix(1700000000, 125000000)
	runner := &syncRunner{status: syncRunnerStatus{
		State: "draining", LastError: "shutdown_incomplete", StopRequested: true,
		StartedAt: started, UpdatedAt: started.Add(time.Second), FinishedAt: started.Add(2 * time.Second),
		Captured: syncRunnerMetricPosition(1000), Applied: syncRunnerMetricPosition(950),
		Confirmed: syncRunnerMetricPosition(900), DrainTarget: syncRunnerMetricPosition(990),
		SourceAttempts: 7, SourceRetries: 3, ColdRetries: 4,
		ResourcesObserved: true, ResourceUpdatedAt: started.Add(3 * time.Second),
		Resources: syncResourceStatus{AvailableKnown: true, AvailableBytes: 4096, UnconfirmedBytes: 100, LiveBytes: 2048, Views: 2, DiskPaused: true},
	}}
	before := runner.snapshot()
	registry := syncRunnerMetricsForTest(t, runner)
	samples := syncRunnerMetricsRead(t, registry)
	if !reflect.DeepEqual(before, runner.snapshot()) {
		t.Fatal("scrape mutated runner or refreshed status")
	}
	requireSyncRunnerMetric(t, samples, "runner_present", 1)
	requireSyncRunnerMetric(t, samples, "state", 1, "state=draining")
	requireSyncRunnerMetric(t, samples, "last_error", 1, "reason=shutdown_incomplete")
	requireSyncRunnerMetric(t, samples, "stop_requested", 1)
	requireSyncRunnerMetric(t, samples, "timestamp_seconds", 1700000000.125, "event=started")
	requireSyncRunnerMetric(t, samples, "timestamp_seconds", 1700000001.125, "event=updated")
	requireSyncRunnerMetric(t, samples, "timestamp_seconds", 1700000002.125, "event=finished")
	requireSyncRunnerMetric(t, samples, "timestamp_seconds", 1700000003.125, "event=resources")
	requireSyncRunnerMetric(t, samples, "position_offset_bytes", 1000, "position=captured")
	requireSyncRunnerMetric(t, samples, "position_valid", 1, "position=applied")
	requireSyncRunnerMetric(t, samples, "lag_bytes", 50, "kind=captured_applied")
	requireSyncRunnerMetric(t, samples, "lag_bytes", 100, "kind=captured_confirmed")
	requireSyncRunnerMetric(t, samples, "lag_bytes", 40, "kind=drain_target_applied")
	requireSyncRunnerMetric(t, samples, "resources_observed", 1)
	requireSyncRunnerMetric(t, samples, "available_bytes_valid", 1)
	requireSyncRunnerMetric(t, samples, "resource_bytes", 4096, "kind=available")
	requireSyncRunnerMetric(t, samples, "resource_bytes", 2048, "kind=live")
	requireSyncRunnerMetric(t, samples, "resource_bytes", 100, "kind=unconfirmed")
	requireSyncRunnerMetric(t, samples, "open_views", 2)
	requireSyncRunnerMetric(t, samples, "capture_paused", 1, "reason=disk")
	requireSyncRunnerMetric(t, samples, "capture_paused", 0, "reason=backlog")
	requireSyncRunnerMetric(t, samples, "source_attempts", 7)
	requireSyncRunnerMetric(t, samples, "connection_retries", 3, "side=source")
	requireSyncRunnerMetric(t, samples, "connection_retries", 4, "side=cold")
	for key := range samples {
		if strings.Contains(key, "SECRET") || strings.Contains(key, before.Captured.History) {
			t.Fatal("identity or secret leaked as metric label")
		}
	}
	// A constructor may be used again for another explicit registry; it does
	// not register anything globally or start a listener in either case.
	_ = syncRunnerMetricsRead(t, syncRunnerMetricsForTest(t, runner))
}

func TestSyncRunnerMetricsUnknownAndReset(t *testing.T) {
	runner := &syncRunner{status: syncRunnerStatus{
		State: "SECRET-UNRECOGNIZED-STATE", LastError: "SECRET-REMOTE-ERROR", SourceAttempts: 12, SourceRetries: 5,
		StartedAt: time.Unix(1700000000, 0), UpdatedAt: time.Unix(1700000001, 0), FinishedAt: time.Unix(1700000002, 0),
		Captured: syncRunnerMetricPosition(500), Applied: syncRunnerMetricPosition(500), Confirmed: syncRunnerMetricPosition(500),
	}}
	registry := syncRunnerMetricsForTest(t, runner)
	samples := syncRunnerMetricsRead(t, registry)
	requireSyncRunnerMetric(t, samples, "state", 1, "state=unknown")
	requireSyncRunnerMetric(t, samples, "last_error", 1, "reason=unknown")
	for key := range samples {
		if strings.Contains(key, "SECRET") {
			t.Fatal("untrusted status became a label")
		}
	}
	stateCount, errorCount := 0, 0
	for key, value := range samples {
		if strings.HasPrefix(key, syncRunnerMetricPrefix+"state{") && value == 1 {
			stateCount++
		}
		if strings.HasPrefix(key, syncRunnerMetricPrefix+"last_error{") && value == 1 {
			errorCount++
		}
	}
	if stateCount != 1 || errorCount != 1 {
		t.Fatal("state/error metrics are not fixed one-hot values")
	}
	runner.update(func(s *syncRunnerStatus) {
		*s = syncRunnerStatus{State: "starting", StartedAt: time.Unix(1700000100, 0)}
	})
	samples = syncRunnerMetricsRead(t, registry)
	requireSyncRunnerMetric(t, samples, "state", 1, "state=starting")
	requireSyncRunnerMetric(t, samples, "last_error", 1, "reason=none")
	requireSyncRunnerMetric(t, samples, "source_attempts", 0)
	requireSyncRunnerMetric(t, samples, "connection_retries", 0, "side=source")
	requireSyncRunnerMetric(t, samples, "timestamp_valid", 0, "event=finished")
	absentSyncRunnerMetric(t, samples, "timestamp_seconds", "event=finished")
	requireSyncRunnerMetric(t, samples, "position_valid", 0, "position=captured")
	absentSyncRunnerMetric(t, samples, "position_offset_bytes", "position=captured")
	requireSyncRunnerMetric(t, samples, "lag_valid", 0, "kind=captured_applied")
	absentSyncRunnerMetric(t, samples, "lag_bytes", "kind=captured_applied")
}

func TestSyncRunnerMetricsShutdownAborted(t *testing.T) {
	runner := &syncRunner{status: syncRunnerStatus{State: "failed", LastError: "shutdown_aborted"}}
	samples := syncRunnerMetricsRead(t, syncRunnerMetricsForTest(t, runner))
	requireSyncRunnerMetric(t, samples, "last_error", 1, "reason=shutdown_aborted")
	requireSyncRunnerMetric(t, samples, "last_error", 0, "reason=shutdown_timeout")
	requireSyncRunnerMetric(t, samples, "last_error", 0, "reason=unknown")
}

func TestSyncRunnerMetricsPositionValidity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*syncRunnerStatus)
	}{
		{"missing-captured", func(s *syncRunnerStatus) { s.Captured = syncPosition{} }},
		{"missing-applied", func(s *syncRunnerStatus) { s.Applied = syncPosition{} }},
		{"different-source", func(s *syncRunnerStatus) { s.Applied.Source = "OTHER-SOURCE" }},
		{"different-history", func(s *syncRunnerStatus) { s.Applied.History = strings.Repeat("b", 40) }},
		{"reversed-offsets", func(s *syncRunnerStatus) { s.Applied.Offset = 101 }},
		{"negative-offset", func(s *syncRunnerStatus) { s.Applied.Offset = -1 }},
		{"overflow-offset", func(s *syncRunnerStatus) { s.Captured.Offset = math.MaxInt64 }},
		{"invalid-history", func(s *syncRunnerStatus) { s.Applied.History = "invalid" }},
		{"invalid-source", func(s *syncRunnerStatus) { s.Applied.Source = "bad\xff" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := syncRunnerStatus{Captured: syncRunnerMetricPosition(100), Applied: syncRunnerMetricPosition(90)}
			tc.mutate(&status)
			samples := syncRunnerMetricsRead(t, syncRunnerMetricsForTest(t, &syncRunner{status: status}))
			requireSyncRunnerMetric(t, samples, "lag_valid", 0, "kind=captured_applied")
			absentSyncRunnerMetric(t, samples, "lag_bytes", "kind=captured_applied")
		})
	}
	// Real offset zero in a known lineage is valid; all-zero/missing identity
	// is not. Export zero lag only for the former.
	knownZero := &syncRunner{status: syncRunnerStatus{Captured: syncRunnerMetricPosition(0), Applied: syncRunnerMetricPosition(0)}}
	samples := syncRunnerMetricsRead(t, syncRunnerMetricsForTest(t, knownZero))
	requireSyncRunnerMetric(t, samples, "position_valid", 1, "position=captured")
	requireSyncRunnerMetric(t, samples, "position_offset_bytes", 0, "position=captured")
	requireSyncRunnerMetric(t, samples, "lag_valid", 1, "kind=captured_applied")
	requireSyncRunnerMetric(t, samples, "lag_bytes", 0, "kind=captured_applied")
}

func TestSyncRunnerMetricsMissingResourcesAndNilRunner(t *testing.T) {
	for _, runner := range []*syncRunner{nil, {}, {status: syncRunnerStatus{ResourcesObserved: true, Resources: syncResourceStatus{DiskPaused: true}}}} {
		samples := syncRunnerMetricsRead(t, syncRunnerMetricsForTest(t, runner))
		requireSyncRunnerMetric(t, samples, "available_bytes_valid", 0)
		absentSyncRunnerMetric(t, samples, "resource_bytes", "kind=available")
		requireSyncRunnerMetric(t, samples, "lag_valid", 0, "kind=captured_applied")
		absentSyncRunnerMetric(t, samples, "lag_bytes", "kind=captured_applied")
		if runner == nil {
			requireSyncRunnerMetric(t, samples, "runner_present", 0)
			requireSyncRunnerMetric(t, samples, "state", 1, "state=unknown")
		}
	}
	// A successful sample of genuinely zero available bytes must remain
	// distinguishable from the failed or never-performed sample above.
	runner := &syncRunner{status: syncRunnerStatus{ResourcesObserved: true, Resources: syncResourceStatus{AvailableKnown: true}}}
	samples := syncRunnerMetricsRead(t, syncRunnerMetricsForTest(t, runner))
	requireSyncRunnerMetric(t, samples, "available_bytes_valid", 1)
	requireSyncRunnerMetric(t, samples, "resource_bytes", 0, "kind=available")
}

func TestSyncRunnerMetricsConcurrentScrapeAndReset(t *testing.T) {
	runner := &syncRunner{status: syncRunnerStatus{State: "starting"}}
	registry := syncRunnerMetricsForTest(t, runner)
	stop, writerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(writerDone)
		for generation := int64(0); ; generation++ {
			select {
			case <-stop:
				return
			default:
			}
			runner.update(func(s *syncRunnerStatus) {
				*s = syncRunnerStatus{State: "starting"}
				if generation%2 == 0 {
					*s = syncRunnerStatus{State: "running", Captured: syncRunnerMetricPosition(generation + 100), Applied: syncRunnerMetricPosition(generation + 100), SourceAttempts: int(generation + 100)}
				}
			})
			runtime.Gosched()
		}
	}()
	var readers sync.WaitGroup
	errorsFound := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 100; i++ {
				samples, err := syncRunnerMetricsGather(registry)
				if err != nil {
					errorsFound <- err
					return
				}
				captured, present := samples[syncRunnerMetricKey("position_offset_bytes", "position=captured")]
				if samples[syncRunnerMetricKey("state", "state=running")] == 1 {
					lag, lagPresent := samples[syncRunnerMetricKey("lag_bytes", "kind=captured_applied")]
					if !present || captured != samples[syncRunnerMetricKey("source_attempts")] || !lagPresent || lag != 0 {
						errorsFound <- errors.New("scrape combined different runner snapshots")
						return
					}
				} else if present || samples[syncRunnerMetricKey("source_attempts")] != 0 || samples[syncRunnerMetricKey("position_valid", "position=captured")] != 0 {
					errorsFound <- errors.New("reset retained old positions or attempts")
					return
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	<-writerDone
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}
