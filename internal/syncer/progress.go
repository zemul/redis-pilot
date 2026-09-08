package syncer

import (
	"context"
	"errors"
	mathrand "math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Explicit opt-in: these read-only source probes require AUTH/INFO permissions
// and consume connections. They are never run during status GET or check-config.
type ProgressConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

func (c Config) validateProgress() error {
	if !c.Progress.Enabled {
		return nil
	}
	if len(c.Tasks) == 0 || c.Progress.Interval < time.Second || c.Progress.Interval > 10*time.Second || c.Progress.Timeout <= 0 || c.Progress.Timeout > 2*time.Second {
		return errors.New("progress sampling requires named tasks, interval 1s..10s and timeout (0,2s]")
	}
	return nil
}

type progressSampler interface {
	SampleProgress(context.Context, time.Duration) apitypes.SyncProgress
}

// One bounded sampler per task, independent from reporter and replication loop.
// A failed task stops sampling; cancellation closes probes and joins all workers.
func (s *Service) startProgress(ctx context.Context) func() {
	if !s.cfg.Progress.Enabled {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i, entry := range s.tasks {
		wg.Add(1)
		go func(index int, sampler progressSampler) {
			defer wg.Done()
			for ctx.Err() == nil {
				s.mu.Lock()
				finished := s.tasks[index].finished
				s.mu.Unlock()
				if finished {
					return
				}
				sample := sampler.SampleProgress(ctx, s.cfg.Progress.Timeout)
				if sample.Validate() != nil {
					sample = apitypes.SyncProgress{Source: apitypes.SyncSourceProgress{SampledAt: time.Now(), Error: "invalid_response"}}
				}
				s.mu.Lock()
				s.progress[index] = &sample
				s.mu.Unlock()
				timer := time.NewTimer(s.cfg.Progress.Interval + time.Duration(mathrand.Int64N(int64(s.cfg.Progress.Interval/5)+1)))
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}(i, entry.task.(progressSampler))
	}
	return func() { cancel(); wg.Wait() }
}

var progressConditions = []string{"unknown", "stopped", "needs_attention", "cold_unavailable", "source_unavailable", "resource_pressure", "stopping", "invalid_observation", "observation_stale", "source_observation_error", "initializing", "history_mismatch", "samples_not_aligned", "journal_budget_warning", "catching_up", "sample_aligned"}

type progressCollector struct {
	service                    *Service
	condition, lag, known, age *prometheus.Desc
}

func newProgressCollector(s *Service) *progressCollector {
	return &progressCollector{service: s,
		condition: prometheus.NewDesc("redis_pilot_syncer_progress_condition", "One-hot sampled progress condition; sample_aligned is not current cold health or eviction permission.", []string{"task_id", "condition"}, nil),
		lag:       prometheus.NewDesc("redis_pilot_syncer_progress_lag_bytes", "Same-history sampled byte gaps, omitted when unknown. Captured_gc is persisted log-GC confirmation, not exact cold apply lag.", []string{"task_id", "kind"}, nil),
		known:     prometheus.NewDesc("redis_pilot_syncer_progress_lag_known", "Whether the corresponding sampled byte gap can be compared; zero is unknown, not caught up.", []string{"task_id", "kind"}, nil),
		age:       prometheus.NewDesc("redis_pilot_syncer_progress_sample_age_seconds", "Age of the cached sample; not time-based replication lag.", []string{"task_id", "side"}, nil),
	}
}

func (c *progressCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.condition
	ch <- c.lag
	ch <- c.known
	ch <- c.age
}

func (c *progressCollector) Collect(ch chan<- prometheus.Metric) {
	status := c.service.Status()
	metric := func(d *prometheus.Desc, n float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, n, labels...)
	}
	for _, task := range status.Tasks {
		view := task.ProgressView
		if view == nil {
			continue
		}
		for _, label := range progressConditions {
			n := 0.
			if label == view.Condition {
				n = 1
			}
			metric(c.condition, n, task.TaskID, label)
		}
		for _, gap := range []struct {
			name string
			n    *int64
		}{{"source_capture", view.SourceCaptureBytes}, {"captured_applied", view.CapturedAppliedBytes}, {"captured_gc", view.CapturedGCBytes}} {
			known := 0.
			if gap.n != nil {
				known = 1
				metric(c.lag, float64(*gap.n), task.TaskID, gap.name)
			}
			metric(c.known, known, task.TaskID, gap.name)
		}
		if p := task.Progress; p != nil {
			if p.Source.AgeMillis != nil {
				metric(c.age, float64(*p.Source.AgeMillis)/1000, task.TaskID, "source")
			}
			if p.Local != nil && p.Local.AgeMillis != nil {
				metric(c.age, float64(*p.Local.AgeMillis)/1000, task.TaskID, "local")
			}
		}
	}
}
