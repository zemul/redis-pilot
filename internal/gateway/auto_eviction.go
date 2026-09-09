package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// These inspection commands must not refresh the business key's LRU clock.
// In particular, do not replace MEMORY USAGE with GET/STRLEN. Memory is a
// conservative candidate estimate; prepare still checks the actual value limit.
const autoEvictionCandidateScript = `
if redis.call('TYPE', KEYS[1]).ok ~= 'string' or redis.call('PTTL', KEYS[1]) ~= -1 then return -1 end
local idle = redis.call('OBJECT', 'IDLETIME', KEYS[1])
local size = redis.call('MEMORY', 'USAGE', KEYS[1])
if not idle or not size or idle < tonumber(ARGV[1]) or size < tonumber(ARGV[2]) or size > tonumber(ARGV[3]) then return -1 end
return idle
`

type evictionMemory struct{ Used, Excluded, Limit int64 }

func (m evictionMemory) ratio() float64 {
	return float64(max(int64(0), m.Used-m.Excluded)) / float64(m.Limit)
}

type evictionCandidate struct {
	key  string
	idle int64
}

// Testable scheduling boundary. The production executor below always enters
// the same gate / durable preparation / exact cold confirmation as manual evict.
type autoEvictionBackend interface {
	memory(context.Context) (evictionMemory, error)
	available(context.Context) error
	sample(context.Context, string) (string, []evictionCandidate, error)
	evict(context.Context, string) (bool, error)
}

type autoEviction struct {
	cfg                        AutoEvictionConfig
	backend                    autoEvictionBackend
	cursor                     string
	pressure                   bool // one background worker; not a persisted routing decision
	ratio, active, lastSuccess prometheus.Gauge
	cycles, keys               *prometheus.CounterVec
}

func newAutoEviction(c AutoEvictionConfig, backend autoEvictionBackend, registry *prometheus.Registry) *autoEviction {
	a := &autoEviction{cfg: c, backend: backend, cursor: "0"}
	a.ratio = prometheus.NewGauge(prometheus.GaugeOpts{Name: "redis_pilot_gateway_auto_eviction_memory_ratio", Help: "Redis eviction-accounted memory divided by maxmemory; last successful sample."})
	a.active = prometheus.NewGauge(prometheus.GaugeOpts{Name: "redis_pilot_gateway_auto_eviction_pressure", Help: "High watermark reached and low watermark not yet reached; not backend health."})
	a.lastSuccess = prometheus.NewGauge(prometheus.GaugeOpts{Name: "redis_pilot_gateway_auto_eviction_last_success_timestamp_seconds", Help: "Last successful automatic eviction attempt; zero before first success."})
	a.cycles = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "redis_pilot_gateway_auto_eviction_cycles_total", Help: "Bounded background batches by outcome."}, []string{"outcome"})
	a.keys = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "redis_pilot_gateway_auto_eviction_keys_total", Help: "Candidate operation results, not exact bytes freed."}, []string{"outcome"})
	registry.MustRegister(a.ratio, a.active, a.lastSuccess, a.cycles, a.keys)
	return a
}

func (a *autoEviction) run(ctx context.Context) {
	// Fixed delay AFTER completion prevents a slow batch from creating a busy
	// retry loop or accumulating work. Restart simply samples the current memory.
	timer := time.NewTimer(a.cfg.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		batch, cancel := context.WithTimeout(ctx, a.cfg.BatchTimeout)
		a.cycles.WithLabelValues(a.cycle(batch)).Inc()
		cancel()
		timer.Reset(a.cfg.Interval)
	}
}

func (a *autoEviction) observe(ctx context.Context) error {
	m, err := a.backend.memory(ctx)
	if err != nil || m.Limit <= 0 || m.Used < 0 || m.Excluded < 0 {
		return errConfirmationRPC
	}
	ratio := m.ratio()
	a.ratio.Set(ratio)
	if ratio >= a.cfg.HighWatermark {
		a.pressure = true
	}
	if ratio <= a.cfg.LowWatermark {
		a.pressure = false
	}
	if a.pressure {
		a.active.Set(1)
	} else {
		a.active.Set(0)
	}
	return nil
}

func (a *autoEviction) cycle(ctx context.Context) string {
	if a.observe(ctx) != nil {
		return "memory_unavailable"
	}
	if !a.pressure {
		return "below_watermark"
	}
	if a.backend.available(ctx) != nil {
		return "sync_or_cold_unavailable"
	}
	cursor, candidates, err := a.backend.sample(ctx, a.cursor)
	if err != nil {
		return "sample_unavailable"
	}
	a.cursor = cursor
	if len(candidates) == 0 {
		return "no_candidates"
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].idle > candidates[j].idle })
	for n, candidate := range candidates {
		if n >= a.cfg.MaxKeysPerBatch {
			break
		}
		if ctx.Err() != nil {
			return "timeout"
		}
		// Check before each key: UNLINK is asynchronous and other traffic can
		// change memory. Never infer reclaimed bytes from the candidate size.
		if a.observe(ctx) != nil {
			return "memory_unavailable"
		}
		if !a.pressure {
			return "low_watermark"
		}
		evicted, err := a.backend.evict(ctx, candidate.key)
		if err != nil {
			a.keys.WithLabelValues("deferred").Inc()
			return "operation_deferred"
		}
		if evicted {
			a.keys.WithLabelValues("completed").Inc()
			a.lastSuccess.SetToCurrentTime()
		} else {
			a.keys.WithLabelValues("skipped").Inc()
		}
	}
	return "batch_completed"
}

type redisAutoEviction struct {
	s   *Service
	cfg AutoEvictionConfig
}

func (b *redisAutoEviction) call(ctx context.Context, args ...string) ([]byte, error) {
	c, r, err := b.s.openBackend(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if err = writeAll(c, encodeCommand(args...)); err != nil {
		return nil, err
	}
	return r.ReadFrame()
}

func parseEvictionMemory(raw []byte) (evictionMemory, error) {
	value, err := residencyBulk(raw)
	if err != nil {
		return evictionMemory{}, err
	}
	var m evictionMemory
	fields := map[string]*int64{"used_memory": &m.Used, "mem_not_counted_for_evict": &m.Excluded, "maxmemory": &m.Limit}
	seen := make(map[string]bool, 3)
	for _, line := range strings.Split(string(value), "\r\n") {
		key, value, ok := strings.Cut(line, ":")
		if target, wanted := fields[key]; wanted && ok {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 || seen[key] {
				return m, errConfirmationRPC
			}
			*target, seen[key] = n, true
		}
	}
	if len(seen) != 3 || m.Limit <= 0 {
		return m, errConfirmationRPC
	}
	return m, nil
}

func (b *redisAutoEviction) memory(ctx context.Context) (evictionMemory, error) {
	raw, err := b.call(ctx, "INFO", "memory")
	if err != nil {
		return evictionMemory{}, err
	}
	return parseEvictionMemory(raw)
}

func (b *redisAutoEviction) available(ctx context.Context) error {
	h := b.s.preparation
	// Health/lag is only a scheduling hint. Never a replacement for the exact
	// receipt required by manualEvict after a new preparation marker.
	raw, err := b.call(ctx, "INFO", "replication")
	pos, code := parseSyncSourceInfo(raw)
	if err != nil || code != "none" || pos.History != h.options.Confirmation.Binding.History {
		return errConfirmationRPC
	}
	client := confirmationHTTPClient(h.options.Confirmation.Timeout)
	defer client.CloseIdleConnections()
	var session confirmationSession
	if confirmationCall(ctx, client, h.options.Confirmation.Endpoint, h.syncerToken, http.MethodGet, nil, &session) != nil ||
		session.Version != 1 || session.BindingSHA256 != evictionHash(h.options.Confirmation.Binding) || !canonicalEvictionHex(session.BootID, 16) {
		return errConfirmationRPC
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.options.Confirmation.Endpoint+confirmationRPCPath+"/status", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.syncerToken)
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var status confirmationLiveStatus
	d := json.NewDecoder(io.LimitReader(response.Body, confirmationRPCMaxBytes))
	if response.StatusCode != http.StatusOK || d.Decode(&status) != nil || !status.CapturedValid || !status.AppliedValid || status.CapturePaused || status.Barrier != "none" ||
		status.AppliedOffset < 0 || status.AppliedOffset > pos.Offset || pos.Offset-status.AppliedOffset > b.cfg.MaxSyncLagBytes {
		return errConfirmationRPC
	}
	target := syncColdTarget{address: h.options.Confirmation.Binding.Cold, passwordFile: h.options.ColdPasswordFile, timeout: h.options.Confirmation.Timeout, taskID: h.options.Confirmation.Binding.TaskID, evictionReceipt: true}
	c, closeCold, err := target.open(ctx)
	if err != nil {
		return err
	}
	defer closeCold()
	// The target initialization already checks connectivity, role and identity
	// prerequisites. PING still must return the expected protocol response.
	raw, err = c.request("PING")
	if err != nil || string(raw) != "+PONG\r\n" {
		return errConfirmationRPC
	}
	return nil
}

func parseEvictionScan(raw []byte, limits Limits, count int) (string, []string, error) {
	r, _ := NewReader(bytes.NewReader(raw), limits)
	take := func(want byte) bool { b, e := r.take(); return e == nil && b == want }
	if !take('*') {
		return "", nil, errConfirmationRPC
	}
	n, err := r.arrayCount(false)
	if err != nil || n != 2 || !take('$') {
		return "", nil, errConfirmationRPC
	}
	cursor, err := r.bulk(true)
	if err != nil {
		return "", nil, err
	}
	next := string(cursor)
	if _, err = strconv.ParseUint(next, 10, 64); err != nil || !take('*') {
		return "", nil, errConfirmationRPC
	}
	n, err = r.arrayCount(false)
	if err != nil {
		return "", nil, err
	}
	keys := make([]string, 0, min(n, count))
	seen := make(map[string]bool, min(n, count))
	for i := 0; i < n; i++ {
		if !take('$') {
			return "", nil, errConfirmationRPC
		}
		key, err := r.bulk(true)
		if err != nil {
			return "", nil, err
		}
		if len(keys) < count && len(key) <= 256 && !bytes.HasPrefix(key, []byte(syncMetadataPrefix)) && !seen[string(key)] {
			keys = append(keys, string(key))
			seen[string(key)] = true
		}
	}
	if len(r.raw) != len(raw) {
		return "", nil, errConfirmationRPC
	}
	return next, keys, nil
}

func (b *redisAutoEviction) eligible(ctx context.Context, key string) (int64, error) {
	return b.inspectCandidate(key, func(args ...string) ([]byte, error) { return b.call(ctx, args...) })
}

func (b *redisAutoEviction) inspectCandidate(key string, call func(...string) ([]byte, error)) (int64, error) {
	raw, err := call("EVAL", autoEvictionCandidateScript, "1", key, strconv.FormatInt(int64(b.cfg.MinIdle/time.Second), 10), strconv.Itoa(b.cfg.MinMemoryBytes), strconv.Itoa(syncPendingValueLimit))
	if err != nil {
		return -1, err
	}
	if len(raw) < 4 || raw[0] != ':' || !bytes.HasSuffix(raw, []byte("\r\n")) {
		return -1, errConfirmationRPC
	}
	return number(raw[1 : len(raw)-2])
}

func (b *redisAutoEviction) sample(ctx context.Context, cursor string) (string, []evictionCandidate, error) {
	c, r, err := b.s.openBackend(ctx)
	if err != nil {
		return "", nil, err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	call := func(args ...string) ([]byte, error) {
		if err := writeAll(c, encodeCommand(args...)); err != nil {
			return nil, err
		}
		return r.ReadFrame()
	}
	raw, err := call("SCAN", cursor, "COUNT", strconv.Itoa(b.cfg.SampleCount))
	if err != nil {
		return "", nil, err
	}
	next, keys, err := parseEvictionScan(raw, b.s.cfg.Limits, b.cfg.SampleCount)
	if err != nil {
		return "", nil, err
	}
	candidates := make([]evictionCandidate, 0, len(keys))
	for _, key := range keys {
		idle, err := b.inspectCandidate(key, call)
		if err != nil {
			return "", nil, err
		}
		if idle >= 0 {
			candidates = append(candidates, evictionCandidate{key, idle})
		}
	}
	return next, candidates, nil
}

func (b *redisAutoEviction) evict(ctx context.Context, key string) (bool, error) {
	h := b.s.preparation
	select {
	case h.gate <- struct{}{}:
		defer func() { <-h.gate }()
	default:
		return false, errKeyBarrierBusy
	}
	if h.closed || h.stopping.Load() || h.poisoned || h.request != nil {
		return false, errKeyBarrierBusy
	}
	ctx, cancel := context.WithTimeout(ctx, h.options.Confirmation.Timeout)
	defer cancel()
	// Recheck after acquiring the controller. A newly accessed key is skipped.
	idle, err := b.eligible(ctx, key)
	if err != nil || idle < 0 {
		return false, err
	}
	req := hotPreparationRequest{Key: []byte(key)}
	cold, err := h.assignOperationID(ctx, &req)
	if err != nil || cold {
		return false, err
	}
	err = h.manualEvict(ctx, req)
	return err == nil, err
}
