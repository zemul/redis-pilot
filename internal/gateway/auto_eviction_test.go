package gateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type autoEvictionFake struct {
	used                      []int64
	reads                     int
	availableErr, evictionErr error
	candidates                []evictionCandidate
	evicted                   []string
	samples                   int
}

func (f *autoEvictionFake) memory(context.Context) (evictionMemory, error) {
	v := f.used[min(f.reads, len(f.used)-1)]
	f.reads++
	return evictionMemory{Used: v, Limit: 100}, nil
}
func (f *autoEvictionFake) available(context.Context) error { return f.availableErr }
func (f *autoEvictionFake) sample(context.Context, string) (string, []evictionCandidate, error) {
	f.samples++
	return "7", append([]evictionCandidate(nil), f.candidates...), nil
}
func (f *autoEvictionFake) evict(_ context.Context, key string) (bool, error) {
	f.evicted = append(f.evicted, key)
	return f.evictionErr == nil, f.evictionErr
}

func TestAutoEvictionHysteresisAndBounds(t *testing.T) {
	c := (&AutoEvictionConfig{Enabled: true, MaxKeysPerBatch: 2}).resolved()
	f := &autoEvictionFake{used: []int64{79, 81, 78, 70, 75, 85, 84, 83}, candidates: []evictionCandidate{{"newer", 10}, {"oldest", 90}, {"middle", 30}}}
	a := newAutoEviction(c, f, prometheus.NewRegistry())
	if got := a.cycle(context.Background()); got != "below_watermark" || f.samples != 0 {
		t.Fatal(got)
	}
	if got := a.cycle(context.Background()); got != "low_watermark" || !reflect.DeepEqual(f.evicted, []string{"oldest"}) {
		t.Fatal(got, f.evicted)
	}
	if got := a.cycle(context.Background()); got != "below_watermark" {
		t.Fatal(got)
	}
	if got := a.cycle(context.Background()); got != "batch_completed" || !reflect.DeepEqual(f.evicted, []string{"oldest", "oldest", "middle"}) {
		t.Fatal(got, f.evicted)
	}
	if a.cursor != "7" {
		t.Fatal(a.cursor)
	}
}

func TestAutoEvictionOutageRecoversAndStopsBatch(t *testing.T) {
	f := &autoEvictionFake{used: []int64{90}, availableErr: errors.New("cold down"), candidates: []evictionCandidate{{"one", 60}, {"two", 50}}}
	a := newAutoEviction((&AutoEvictionConfig{Enabled: true}).resolved(), f, prometheus.NewRegistry())
	if a.cycle(context.Background()) != "sync_or_cold_unavailable" || f.samples != 0 || len(f.evicted) != 0 {
		t.Fatal("outage dispatched work")
	}
	f.availableErr = nil
	f.evictionErr = errors.New("lost reply")
	if a.cycle(context.Background()) != "operation_deferred" || len(f.evicted) != 1 {
		t.Fatal("batch continued after uncertainty")
	}
	f.evictionErr = nil
	if a.cycle(context.Background()) != "batch_completed" || len(f.evicted) != 3 {
		t.Fatal("did not resume")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if a.cycle(ctx) != "timeout" || len(f.evicted) != 3 {
		t.Fatal("cancel dispatched eviction")
	}
}

func TestAutoEvictionConfig(t *testing.T) {
	for _, mutate := range []func(*AutoEvictionConfig){
		func(c *AutoEvictionConfig) { c.HighWatermark = math.NaN() }, func(c *AutoEvictionConfig) { c.LowWatermark = .9 },
		func(c *AutoEvictionConfig) { c.Interval = time.Millisecond }, func(c *AutoEvictionConfig) { c.BatchTimeout = time.Minute },
		func(c *AutoEvictionConfig) { c.SampleCount = 257 }, func(c *AutoEvictionConfig) { c.MaxKeysPerBatch = 300 },
		func(c *AutoEvictionConfig) { c.MinMemoryBytes = 1 }, func(c *AutoEvictionConfig) { c.MinIdle = -time.Second }, func(c *AutoEvictionConfig) { c.MaxSyncLagBytes = -1 },
	} {
		c := &AutoEvictionConfig{Enabled: true}
		mutate(c)
		if c.validate() == nil {
			t.Fatal("invalid config accepted", c)
		}
	}
	c := hotColdTestConfig(t)
	s, err := NewService(c)
	if err != nil || s.autoEviction != nil {
		t.Fatal("manual mode changed", err)
	}
	c.HotCold.AutoEviction = &AutoEvictionConfig{Enabled: true}
	s, err = NewService(c)
	if err != nil || s.autoEviction == nil {
		t.Fatal("automatic mode not wired", err)
	}
	c.BufferBudgetBytes = 2*c.workspaceBytes() + c.probeWorkspaceBytes()
	if c.Validate() == nil {
		t.Fatal("missing background memory reservation accepted")
	}
}

func TestAutoEvictionCancelAndBusyGate(t *testing.T) {
	f := &autoEvictionFake{used: []int64{90}}
	a := newAutoEviction((&AutoEvictionConfig{Enabled: true, Interval: time.Hour}).resolved(), f, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop")
	}
	if f.reads != 0 {
		t.Fatal("cancelled scheduler started work")
	}
	h := &hotPreparation{gate: make(chan struct{}, 1)}
	b := &redisAutoEviction{s: &Service{preparation: h}}
	h.gate <- struct{}{}
	if evicted, err := b.evict(context.Background(), "key"); evicted || err != errKeyBarrierBusy {
		t.Fatal("manual gate not respected")
	}
	<-h.gate
	h.request = &hotPreparationRequest{Key: []byte("other")}
	if evicted, err := b.evict(context.Background(), "key"); evicted || err != errKeyBarrierBusy {
		t.Fatal("pending operation not respected")
	}
	h.request = nil
	h.stopping.Store(true)
	if evicted, err := b.evict(context.Background(), "key"); evicted || err != errKeyBarrierBusy {
		t.Fatal("stopping controller accepted work")
	}
}

func TestAutoEvictionMemoryAndScan(t *testing.T) {
	bulk := func(s string) []byte { return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)) }
	m, err := parseEvictionMemory(bulk("used_memory:90\r\nmem_not_counted_for_evict:20\r\nmaxmemory:100\r\n"))
	if err != nil || m.ratio() != .7 {
		t.Fatal(m, err)
	}
	for _, body := range []string{"used_memory:1\r\nmaxmemory:100\r\n", "used_memory:90\r\nmem_not_counted_for_evict:0\r\nmaxmemory:0\r\n", "used_memory:-1\r\nmem_not_counted_for_evict:0\r\nmaxmemory:100\r\n", "used_memory:1\r\nused_memory:2\r\nmem_not_counted_for_evict:0\r\nmaxmemory:100\r\n"} {
		if _, err := parseEvictionMemory(bulk(body)); err == nil {
			t.Fatal("bad memory accepted")
		}
	}
	raw := []byte("*2\r\n$1\r\n9\r\n*6\r\n")
	for _, k := range []string{syncMetadataPrefix + "control", strings.Repeat("x", 257), "key\x00\xff", "key\x00\xff", "", "tail"} {
		raw = append(raw, bulk(k)...)
	}
	next, keys, err := parseEvictionScan(raw, DefaultConfig().Limits, 2)
	if err != nil || next != "9" || !reflect.DeepEqual(keys, []string{"key\x00\xff", ""}) {
		t.Fatal(next, keys, err)
	}
	for _, bad := range [][]byte{[]byte("-ERR nope\r\n"), append(append([]byte{}, raw...), '+'), []byte("*2\r\n$1\r\nx\r\n*0\r\n")} {
		if _, _, err := parseEvictionScan(bad, DefaultConfig().Limits, 2); err == nil {
			t.Fatal("malformed scan accepted")
		}
	}
}

func TestAutoEvictionCandidateRedisNoTouch(t *testing.T) {
	addr := syncTestRedis(t)
	c, r, err := authenticated(addr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	client := &kvCommitClient{c, r}
	client.expect(t, "+OK\r\n", "SET", "large", strings.Repeat("v", 16384))
	client.expect(t, "+OK\r\n", "SET", "small", "v")
	client.expect(t, "+OK\r\n", "SET", "ttl", strings.Repeat("v", 16384), "PX", "60000")
	client.expect(t, ":1\r\n", "LPUSH", "list", "v")
	time.Sleep(2100 * time.Millisecond)
	before, err := client.request("OBJECT", "IDLETIME", "large")
	if err != nil {
		t.Fatal(err)
	}
	cfg := passConfig(addr)
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b := &redisAutoEviction{s: s, cfg: (&AutoEvictionConfig{Enabled: true, MinIdle: time.Second}).resolved()}
	for i := 0; i < 2; i++ {
		idle, err := b.eligible(context.Background(), "large")
		if err != nil || idle < 1 {
			t.Fatal("inspection refreshed idle", idle, err)
		}
	}
	after, _ := client.request("OBJECT", "IDLETIME", "large")
	n1, _ := number(before[1 : len(before)-2])
	n2, _ := number(after[1 : len(after)-2])
	if n2 < n1 {
		t.Fatal("sampling touched LRU", string(before), string(after))
	}
	for _, key := range []string{"small", "ttl", "list", "absent"} {
		idle, err := b.eligible(context.Background(), key)
		if err != nil || idle != -1 {
			t.Fatal("ineligible key selected", key, idle, err)
		}
	}
	client.request("GET", "large")
	if idle, err := b.eligible(context.Background(), "large"); err != nil || idle != -1 {
		t.Fatal("recent access not excluded", idle, err)
	}
}
