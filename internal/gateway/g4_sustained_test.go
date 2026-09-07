package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// Independent client implementation to cross-check pure-command throughput.
// Fixed-count native results are supplementary, not the timed mixed workload.
func TestG4NativeBenchmark(t *testing.T) {
	binary := os.Getenv("GATEWAY_TEST_BENCHMARK_BINARY")
	if binary == "" {
		t.Skip("set GATEWAY_TEST_BENCHMARK_BINARY for native cross-check")
	}
	pipeline, requests := "8", "500000"
	switch os.Getenv("GATEWAY_TEST_G4_PIPELINE") {
	case "", "8":
	case "1":
		pipeline, requests = "1", "100000"
	default:
		t.Fatal("GATEWAY_TEST_G4_PIPELINE must be 1 or 8")
	}
	redis := g4Redis(t)
	proxy := startTestProxy(t, proxyBinary(t), redis, "g4-backend-test")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, route := range []struct{ name, addr string }{{"direct", redis}, {"proxy", proxy}} {
		host, port, err := net.SplitHostPort(route.addr)
		if err != nil {
			t.Fatal(err)
		}
		for _, count := range []string{"20000", requests} {
			args := []string{"-h", host, "-p", port, "-a", "g4-backend-test", "-c", "32", "-P", pipeline, "-d", "128", "-n", count, "-t", "set,get", "--csv"}
			out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
			if err != nil {
				t.Fatalf("native %s: %v %s", route.name, err, out)
			}
			t.Logf("G4_NATIVE route=%s requests=%s\n%s", route.name, count, out)
		}
	}
}

type sustainedResult struct {
	Route           string  `json:"route"`
	Workload        string  `json:"workload"`
	Clients         int     `json:"clients"`
	Pipeline        int     `json:"pipeline"`
	Repeat          int     `json:"repeat"`
	Commands        int64   `json:"commands"`
	Seconds         float64 `json:"seconds"`
	QPS             float64 `json:"qps"`
	BatchP99MS      float64 `json:"batch_p99_ms"`
	BatchP50MS      float64 `json:"batch_p50_ms"`
	BatchP95MS      float64 `json:"batch_p95_ms"`
	BatchMeanMS     float64 `json:"batch_mean_ms"`
	GetCommands     int64   `json:"get_commands"`
	SetCommands     int64   `json:"set_commands"`
	LatencyOverflow int64   `json:"latency_ge_100ms_batches"`
}

// Separate process: load generation must not share the gateway's Go runtime.
func sustainedGateway(t *testing.T, cfg Config) string {
	t.Helper()
	binary := os.Getenv("GATEWAY_TEST_GATEWAY_BINARY")
	if binary == "" {
		binary = filepath.Join(t.TempDir(), "redis-gateway")
		if out, err := exec.Command("go", "build", "-o", binary, "../../cmd/gateway").CombinedOutput(); err != nil {
			t.Fatalf("gateway build: %v %s", err, out)
		}
	}
	cfg.Listen, cfg.AdminListen = freeAddress(t), freeAddress(t)
	b, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "gateway.yaml")
	if err = os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	ownProcess(t, binary, "-config", p)
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, _, err := authenticated(cfg.Listen, "g4-front-test")
		if err == nil {
			c.Close()
			return cfg.Listen
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway subprocess not ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func sustainedLoad(addr, password, workload string, clients, pipeline int, warmup, duration time.Duration) (sustainedResult, error) {
	// All connections, authentication, initialization and request encoding happen
	// before the warmup. The same connections are reused for measurement.
	type worker struct {
		c     net.Conn
		r     *Reader
		raw   [2][]byte
		wants [2][][]byte
		gets  [2]int64
	}
	workers := make([]worker, 0, clients)
	defer func() {
		for _, w := range workers {
			w.c.Close()
		}
	}()
	value := bytes.Repeat([]byte("v"), 128)
	getReply := append([]byte("$128\r\n"), value...)
	getReply = append(getReply, '\r', '\n')
	for i := 0; i < clients; i++ {
		c, r, err := authenticated(addr, password)
		if err != nil {
			return sustainedResult{}, err
		}
		w := worker{c: c, r: r}
		workers = append(workers, w)
		key := fmt.Sprintf("g4-sustained:%d", i)
		if err := writeAll(c, encodeCommand("SET", key, string(value))); err != nil {
			return sustainedResult{}, err
		}
		b, err := r.ReadFrame()
		if err != nil || string(b) != "+OK\r\n" {
			return sustainedResult{}, fmt.Errorf("seed failed: %v", err)
		}
		for phase := 0; phase < 2; phase++ {
			for j := 0; j < pipeline; j++ {
				if workload == "get" || (workload == "mixed" && (j+phase)%2 == 1) {
					w.raw[phase] = append(w.raw[phase], encodeCommand("GET", key)...)
					w.wants[phase] = append(w.wants[phase], getReply)
					w.gets[phase]++
				} else {
					w.raw[phase] = append(w.raw[phase], encodeCommand("SET", key, string(value))...)
					w.wants[phase] = append(w.wants[phase], []byte("+OK\r\n"))
				}
			}
		}
		workers[i] = w
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var commands int64
	var gets, latencyNS int64
	var histogram [10001]int64 // 0.01 ms bins, final bin is >= 100 ms.
	start := time.Now().Add(warmup)
	end := start.Add(duration)
	last := end
	for _, w := range workers {
		wg.Add(1)
		go func(w worker) {
			defer wg.Done()
			var local [10001]int64
			var n, localGets, localNS int64
			phase := 0
			var err error
			finished := start
			for time.Now().Before(end) {
				w.c.SetDeadline(time.Now().Add(5 * time.Second))
				sent := time.Now()
				err = writeAll(w.c, w.raw[phase])
				for _, want := range w.wants[phase] {
					if err != nil {
						break
					}
					var b []byte
					b, err = w.r.ReadFrame()
					if err == nil && !bytes.Equal(b, want) {
						err = fmt.Errorf("unexpected %s response", workload)
					}
				}
				finished = time.Now()
				if err != nil {
					break
				}
				// Exclude batches crossing the warmup boundary. Include and drain
				// the final measured batch; elapsed time includes that drain.
				if !sent.Before(start) {
					n += int64(pipeline)
					localGets += w.gets[phase]
					localNS += finished.Sub(sent).Nanoseconds()
					bin := min(int(finished.Sub(sent)/(10*time.Microsecond)), len(local)-1)
					local[bin]++
				}
				// Odd-sized pipelines (especially 1) alternate across batches.
				phase ^= pipeline % 2
			}
			mu.Lock()
			defer mu.Unlock()
			if firstErr == nil {
				firstErr = err
			}
			commands += n
			gets += localGets
			latencyNS += localNS
			if finished.After(last) {
				last = finished
			}
			for i, n := range local {
				histogram[i] += n
			}
		}(w)
	}
	wg.Wait()
	if firstErr != nil {
		return sustainedResult{}, firstErr
	}
	if commands == 0 {
		return sustainedResult{}, fmt.Errorf("no measured commands")
	}
	percentile := func(p int64) float64 {
		target := (commands/int64(pipeline)*p + 99) / 100
		var cumulative int64
		for i, n := range histogram {
			cumulative += n
			if cumulative >= target {
				return float64(i+1) / 100
			}
		}
		return 0
	}
	seconds := last.Sub(start).Seconds()
	return sustainedResult{Workload: workload, Clients: clients, Pipeline: pipeline, Commands: commands, Seconds: seconds, QPS: float64(commands) / seconds, BatchP99MS: percentile(99), BatchP50MS: percentile(50), BatchP95MS: percentile(95), BatchMeanMS: float64(latencyNS) / float64(commands/int64(pipeline)) / 1e6, GetCommands: gets, SetCommands: commands - gets, LatencyOverflow: histogram[len(histogram)-1]}, nil
}

func TestG4SustainedLoadSmoke(t *testing.T) {
	addr := g4Redis(t)
	for _, pipeline := range []int{1, 8} {
		for _, workload := range []string{"get", "set", "mixed"} {
			r, err := sustainedLoad(addr, "g4-backend-test", workload, 2, pipeline, 20*time.Millisecond, 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			if r.Commands == 0 || r.Seconds < .1 || r.QPS <= 0 {
				t.Fatalf("invalid measurement: %+v", r)
			}
			if r.GetCommands+r.SetCommands != r.Commands {
				t.Fatal("command counters mismatch")
			}
			if workload == "mixed" && (r.GetCommands == 0 || r.SetCommands == 0 || r.GetCommands-r.SetCommands > 2 || r.SetCommands-r.GetCommands > 2) {
				t.Fatalf("mixed workload did not alternate: %+v", r)
			}
			if workload == "get" && r.SetCommands != 0 || workload == "set" && r.GetCommands != 0 {
				t.Fatal("pure workload includes wrong operation")
			}
		}
	}
}

func TestG4Sustained(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_G4_SUSTAINED") != "1" {
		t.Skip("opt-in sustained process comparison")
	}
	pipeline := 8
	clientsList := []int{8, 32}
	switch os.Getenv("GATEWAY_TEST_G4_PIPELINE") {
	case "", "8":
	case "1":
		pipeline = 1
		clientsList = []int{1, 8, 32}
	default:
		t.Fatal("GATEWAY_TEST_G4_PIPELINE must be 1 or 8")
	}
	output := os.Getenv("GATEWAY_TEST_RESULT_FILE")
	if output == "" {
		t.Fatal("GATEWAY_TEST_RESULT_FILE required to preserve raw results")
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	redis := g4Redis(t)
	binary := proxyBinary(t)
	cfg := passConfig(redis)
	cfg.RequestTimeout = 5 * time.Second
	cfg.MaxConnections = 256
	cfg.ClientIdleTimeout = time.Hour
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	cfg.FrontendPasswordFile = secretFile(t, "g4-front-test")
	gw := sustainedGateway(t, cfg)
	p := startTestProxy(t, binary, redis, "g4-backend-test")
	chain := startTestProxy(t, binary, gw, "g4-front-test")
	routes := []struct{ name, addr, password string }{{"direct", redis, "g4-backend-test"}, {"proxy", p, "g4-backend-test"}, {"gateway", gw, "g4-front-test"}, {"proxy_gateway", chain, "g4-front-test"}}
	for repeat := 1; repeat <= 2; repeat++ {
		for _, clients := range clientsList {
			for _, workload := range []string{"get", "set", "mixed"} {
				for i := range routes {
					route := routes[(i+repeat-1)%len(routes)]
					result, err := sustainedLoad(route.addr, route.password, workload, clients, pipeline, 2*time.Second, 10*time.Second)
					if err != nil {
						t.Fatalf("%s %s c=%d: %v", route.name, workload, clients, err)
					}
					result.Route, result.Repeat = route.name, repeat
					if err = encoder.Encode(result); err != nil {
						t.Fatal(err)
					}
					if err = f.Sync(); err != nil {
						t.Fatal(err)
					}
					b, _ := json.Marshal(result)
					t.Logf("G4_SUSTAINED %s", b)
				}
			}
		}
	}
}
