package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRedisMemoryBudgetAndLegacyRewrite(t *testing.T) {
	for _, memory := range []string{"128Mi", "1Gi", "10Gi"} {
		budget, err := redisMemoryBudget(memory)
		if err != nil || budget == "" {
			t.Fatal(memory, budget, err)
		}
	}
	for _, memory := range []string{"", "0", "-1", "bad"} {
		if _, err := redisMemoryBudget(memory); err == nil {
			t.Fatal("accepted", memory)
		}
	}
	dir := t.TempDir()
	p := RedisConfigParams{Memory: "1Gi", MaxmemoryPolicy: "noeviction", Appendonly: "yes"}
	if err := writeRedisConfig(dir, p); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "conf", "redis.conf")
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "maxmemory 805306368") {
		t.Fatal(string(b))
	}
	legacy := strings.Replace(string(b), "maxmemory 805306368", "maxmemory 1073741824", 1)
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeRedisConfig(dir, p); err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(path)
	if !strings.Contains(string(b), "maxmemory 1073741824") {
		t.Fatal("rewrite changed existing budget")
	}
	p.RecalculateMemory = true
	if err := writeRedisConfig(dir, p); err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(path)
	if !strings.Contains(string(b), "maxmemory 805306368") {
		t.Fatal("resize did not recalculate budget")
	}
}

func TestMemoryObservationSeparatesRSS(t *testing.T) {
	m := &monitor{instances: map[string]*metrics{
		"redis":   {Info: "used_memory:100\r\nused_memory_rss:200\r\n", UpdatedAt: time.Now()},
		"kvrocks": {Info: "used_memory_rss:300\r\n", UpdatedAt: time.Now()},
		"old":     {Info: "used_memory_rss:400\r\n", UpdatedAt: time.Now().Add(-3 * time.Minute)},
	}}
	if m.usedMemoryMap()["redis"] != 100 || m.usedMemoryMap()["kvrocks"] != 0 {
		t.Fatal("mixed metrics")
	}
	rss := m.memoryFieldMap("used_memory_rss")
	if rss["redis"] != 200 || rss["kvrocks"] != 300 || rss["old"] != 0 {
		t.Fatal(rss)
	}
}

func TestOrdinaryResizeUsesReservedBudgetOnDiskAndLive(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	previous := agentDataDir
	agentDataDir = a.cfg.DataDir
	t.Cleanup(func() { agentDataDir = previous })
	bin := t.TempDir()
	log := filepath.Join(bin, "args")
	t.Setenv("MEMORY_TEST_ARGS", log)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(bin, "redis-cli"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$MEMORY_TEST_ARGS\"\nprintf 'OK\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	w := doAgentRequest(a.Router(), "POST", "/instance/resize", map[string]interface{}{"name": "resize-budget", "engine": "redis", "memory": "1Gi", "port": 6379, "category": "persistent"})
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !fake.hasCalled("Update:redis-resize-budget:mem=1Gi:cpus=0") {
		t.Fatal(fake.calls)
	}
	args, err := os.ReadFile(log)
	if err != nil || !strings.Contains(string(args), "CONFIG\nSET\nmaxmemory\n805306368") {
		t.Fatal(string(args), err)
	}
	conf, err := os.ReadFile(filepath.Join(a.cfg.DataDir, "resize-budget", "conf", "redis.conf"))
	if err != nil || !strings.Contains(string(conf), "maxmemory 805306368") {
		t.Fatal(string(conf), err)
	}
}
