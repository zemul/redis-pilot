package gateway

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func hotColdTestConfig(t *testing.T) Config {
	t.Helper()
	c := DefaultConfig()
	c.Mode = "passthrough"
	c.RedisAddress = "127.0.0.1:16390"
	c.AllowUnauthenticated = true
	c.Limits.MaxBytes = 256 << 10
	c.MaxBatchBytes = 256 << 10
	c.BufferBudgetBytes = 64 << 20
	c.StateDir = confirmationPrivateDir(t)
	c.HotCold = &HotColdConfig{Binding: ConfirmationBinding{TaskID: "lane", GatewayID: c.GatewayID, SyncerID: "syncer", Source: c.RedisAddress, Cold: "127.0.0.1:16666", History: strings.Repeat("a", 40), Epoch: 1}, SyncerEndpoint: "http://127.0.0.1:19301", SyncerTokenFile: secretFile(t, strings.Repeat("syncer", 10)), AdminTokenFile: secretFile(t, strings.Repeat("admin", 12)), ConfirmExclusiveHot: true, Timeout: 3 * time.Second}
	return c
}

func TestHotColdConfigLifecycle(t *testing.T) {
	c := hotColdTestConfig(t)
	raw, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeConfig(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewService(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if s.preparationOptions == nil || !s.preparationOptions.ManualResidency || s.admissionBoot.Create {
		t.Fatal("not wired for resume")
	}
	entries, _ := os.ReadDir(c.StateDir)
	if len(entries) != 0 {
		t.Fatal("check-config mutated state")
	}
	if boot, err := openAdmissionRun(*s.admissionBoot, nil); err == nil {
		boot.close()
		t.Fatal("missing state silently initialized")
	}
	if err := s.InitializeHotCold(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.StateDir, "manual-residency.enabled")); err != nil {
		t.Fatal(err)
	}
	if err := s.InitializeHotCold(); err == nil {
		t.Fatal("reinitialization accepted")
	}
	resumed, err := NewService(decoded)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := openAdmissionRun(*resumed.admissionBoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	boot.close()
}

func TestHotColdConfigRejectsUnsafe(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.HotCold = nil }, func(c *Config) { c.StateDir = "relative" },
		func(c *Config) { c.HotCold.ConfirmExclusiveHot = false },
		func(c *Config) { c.HotCold.Binding.Source = "127.0.0.1:12345" },
		func(c *Config) { c.HotCold.SyncerEndpoint = "http://example.com:19301" },
		func(c *Config) { c.HotCold.Timeout = 5 * time.Second },
		func(c *Config) { c.HotCold.AdminTokenFile = c.HotCold.SyncerTokenFile },
		func(c *Config) { c.Limits.MaxDepth = 1 }, func(c *Config) { c.BufferBudgetBytes = 1 },
	} {
		c := hotColdTestConfig(t)
		mutate(&c)
		if _, err := NewService(c); err == nil {
			t.Fatal("unsafe hot_cold config accepted")
		}
	}
}

func TestNextOperationID(t *testing.T) {
	for _, pair := range [][2]string{{strings.Repeat("0", 32), strings.Repeat("0", 31) + "1"}, {strings.Repeat("0", 30) + "ff", strings.Repeat("0", 29) + "100"}, {"00000000000000000020000000000000", "00000000000000000020000000000001"}} {
		got, err := nextOperationID(pair[0])
		if err != nil || got != pair[1] {
			t.Fatal(got, err)
		}
	}
	if _, err := nextOperationID(strings.Repeat("f", 32)); err == nil {
		t.Fatal("wrapped")
	}
	if _, err := nextOperationID("random"); err == nil {
		t.Fatal("invalid ID accepted")
	}
}
