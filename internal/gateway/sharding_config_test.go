package gateway

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
	"gopkg.in/yaml.v3"
)

func shardConfigForTest(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	c := passConfig("127.0.0.1:17100")
	c.Mode, c.RequestTimeout = "cache", 3*time.Second
	c.Cache = &CacheConfig{ExclusiveBackends: true, LockFile: filepath.Join(dir, "owner.lock"), Sharding: &ShardConfig{
		StateDir: filepath.Join(dir, "state"), Nodes: []ShardNodeConfig{{ID: "a", Address: "127.0.0.1:17101"}, {ID: "b", Address: "127.0.0.1:17102"}},
	}}
	return c
}

func TestShardConfigValidation(t *testing.T) {
	for _, policy := range []string{"invalidate", "update_existing"} {
		c := shardConfigForTest(t)
		c.Cache.WritePolicy = policy
		b, _ := yaml.Marshal(c)
		decoded, err := DecodeConfig(strings.NewReader(string(b)))
		if err != nil || decoded.Cache.Sharding.Nodes[1].ID != "b" {
			t.Fatalf("roundtrip: %v", err)
		}
	}
	for name, mutate := range map[string]func(*Config){
		"mixed-address":       func(c *Config) { c.Cache.ColdAddress = "127.0.0.1:1" },
		"mixed-password":      func(c *Config) { c.Cache.ColdPasswordFile = "/secret" },
		"relative-state":      func(c *Config) { c.Cache.Sharding.StateDir = "state" },
		"root-state":          func(c *Config) { c.Cache.Sharding.StateDir = "/" },
		"unclean-state":       func(c *Config) { c.Cache.Sharding.StateDir += "/../state" },
		"empty-nodes":         func(c *Config) { c.Cache.Sharding.Nodes = nil },
		"duplicate-id":        func(c *Config) { c.Cache.Sharding.Nodes[1].ID = "a" },
		"duplicate-address":   func(c *Config) { c.Cache.Sharding.Nodes[1].Address = c.Cache.Sharding.Nodes[0].Address },
		"hot-reuse":           func(c *Config) { c.Cache.Sharding.Nodes[0].Address = c.RedisAddress },
		"hot-canonical-reuse": func(c *Config) { c.Cache.Sharding.Nodes[0].Address = "[::ffff:127.0.0.1]:17100" },
		"remote-backend":      func(c *Config) { c.Cache.Sharding.Nodes[0].Address = "10.0.0.1:6379" },
		"relative-secret":     func(c *Config) { c.Cache.Sharding.Nodes[0].PasswordFile = "secret" },
		"scan-limit":          func(c *Config) { c.Cache.Sharding.Migration.ScanCount = 1001 },
		"rate-limit":          func(c *Config) { c.Cache.Sharding.Migration.KeysPerSecond = -1 },
		"byte-limit":          func(c *Config) { c.Cache.Sharding.Migration.BytesPerSecond = -1 },
		"timeout":             func(c *Config) { c.Cache.Sharding.Migration.StepTimeout = 2 * time.Hour },
	} {
		t.Run(name, func(t *testing.T) {
			c := shardConfigForTest(t)
			mutate(&c)
			if c.Validate() == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	c := shardConfigForTest(t)
	c.Cache.Sharding.Nodes[1].PasswordFile = filepath.Join(t.TempDir(), "missing")
	if _, err := NewService(c); err == nil {
		t.Fatal("missing per-node credential accepted")
	}
}

func TestShardStateDurabilityAndValidation(t *testing.T) {
	ctx := context.Background()
	c := shardConfigForTest(t)
	if _, err := openShardDisk(ctx, c, false); err == nil {
		t.Fatal("missing state silently initialized")
	}
	if _, err := os.Stat(c.Cache.Sharding.StateDir); !os.IsNotExist(err) {
		t.Fatal("read created state")
	}
	s, err := openShardDisk(ctx, c, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openShardDisk(ctx, c, true); err == nil {
		t.Fatal("initialization overwrote state")
	}
	if _, err := openShardDisk(ctx, c, false); err == nil {
		t.Fatal("concurrent state open accepted")
	}
	s.close()
	for _, field := range []string{"gateway", "service", "hot"} {
		bad := c
		switch field {
		case "gateway":
			bad.GatewayID += "x"
		case "service":
			bad.ServiceID += "x"
		case "hot":
			bad.RedisAddress = "127.0.0.1:17103"
		}
		if _, err := openShardDisk(ctx, bad, false); err == nil {
			t.Fatal("identity mismatch accepted", field)
		}
	}
	s, err = openShardDisk(ctx, c, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for _, id := range []string{"", ".", "..", "../escape", "a/b", "a\\b"} {
		if _, err := s.taskPath(id); err == nil {
			t.Fatal("unsafe task path", id)
		}
	}
	from, _ := sharding.New(s.manifest.Source)
	target := from.Config()
	target.Revision++
	target.Nodes = target.Nodes[:1]
	to, _ := sharding.New(target)
	if err := s.bindTask(ctx, "missing", to); err == nil {
		t.Fatal("missing task recreated")
	}
	p, _ := s.taskPath("shrink-1")
	j, err := sharding.CreateJournal(ctx, p, "shrink-1", from, to)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	if err := s.bindTask(ctx, "shrink-1", to); err != nil {
		t.Fatal(err)
	}
	if err := s.bindTask(ctx, "overlap", to); err == nil {
		t.Fatal("overlapping task accepted")
	}
	s.close()
	s, err = openShardDisk(ctx, c, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if s.manifest.Task.ID != "shrink-1" || !s.journal.Target().Equal(to) {
		t.Fatal("task reference was not restored")
	}
	s.close()
	if err := os.Rename(p, p+"-unavailable"); err != nil {
		t.Fatal(err)
	}
	if _, err := openShardDisk(ctx, c, false); err == nil {
		t.Fatal("missing active journal silently reset")
	}
}

func TestShardStateCorruption(t *testing.T) {
	for _, mode := range []string{"empty-db", "invalid-json", "version", "oversize", "symlink-db", "public-db", "symlink-dir"} {
		t.Run(mode, func(t *testing.T) {
			c := shardConfigForTest(t)
			s, err := openShardDisk(context.Background(), c, true)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "invalid-json":
				_, err = s.db.Exec(`UPDATE runtime SET manifest='{'`)
			case "version":
				_, err = s.db.Exec(`PRAGMA user_version=999`)
			case "oversize":
				_, err = s.db.Exec(`UPDATE runtime SET manifest=zeroblob(1048577)`)
			}
			if err != nil {
				t.Fatal(err)
			}
			s.close()
			p := filepath.Join(s.dir, "runtime.db")
			switch mode {
			case "empty-db":
				err = os.Truncate(p, 0)
			case "public-db":
				err = os.Chmod(p, 0644)
			case "symlink-db":
				if err = os.Rename(p, p+"-original"); err == nil {
					err = os.Symlink(p+"-original", p)
				}
			case "symlink-dir":
				if err = os.Rename(s.dir, s.dir+"-original"); err == nil {
					err = os.Symlink(s.dir+"-original", s.dir)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if state, err := openShardDisk(context.Background(), c, false); err == nil {
				state.close()
				t.Fatal("invalid state accepted")
			}
		})
	}
}

// A manifest is deliberately secret-free; credential contents never enter it.
func TestShardStateNoSecrets(t *testing.T) {
	c := shardConfigForTest(t)
	c.Cache.Sharding.Nodes[0].PasswordFile = secretFile(t, "private-test-password")
	s, err := openShardDisk(context.Background(), c, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	b, _ := json.Marshal(s.manifest)
	if strings.Contains(string(b), "password") || strings.Contains(string(b), c.Cache.Sharding.Nodes[0].PasswordFile) {
		t.Fatal("credential in manifest")
	}
}
