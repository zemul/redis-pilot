package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	// 隔离环境变量对 LoadConfig 的干扰
	t.Setenv("REDIS_SERVER_TOKEN", "")

	want := &Config{Server: "10.0.1.10:8080", Token: "rpt_secrettoken1234"}
	if err := SaveConfig(want); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}

	// 文件权限必须是 0600（含 token）
	path := filepath.Join(dir, ".redis-pilot-cli", "config.yaml")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("config perm = %o, want 600", perm)
	}

	got := LoadConfig()
	if got.Server != want.Server {
		t.Errorf("Server = %q, want %q", got.Server, want.Server)
	}
	if got.Token != want.Token {
		t.Errorf("Token = %q, want %q", got.Token, want.Token)
	}
}
