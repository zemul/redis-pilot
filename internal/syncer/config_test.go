package syncer

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func validConfig(t *testing.T) Config {
	t.Helper()
	c := DefaultConfig()
	c.ConfirmExclusiveCold = true
	c.SourceAddress = "127.0.0.1:16389"
	c.ColdAddress = "127.0.0.1:16390"
	c.JournalDir = filepath.Join(t.TempDir(), "not-created")
	c.AllowUnauthenticated = true
	c.AdminListen = "127.0.0.1:0"
	return c
}

func validYAML(t *testing.T) string {
	t.Helper()
	return "mode: isolated\nconfirm_exclusive_cold: true\nsource_address: 127.0.0.1:16389\ncold_address: 127.0.0.1:16390\njournal_dir: " + filepath.Join(t.TempDir(), "journal") + "\nallow_unauthenticated: true\n"
}

func TestDefaultConfigRequiresExplicitIsolation(t *testing.T) {
	c := DefaultConfig()
	if c.ConfirmExclusiveCold || c.AllowUnauthenticated || c.SourceAddress != "" || c.ColdAddress != "" || c.JournalDir != "" {
		t.Fatal("defaults must not opt into a writable isolated task")
	}
	if c.Mode != "isolated" || c.IOTimeout != 30*time.Second || c.ShutdownTimeout != 10*time.Second || c.SegmentBytes != 4<<20 || c.JournalBytes != 128<<20 || c.AdminListen != "127.0.0.1:19300" {
		t.Fatalf("unexpected non-sensitive defaults: %+v", c)
	}
	if err := c.Validate(); err == nil {
		t.Fatal("default config must not be runnable")
	}
}

func TestConfigDecodeStrictAndRedacted(t *testing.T) {
	base := validYAML(t)
	c, err := DecodeConfig(strings.NewReader(base))
	if err != nil {
		t.Fatal(err)
	}
	if c.IOTimeout != 30*time.Second || !c.ConfirmExclusiveCold {
		t.Fatal("defaults or explicit consent were lost")
	}
	secret := "SENSITIVE_CONFIG_VALUE"
	tests := map[string]string{
		"unknown":               base + "unknown_" + secret + ": true\n",
		"inline_password":       base + "source_password: " + secret + "\n",
		"wrong_type":            base + "io_timeout: [" + secret + "]\n",
		"invalid_yaml":          base + "io_timeout: [" + secret + "\n",
		"duplicate":             base + "mode: " + secret + "\n",
		"second_document":       base + "---\n" + secret + ": true\n",
		"empty_second_document": base + "---\n",
		"invalid_mode":          strings.Replace(base, "mode: isolated", "mode: "+secret, 1),
		"empty":                 "",
		"too_large":             strings.Repeat("x", maxConfigBytes+1),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeConfig(strings.NewReader(input))
			if err == nil {
				t.Fatal("invalid config accepted")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("sensitive config echoed: %v", err)
			}
		})
	}
}

type badConfigReader struct{}

func (badConfigReader) Read([]byte) (int, error) {
	return 0, errors.New("SENSITIVE_READER_ERROR")
}

func TestConfigReadErrorsRedacted(t *testing.T) {
	for _, r := range []io.Reader{badConfigReader{}, nil} {
		if _, err := DecodeConfig(r); err == nil || strings.Contains(err.Error(), "SENSITIVE") {
			t.Fatalf("unsafe read error: %v", err)
		}
	}
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "SENSITIVE_MISSING_FILE")); err == nil || strings.Contains(err.Error(), "SENSITIVE") {
		t.Fatalf("unsafe file error: %v", err)
	}
}

func TestConfigLoadRejectsNonRegularFilesWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "config-fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	regular := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(regular, []byte(validYAML(t)), 0600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(dir, "config-link")
	if err := os.Symlink(regular, symlink); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{dir, fifo, symlink, "/dev/null"} {
		done := make(chan error, 1)
		go func() { _, err := LoadConfig(name); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("non-regular or symlink config accepted")
			}
		case <-time.After(time.Second):
			t.Fatal("config validation blocked on a special file")
		}
	}
	if _, err := LoadConfig(regular); err != nil {
		t.Fatalf("regular config rejected: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := map[string]func(*Config){
		"mode":                        func(c *Config) { c.Mode = "production" },
		"cold_consent":                func(c *Config) { c.ConfirmExclusiveCold = false },
		"source_missing":              func(c *Config) { c.SourceAddress = "" },
		"source_hostname":             func(c *Config) { c.SourceAddress = "localhost:16389" },
		"source_remote":               func(c *Config) { c.SourceAddress = "192.0.2.1:16389" },
		"source_zero_port":            func(c *Config) { c.SourceAddress = "127.0.0.1:0" },
		"source_same_cold":            func(c *Config) { c.SourceAddress = c.ColdAddress },
		"source_same_cold_normalized": func(c *Config) { c.SourceAddress = "[::ffff:127.0.0.1]:016390" },
		"cold_missing":                func(c *Config) { c.ColdAddress = "" },
		"cold_hostname":               func(c *Config) { c.ColdAddress = "localhost:16390" },
		"cold_remote":                 func(c *Config) { c.ColdAddress = "192.0.2.1:16390" },
		"cold_zero_port":              func(c *Config) { c.ColdAddress = "127.0.0.1:0" },
		"admin_remote":                func(c *Config) { c.AdminListen = "0.0.0.0:19300" },
		"admin_hostname":              func(c *Config) { c.AdminListen = "localhost:19300" },
		"admin_source":                func(c *Config) { c.AdminListen = c.SourceAddress },
		"admin_cold_normalized":       func(c *Config) { c.AdminListen = "[::ffff:127.0.0.1]:016390" },
		"admin_bad_port":              func(c *Config) { c.AdminListen = "127.0.0.1:65536" },
		"journal_missing":             func(c *Config) { c.JournalDir = "" },
		"journal_relative":            func(c *Config) { c.JournalDir = "relative/journal" },
		"source_password_relative":    func(c *Config) { c.SourcePasswordFile = "secret" },
		"cold_password_relative":      func(c *Config) { c.ColdPasswordFile = "secret" },
		"auth_no_opt_in":              func(c *Config) { c.AllowUnauthenticated = false },
		"auth_one_missing":            func(c *Config) { c.AllowUnauthenticated = false; c.SourcePasswordFile = "/nonexistent/source" },
		"io_timeout":                  func(c *Config) { c.IOTimeout = 0 },
		"shutdown_timeout":            func(c *Config) { c.ShutdownTimeout = 0 },
		"segment_bytes":               func(c *Config) { c.SegmentBytes = 0 },
		"journal_bytes":               func(c *Config) { c.JournalBytes = 0 },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			c := validConfig(t)
			change(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	for _, admin := range []string{"127.0.0.1:0", "[::1]:0", "127.0.0.1:19300"} {
		c := validConfig(t)
		c.AdminListen = admin
		if err := c.Validate(); err != nil {
			t.Fatalf("valid admin address rejected: %v", err)
		}
	}
}

func TestConfigAndServiceConstructionHaveNoStorageSideEffects(t *testing.T) {
	c := validConfig(t)
	c.AllowUnauthenticated = false
	c.SourcePasswordFile = filepath.Join(t.TempDir(), "missing-source")
	c.ColdPasswordFile = filepath.Join(t.TempDir(), "missing-cold")
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(c)
	if err != nil {
		t.Fatalf("construction must not read missing credentials: %v", err)
	}
	if err := s.CheckCredentials(); err == nil {
		t.Fatal("explicit credential preflight accepted missing files")
	}
	if _, err := os.Stat(c.JournalDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation, construction, or credential preflight touched journal: %v", err)
	}
}
