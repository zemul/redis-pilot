package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRunReportingCheckConfigNeverContactsServer(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
	defer s.Close()
	dir := t.TempDir()
	token := filepath.Join(dir, "server-token")
	if err := os.WriteFile(token, []byte("secret-observation-token-long-enough"), 0600); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(dir, "journal")
	config := filepath.Join(dir, "syncer.yaml")
	data := fmt.Sprintf(`mode: isolated
tasks:
  - task_id: task-check
    confirm_exclusive_cold: true
    source_address: 127.0.0.1:26379
    cold_address: 127.0.0.1:26666
    journal_dir: %q
    allow_unauthenticated: true
control_plane:
  server_endpoint: %q
  token_file: %q
  syncer_id: syncer-check
  node_id: node-check
progress:
  enabled: true
`, journal, s.URL, token)
	if err := os.WriteFile(config, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []bool{false, true} {
		if unsafe {
			if err := os.Chmod(token, 0644); err != nil {
				t.Fatal(err)
			}
		}
		var output bytes.Buffer
		want := 0
		if unsafe {
			want = 1
		}
		if code := run(context.Background(), []string{"-config", config, "-check-config"}, &output); code != want {
			t.Fatal("config check", code, output.String())
		}
		for _, private := range []string{dir, s.URL, "secret-observation-token"} {
			if strings.Contains(output.String(), private) {
				t.Fatal("check leaked private input")
			}
		}
	}
	if requests.Load() != 0 {
		t.Fatal("read-only config check contacted Server")
	}
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("config check created journal")
	}
}
