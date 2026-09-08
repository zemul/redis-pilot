package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/syncer"
)

func TestRunRequiresExplicitConfig(t *testing.T) {
	for _, args := range [][]string{nil, {"-check-config"}, {"-config", "", "-check-config"}} {
		var output bytes.Buffer
		if code := run(context.Background(), args, &output); code != 2 {
			t.Fatalf("args %v: exit code = %d, want 2", args, code)
		}
	}
}

func TestRunArgumentErrorsDoNotEchoInputs(t *testing.T) {
	secret := "sensitive-argument-must-not-appear"
	for _, args := range [][]string{
		{"-" + secret},
		{"-check-config=" + secret},
		{"-config", "/missing/config", secret},
	} {
		var output bytes.Buffer
		if code := run(context.Background(), args, &output); code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if strings.Contains(output.String(), secret) {
			t.Fatal("argument leaked into diagnostics")
		}
	}
}

func TestRunHelp(t *testing.T) {
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-h"}, &output); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(output.String(), "-config") {
		t.Fatal("help did not describe the required config")
	}
}

func TestRunCheckConfigIsReadOnly(t *testing.T) {
	// Occupying the admin port proves check-config does not start its listener.
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	dir := t.TempDir()
	journal := filepath.Join(dir, "not-created", "journal")
	config := writeConfig(t, dir, journal, admin.Addr().String(), "")
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-config", config, "-check-config"}, &output); code != 0 {
		t.Fatalf("exit code = %d, want 0; output: %s", code, &output)
	}
	if _, err := os.Stat(filepath.Dir(journal)); !os.IsNotExist(err) {
		t.Fatalf("check-config created its journal parent or stat failed: %v", err)
	}
}

func TestRunMultiTaskCheckConfigIsReadOnlyAndRedacted(t *testing.T) {
	for _, invalidCredential := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid-credential-%t", invalidCredential), func(t *testing.T) {
			admin, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			dir := t.TempDir()
			config := filepath.Join(dir, "syncer.yaml")
			var yaml strings.Builder
			fmt.Fprintf(&yaml, "mode: isolated\nadmin_listen: %q\ntasks:\n", admin.Addr().String())
			var journals []string
			for i := range 3 {
				journal := filepath.Join(dir, fmt.Sprintf("journal-%d", i))
				journals = append(journals, journal)
				fmt.Fprintf(&yaml, "  - task_id: shard-%d\n    confirm_exclusive_cold: true\n    source_address: 127.0.0.1:%d\n    cold_address: 127.0.0.1:%d\n    journal_dir: %q\n    allow_unauthenticated: true\n", i, 26379+i, 26666+i, journal)
				if invalidCredential && i == 1 {
					fmt.Fprintf(&yaml, "    source_password_file: %q\n", filepath.Join(dir, "missing-secret-credential"))
				}
			}
			if err := os.WriteFile(config, []byte(yaml.String()), 0600); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			want := 0
			if invalidCredential {
				want = 1
			}
			if code := run(context.Background(), []string{"-config", config, "-check-config"}, &output); code != want {
				t.Fatalf("exit code = %d, want %d; output: %s", code, want, &output)
			}
			for _, journal := range journals {
				if _, err := os.Stat(journal); !os.IsNotExist(err) {
					t.Fatalf("check-config created journal or stat failed: %v", err)
				}
			}
			for _, secret := range []string{dir, "missing-secret-credential", "127.0.0.1:"} {
				if strings.Contains(output.String(), secret) {
					t.Fatalf("multi-task config check leaked private configuration: %s", &output)
				}
			}
		})
	}
}

func TestRunCheckConfigRejectsCredentialFileWithoutLeakingPath(t *testing.T) {
	dir := t.TempDir()
	secret := "credential-path-must-not-appear"
	config := writeConfig(t, dir, filepath.Join(dir, "journal"), "127.0.0.1:19300",
		fmt.Sprintf("source_password_file: %q\n", filepath.Join(dir, secret)))
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-config", config, "-check-config"}, &output); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if strings.Contains(output.String(), secret) || strings.Contains(output.String(), dir) {
		t.Fatal("credential path leaked into diagnostics")
	}
	if _, err := os.Stat(filepath.Join(dir, "journal")); !os.IsNotExist(err) {
		t.Fatal("failed credential validation must not create a journal")
	}
}

func TestRunCheckConfigRejectsInvalidYAMLWithoutLeakingContents(t *testing.T) {
	dir := t.TempDir()
	secret := "unknown-secret-content"
	config := filepath.Join(dir, "syncer.yaml")
	if err := os.WriteFile(config, []byte(secret+": ["), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-config", config, "-check-config"}, &output); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if strings.Contains(output.String(), secret) || strings.Contains(output.String(), dir) {
		t.Fatal("invalid config leaked into diagnostics")
	}
}

func TestRunCheckConfigRejectsMissingConfigWithoutLeakingPath(t *testing.T) {
	secret := "missing-config-path-must-not-appear"
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-config", filepath.Join(t.TempDir(), secret), "-check-config"}, &output); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if strings.Contains(output.String(), secret) {
		t.Fatal("config path leaked into diagnostics")
	}
}

func TestRunAlreadyCanceledDoesNotOpenJournal(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "journal")
	config := writeConfig(t, dir, journal, "127.0.0.1:19300", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output bytes.Buffer
	if code := run(ctx, []string{"-config", config}, &output); code != 0 {
		t.Fatalf("exit code = %d, want 0; output: %s", code, &output)
	}
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("pre-start cancellation must not create a journal")
	}
	if strings.Contains(output.String(), "drained") {
		t.Fatal("pre-start cancellation must not claim a completed drain")
	}
}

func TestRunAdminConflictExitsWithoutOpeningJournal(t *testing.T) {
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	dir := t.TempDir()
	journal := filepath.Join(dir, "journal")
	config := writeConfig(t, dir, journal, admin.Addr().String(), "")
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-config", config}, &output); code != 1 {
		t.Fatalf("exit code = %d, want 1; output: %s", code, &output)
	}
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("admin bind failure must not create a journal")
	}
	if strings.Contains(output.String(), admin.Addr().String()) || strings.Contains(output.String(), dir) {
		t.Fatal("run failure leaked endpoint or file path")
	}
	if !strings.Contains(output.String(), "state=failed") {
		t.Fatal("run failure did not include final lifecycle status")
	}
	if !strings.Contains(output.String(), "service_error=admin_listen") {
		t.Fatal("run failure did not include the fixed service error category")
	}
}

func TestLogFinalTaskStatusIncludesEveryTaskWithoutLegacyPlaceholder(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	logFinalTaskStatus(logger, syncer.Status{
		Task: syncer.TaskStatus{State: "legacy-placeholder-must-not-appear"},
		Tasks: []syncer.TaskStatus{
			{TaskID: "shard-a", State: "failed", LastError: "cold_outcome_uncertain"},
			{TaskID: "shard-b", State: "drained", LastError: "none"},
			{TaskID: "shard-c", State: "stopped", LastError: "none"},
		},
	})
	text := output.String()
	for _, expected := range []string{
		"task_id=shard-a task_state=failed task_error=cold_outcome_uncertain task_running=false",
		"task_id=shard-b task_state=drained task_error=none task_running=false",
		"task_id=shard-c task_state=stopped task_error=none task_running=false",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("missing task summary %q: %s", expected, text)
		}
	}
	if strings.Contains(text, "legacy-placeholder") || strings.Count(text, `msg="isolated syncer task stopped"`) != 3 {
		t.Fatalf("unexpected task summaries: %s", text)
	}
}

func TestLogFinalTaskStatusPreservesLegacySummaryWithoutTaskID(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	logFinalTaskStatus(logger, syncer.Status{Task: syncer.TaskStatus{State: "drained", LastError: "none"}})
	text := output.String()
	if !strings.Contains(text, "task_state=drained task_error=none task_running=false") || strings.Contains(text, "task_id") {
		t.Fatalf("unexpected legacy task summary: %s", text)
	}
}

func writeConfig(t *testing.T, dir, journal, admin, extra string) string {
	t.Helper()
	config := filepath.Join(dir, "syncer.yaml")
	data := fmt.Sprintf("mode: isolated\nconfirm_exclusive_cold: true\nsource_address: 127.0.0.1:26379\ncold_address: 127.0.0.1:26666\nadmin_listen: %q\njournal_dir: %q\nallow_unauthenticated: true\n%s", admin, journal, extra)
	if err := os.WriteFile(config, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return config
}
