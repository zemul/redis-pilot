package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/syncer"
	"gopkg.in/yaml.v3"
)

func TestConfirmationConfigReadOnly(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte(strings.Repeat("s", 32)), 0600); err != nil {
		t.Fatal(err)
	}
	c := syncer.ConfirmationConfig{Mode: "isolated-confirmation", Listen: listener.Addr().String(), ConfirmExclusiveCold: true,
		Options: gateway.IsolatedConfirmationOptions{Binding: gateway.ConfirmationBinding{SyncerID: "syncer-1", GatewayID: "gateway-1", TaskID: "task-1", Source: "127.0.0.1:12345", Cold: "127.0.0.1:23456", History: strings.Repeat("a", 40), Epoch: 1},
			JournalDir: filepath.Join(dir, "missing-journal"), OperationDir: filepath.Join(dir, "missing-operation"), TokenFile: token, AllowUnauthenticated: true, SegmentBytes: 1 << 20, JournalBytes: 4 << 20, Timeout: time.Second}}
	raw, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "confirmation.yaml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-confirmation-config", path, "-check-config"}, &output); code != 0 {
		t.Fatal(code, output.String())
	}
	for _, path := range []string{c.Options.OperationDir, c.Options.JournalDir} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("check created storage", err)
		}
	}
	if code := run(context.Background(), []string{"-confirmation-config", path, "-config", path}, &output); code != 2 {
		t.Fatal("mixed modes accepted")
	}
	if strings.Contains(output.String(), dir) {
		t.Fatal("path leaked")
	}
	for _, extra := range []string{"unknown: secret\n", "---\nmode: isolated-confirmation\n", "confirmation: {}\n"} {
		if err := os.WriteFile(path, append(bytes.Clone(raw), []byte(extra)...), 0600); err != nil {
			t.Fatal(err)
		}
		if code := run(context.Background(), []string{"-confirmation-config", path, "-check-config"}, &output); code != 1 {
			t.Fatal("invalid document accepted", code)
		}
	}
}
