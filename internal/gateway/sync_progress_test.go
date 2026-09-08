package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sourceInfoFixture() string {
	return "# Replication\r\nrole:master\r\nmaster_replid:" + strings.Repeat("a", 40) + "\r\nmaster_repl_offset:1000\r\nconnected_slaves:0\r\n"
}

func TestSyncProgressSourceInfoParser(t *testing.T) {
	for name, tc := range map[string]struct{ body, reason string }{
		"master":              {sourceInfoFixture(), "none"},
		"replica":             {strings.Replace(sourceInfoFixture(), "role:master", "role:slave", 1), "not_master"},
		"duplicate-role":      {sourceInfoFixture() + "role:master\r\n", "invalid_response"},
		"duplicate-offset":    {sourceInfoFixture() + "master_repl_offset:1001\r\n", "invalid_response"},
		"negative":            {strings.Replace(sourceInfoFixture(), "offset:1000", "offset:-1", 1), "invalid_response"},
		"overflow":            {strings.Replace(sourceInfoFixture(), "offset:1000", "offset:9223372036854775807", 1), "invalid_response"},
		"missing-history":     {strings.Replace(sourceInfoFixture(), "master_replid:", "other_id:", 1), "invalid_response"},
		"invalid-history":     {strings.Replace(sourceInfoFixture(), strings.Repeat("a", 40), "SENSITIVE", 1), "invalid_response"},
		"zero-history":        {strings.Replace(sourceInfoFixture(), strings.Repeat("a", 40), strings.Repeat("0", 40), 1), "invalid_response"},
		"leading-zero-offset": {strings.Replace(sourceInfoFixture(), "offset:1000", "offset:01000", 1), "invalid_response"},
	} {
		t.Run(name, func(t *testing.T) {
			position, reason := parseSyncSourceInfo(syncTransportBulk(tc.body))
			if reason != tc.reason || (reason != "none" && position != nil) {
				t.Fatal("parser", position, reason)
			}
			if reason == "none" && (position.Offset != 1000 || position.History != strings.Repeat("a", 40)) {
				t.Fatal("position", position)
			}
		})
	}
	for _, raw := range []string{"+OK\r\n", "-ERR SENSITIVE\r\n", "$-1\r\n", "$999999999\r\n", "$1\r\nx\r\ntrailing", strings.Repeat("x", 17000)} {
		if p, reason := parseSyncSourceInfo([]byte(raw)); p != nil || reason != "invalid_response" {
			t.Fatal("invalid framing", p, reason)
		}
	}
}

func TestSyncProgressProbeOnlyAuthInfoAndSnapshotBeforeSource(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	o := isolatedSyncOptionsForTest(t)
	o.TaskID = "task-observe"
	o.SourceAddress = l.Addr().String()
	o.SourcePasswordFile = filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(o.SourcePasswordFile, []byte("SENSITIVE-password"), 0600); err != nil {
		t.Fatal(err)
	}
	task, err := NewIsolatedSyncTask(o)
	if err != nil {
		t.Fatal(err)
	}
	pos := func(n int64) syncPosition {
		return syncPosition{Source: o.SourceAddress, History: strings.Repeat("a", 40), Offset: n}
	}
	task.runner.update(func(s *syncRunnerStatus) {
		recordSyncProgress(s, pos(900), pos(800), pos(700), syncResourceStatus{LiveBytes: 100}, time.Now())
	})
	done := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		r, _ := NewReader(conn, Limits{MaxBytes: 1024, MaxElements: 8, MaxDepth: 2})
		_, args, err := r.ReadCommand()
		if err != nil || len(args) != 2 || string(args[0]) != "AUTH" || string(args[1]) != "SENSITIVE-password" {
			done <- fmt.Errorf("unexpected authentication command")
			return
		}
		io.WriteString(conn, "+OK\r\n")
		_, args, err = r.ReadCommand()
		if err != nil || len(args) != 2 || string(args[0]) != "INFO" || string(args[1]) != "replication" {
			done <- fmt.Errorf("probe issued unexpected command")
			return
		}
		// Local capture can advance while INFO is in flight: return the earlier
		// paired snapshot, not a later value that exceeds this source sample.
		task.runner.update(func(s *syncRunnerStatus) {
			recordSyncProgress(s, pos(2000), pos(1900), pos(1800), syncResourceStatus{LiveBytes: 200}, time.Now())
		})
		if _, err := conn.Write(syncTransportBulk(sourceInfoFixture())); err != nil {
			done <- err
			return
		}
		b := make([]byte, 1)
		_, err = conn.Read(b)
		if err != io.EOF {
			done <- fmt.Errorf("probe did not close after INFO: %w", err)
			return
		}
		done <- nil
	}()
	sample := task.SampleProgress(context.Background(), time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("probe fixture did not join")
	}
	if sample.Validate() != nil || sample.Source.Error != "none" || sample.Source.Position.Offset != 1000 || sample.Local.Captured.Offset != 900 || sample.Local.Applied.Offset != 800 || sample.Local.GCConfirmed.Offset != 700 {
		t.Fatalf("bad paired sample %+v", sample)
	}
	raw, _ := json.Marshal(sample)
	for _, private := range []string{o.SourceAddress, o.JournalDir, "SENSITIVE"} {
		if strings.Contains(string(raw), private) {
			t.Fatal("progress leaked private data")
		}
	}
	if _, err := os.Stat(o.JournalDir); !os.IsNotExist(err) {
		t.Fatal("probe touched journal")
	}
	local := task.localProgress()
	local.Captured.Offset = 123
	if task.localProgress().Captured.Offset != 2000 {
		t.Fatal("cached progress exposed mutable position")
	}
}

func TestSyncProgressProbeBoundedErrorsAndCancellation(t *testing.T) {
	for name, reply := range map[string]string{"bad-reply": "-ERR SENSITIVE\r\n", "oversized": "$999999999\r\n", "timeout": ""} {
		t.Run(name, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			o := isolatedSyncOptionsForTest(t)
			o.SourceAddress = l.Addr().String()
			task, err := NewIsolatedSyncTask(o)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := l.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(time.Second))
				r, _ := NewReader(conn, Limits{MaxBytes: 1024, MaxElements: 8, MaxDepth: 2})
				r.ReadCommand()
				if reply != "" {
					io.WriteString(conn, reply)
				}
				io.Copy(io.Discard, conn)
			}()
			sample := task.SampleProgress(context.Background(), 50*time.Millisecond)
			want := "invalid_response"
			if name == "timeout" {
				want = "source_unavailable"
			}
			if sample.Source.Error != want || sample.Source.Position != nil || sample.Source.SampledAt.IsZero() {
				t.Fatal("error exposed stale/false progress", sample.Source)
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("probe connection not joined")
			}
		})
	}
	o := isolatedSyncOptionsForTest(t)
	task, _ := NewIsolatedSyncTask(o)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sample := task.SampleProgress(ctx, time.Second); sample.Source.Error != "canceled" || sample.Source.Position != nil {
		t.Fatal("canceled probe", sample)
	}
	o.SourcePasswordFile = filepath.Join(t.TempDir(), "missing")
	task, _ = NewIsolatedSyncTask(o)
	if sample := task.SampleProgress(context.Background(), time.Second); sample.Source.Error != "credential_file" {
		t.Fatal("credential failure hidden", sample)
	}
}

func TestSyncProgressSnapshotFreshnessAndSourceBinding(t *testing.T) {
	o := isolatedSyncOptionsForTest(t)
	task, _ := NewIsolatedSyncTask(o)
	if task.localProgress() != nil {
		t.Fatal("fabricated initial progress")
	}
	at := time.Now().Add(-time.Minute)
	p := syncPosition{Source: o.SourceAddress, History: strings.Repeat("a", 40), Offset: 100}
	task.runner.update(func(s *syncRunnerStatus) { recordSyncProgress(s, p, p, p, syncResourceStatus{}, at) })
	task.runner.update(func(s *syncRunnerStatus) { s.State = "retry_cold" })
	if !task.localProgress().SampledAt.Equal(at) {
		t.Fatal("lifecycle update refreshed data observation")
	}
	task.runner.update(func(s *syncRunnerStatus) { s.Progress.Applied.Source = "127.0.0.1:1" })
	if task.localProgress().Applied != nil {
		t.Fatal("foreign source exposed comparable offset")
	}
}
