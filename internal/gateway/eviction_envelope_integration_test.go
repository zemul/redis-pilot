package gateway

// These scripts execute ONLY against a disposable Redis started by the test.
// There is no cold acknowledgement, durable intent, or production executor.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

const evictionProbeScript = `
redis.replicate_commands()
if #KEYS ~= 2 or #ARGV ~= 3 then return redis.error_reply('ERR probe arguments') end
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  if redis.call('TYPE', KEYS[2]).ok == 'string'
     and redis.call('PTTL', KEYS[2]) == -1
     and redis.call('GET', KEYS[2]) == ARGV[3]
     and redis.call('EXISTS', KEYS[1]) == 0 then return 2 end
  return redis.error_reply('ERR probe marker conflict')
end
if redis.call('TYPE', KEYS[1]).ok ~= 'string'
   or redis.call('PTTL', KEYS[1]) ~= -1
   or redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return redis.error_reply('ERR probe key precondition')
end
redis.call('SET', KEYS[2], ARGV[2])
-- AFTER_BEGIN
redis.call('UNLINK', KEYS[1])
-- AFTER_UNLINK
redis.call('SET', KEYS[2], ARGV[3])
return 1
`

func evictionProbeArgs(i evictionIntent, script, value string) []string {
	return []string{"EVAL", script, "2", string(i.Key), i.markerKey(), value, "begin:" + i.encode(), "complete:" + i.encode()}
}

// The relay forwards exactly one request and consumes, then drops, its reply.
// The client sees EOF; test code separately verifies backend completion. This
// models response loss, not a Redis crash or a durability guarantee.
func evictionProbeDropReply(t *testing.T, addr string, args []string) {
	t.Helper()
	front, relay := net.Pipe()
	defer front.Close()
	front.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan error, 1)
	go func() {
		defer relay.Close()
		relay.SetDeadline(time.Now().Add(5 * time.Second))
		r, _ := NewReader(relay, DefaultConfig().Limits)
		frame, _, err := r.ReadCommand()
		if err != nil {
			done <- err
			return
		}
		back, reader, err := authenticated(addr, "g4-backend-test")
		if err != nil {
			done <- err
			return
		}
		defer back.Close()
		back.SetDeadline(time.Now().Add(5 * time.Second))
		if err = writeAll(back, frame); err != nil {
			done <- err
			return
		}
		raw, err := reader.ReadFrame()
		if err == nil && string(raw) != ":1\r\n" {
			err = errors.New("unexpected probe backend result")
		}
		done <- err
	}()
	writeErr := writeAll(front, encodeCommand(args...))
	r, _ := NewReader(front, DefaultConfig().Limits)
	_, readErr := r.ReadFrame()
	if err := <-done; err != nil || writeErr != nil || !errors.Is(readErr, io.EOF) {
		t.Fatalf("reply fault: backend=%v write=%v read=%v", err, writeErr, readErr)
	}
}

func TestEvictionEnvelopeRedis(t *testing.T) {
	for _, scenario := range []string{"success", "selected_db", "lost_reply", "changed_value", "ttl", "wrong_type", "marker_conflict", "after_begin", "after_unlink", "recreated_retry"} {
		t.Run(scenario, func(t *testing.T) {
			addr := syncTestRedis(t)
			conn, reader, err := authenticated(addr, "g4-backend-test")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			c := &kvCommitClient{conn, reader}
			c.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-diskless-sync", "no")
			c.expect(t, "+OK\r\n", "CONFIG", "SET", "maxmemory-policy", "noeviction")
			key, value := "{g6b-probe}:\x00\xffkey", "\x00\xffvalue"
			i := evictionTestIntent(key, value)
			c.expect(t, "+OK\r\n", "SET", key, value)
			switch scenario {
			case "changed_value":
				c.expect(t, "+OK\r\n", "SET", key, "newer")
			case "ttl":
				c.expect(t, ":1\r\n", "PEXPIRE", key, "60000")
			case "wrong_type":
				c.expect(t, ":1\r\n", "DEL", key)
				c.expect(t, ":1\r\n", "RPUSH", key, "member")
			case "marker_conflict":
				c.expect(t, "+OK\r\n", "SET", i.markerKey(), "foreign")
			}
			j, path := captureJournal(t, 4<<20)
			ctx, cancel := context.WithCancel(context.Background())
			source := syncSource{address: addr, passwordFile: secretFile(t, "g4-backend-test"), timeout: 5 * time.Second, maxSnapshot: 1 << 20}
			done := make(chan error, 1)
			go func() { done <- source.capture(ctx, j) }()
			stopped := false
			t.Cleanup(func() {
				cancel()
				if stopped {
					return
				}
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("probe capture did not exit")
				}
			})
			waitCaptured(t, j, 0)
			i.History = j.durablePosition().History
			if scenario == "selected_db" {
				c.expect(t, "+OK\r\n", "SET", "selection-seed", "present")
			}
			script := evictionProbeScript
			if scenario == "after_begin" {
				script = strings.Replace(script, "-- AFTER_BEGIN", "error('injected after begin')", 1)
			}
			if scenario == "after_unlink" {
				script = strings.Replace(script, "-- AFTER_UNLINK", "error('injected after unlink')", 1)
			}
			success := scenario == "success" || scenario == "selected_db" || scenario == "lost_reply" || scenario == "recreated_retry"
			if scenario == "lost_reply" {
				evictionProbeDropReply(t, addr, evictionProbeArgs(i, script, value))
			} else {
				raw, err := c.request(evictionProbeArgs(i, script, value)...)
				if err != nil || (success && string(raw) != ":1\r\n") || (!success && !bytes.HasPrefix(raw, []byte("-ERR"))) {
					t.Fatalf("probe result: %q %v", raw, err)
				}
			}
			if success {
				c.expect(t, "$-1\r\n", "GET", key)
				kvCommitGet(t, c, i.markerKey(), "complete:"+i.encode())
				// A repeat does not emit another removal transaction.
				c.expect(t, ":2\r\n", evictionProbeArgs(i, evictionProbeScript, value)...)
				if scenario == "recreated_retry" {
					c.expect(t, "+OK\r\n", "SET", key, "new-generation")
					raw, err := c.request(evictionProbeArgs(i, evictionProbeScript, value)...)
					if err != nil || !bytes.HasPrefix(raw, []byte("-ERR probe marker conflict")) {
						t.Fatal("stale retry allowed", err)
					}
					kvCommitGet(t, c, key, "new-generation")
					c.expect(t, ":1\r\n", "DEL", key)
					c.expect(t, "$-1\r\n", "GET", key)
				}
			} else if scenario == "after_begin" || scenario == "after_unlink" {
				kvCommitGet(t, c, i.markerKey(), "begin:"+i.encode())
				if scenario == "after_begin" {
					kvCommitGet(t, c, key, value)
				} else {
					c.expect(t, "$-1\r\n", "GET", key)
				}
				raw, err := c.request(evictionProbeArgs(i, evictionProbeScript, value)...)
				if err != nil || !bytes.HasPrefix(raw, []byte("-ERR probe marker conflict")) {
					t.Fatal("partial operation retried", err)
				}
			} else {
				c.expect(t, ":1\r\n", "EXISTS", key)
				if scenario == "marker_conflict" {
					kvCommitGet(t, c, i.markerKey(), "foreign")
				} else {
					c.expect(t, "$-1\r\n", "GET", i.markerKey())
				}
			}
			raw, err := c.request("INFO", "replication")
			if err != nil {
				t.Fatal(err)
			}
			position, reason := parseSyncSourceInfo(raw)
			if reason != "none" {
				t.Fatal("source info", reason)
			}
			waitCaptured(t, j, position.Offset)
			cancel()
			select {
			case <-done:
				stopped = true
			case <-time.After(2 * time.Second):
				t.Fatal("probe capture did not stop")
			}
			records := capturedRecords(t, path)
			matches, unlinks, begins, completes, businessDeletes := 0, 0, 0, 0, 0
			var matched []syncRecord
			var before syncPosition
			for index, r := range records {
				if bytes.Equal(r.Data, encodeCommand("DEL", key)) {
					cmd, err := syncPendingCommand(r.Data)
					if err != nil {
						t.Fatal("ordinary deletion rejected", err)
					}
					after, err := syncPendingAfter(cmd, syncPendingValue{Exists: true, Data: []byte("new-generation")})
					if err != nil || after.Exists {
						t.Fatal("ordinary deletion lost its logical effect", err)
					}
					businessDeletes++
				}
				if bytes.Equal(r.Data, i.frames()[1]) {
					begins++
				}
				if bytes.Equal(r.Data, i.frames()[2]) {
					unlinks++
				}
				if bytes.Equal(r.Data, i.frames()[3]) {
					completes++
				}
				if index > 0 && bytes.Equal(r.Data, encodeCommand("MULTI")) {
					for _, length := range []int{5, 6} {
						if index+length > len(records) {
							continue
						}
						if _, err := inspectEvictionEnvelope(records[index-1].Position, records[index:index+length], i); err == nil {
							matches++
							matched = records[index : index+length]
							before = records[index-1].Position
						}
					}
				}
			}
			wantMatch, wantBegin, wantUnlink := 0, 0, 0
			if success {
				wantMatch, wantBegin, wantUnlink = 1, 1, 1
			}
			if scenario == "after_begin" {
				wantBegin = 1
			}
			if scenario == "after_unlink" {
				wantBegin, wantUnlink = 1, 1
			}
			if matches != wantMatch || begins != wantBegin || unlinks != wantUnlink || completes != wantMatch {
				for _, r := range records {
					if r.Kind != "command" {
						continue
					}
					parser, _ := NewReader(bytes.NewReader(r.Data), DefaultConfig().Limits)
					_, args, parseErr := parser.ReadCommand()
					if parseErr == nil && len(args) > 0 {
						t.Logf("wire command=%q offset=%d bytes=%d", args[0], r.Position.Offset, len(r.Data))
					}
				}
				t.Fatalf("actual PSYNC matches/begin/unlink/complete = %d/%d/%d/%d, want %d/%d/%d/%d", matches, begins, unlinks, completes, wantMatch, wantBegin, wantUnlink, wantMatch)
			}
			if scenario == "success" {
				if len(matched) != 6 {
					t.Fatalf("first-write envelope length = %d", len(matched))
				}
				evictionProbeJournalRestart(t, before, matched, i)
			}
			if scenario == "selected_db" && len(matched) != 5 {
				t.Fatalf("selected-db envelope length = %d", len(matched))
			}
			if scenario == "recreated_retry" && businessDeletes != 1 {
				t.Fatalf("ordinary DEL count = %d", businessDeletes)
			}
			t.Logf("real PSYNC: complete_envelopes=%d begin=%d unlink=%d complete_marker=%d; no cold apply", matches, begins, unlinks, completes)
		})
	}
}

// Replay the real captured frame group across journal reopen at each prefix.
// This is a local capture-journal test, not Redis/AOF or gateway crash recovery.
func evictionProbeJournalRestart(t *testing.T, before syncPosition, records []syncRecord, i evictionIntent) {
	t.Helper()
	for prefix := 1; prefix < len(records); prefix++ {
		t.Run(fmt.Sprintf("journal_prefix_%d", prefix), func(t *testing.T) {
			j, path := captureJournal(t, 1<<20)
			snapshot := captureSnapshot(before.Source)
			snapshot.Position = before // opaque synthetic anchor for this local fixture
			if err := j.append(snapshot); err != nil {
				t.Fatal(err)
			}
			for _, record := range records[:prefix] {
				if err := j.append(record); err != nil {
					t.Fatal(err)
				}
			}
			j.close()
			reopened, err := openSyncJournal(path, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.close()
			if _, err := inspectEvictionEnvelope(before, capturedRecords(t, path)[1:], i); err != errEvictionEnvelope {
				t.Fatal("partial durable capture accepted", err)
			}
			for _, record := range records[prefix:] {
				if err := reopened.append(record); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := inspectEvictionEnvelope(before, capturedRecords(t, path)[1:], i); err != nil {
				t.Fatal("complete resumed group rejected", err)
			}
		})
	}
}
