package gateway

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Probe only. Unlike the earlier removal script, this emits a preparation
// marker and never deletes or rewrites the business key. No cold ACK implied.
const evictionPrepareProbeScript = `
redis.replicate_commands()
if #KEYS ~= 2 or #ARGV ~= 2 then return redis.error_reply('ERR prepare arguments') end
if redis.call('TYPE', KEYS[1]).ok ~= 'string'
   or redis.call('PTTL', KEYS[1]) ~= -1
   or redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return redis.error_reply('ERR prepare key changed')
end
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  if redis.call('TYPE', KEYS[2]).ok == 'string'
     and redis.call('PTTL', KEYS[2]) == -1
     and redis.call('GET', KEYS[2]) == ARGV[2] then return 2 end
  return redis.error_reply('ERR prepare marker conflict')
end
redis.call('SET', KEYS[2], ARGV[2])
return 1
`

func TestEvictionOperationRedisPrepare(t *testing.T) {
	addr := syncTestRedis(t)
	conn, reader, err := authenticated(addr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &kvCommitClient{conn, reader}
	c.expect(t, "+OK\r\n", "CONFIG", "SET", "repl-diskless-sync", "no")
	c.expect(t, "+OK\r\n", "CONFIG", "SET", "maxmemory-policy", "noeviction")
	key, value := "{g6b-probe}:\x00\xffprepare", "\x00\xffvalue"
	c.expect(t, "+OK\r\n", "SET", key, value)
	j, capturePath := captureJournal(t, 4<<20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	source := syncSource{address: addr, passwordFile: secretFile(t, "g4-backend-test"), timeout: 5 * time.Second, maxSnapshot: 1 << 20}
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
			t.Error("prepare capture did not stop")
		}
	})
	waitCaptured(t, j, 0)
	i, err := newEvictionIntent("prepare-probe", 1, j.durablePosition().History, []byte(key), []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	// The cold endpoint is identity metadata only; this test never connects it.
	identity := syncTaskIdentity{TaskID: i.TaskID, Source: addr, Cold: "127.0.0.1:1"}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "operation.log")
	op, err := openEvictionOperation(path, identity, &i, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	args := func(expected string) []string {
		return []string{"EVAL", evictionPrepareProbeScript, "2", key, i.prepareKey(), expected, "prepare:" + i.encode()}
	}
	raw, err := c.request(args("wrong-value")...)
	if err != nil || !bytes.HasPrefix(raw, []byte("-ERR prepare key changed")) {
		t.Fatal("wrong value allowed", err)
	}
	c.expect(t, "$-1\r\n", "GET", i.prepareKey())
	c.expect(t, ":1\r\n", args(value)...)
	c.expect(t, ":2\r\n", args(value)...)
	kvCommitGet(t, c, key, value)
	kvCommitGet(t, c, i.prepareKey(), "prepare:"+i.encode())
	raw, err = c.request("INFO", "replication")
	if err != nil {
		t.Fatal(err)
	}
	p, reason := parseSyncSourceInfo(raw)
	if reason != "none" {
		t.Fatal(reason)
	}
	waitCaptured(t, j, p.Offset)
	cancel()
	select {
	case <-done:
		stopped = true
	case <-time.After(2 * time.Second):
		t.Fatal("prepare capture did not stop")
	}
	records := capturedRecords(t, capturePath)
	count := 0
	for index, r := range records {
		if r.Kind != "command" {
			continue
		}
		parser, _ := NewReader(bytes.NewReader(r.Data), DefaultConfig().Limits)
		_, argv, err := parser.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if string(argv[0]) == "UNLINK" || string(argv[0]) == "DEL" {
			t.Fatal("preparation removed data")
		}
		if bytes.Equal(r.Data, i.prepareFrame()) {
			if index == 0 {
				t.Fatal("missing capture anchor")
			}
			prepare := evictionPreparation{Before: records[index-1].Position, Record: r}
			if err := op.recordPreparation(prepare); err != nil {
				t.Fatal(err)
			}
			count++
		}
	}
	if count != 1 {
		t.Fatalf("preparation record count=%d", count)
	}
	op.close()
	op, err = openEvictionOperation(path, identity, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	s, err := op.snapshot()
	if err != nil || s.Prepare == nil || s.Intent.OperationID != i.OperationID {
		t.Fatal("prepare recovery", err)
	}
	kvCommitGet(t, c, key, value)
	// A retained marker cannot turn a changed value into valid preparation.
	c.expect(t, "+OK\r\n", "SET", key, "newer")
	raw, err = c.request(args(value)...)
	if err != nil || !bytes.HasPrefix(raw, []byte("-ERR prepare key changed")) {
		t.Fatal("stale retry accepted", err)
	}
	kvCommitGet(t, c, key, "newer")
	t.Log("durable intent -> real Redis preparation -> durable capture -> operation reopen; hot key retained; no cold ACK or delete")
}
