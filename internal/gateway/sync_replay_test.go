package gateway

// Test-only protocol probe: one preinitialized, non-expiring integer, one
// exclusive applier, one history. NOT the production PSYNC applier. In
// particular, sequence numbers below are not Redis byte offsets.

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

type kvReplayToken struct {
	History  string
	Sequence int64
	Digest   string
}

func (p kvReplayToken) encode() string {
	b, _ := json.Marshal(p)
	return string(b)
}

func kvReplayNext(history string, sequence int64, key string) kvReplayToken {
	return kvReplayToken{history, sequence, fmt.Sprintf("%x", sha256.Sum256(encodeCommand("INCR", key)))}
}

func kvReplayBulk(c *kvCommitClient, key string) (string, error) {
	raw, err := c.request("GET", key)
	if err != nil {
		return "", err
	}
	line, tail, ok := strings.Cut(string(raw), "\r\n")
	if !ok || !strings.HasPrefix(line, "$") {
		return "", fmt.Errorf("expected bulk string")
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 || len(tail) != n+2 || !strings.HasSuffix(tail, "\r\n") {
		return "", fmt.Errorf("missing or malformed bulk string")
	}
	return tail[:n], nil
}

// A new connection on every invocation models losing all in-memory progress.
// Any error stops this attempt. Callers only retry injected network faults;
// an unexpected EXEC result is NOT a recoverable protocol outcome here.
func kvReplayIncrement(addr, key, cp string, next kvReplayToken) (string, error) {
	conn, reader, err := authenticated(addr, "")
	if err != nil {
		return "", err
	}
	c := &kvCommitClient{conn, reader}
	defer conn.Close() // also clears WATCH/queued transactions on early return
	expect := func(want string, args ...string) error {
		raw, err := c.request(args...)
		if err != nil {
			return err
		}
		if string(raw) != want {
			return fmt.Errorf("unexpected %s result: %q", args[0], raw)
		}
		return nil
	}
	if key == cp || next.History == "" || next.Sequence <= 0 || next != kvReplayNext(next.History, next.Sequence, key) {
		return "", fmt.Errorf("invalid input token")
	}
	if err := expect("+OK\r\n", "WATCH", cp, key); err != nil {
		return "", err
	}
	saved, err := kvReplayBulk(c, cp)
	if err != nil {
		return "", err
	}
	var prev kvReplayToken
	if err := json.Unmarshal([]byte(saved), &prev); err != nil || prev.History != next.History || prev.Sequence < 0 {
		return "", fmt.Errorf("invalid checkpoint/history")
	}
	if prev == next {
		return "duplicate", nil
	}
	if prev.Sequence == math.MaxInt64 || next.Sequence != prev.Sequence+1 {
		return "", fmt.Errorf("sequence gap, stale replay or payload mismatch")
	}
	// Preconditions keep the intentional WRONGTYPE/overflow control outside
	// EXEC. This does not establish generic error rollback or TTL support.
	if err := expect(":-1\r\n", "PTTL", cp); err != nil {
		return "", err
	}
	if err := expect(":-1\r\n", "PTTL", key); err != nil {
		return "", err
	}
	value, err := kvReplayBulk(c, key)
	if err != nil {
		return "", err
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != value || n == math.MaxInt64 {
		return "", fmt.Errorf("counter must be a canonical integer without overflow")
	}
	for _, step := range []struct {
		want string
		args []string
	}{
		{"+OK\r\n", []string{"MULTI"}},
		{"+QUEUED\r\n", []string{"INCR", key}},
		{"+QUEUED\r\n", []string{"SET", cp, next.encode()}},
		{fmt.Sprintf("*2\r\n:%d\r\n+OK\r\n", n+1), []string{"EXEC"}},
	} {
		if err := expect(step.want, step.args...); err != nil {
			return "", err
		}
	}
	return "applied", nil
}

func kvCommitReplayExperiments(t *testing.T, addr string, c *kvCommitClient) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprintf("retry_exec_drop_before_%t", before), func(t *testing.T) {
			key, cp := kvCommitPrefix+"retry-counter", kvCommitPrefix+"retry-cp"
			initial := kvReplayToken{History: "isolated-history"}
			c.expect(t, "+OK\r\n", "MSET", key, "41", cp, initial.encode())
			next := kvReplayNext(initial.History, 1, key)
			relay, done := kvCommitFaultExec(t, addr, before)
			if _, err := kvReplayIncrement(relay, key, cp, next); err == nil {
				t.Fatal("fault not injected")
			}
			<-done
			want := "duplicate"
			if before {
				want = "applied"
			}
			if result, err := kvReplayIncrement(addr, key, cp, next); err != nil || result != want {
				t.Fatalf("reconnected attempt: %s %v", result, err)
			}
			kvCommitGet(t, c, key, "42")
			kvCommitGet(t, c, cp, next.encode())
			if result, err := kvReplayIncrement(addr, key, cp, next); err != nil || result != "duplicate" {
				t.Fatalf("duplicate: %s %v", result, err)
			}
			for _, invalid := range []kvReplayToken{
				kvReplayNext("other-history", 2, key),
				kvReplayNext(initial.History, 3, key),
				{initial.History, 1, "changed-payload"},
			} {
				if _, err := kvReplayIncrement(addr, key, cp, invalid); err == nil {
					t.Fatal("invalid replay accepted")
				}
			}
			kvCommitGet(t, c, cp, next.encode())
			next = kvReplayNext(initial.History, 2, key)
			if result, err := kvReplayIncrement(addr, key, cp, next); err != nil || result != "applied" {
				t.Fatalf("next: %s %v", result, err)
			}
			kvCommitGet(t, c, key, "43")
			kvCommitGet(t, c, cp, next.encode())
			t.Logf("fresh-connection retry=%s; duplicate did not increment; next value=43 sequence=2", want)
		})
	}
	t.Run("preconditions_reject_before_transaction", func(t *testing.T) {
		key, cp := kvCommitPrefix+"precheck-counter", kvCommitPrefix+"precheck-cp"
		initial := kvReplayToken{History: "isolated-history"}
		for _, value := range []string{"not-integer", "9223372036854775807", "01"} {
			c.expect(t, "+OK\r\n", "MSET", key, value, cp, initial.encode())
			if _, err := kvReplayIncrement(addr, key, cp, kvReplayNext(initial.History, 1, key)); err == nil {
				t.Fatal("invalid counter accepted")
			}
			kvCommitGet(t, c, cp, initial.encode())
			kvCommitGet(t, c, key, value)
		}
		c.expect(t, "+OK\r\n", "SET", key, "41", "PX", "60000")
		if _, err := kvReplayIncrement(addr, key, cp, kvReplayNext(initial.History, 1, key)); err == nil {
			t.Fatal("expiring counter accepted")
		}
		kvCommitGet(t, c, cp, initial.encode())
	})
	t.Run("delayed_expiry_then_incr_persist", func(t *testing.T) {
		key, cp := kvCommitPrefix+"expiry-counter", kvCommitPrefix+"expiry-cp"
		// Same command shape as the real Redis PSYNC capture test. A fixed past
		// deadline models consuming the stream after its original expiration.
		c.expect(t, "+OK\r\n", "SET", cp, "0")
		c.expect(t, "+OK\r\n", "MULTI")
		c.expect(t, "+QUEUED\r\n", "SET", key, "41", "PXAT", "1000")
		c.expect(t, "+QUEUED\r\n", "INCR", key)
		c.expect(t, "+QUEUED\r\n", "PERSIST", key)
		c.expect(t, "+QUEUED\r\n", "SET", cp, "3")
		c.expect(t, "*4\r\n+OK\r\n:1\r\n:0\r\n+OK\r\n", "EXEC")
		kvCommitGet(t, c, key, "1")
		kvCommitGet(t, c, cp, "3")
		c.expect(t, ":-1\r\n", "PTTL", key)
		t.Log("normal commands, no execution errors: delayed replay value=1 permanent; source before deadline value=42 permanent; MULTI alone does not preserve historical expiry")
	})
	t.Run("delayed_expiry_then_incr_extension", func(t *testing.T) {
		key, cp := kvCommitPrefix+"extension-counter", kvCommitPrefix+"extension-cp"
		until := strconv.FormatInt(time.Now().Add(120*time.Second).UnixMilli(), 10)
		c.expect(t, "+OK\r\n", "SET", cp, "0")
		c.expect(t, "+OK\r\n", "MULTI")
		c.expect(t, "+QUEUED\r\n", "SET", key, "41", "PXAT", "1000")
		c.expect(t, "+QUEUED\r\n", "INCR", key)
		c.expect(t, "+QUEUED\r\n", "PEXPIREAT", key, until)
		c.expect(t, "+QUEUED\r\n", "SET", cp, "3")
		c.expect(t, "*4\r\n+OK\r\n:1\r\n:1\r\n+OK\r\n", "EXEC")
		kvCommitGet(t, c, key, "1")
		kvCommitGet(t, c, cp, "3")
		t.Log("expiry extension succeeded but retained value=1, not source value=42; first-stage PEXPIRE support also needs expiry-aware replay")
	})
}
