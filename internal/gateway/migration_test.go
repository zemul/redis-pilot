package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

type migrationFixture struct {
	j        *sharding.Journal
	dir      string
	from, to *sharding.Topology
	a, b     *cacheConn
	stopA    func()
}

func newMigrationFixture(t *testing.T) *migrationFixture {
	t.Helper()
	a, stopA := migrationColdManaged(t)
	b := migrationCold(t)
	connect := func(addr string) *cacheConn {
		c, r, err := authenticated(addr, "g4-backend-test")
		if err != nil {
			t.Fatal(err)
		}
		cfg := passConfig(addr)
		cfg.RequestTimeout = 3 * time.Second
		x := &cacheConn{Conn: c, reader: r, cfg: cfg}
		t.Cleanup(func() { x.Close() })
		cacheCall(t, x, "SET", cachePrefix+"owner", "migration-test-owner")
		return x
	}
	f := &migrationFixture{a: connect(a), b: connect(b), dir: filepath.Join(t.TempDir(), "journal"), stopA: stopA}
	var err error
	f.from, err = sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 1, Nodes: []sharding.Node{{ID: "a", Address: a}}})
	if err != nil {
		t.Fatal(err)
	}
	f.to, err = sharding.New(sharding.Config{Algorithm: sharding.Algorithm, Revision: 2, Nodes: []sharding.Node{{ID: "a", Address: a}, {ID: "b", Address: b}}})
	if err != nil {
		t.Fatal(err)
	}
	f.j, err = sharding.CreateJournal(context.Background(), f.dir, "move-1", f.from, f.to)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.j.Close() })
	return f
}

func (f *migrationFixture) key(t *testing.T, prefix string) (sharding.Handoff, *migrationFence, *migrationFence) {
	t.Helper()
	var key []byte
	for i := 0; ; i++ {
		key = []byte(fmt.Sprintf("%s:%d", prefix, i))
		if f.to.Owner(key).ID == "b" {
			break
		}
	}
	h, err := f.j.Prepare(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	a, err := newMigrationFence(f.a, h.Placement.Source, cachePrefix+"owner", "migration-test-owner", h)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newMigrationFence(f.b, h.Placement.Target, cachePrefix+"owner", "migration-test-owner", h)
	if err != nil {
		t.Fatal(err)
	}
	return h, a, b
}

func (f *migrationFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.j.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.j, err = sharding.OpenJournal(context.Background(), f.dir, "move-1", f.from, f.to)
	if err != nil {
		t.Fatal(err)
	}
}

// Compare every byte/member/field, not merely cardinality. Hash/Set physical
// iteration order may differ after a logical chunked copy.
func migrationContents(t *testing.T, c *cacheConn, key, kind string) []string {
	t.Helper()
	if kind == "string" {
		s, ok := frameString(cacheCall(t, c, "GET", key))
		if !ok {
			t.Fatal("missing string")
		}
		return []string{s}
	}
	a := map[string][]string{"hash": {"HGETALL", key}, "list": {"LRANGE", key, "0", "-1"}, "set": {"SMEMBERS", key}, "zset": {"ZRANGE", key, "0", "-1", "WITHSCORES"}}[kind]
	values, err := cacheValues(cacheCall(t, c, a...))
	if err != nil {
		t.Fatal(err)
	}
	if kind == "hash" {
		pairs := make([]string, 0, len(values)/2)
		for i := 0; i < len(values); i += 2 {
			pairs = append(pairs, strconv.Quote(values[i])+":"+strconv.Quote(values[i+1]))
		}
		values = pairs
	}
	if kind == "hash" || kind == "set" {
		sort.Strings(values)
	}
	return values
}

func TestMigrationFiveTypes(t *testing.T) {
	f := newMigrationFixture(t)
	ctx := context.Background()
	for _, kind := range []string{"string", "hash", "list", "set", "zset"} {
		for _, expiring := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ttl-%v", kind, expiring), func(t *testing.T) {
				h, source, target := f.key(t, t.Name()+"\x00\xff")
				key := string(h.Key)
				if kind == "string" {
					cacheCall(t, f.a, "SET", key, strings.Repeat("data\x00\xff", 50000))
				} else {
					for base := 0; base < 600; base += 100 {
						a := []string{map[string]string{"hash": "HSET", "list": "RPUSH", "set": "SADD", "zset": "ZADD"}[kind], key}
						for i := base; i < base+100; i++ {
							v := fmt.Sprintf("%d\x00\xff", i)
							switch kind {
							case "hash":
								a = append(a, "field:"+v, "val:"+v)
							case "zset":
								a = append(a, strconv.FormatFloat(float64(i)/7, 'g', 17, 64), v)
							default:
								a = append(a, v)
							}
						}
						cacheCall(t, f.a, a...)
					}
				}
				if expiring {
					cacheCall(t, f.a, "PEXPIRE", key, "60000")
				}
				want := migrationContents(t, f.a, key, kind)
				attempt, err := copyMigrationKey(ctx, f.j, h, source, target)
				if err != nil {
					t.Fatal(err)
				}
				got := migrationContents(t, f.b, key, kind)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatal("full contents differ")
				}
				// Ready is not serving; neither source nor target may accept a
				// business command through the new guard before journal commit.
				for _, fence := range []*migrationFence{source, target} {
					if _, err := fence.run(ctx, migrationBusinessGuard+`return redis.call('GET',KEYS[3])`, ""); err == nil {
						t.Fatal("old admission passed a migration marker")
					}
				}
				if _, err := target.run(ctx, migrationBusinessGuard+`return 1`, "serving"); err == nil {
					t.Fatal("ready target was published early")
				}
				if err := finishMigrationKey(ctx, f.j, h, source, target, attempt); err != nil {
					t.Fatal(err)
				}
				if string(cacheCall(t, f.a, "EXISTS", key)) != ":0\r\n" {
					t.Fatal("source not cleaned")
				}
				loc, err := f.j.Locate(ctx, h.Key)
				if err != nil || loc.Node.ID != "b" || loc.State != sharding.Cleaned {
					t.Fatalf("wrong location: %+v %v", loc, err)
				}
				if expiring {
					ttl, _ := frameString(cacheCall(t, f.b, "PTTL", key))
					n, e := strconv.Atoi(ttl)
					if e != nil || n <= 0 || n > 60000 {
						t.Fatal("invalid transferred TTL", ttl)
					}
				} else if string(cacheCall(t, f.b, "PTTL", key)) != ":-1\r\n" {
					t.Fatal("persistent key gained TTL")
				}
				if err := finishMigrationKey(ctx, f.j, h, source, target, attempt); err != nil {
					t.Fatal("repeat finish", err)
				}
				if _, err := copyMigrationKey(ctx, f.j, h, source, target); err == nil {
					t.Fatal("committed key copied again")
				}
				if err := target.begin(ctx, attempt, "late-attempt"); err == nil {
					t.Fatal("serving target reset")
				}
			})
		}
	}
	for _, emptyString := range []bool{false, true} {
		h, s, d := f.key(t, fmt.Sprint("empty-", emptyString))
		if emptyString {
			cacheCall(t, f.a, "SET", string(h.Key), "")
		}
		attempt, err := copyMigrationKey(ctx, f.j, h, s, d)
		if err != nil {
			t.Fatal(err)
		}
		if err := finishMigrationKey(ctx, f.j, h, s, d, attempt); err != nil {
			t.Fatal(err)
		}
		want := "$-1\r\n"
		if emptyString {
			want = "$0\r\n\r\n"
		}
		if string(cacheCall(t, f.b, "GET", string(h.Key))) != want {
			t.Fatal("empty source/string confused")
		}
	}
}

type migrationIntercept struct {
	base migrationCaller
	hook func(context.Context, []string) ([]byte, error, bool)
}

func (c *migrationIntercept) call(ctx context.Context, a ...string) ([]byte, error) {
	if b, err, handled := c.hook(ctx, a); handled {
		return b, err
	}
	return c.base.call(ctx, a...)
}

func TestMigrationCopyRetryFencesLateChunks(t *testing.T) {
	for _, executeFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(executeFirst), func(t *testing.T) {
			f := newMigrationFixture(t)
			ctx := context.Background()
			h, s, d := f.key(t, "retry")
			cacheCall(t, f.a, "RPUSH", s.key, "one", "two", "three")
			var delayed []string
			d.c = &migrationIntercept{base: f.b, hook: func(ctx context.Context, a []string) ([]byte, error, bool) {
				if len(delayed) == 0 && len(a) > 1 && strings.Contains(a[1], migrationAppendScript) {
					delayed = append([]string(nil), a...)
					if executeFirst {
						if _, err := f.b.call(ctx, a...); err != nil {
							t.Fatal(err)
						}
					}
					return nil, errors.New("injected lost copy acknowledgement"), true
				}
				return nil, nil, false
			}}
			if _, err := copyMigrationKey(ctx, f.j, h, s, d); err == nil {
				t.Fatal("lost acknowledgement accepted")
			}
			f.reopen(t)
			d.c = f.b
			attempt, err := copyMigrationKey(ctx, f.j, h, s, d)
			if err != nil {
				t.Fatal(err)
			}
			if err := finishMigrationKey(ctx, f.j, h, s, d, attempt); err != nil {
				t.Fatal(err)
			}
			if _, err := d.run(ctx, migrationBusinessGuard+`return redis.call('RPUSH',KEYS[3],ARGV[4])`, "serving", "new"); err != nil {
				t.Fatal(err)
			}
			b, err := f.b.call(ctx, delayed...)
			if err != nil || len(b) == 0 || b[0] != '-' {
				t.Fatalf("late chunk accepted: %q %v", b, err)
			}
			got := migrationContents(t, f.b, d.key, "list")
			if fmt.Sprint(got) != "[one two three new]" {
				t.Fatal("duplicated/lost list values", got)
			}
		})
	}
}

func TestMigrationRecoveryAfterCommit(t *testing.T) {
	for _, failStage := range []string{"activate", "clean"} {
		t.Run(failStage, func(t *testing.T) {
			f := newMigrationFixture(t)
			ctx := context.Background()
			h, s, d := f.key(t, "committed")
			cacheCall(t, f.a, "SET", s.key, "old")
			attempt, err := copyMigrationKey(ctx, f.j, h, s, d)
			if err != nil {
				t.Fatal(err)
			}
			fence, script := d, migrationActivateScript
			if failStage == "clean" {
				fence, script = s, migrationCleanScript
			}
			base := fence.c
			var delayed []string
			fence.c = &migrationIntercept{base: base, hook: func(ctx context.Context, a []string) ([]byte, error, bool) {
				if len(a) > 1 && strings.Contains(a[1], script) {
					delayed = append([]string(nil), a...)
					return nil, errors.New("injected interrupted finish"), true
				}
				return nil, nil, false
			}}
			if err := finishMigrationKey(ctx, f.j, h, s, d, attempt); err == nil {
				t.Fatal("failure hidden")
			}
			loc, err := f.j.Locate(ctx, h.Key)
			if err != nil || loc.State != sharding.Committed || loc.Node.ID != "b" {
				t.Fatalf("commit lost: %+v %v", loc, err)
			}
			fence.c = base
			f.reopen(t)
			if err := finishMigrationKey(ctx, f.j, h, s, d, attempt); err != nil {
				t.Fatal(err)
			}
			if _, err := d.run(ctx, migrationBusinessGuard+`return redis.call('SET',KEYS[3],ARGV[4])`, "serving", "new"); err != nil {
				t.Fatal(err)
			}
			// A delayed activate/cleanup is harmless, never recopies old data.
			if _, err := base.call(ctx, delayed...); err != nil {
				t.Fatal(err)
			}
			if string(cacheCall(t, f.b, "GET", d.key)) != "$3\r\nnew\r\n" {
				t.Fatal("late finish overwrote new value")
			}
			if _, err := d.run(ctx, migrationBusinessGuard+`return redis.call('DEL',KEYS[3])`, "serving"); err != nil {
				t.Fatal(err)
			}
			loc, err = f.j.Locate(ctx, h.Key)
			if err != nil || loc.Node.ID != "b" {
				t.Fatal("deleted target fell back to source")
			}
		})
	}
}

func TestMigrationFenceProtocol(t *testing.T) {
	f := newMigrationFixture(t)
	ctx := context.Background()
	h, s, d := f.key(t, "protocol")
	cacheCall(t, f.a, "SET", s.key, "old")
	if err := s.freeze(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.begin(ctx, "", "attempt-a"); err != nil {
		t.Fatal(err)
	}
	if err := d.append(ctx, "attempt-a", 0, "RPUSH", "one"); err != nil {
		t.Fatal(err)
	}
	if err := d.begin(ctx, "", "attempt-a"); err != nil {
		t.Fatal("duplicate begin", err)
	}
	if got := migrationContents(t, f.b, d.key, "list"); fmt.Sprint(got) != "[one]" {
		t.Fatal("duplicate begin deleted copied chunks")
	}
	if err := d.append(ctx, "attempt-a", 0, "RPUSH", "one"); err == nil {
		t.Fatal("duplicate chunk accepted")
	}
	if err := d.append(ctx, "attempt-a", 2, "RPUSH", "skipped"); err == nil {
		t.Fatal("out of order chunk accepted")
	}
	// A wrong-type mutation leaves an unusable applying marker. Retrying a
	// non-idempotent command on that attempt must fail; restart under a fresh one.
	if err := d.append(ctx, "attempt-a", 1, "HSET", "f", "v"); err == nil {
		t.Fatal("wrong type succeeded")
	}
	if err := d.append(ctx, "attempt-a", 1, "RPUSH", "two"); err == nil {
		t.Fatal("failed attempt remained usable")
	}
	if err := d.begin(ctx, "attempt-a", "attempt-b"); err != nil {
		t.Fatal(err)
	}
	if err := d.append(ctx, "attempt-a", 1, "RPUSH", "late"); err == nil {
		t.Fatal("old attempt accepted")
	}
	if err := d.begin(ctx, "", "attempt-a"); err == nil {
		t.Fatal("old begin rewound attempt")
	}
	// Old process tokens are checked inside the backend script, not only in Go.
	cacheCall(t, f.b, "SET", cachePrefix+"owner", "new-owner")
	if err := d.append(ctx, "attempt-b", 0, "RPUSH", "late"); err == nil {
		t.Fatal("old owner accepted")
	}
	d.owner = "new-owner"
	if err := d.append(ctx, "attempt-b", 0, "RPUSH", "correct"); err != nil {
		t.Fatal(err)
	}
	if err := d.seal(ctx, "attempt-b", "list", 1, 2, -1); err == nil {
		t.Fatal("incomplete copy sealed")
	}
	if err := d.activate(ctx, "attempt-b"); err == nil {
		t.Fatal("incomplete copy activated")
	}
	if err := finishMigrationKey(ctx, f.j, h, s, d, "attempt-b"); err == nil {
		t.Fatal("incomplete copy committed")
	}
	if loc, err := f.j.Locate(ctx, h.Key); err != nil || loc.State != sharding.Prepared {
		t.Fatal("journal advanced on incomplete copy")
	}
}

func TestMigrationTTLAndExpiredDuringCopy(t *testing.T) {
	f := newMigrationFixture(t)
	ctx := context.Background()
	for _, expireDuringCopy := range []bool{false, true} {
		h, s, d := f.key(t, fmt.Sprint("expiry-", expireDuringCopy))
		cacheCall(t, f.a, "SET", s.key, strings.Repeat("s", 200000), "PX", "10000")
		expected := cacheCall(t, f.a, "PEXPIRETIME", s.key)
		intercepted := false
		d.c = &migrationIntercept{base: f.b, hook: func(ctx context.Context, a []string) ([]byte, error, bool) {
			if !intercepted && len(a) > 1 && strings.Contains(a[1], migrationAppendScript) {
				intercepted = true
				// Deterministic natural-expiry equivalent, without timing sleeps.
				if expireDuringCopy {
					cacheCall(t, f.a, "PEXPIREAT", s.key, "1")
				}
			}
			return nil, nil, false
		}}
		attempt, err := copyMigrationKey(ctx, f.j, h, s, d)
		if err != nil {
			t.Fatal(err)
		}
		if err := finishMigrationKey(ctx, f.j, h, s, d, attempt); err != nil {
			t.Fatal(err)
		}
		if expireDuringCopy {
			if string(cacheCall(t, f.b, "EXISTS", d.key)) != ":0\r\n" {
				t.Fatal("expired source resurrected")
			}
		} else {
			got := cacheCall(t, f.b, "PEXPIRETIME", d.key)
			if !bytes.Equal(got, expected) {
				t.Fatalf("TTL restarted: %q != %q", got, expected)
			}
		}
	}
}

func TestMigrationFreezeAgainstConcurrentGuardedWrites(t *testing.T) {
	f := newMigrationFixture(t)
	ctx := context.Background()
	h, s, d := f.key(t, "concurrent")
	cacheCall(t, f.a, "SET", s.key, "0")
	c, r, err := authenticated(s.node.Address, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	writer := *s
	writer.c = &cacheConn{Conn: c, reader: r, cfg: f.a.cfg}
	started := make(chan struct{})
	done := make(chan int, 1)
	errs := make(chan error, 1)
	go func() {
		confirmed := 0
		defer func() { done <- confirmed }()
		for i := 0; i < 200; i++ {
			b, err := writer.run(ctx, migrationBusinessGuard+`return redis.call('INCR',KEYS[3])`, "")
			if err != nil {
				if i == 0 {
					close(started)
				}
				if !strings.Contains(err.Error(), "migration state changed") {
					errs <- err
				}
				return
			}
			confirmed++
			if string(b) != fmt.Sprintf(":%d\r\n", confirmed) {
				errs <- fmt.Errorf("bad increment reply %q", b)
				if i == 0 {
					close(started)
				}
				return
			}
			if i == 0 {
				close(started)
			}
		}
	}()
	<-started
	attempt, err := copyMigrationKey(ctx, f.j, h, s, d)
	if err != nil {
		t.Fatal(err)
	}
	confirmed := <-done
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if err := finishMigrationKey(ctx, f.j, h, s, d, attempt); err != nil {
		t.Fatal(err)
	}
	got, ok := frameString(cacheCall(t, f.b, "GET", d.key))
	if !ok || got != strconv.Itoa(confirmed) {
		t.Fatalf("lost successful write: got=%q confirmed=%d", got, confirmed)
	}
}

func TestMigrationJournalFailureDoesNotActivate(t *testing.T) {
	f := newMigrationFixture(t)
	h, s, d := f.key(t, "journal-error")
	cacheCall(t, f.a, "SET", s.key, "value")
	attempt, err := copyMigrationKey(context.Background(), f.j, h, s, d)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel after backend prechecks but before the SQLite commit is attempted.
	s.c = &migrationIntercept{base: f.a, hook: func(ctx context.Context, a []string) ([]byte, error, bool) {
		if len(a) > 1 && strings.Contains(a[1], migrationStateScript) {
			b, err := f.a.call(ctx, a...)
			cancel()
			return b, err, true
		}
		return nil, nil, false
	}}
	if err := finishMigrationKey(ctx, f.j, h, s, d, attempt); err == nil {
		t.Fatal("canceled journal commit acknowledged")
	}
	phase, _, err := d.state(context.Background())
	if err != nil || phase != "ready" {
		t.Fatalf("target activated despite journal failure: %s %v", phase, err)
	}
	loc, err := f.j.Locate(context.Background(), h.Key)
	if err != nil || loc.State != sharding.Prepared {
		t.Fatal("journal advanced")
	}
	s.c = f.a
	if err := finishMigrationKey(context.Background(), f.j, h, s, d, attempt); err != nil {
		t.Fatal("recovery failed", err)
	}
}
