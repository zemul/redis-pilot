package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type cacheRuntime struct {
	s               *Service
	cfg             CacheConfig
	coldPassword    string
	guard           string
	lock            *os.File
	mu              sync.Mutex
	token, previous string
	settled         bool
	hotGeneration   atomic.Uint64
	keys            [256]chan struct{}
	loads           chan struct{}
	blocking        chan struct{}
	events          *prometheus.CounterVec
}

func newCacheRuntime(s *Service) (*cacheRuntime, error) {
	o := s.cfg.Cache.resolved()
	p, err := readPassword(o.ColdPasswordFile)
	if err != nil {
		return nil, err
	}
	r := &cacheRuntime{s: s, cfg: o, coldPassword: p, guard: cachePrefix + "owner", loads: make(chan struct{}, o.MaxLoads), blocking: make(chan struct{}, o.MaxBlocking)}
	for i := range r.keys {
		r.keys[i] = make(chan struct{}, 1)
	}
	r.events = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "redis_pilot_gateway_cache_events_total", Help: "Bounded cache outcomes, without business key labels."}, []string{"outcome"})
	s.registry.MustRegister(r.events)
	return r, nil
}

func (r *cacheRuntime) start(ctx context.Context) error {
	parent, err := os.Lstat(filepath.Dir(r.cfg.LockFile))
	if err != nil || !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return errors.New("cache lock parent must be a private directory")
	}
	f, err := os.OpenFile(r.cfg.LockFile, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return errors.New("cannot open cache ownership lock")
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		f.Close()
		return errors.New("invalid cache lock file")
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return errors.New("cache ownership lock is held")
	}
	r.lock = f
	// One reserved cold key fences delayed requests from a previous process.
	// No user data is copied or deleted when a new cache generation starts.
	c, err := r.connect(ctx, false)
	if err != nil {
		r.close()
		return errors.New("cold backend unavailable at startup")
	}
	defer c.Close()
	// Native blocking commands are safe only while their key ranges remain
	// permanently uncached, including after a Gateway restart. Refuse an
	// in-place policy change instead of trusting that old pops have finished.
	prefixes := append([]string{}, r.cfg.ColdOnlyPrefixes...)
	sort.Strings(prefixes)
	policy, _ := json.Marshal(prefixes)
	b, err := c.call(ctx, "SET", cachePrefix+"cold-only-policy", string(policy), "NX")
	if err != nil || len(b) == 0 || b[0] == '-' {
		r.close()
		return errors.New("cannot bind cold-only policy")
	}
	b, err = c.call(ctx, "GET", cachePrefix+"cold-only-policy")
	stored, valid := frameString(b)
	if err != nil || !valid || stored != string(policy) {
		r.close()
		return errors.New("cold-only prefix policy differs; in-place changes require a separate migration")
	}
	b, err = c.call(ctx, "GET", r.guard)
	if err != nil || len(b) == 0 || b[0] == '-' {
		r.close()
		return errors.New("cannot read cold ownership")
	}
	previous, ok := frameString(b)
	if !ok && string(b) != "$-1\r\n" {
		r.close()
		return errors.New("invalid cold ownership")
	}
	r.mu.Lock()
	r.token = rand.Text()
	r.previous = previous
	r.settled = false
	r.mu.Unlock()
	if _, err = r.ensureEpoch(ctx); err != nil {
		r.close()
		return err
	}
	r.s.backendReady.Store(true)
	return nil
}

func (r *cacheRuntime) close() {
	if r.lock != nil {
		syscall.Flock(int(r.lock.Fd()), syscall.LOCK_UN)
		r.lock.Close()
		r.lock = nil
	}
}

// CAS retries are safe even when the prior confirmation was lost. An old CAS
// cannot overwrite a newer token, and business writes never retry themselves.
func (r *cacheRuntime) ensureEpoch(ctx context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.settled {
		return r.token, nil
	}
	c, err := r.connect(ctx, false)
	if err != nil {
		return "", errors.New("cold recovery unavailable")
	}
	defer c.Close()
	b, err := c.call(ctx, "EVAL", cacheClaimScript, "1", r.guard, r.previous, r.token)
	if err != nil || string(b) != ":1\r\n" {
		if err == nil && string(b) == ":0\r\n" {
			current, e := c.call(ctx, "GET", r.guard)
			if e == nil && string(current) == "$-1\r\n" {
				r.previous = ""
			}
		}
		return "", errors.New("cold ownership not confirmed")
	}
	r.settled = true
	r.events.WithLabelValues("epoch_ready").Inc()
	return r.token, nil
}

func (r *cacheRuntime) uncertainEpoch(token string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.token != token || !r.settled {
		return
	}
	r.previous = r.token
	r.token = rand.Text()
	r.settled = false
	r.events.WithLabelValues("cold_result_unknown").Inc()
}

func (r *cacheRuntime) hotFailed() {
	r.hotGeneration.Add(1)
	r.events.WithLabelValues("hot_bypass").Inc()
}

func (r *cacheRuntime) probe(ctx context.Context) {
	for {
		token, err := r.ensureEpoch(ctx)
		if err == nil {
			c, e := r.connect(ctx, false)
			err = e
			if e == nil {
				var b []byte
				b, err = c.call(ctx, "GET", r.guard)
				c.Close()
				current, ok := frameString(b)
				if err != nil || !ok || current != token {
					r.uncertainEpoch(token)
					err = errors.New("cold ownership requires recovery")
				}
			}
		}
		r.s.backendReady.Store(err == nil)
		t := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (r *cacheRuntime) acquire(ctx context.Context, key []byte) (func(), error) {
	h := sha256.Sum256(key)
	q := r.keys[h[0]]
	select {
	case q <- struct{}{}:
		return func() { <-q }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *cacheRuntime) hotKeys(token string, key []byte) (string, string) {
	h := sha256.Sum256(key)
	k := fmt.Sprintf("%s%s:%d:%s", cachePrefix, token, r.hotGeneration.Load(), hex.EncodeToString(h[:]))
	return k + ":data", k + ":complete"
}

func (r *cacheRuntime) execute(parent context.Context, args [][]byte) []byte {
	if len(args) > 0 {
		n := strings.ToUpper(string(args[0]))
		if n == "PING" && len(args) == 1 {
			return []byte("+PONG\r\n")
		}
		if (n == "PING" || n == "ECHO") && len(args) == 2 {
			return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1]))
		}
		if n == "BLPOP" || n == "BRPOP" {
			return r.block(parent, args)
		}
	}
	cmd, ok := classifyCacheCommand(args)
	if !ok {
		return []byte("-ERR unsupported cache command, options or key\r\n")
	}
	ctx, cancel := context.WithTimeout(parent, r.s.cfg.RequestTimeout)
	defer cancel()
	release, err := r.acquire(ctx, args[1])
	if err != nil {
		return []byte("-TRYAGAIN key coordination timeout; command not sent\r\n")
	}
	defer release()
	token, err := r.ensureEpoch(ctx)
	if err != nil {
		return []byte("-TRYAGAIN cold recovery unavailable; command not sent\r\n")
	}
	data, complete := r.hotKeys(token, args[1])
	var hot *cacheConn
	if !r.coldOnly(args[1]) {
		hot, err = r.connect(ctx, true)
		if err != nil {
			r.hotFailed()
		} else {
			defer hot.Close()
		}
	}
	present := false
	if hot != nil && (cmd.cacheRead || (cmd.write && r.cfg.WritePolicy == "update_existing")) {
		script := cachePresentScript
		a := []string{"EVAL", script, "2", data, complete}
		if cmd.cacheRead {
			a[1] = cacheReadScript
			a = append(a, cmd.name)
			for _, v := range args[2:] {
				a = append(a, string(v))
			}
		}
		b, e := hot.call(ctx, a...)
		if e != nil || len(b) == 0 || b[0] == '-' {
			r.hotFailed()
			hot.Close()
			hot = nil
		} else if cmd.cacheRead {
			parts, e := frameParts(b)
			if e != nil || len(parts) < 1 {
				r.hotFailed()
				hot.Close()
				hot = nil
			} else if string(parts[0]) == ":1\r\n" && len(parts) == 2 {
				r.events.WithLabelValues("hit").Inc()
				return parts[1]
			}
		} else {
			present = string(b) == ":1\r\n"
		}
	}
	r.events.WithLabelValues("cold_request").Inc()
	cold, err := r.connect(ctx, false)
	if err != nil {
		return []byte("-TRYAGAIN cold backend unavailable; command not sent\r\n")
	}
	defer cold.Close()
	a := []string{"EVAL", cacheColdScript, "2", r.guard, string(args[1]), token, cmd.name, "normal"}
	if cmd.exactInteger {
		a[7] = "integer"
	}
	for _, v := range args[2:] {
		a = append(a, string(v))
	}
	b, err := cold.call(ctx, a...)
	if err != nil {
		if cmd.write {
			r.uncertainEpoch(token)
			r.s.uncertain.Inc()
		} else {
			r.events.WithLabelValues("cold_read_error").Inc()
			return []byte("-ERR cold read failed\r\n")
		}
		return []byte("-ERR cold result unknown; command was not retried\r\n")
	}
	if cmd.exactInteger && len(b) > 0 && b[0] == '$' {
		v, ok := frameString(b)
		if !ok {
			return []byte("-ERR invalid cold integer response\r\n")
		}
		b = []byte(":" + v + "\r\n")
	}
	// Kvrocks 2.15 scripting returns some statuses as bulk strings. Restore
	// the native public command's RESP2 shape.
	if cmd.name == "SET" || cmd.name == "LSET" || cmd.name == "LTRIM" {
		if v, ok := frameString(b); ok && v == "OK" {
			b = []byte("+OK\r\n")
		}
	}
	if cmd.name == "TYPE" {
		if v, ok := frameString(b); ok {
			b = []byte("+" + v + "\r\n")
		}
	}
	// Error replies can follow partially effective engine commands. Invalidate
	// writes even on a server error; never assume an error implies no mutation.
	if cmd.write {
		if hot != nil {
			x, e := hot.call(ctx, "DEL", data, complete)
			if e != nil || len(x) == 0 || x[0] == '-' {
				r.hotFailed()
				hot.Close()
				hot = nil
			} else {
				r.events.WithLabelValues("invalidated").Inc()
			}
		}
		if len(b) > 0 && b[0] != '-' && hot != nil && r.cfg.WritePolicy == "update_existing" && ((cmd.update && present) || cmd.create) {
			r.fill(ctx, cold, hot, args[1], data, complete)
			r.events.WithLabelValues("update_attempt").Inc()
		}
	} else if cmd.cacheRead && len(b) > 0 && b[0] != '-' && hot != nil {
		r.fill(ctx, cold, hot, args[1], data, complete)
	}
	return b
}

// snapshot returns a native Redis creation command only for a complete,
// non-expiring, small object. TTL-bearing business keys bypass this version.
func (r *cacheRuntime) snapshot(ctx context.Context, c *cacheConn, key []byte) ([]string, error) {
	b, err := c.call(ctx, "TYPE", string(key))
	if err != nil {
		return nil, err
	}
	kind, ok := frameString(b)
	if !ok {
		return nil, errors.New("invalid type")
	}
	b, err = c.call(ctx, "PTTL", string(key))
	if err != nil || string(b) != ":-1\r\n" {
		return nil, errors.New("expiring or absent object")
	}
	countCmd, readCmd, create := "", "", ""
	switch kind {
	case "string":
		countCmd, readCmd, create = "STRLEN", "GET", "SET"
	case "hash":
		countCmd, readCmd, create = "HLEN", "HGETALL", "HSET"
	case "list":
		countCmd, readCmd, create = "LLEN", "LRANGE", "RPUSH"
	case "set":
		countCmd, readCmd, create = "SCARD", "SMEMBERS", "SADD"
	case "zset":
		countCmd, readCmd, create = "ZCARD", "ZRANGE", "ZADD"
	default:
		return nil, errors.New("uncacheable type")
	}
	b, err = c.call(ctx, countCmd, string(key))
	if err != nil {
		return nil, err
	}
	str, ok := frameString(b)
	n, e := strconv.Atoi(str)
	objectLimits := r.cfg.objectLimits(kind)
	limit := objectLimits.MaxElements
	if kind == "string" {
		limit = objectLimits.MaxBytes
	}
	if !ok || e != nil || n < 0 || n > limit {
		return nil, errors.New("object exceeds element limit")
	}
	a := []string{readCmd, string(key)}
	if kind == "list" || kind == "zset" {
		a = append(a, "0", "-1")
	}
	if kind == "zset" {
		a = append(a, "WITHSCORES")
	}
	// Abandon oversized snapshot frames, with a separate cap from business
	// replies. The read-only connection is discarded after any decoder error.
	c.reader.limits = Limits{MaxBytes: objectLimits.MaxBytes + 32*objectLimits.MaxElements + 1024, MaxElements: 2*objectLimits.MaxElements + 4, MaxDepth: 4}
	b, err = c.call(ctx, a...)
	if err != nil {
		return nil, err
	}
	values := []string{}
	if kind == "string" {
		v, ok := frameString(b)
		if !ok {
			return nil, errors.New("invalid string")
		}
		values = append(values, v)
	} else {
		parts, e := frameParts(b)
		if e != nil {
			return nil, e
		}
		for _, part := range parts {
			v, ok := frameString(part)
			if !ok {
				return nil, errors.New("invalid element")
			}
			values = append(values, v)
		}
		if len(values) == 0 {
			return nil, errors.New("absent collection")
		}
		want := n
		if kind == "hash" || kind == "zset" {
			want *= 2
		}
		if len(values) != want {
			return nil, errors.New("incomplete object")
		}
	}
	size := 0
	for _, v := range values {
		size += len(v)
	}
	if size > objectLimits.MaxBytes {
		return nil, errors.New("object exceeds byte limit")
	}
	if kind == "zset" {
		for i := 0; i < len(values); i += 2 {
			values[i], values[i+1] = values[i+1], values[i]
		}
	}
	return append([]string{create}, values...), nil
}

func (r *cacheRuntime) fill(ctx context.Context, cold, hot *cacheConn, key []byte, data, complete string) {
	select {
	case r.loads <- struct{}{}:
		defer func() { <-r.loads }()
	default:
		r.events.WithLabelValues("load_limited").Inc()
		return
	}
	a, err := r.snapshot(ctx, cold, key)
	if err != nil {
		r.events.WithLabelValues("load_skipped").Inc()
		return
	}
	request := []string{"EVAL", cachePublishScript, "2", data, complete, a[0], strconv.FormatInt(r.cfg.TTL.Milliseconds(), 10)}
	request = append(request, a[1:]...)
	b, err := hot.call(ctx, request...)
	if err != nil || string(b) != ":1\r\n" {
		r.hotFailed()
		return
	}
	r.events.WithLabelValues("filled").Inc()
}
