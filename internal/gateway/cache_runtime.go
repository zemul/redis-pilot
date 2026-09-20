package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type cacheRuntime struct {
	shards          *cacheShards
	shardState      *shardDiskState // owned only by configured sharding, closed after workers exit
	s               *Service
	cfg             CacheConfig
	coldPassword    string
	guard           string
	lock            *os.File
	mu              sync.Mutex
	token, previous string
	settled         bool
	hotMu           sync.RWMutex
	hotStateMu      sync.Mutex
	hotFailures     uint64
	hotReady        atomic.Bool
	keys            [256]chan struct{}
	loads           chan struct{}
	blocking        chan struct{}
	events          *prometheus.CounterVec
	scriptsMu       sync.Mutex
	scripts         map[string]string
	scriptBytes     int
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
	if r.cfg.Sharding != nil {
		if err := r.restoreSharding(ctx); err != nil {
			r.close()
			return err
		}
	}
	if r.shards != nil {
		if err := r.shards.start(ctx); err != nil {
			r.close()
			return err
		}
		ready := true
		for _, n := range r.shards.requiredNodes() {
			if n.blocked.Load() {
				ready = false
			}
		}
		r.s.backendReady.Store(ready)
		return nil
	}
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
	if r.shardState != nil {
		r.shardState.close()
		r.shardState = nil
	}
	if r.lock != nil {
		syscall.Flock(int(r.lock.Fd()), syscall.LOCK_UN)
		r.lock.Close()
		r.lock = nil
	}
}

// CAS retries are safe even when the prior confirmation was lost. An old CAS
// cannot overwrite a newer token, and business writes never retry themselves.
func (r *cacheRuntime) ensureEpoch(ctx context.Context) (string, error) {
	if r.shards != nil {
		return r.shards.epoch(ctx)
	}
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
	if r.shards != nil {
		r.shards.uncertain(token)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.token != token || !r.settled {
		return
	}
	r.previous = r.token
	r.token = rand.Text()
	r.settled = false
	r.hotFailed()
	r.events.WithLabelValues("cold_result_unknown").Inc()
}

func (r *cacheRuntime) hotFailed() {
	if !r.cfg.enabled() {
		return
	}
	r.hotStateMu.Lock()
	defer r.hotStateMu.Unlock()
	r.hotFailures++
	r.hotReady.Store(false)
	r.events.WithLabelValues("hot_bypass").Inc()
}

func (r *cacheRuntime) probe(ctx context.Context) {
	if r.shards != nil {
		r.shards.probe(ctx)
		return
	}
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
		if err == nil {
			r.recoverHot(ctx)
		}
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
	if a := shardAdmissionFrom(ctx); a != nil {
		return a.reenter(r, [][]byte{key})
	}
	h := sha256.Sum256(key)
	q := r.keys[h[0]]
	select {
	case q <- struct{}{}:
		return func() { <-q }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Use the same buckets as single-key commands, deduplicate collisions, and
// acquire in a global order. Sorting key names alone cannot order lock buckets.
func (r *cacheRuntime) acquireKeys(ctx context.Context, keys [][]byte) (func(), error) {
	if a := shardAdmissionFrom(ctx); a != nil {
		return a.reenter(r, keys)
	}
	var needed [256]bool
	for _, key := range keys {
		h := sha256.Sum256(key)
		needed[h[0]] = true
	}
	held := make([]int, 0, len(r.keys))
	release := func() {
		for i := len(held) - 1; i >= 0; i-- {
			<-r.keys[held[i]]
		}
	}
	for i, want := range needed {
		if !want {
			continue
		}
		select {
		case r.keys[i] <- struct{}{}:
			held = append(held, i)
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
	return release, nil
}

func (r *cacheRuntime) execute(parent context.Context, args [][]byte) []byte {
	if r.shards != nil && shardAdmissionFrom(parent) == nil {
		return r.shards.execute(parent, args)
	}
	if len(args) > 0 {
		n := strings.ToUpper(string(args[0]))
		if n == "SCRIPT" {
			return r.scriptCommand(parent, args)
		}
		if n == "EVAL" || n == "EVALSHA" || n == "EVAL_RO" || n == "EVALSHA_RO" {
			return r.executeScript(parent, args)
		}
		if n == "PING" && len(args) == 1 {
			return []byte("+PONG\r\n")
		}
		if (n == "PING" || n == "ECHO") && len(args) == 2 {
			return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1]))
		}
		if n == "BLPOP" || n == "BRPOP" {
			return r.block(parent, args)
		}
		if n == "MGET" || n == "MSET" || n == "MSETNX" || (len(args) > 2 && (n == "DEL" || n == "UNLINK" || n == "EXISTS")) {
			return r.executeMulti(parent, n, args)
		}
		if p, ok := classifyCacheCross(args); ok {
			return r.executeCross(parent, args, p)
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
	r.recoverHot(ctx)
	// Recovery drains hot users, not cold-only traffic. A write which bypassed
	// hot must invalidate readiness again if it overlaps recovery.
	leased := r.hotMu.TryRLock()
	if leased && !r.hotReady.Load() {
		r.hotMu.RUnlock()
		leased = false
	}
	if leased {
		defer r.hotMu.RUnlock()
	}
	data := string(args[1])
	var hot *cacheConn
	if leased && !r.coldOnly(args[1]) {
		hot, err = r.connect(ctx, true)
		if err != nil {
			r.hotFailed()
		} else {
			defer hot.Close()
		}
	}
	if hot != nil && cmd.cacheRead {
		a := []string{"EVAL", cacheReadScript, "1", data}
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
				return cacheReply(cmd, args, parts[1])
			}
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
	if cmd.name == "SET" && len(b) > 0 && b[0] != '-' {
		parts, e := frameParts(b)
		if e != nil || len(parts) != 2 || (string(parts[1]) != ":0\r\n" && string(parts[1]) != ":1\r\n") {
			r.uncertainEpoch(token)
			return []byte("-ERR invalid cold SET confirmation; command was not retried\r\n")
		}
		b = parts[0]
		if string(parts[1]) == ":0\r\n" {
			return b
		}
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
	if (cmd.name == "SET" && !cacheSetGet(args)) || cmd.name == "SETEX" || cmd.name == "PSETEX" || cmd.name == "HMSET" || cmd.name == "LSET" || cmd.name == "LTRIM" {
		if v, ok := frameString(b); ok && v == "OK" {
			b = []byte("+OK\r\n")
		}
	}
	if cmd.name == "TYPE" {
		if v, ok := frameString(b); ok {
			b = []byte("+" + v + "\r\n")
		}
	}
	b = cacheReply(cmd, args, b)
	// Error replies can follow partially effective engine commands. Invalidate
	// writes even on a server error; never assume an error implies no mutation.
	// Confirmed conditional no-ops must not publish the requested value.
	if (cmd.name == "SETNX" || cmd.name == "HSETNX" || cmd.name == "EXPIRE" || cmd.name == "PEXPIRE" || cmd.name == "EXPIREAT" || cmd.name == "PEXPIREAT" || cmd.name == "PERSIST") && string(b) == ":0\r\n" {
		return b
	}
	if cmd.write {
		if !leased && !r.coldOnly(args[1]) {
			r.hotFailed()
		}
		if hot != nil && len(b) > 0 && b[0] != '-' && r.cfg.WritePolicy == "update_existing" && cmd.name != "DEL" && cmd.name != "UNLINK" && cmd.name != "GETDEL" {
			r.updateHot(ctx, cold, hot, cmd, args, b)
			return b
		}
		if hot != nil {
			x, e := hot.call(ctx, "DEL", data)
			if e != nil || len(x) == 0 || x[0] == '-' {
				r.hotFailed()
				hot.Close()
				hot = nil
			} else {
				r.events.WithLabelValues("invalidated").Inc()
			}
		}
	} else if cmd.cacheRead && len(b) > 0 && b[0] != '-' && hot != nil {
		r.fill(ctx, cold, hot, args[1], data)
	}
	return b
}
