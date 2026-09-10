package gateway

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"
)

func (r *cacheRuntime) coldOnly(key []byte) bool {
	for _, prefix := range r.cfg.ColdOnlyPrefixes {
		if strings.HasPrefix(string(key), prefix) {
			return true
		}
	}
	return false
}

// Native blocking mutation cannot run inside the fenced Lua script. Only
// permanent cold-only prefixes are admitted, so delayed pops cannot invalidate
// a newly filled cache. Removing a prefix requires draining old clients first.
func (r *cacheRuntime) block(parent context.Context, args [][]byte) []byte {
	if len(args) < 3 || len(args) > 18 {
		return []byte("-ERR BLPOP/BRPOP require 1..16 cold-only keys and a timeout\r\n")
	}
	for _, key := range args[1 : len(args)-1] {
		if len(key) > 4096 || strings.HasPrefix(string(key), cachePrefix) || !r.coldOnly(key) {
			return []byte("-ERR blocking keys must belong to configured cold_only_prefixes\r\n")
		}
	}
	seconds, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil || math.IsNaN(seconds) || seconds <= 0 || seconds > 60 {
		return []byte("-ERR blocking timeout must be in (0,60] seconds; infinite waits are not supported yet\r\n")
	}
	select {
	case r.blocking <- struct{}{}:
		defer func() { <-r.blocking }()
	default:
		return []byte("-TRYAGAIN blocking connection limit reached; command not sent\r\n")
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(seconds*float64(time.Second))+r.s.cfg.RequestTimeout)
	defer cancel()
	if _, err := r.ensureEpoch(ctx); err != nil {
		return []byte("-TRYAGAIN cold recovery unavailable; command not sent\r\n")
	}
	c, err := r.connect(ctx, false)
	if err != nil {
		return []byte("-TRYAGAIN cold backend unavailable; command not sent\r\n")
	}
	defer c.Close()
	c.cfg.RequestTimeout = time.Duration(seconds*float64(time.Second)) + r.s.cfg.RequestTimeout
	a := make([]string, len(args))
	for i, v := range args {
		a[i] = string(v)
	}
	b, err := c.call(ctx, a...)
	if err != nil {
		r.s.uncertain.Inc()
		r.events.WithLabelValues("blocking_unknown").Inc()
		return []byte("-ERR blocking result unknown; command was not retried\r\n")
	}
	r.events.WithLabelValues("blocking_completed").Inc()
	// Kvrocks 2.15 uses a nil bulk for a timed-out blocking pop; expose the
	// Redis RESP2 nil-array shape for this command.
	if string(b) == "$-1\r\n" {
		return []byte("*-1\r\n")
	}
	return b
}
