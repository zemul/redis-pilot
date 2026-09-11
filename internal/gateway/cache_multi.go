package gateway

import (
	"context"
	"strconv"
	"strings"
)

func cacheMultiKeys(name string, args [][]byte) ([][]byte, bool) {
	step := 1
	switch name {
	case "MSET", "MSETNX":
		if len(args) < 3 || len(args)%2 != 1 {
			return nil, false
		}
		step = 2
	case "MGET", "DEL", "UNLINK", "EXISTS":
		if len(args) < 2 {
			return nil, false
		}
	default:
		return nil, false
	}
	keys := make([][]byte, 0, (len(args)-1)/step)
	for i := 1; i < len(args); i += step {
		key := args[i]
		if len(key) > 4096 || strings.HasPrefix(string(key), cachePrefix) {
			return nil, false
		}
		keys = append(keys, key)
	}
	return keys, true
}

// One cold command, not a loop of SET/GET calls. The local lock set remains
// held through the cold operation, every hot update, and failed-hot isolation.
func (r *cacheRuntime) executeMulti(parent context.Context, name string, args [][]byte) []byte {
	keys, ok := cacheMultiKeys(name, args)
	if !ok {
		return []byte("-ERR unsupported cache command, options or key\r\n")
	}
	ctx, cancel := context.WithTimeout(parent, r.s.cfg.RequestTimeout)
	defer cancel()
	release, err := r.acquireKeys(ctx, keys)
	if err != nil {
		return []byte("-TRYAGAIN key coordination timeout; command not sent\r\n")
	}
	defer release()
	token, err := r.ensureEpoch(ctx)
	if err != nil {
		return []byte("-TRYAGAIN cold recovery unavailable; command not sent\r\n")
	}
	r.recoverHot(ctx)
	leased := r.hotMu.TryRLock()
	if leased && !r.hotReady.Load() {
		r.hotMu.RUnlock()
		leased = false
	}
	if leased {
		defer r.hotMu.RUnlock()
	}
	cacheable := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	allCacheable := true
	for _, key := range keys {
		if r.coldOnly(key) {
			allCacheable = false
			continue
		}
		if !seen[string(key)] {
			cacheable = append(cacheable, string(key))
			seen[string(key)] = true
		}
	}
	var hot *cacheConn
	if leased && len(cacheable) > 0 && name != "EXISTS" {
		hot, err = r.connect(ctx, true)
		if err != nil {
			r.hotFailed()
		} else {
			defer hot.Close()
		}
	}
	if name == "MGET" && hot != nil && allCacheable {
		a := []string{"EVAL", cacheMGetHotScript, strconv.Itoa(len(keys))}
		for _, key := range keys {
			a = append(a, string(key))
		}
		b, e := hot.call(ctx, a...)
		parts, pe := frameParts(b)
		if e == nil && pe == nil && len(parts) == 2 && string(parts[0]) == ":1\r\n" && cacheMGetReply(parts[1], len(keys)) {
			r.events.WithLabelValues("hit").Inc()
			return parts[1]
		}
		if e != nil || pe != nil || len(parts) != 1 || string(parts[0]) != ":0\r\n" {
			r.hotFailed()
			hot.Close()
			hot = nil
		}
	}
	r.events.WithLabelValues("cold_request").Inc()
	cold, err := r.connect(ctx, false)
	if err != nil {
		return []byte("-TRYAGAIN cold backend unavailable; command not sent\r\n")
	}
	defer cold.Close()
	a := []string{"EVAL", cacheMultiColdScript, strconv.Itoa(len(keys) + 1), r.guard}
	for _, key := range keys {
		a = append(a, string(key))
	}
	a = append(a, token, name)
	if name == "DEL" || name == "UNLINK" {
		// Kvrocks 2.15 may count duplicate deleted keys more than once. Redis
		// counts actual removals, unlike EXISTS which counts each occurrence.
		unique := make(map[string]bool, len(keys))
		for _, key := range keys {
			if !unique[string(key)] {
				a = append(a, string(key))
				unique[string(key)] = true
			}
		}
	} else {
		for _, arg := range args[1:] {
			a = append(a, string(arg))
		}
	}
	b, err := cold.call(ctx, a...)
	write := name != "MGET" && name != "EXISTS"
	if err != nil {
		if write {
			r.uncertainEpoch(token)
			r.s.uncertain.Inc()
			return []byte("-ERR cold result unknown; command was not retried\r\n")
		}
		r.events.WithLabelValues("cold_read_error").Inc()
		return []byte("-ERR cold read failed\r\n")
	}
	if name == "MSET" {
		if v, ok := frameString(b); ok && v == "OK" {
			b = []byte("+OK\r\n")
		}
	}
	if len(b) == 0 || (b[0] != '-' && !cacheMultiReply(name, b, len(keys))) {
		if write {
			r.uncertainEpoch(token)
		}
		return []byte("-ERR invalid cold batch response; command was not retried\r\n")
	}
	if name == "MSETNX" && string(b) == ":0\r\n" {
		return b
	}
	if write {
		if !leased && len(cacheable) > 0 {
			r.hotFailed()
		}
		if hot == nil {
			return b
		}
		if b[0] != '-' && r.cfg.WritePolicy == "update_existing" && (name == "MSET" || name == "MSETNX") {
			// Duplicate MSET keys are legal; publish only each key's final value.
			last := make(map[string][]byte, len(keys))
			for i := 1; i < len(args); i += 2 {
				last[string(args[i])] = args[i+1]
			}
			for _, key := range cacheable {
				if !r.hotReady.Load() {
					break
				}
				r.updateHot(ctx, cold, hot, cacheCommand{name: "SET", write: true}, [][]byte{[]byte("SET"), []byte(key), last[key]}, []byte("+OK\r\n"))
			}
		} else {
			// Invalidations also cover server errors, which need not imply that
			// a storage engine made no changes. Never expose a partially cleaned batch.
			x, e := hot.call(ctx, append([]string{"DEL"}, cacheable...)...)
			if e != nil || len(x) == 0 || x[0] != ':' {
				r.hotFailed()
			} else {
				r.events.WithLabelValues("invalidated").Inc()
			}
		}
	} else if name == "MGET" && b[0] != '-' && hot != nil {
		parts, _ := frameParts(b)
		warmed := make(map[string]bool, len(keys))
		for i, key := range keys {
			if !r.hotReady.Load() {
				break
			}
			if string(parts[i]) == "$-1\r\n" || r.coldOnly(key) || warmed[string(key)] {
				continue
			}
			warmed[string(key)] = true
			r.fill(ctx, cold, hot, key, string(key))
		}
	}
	return b
}

func cacheMultiReply(name string, b []byte, n int) bool {
	if name == "MGET" {
		return cacheMGetReply(b, n)
	}
	if name == "MSET" {
		return string(b) == "+OK\r\n"
	}
	if len(b) == 0 || b[0] != ':' {
		return false
	}
	v, ok := frameString(b)
	count, err := strconv.Atoi(v)
	if name == "MSETNX" {
		n = 1
	}
	return ok && err == nil && count >= 0 && count <= n
}

func cacheMGetReply(b []byte, n int) bool {
	parts, err := frameParts(b)
	if err != nil || len(parts) != n {
		return false
	}
	for _, p := range parts {
		if len(p) == 0 || p[0] != '$' {
			return false
		}
	}
	return true
}

const cacheMultiColdScript = `
if redis.replicate_commands then redis.replicate_commands() end
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cache owner changed; command not executed') end
local a={ARGV[2]}
for i=3,#ARGV do a[#a+1]=ARGV[i] end
return redis.pcall(unpack(a))`

const cacheMGetHotScript = `
for i=1,#KEYS do
 if redis.call('EXISTS',KEYS[i])==0 then return {0} end
end
return {1,redis.call('MGET',unpack(KEYS))}`
