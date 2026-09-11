package gateway

import (
	"context"
	"strconv"
	"strings"
)

// Key extraction is also used before queuing transactions. Validate the entire
// command before taking locks or sending any mutation to either backend.
type cacheCross struct {
	keys, writes [][]byte
}

func classifyCacheCross(a [][]byte) (cacheCross, bool) {
	var p cacheCross
	if len(a) == 0 {
		return p, false
	}
	n := strings.ToUpper(string(a[0]))
	switch n {
	case "RENAME", "RENAMENX", "RPOPLPUSH":
		if len(a) != 3 {
			return p, false
		}
		p.keys = a[1:3]
		p.writes = p.keys
	case "LMOVE":
		if len(a) != 5 || !cacheDirection(a[3]) || !cacheDirection(a[4]) {
			return p, false
		}
		p.keys = a[1:3]
		p.writes = p.keys
	case "SMOVE":
		if len(a) != 4 {
			return p, false
		}
		p.keys = a[1:3]
		p.writes = p.keys
	case "SUNION", "SINTER", "SDIFF":
		if len(a) < 2 {
			return p, false
		}
		p.keys = a[1:]
	case "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE":
		if len(a) < 3 {
			return p, false
		}
		p.keys = a[1:]
		p.writes = a[1:2]
	case "ZUNION", "ZINTER", "ZDIFF", "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE", "SINTERCARD", "ZINTERCARD":
		start := 1
		if strings.HasSuffix(n, "STORE") {
			start = 2
		}
		if len(a) <= start {
			return p, false
		}
		count, e := strconv.Atoi(string(a[start]))
		if e != nil || count < 1 || count > len(a)-start-1 {
			return p, false
		}
		p.keys = a[start+1 : start+1+count]
		if start == 2 {
			p.writes = a[1:2]
			p.keys = append(append([][]byte{}, p.writes...), p.keys...)
		}
		// Remaining operands contain no keys; the engine validates weights,
		// aggregation, WITHSCORES and LIMIT without changing key positions.
	case "ZRANGESTORE":
		if len(a) < 5 {
			return p, false
		}
		p.keys = a[1:3]
		p.writes = a[1:2]
	case "SCAN":
		if len(a) < 2 || !cachePairs(a[2:], "MATCH", "COUNT", "TYPE") {
			return p, false
		}
	case "KEYS":
		if len(a) != 2 {
			return p, false
		}
	default:
		return p, false
	}
	for _, k := range p.keys {
		if !cacheBusinessKey(k) {
			return cacheCross{}, false
		}
	}
	return p, true
}

func cacheBusinessKey(k []byte) bool {
	return len(k) <= 4096 && !strings.HasPrefix(string(k), cachePrefix)
}
func cacheDirection(b []byte) bool {
	n := strings.ToUpper(string(b))
	return n == "LEFT" || n == "RIGHT"
}

func (r *cacheRuntime) executeCross(parent context.Context, a [][]byte, p cacheCross) []byte {
	ctx, cancel := context.WithTimeout(parent, r.s.cfg.RequestTimeout)
	defer cancel()
	release, e := r.acquireKeys(ctx, p.keys)
	if e != nil {
		return []byte("-TRYAGAIN key coordination timeout; command not sent\r\n")
	}
	defer release()
	token, e := r.ensureEpoch(ctx)
	if e != nil {
		return []byte("-TRYAGAIN cold recovery unavailable; command not sent\r\n")
	}
	r.recoverHot(ctx)
	hot, done := r.leaseHot(ctx)
	defer done()
	cold, e := r.connect(ctx, false)
	if e != nil {
		return []byte("-TRYAGAIN cold backend unavailable; command not sent\r\n")
	}
	defer cold.Close()
	b, e := cold.call(ctx, r.crossColdArgs(a, p, token)...)
	if e != nil {
		if len(p.writes) != 0 {
			r.uncertainEpoch(token)
			r.s.uncertain.Inc()
		}
		return []byte("-ERR cold result unknown; command was not retried\r\n")
	}
	b = cacheCrossReply(a, b)
	r.finishCross(ctx, cold, hot, a, p, b)
	return b
}

// Keep the lease even when connect fails: recovery must not overtake this
// operation and publish a clean hot layer before its cold write has finished.
func (r *cacheRuntime) leaseHot(ctx context.Context) (*cacheConn, func()) {
	if !r.hotMu.TryRLock() {
		return nil, func() {}
	}
	if !r.hotReady.Load() {
		r.hotMu.RUnlock()
		return nil, func() {}
	}
	hot, e := r.connect(ctx, true)
	if e != nil {
		r.hotFailed()
		return nil, r.hotMu.RUnlock
	}
	return hot, func() { hot.Close(); r.hotMu.RUnlock() }
}

func (r *cacheRuntime) crossColdArgs(a [][]byte, p cacheCross, token string) []string {
	out := []string{"EVAL", cacheMultiColdScript, strconv.Itoa(len(p.keys) + 1), r.guard}
	for _, k := range p.keys {
		out = append(out, string(k))
	}
	out = append(out, token, strings.ToUpper(string(a[0])))
	for _, v := range a[1:] {
		out = append(out, string(v))
	}
	return out
}

func cacheCrossReply(a [][]byte, b []byte) []byte {
	n := strings.ToUpper(string(a[0]))
	if n == "RENAME" {
		if v, ok := frameString(b); ok && v == "OK" {
			return []byte("+OK\r\n")
		}
	}
	if n != "SCAN" && n != "KEYS" {
		return b
	}
	if len(b) == 0 || b[0] == '-' {
		return b
	}
	parts, e := frameParts(b)
	if e != nil {
		return []byte("-ERR invalid cold scan response\r\n")
	}
	var cursor []byte
	if n == "SCAN" {
		if len(parts) != 2 {
			return []byte("-ERR invalid cold scan response\r\n")
		}
		cursor = parts[0]
		parts, e = frameParts(parts[1])
		if e != nil {
			return []byte("-ERR invalid cold scan response\r\n")
		}
	}
	filtered := make([][]byte, 0, len(parts))
	for _, part := range parts {
		key, ok := frameString(part)
		if !ok {
			return []byte("-ERR invalid cold scan key\r\n")
		}
		if !strings.HasPrefix(key, cachePrefix) {
			filtered = append(filtered, part)
		}
	}
	b = cacheArray(filtered)
	if cursor != nil {
		return cacheArray([][]byte{cursor, b})
	}
	return b
}

func cacheArray(parts [][]byte) []byte {
	b := []byte("*" + strconv.Itoa(len(parts)) + "\r\n")
	for _, p := range parts {
		b = append(b, p...)
	}
	return b
}

func (r *cacheRuntime) finishCross(ctx context.Context, cold, hot *cacheConn, a [][]byte, p cacheCross, b []byte) {
	if len(p.writes) == 0 {
		return
	}
	n := strings.ToUpper(string(a[0]))
	if (n == "RENAMENX" || n == "SMOVE") && string(b) == ":0\r\n" {
		return
	}
	if (n == "LMOVE" || n == "RPOPLPUSH") && string(b) == "$-1\r\n" {
		return
	}
	keys := make([]string, 0, len(p.writes))
	for _, k := range p.writes {
		if !r.coldOnly(k) {
			keys = append(keys, string(k))
		}
	}
	if len(keys) == 0 {
		return
	}
	if hot == nil {
		r.hotFailed()
		return
	}
	if len(b) == 0 || b[0] == '-' || r.cfg.WritePolicy == "invalidate" {
		r.invalidateKeys(ctx, hot, keys)
		return
	}
	if n == "RENAME" || n == "RENAMENX" || n == "LMOVE" || n == "RPOPLPUSH" || n == "SMOVE" {
		// Never materialize a destination collection from only a moved element.
		// RENAME can move a complete cached source in O(1), including a huge Hash.
		if r.coldOnly(a[1]) || r.coldOnly(a[2]) {
			r.invalidateKeys(ctx, hot, keys)
			return
		}
		left, right := "RIGHT", "LEFT"
		if n == "LMOVE" {
			left, right = strings.ToUpper(string(a[3])), strings.ToUpper(string(a[4]))
		}
		value, _ := frameString(b)
		if n == "SMOVE" {
			value = string(a[3])
		}
		x, e := hot.call(ctx, "EVAL", cacheMoveHotScript, "2", string(a[1]), string(a[2]), n, left, right, value)
		if e != nil || string(x) != ":1\r\n" {
			r.hotFailed()
			return
		}
		for _, key := range keys {
			expiry, e := r.cacheExpiry(ctx, cold, hot, key)
			if e != nil || expiry == 0 {
				r.invalidateHot(ctx, hot, key)
				continue
			}
			x, e = hot.call(ctx, "PEXPIREAT", key, strconv.FormatInt(expiry, 10))
			if e != nil || len(x) == 0 || x[0] != ':' {
				r.hotFailed()
				return
			}
		}
		return
	}
	// STORE commands replace the entire destination by definition. Refill an
	// already resident destination, but do not warm a previously absent object.
	for _, key := range keys {
		x, e := hot.call(ctx, "EXISTS", key)
		if e != nil || len(x) == 0 || x[0] != ':' {
			r.hotFailed()
			return
		}
		r.invalidateHot(ctx, hot, key)
		if string(x) == ":1\r\n" && r.hotReady.Load() {
			r.fill(ctx, cold, hot, []byte(key), key)
		}
	}
}

func (r *cacheRuntime) invalidateKeys(ctx context.Context, hot *cacheConn, keys []string) {
	if len(keys) == 0 {
		return
	}
	b, e := hot.call(ctx, append([]string{"DEL"}, keys...)...)
	if e != nil || len(b) == 0 || b[0] != ':' {
		r.hotFailed()
	}
}

const cacheMoveHotScript = `
local s,d=KEYS[1],KEYS[2]
local n=ARGV[1]
if n=='RENAME' or n=='RENAMENX' then
 if redis.call('EXISTS',s)==1 then redis.call('RENAME',s,d) else redis.call('DEL',d) end
elseif n=='SMOVE' then
 if s~=d then
  if redis.call('EXISTS',s)==1 then redis.call('SREM',s,ARGV[4]) end
  if redis.call('EXISTS',d)==1 then redis.call('SADD',d,ARGV[4]) end
 end
elseif s==d then
 if redis.call('EXISTS',s)==1 and redis.call('LMOVE',s,d,ARGV[2],ARGV[3])~=ARGV[4] then
  redis.call('DEL',s); return 0
 end
else
 if redis.call('EXISTS',s)==1 then
  local pop='LPOP'; if ARGV[2]=='RIGHT' then pop='RPOP' end
  if redis.call(pop,s)~=ARGV[4] then redis.call('DEL',s,d); return 0 end
 end
 if redis.call('EXISTS',d)==1 then
  local push='LPUSH'; if ARGV[3]=='RIGHT' then push='RPUSH' end
  redis.call(push,d,ARGV[4])
 end
end
return 1`
