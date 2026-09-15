package gateway

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// A transaction belongs to a frontend connection, never to a pooled request.
// Mutations use native WATCH. Kvrocks 2.15 does not invalidate WATCH on natural
// expiry, so captured cold deadlines are additionally checked inside EXEC.
type cacheSession struct {
	r                    *cacheRuntime
	cold                 *cacheConn
	multi, dirty, broken bool
	queue                [][][]byte
	bytes                int
	watchExpiry          map[string]int64
	watchRevision        uint64
}

func (t *cacheSession) close() {
	if t.cold != nil {
		t.cold.Close()
	}
	t.cold = nil
	t.multi, t.dirty, t.broken = false, false, false
	t.queue = nil
	t.bytes = 0
	t.watchExpiry = nil
	t.watchRevision = 0
}

func (t *cacheSession) execute(parent context.Context, a [][]byte) []byte {
	n := strings.ToUpper(string(a[0]))
	if n == "MULTI" {
		if len(a) != 1 {
			t.dirty = t.multi
			return []byte("-ERR wrong number of arguments for MULTI\r\n")
		}
		if t.multi {
			return []byte("-ERR MULTI calls can not be nested\r\n")
		}
		t.multi = true
		return []byte("+OK\r\n")
	}
	if n == "DISCARD" || n == "EXEC" {
		if len(a) != 1 {
			if t.multi {
				t.dirty = true
			}
			return []byte("-ERR wrong number of arguments\r\n")
		}
		if !t.multi {
			return []byte("-ERR " + n + " without MULTI\r\n")
		}
		defer t.close()
		if n == "DISCARD" {
			return []byte("+OK\r\n")
		}
		if t.dirty {
			return []byte("-EXECABORT Transaction discarded because of previous errors.\r\n")
		}
		if t.broken {
			return []byte("-TRYAGAIN WATCH connection lost; transaction not executed\r\n")
		}
		return t.exec(parent)
	}
	if n == "WATCH" {
		if t.multi {
			return []byte("-ERR WATCH inside MULTI is not allowed\r\n")
		}
		if len(a) < 2 {
			return []byte("-ERR WATCH requires keys\r\n")
		}
		for _, k := range a[1:] {
			if !cacheBusinessKey(k) {
				return []byte("-ERR unsupported WATCH key\r\n")
			}
		}
		ctx, cancel := context.WithTimeout(parent, t.r.s.cfg.RequestTimeout)
		defer cancel()
		if t.broken {
			return []byte("-TRYAGAIN WATCH connection lost; use UNWATCH before retrying\r\n")
		}
		var release func()
		var e error
		if t.r.shards != nil {
			keys := append([][]byte(nil), a[1:]...)
			for k := range t.watchExpiry {
				keys = append(keys, []byte(k))
			}
			ctx, release, e = t.r.shards.admit(ctx, keys, t.watchRevision)
		} else {
			release, e = t.r.acquireKeys(ctx, a[1:])
		}
		if e != nil {
			if t.r.shards != nil {
				return shardRuntimeError(e)
			}
			return []byte("-TRYAGAIN WATCH coordination timeout\r\n")
		}
		defer release()
		if len(t.watchExpiry)+len(a)-1 > t.r.s.cfg.Limits.MaxElements {
			return []byte("-ERR WATCH key budget exceeded\r\n")
		}
		if t.cold == nil {
			var e error
			t.cold, e = t.r.connect(ctx, false)
			if e != nil {
				t.broken = true
				return []byte("-TRYAGAIN cold backend unavailable\r\n")
			}
		}
		if t.watchExpiry == nil {
			t.watchExpiry = make(map[string]int64)
		}
		// Hold key locks across deadline capture and WATCH, preventing a
		// concurrent Gateway writer from changing the object between them.
		for _, key := range a[1:] {
			if _, exists := t.watchExpiry[string(key)]; exists {
				continue
			}
			b, e := t.cold.call(ctx, "PEXPIRETIME", string(key))
			v, ok := frameString(b)
			deadline, parse := strconv.ParseInt(v, 10, 64)
			if e != nil || !ok || parse != nil || deadline > 9007199254740991 {
				t.broken = true
				return []byte("-TRYAGAIN WATCH expiry unavailable\r\n")
			}
			t.watchExpiry[string(key)] = deadline
		}
		b, e := t.cold.call(ctx, cacheStrings(a)...)
		if e != nil {
			t.broken = true
			return []byte("-TRYAGAIN WATCH connection lost\r\n")
		}
		if t.r.shards != nil && string(b) == "+OK\r\n" {
			t.watchRevision = shardAdmissionFrom(ctx).revision
		}
		return b
	}
	if n == "UNWATCH" && !t.multi {
		if len(a) != 1 {
			return []byte("-ERR wrong number of arguments for UNWATCH\r\n")
		}
		t.close()
		return []byte("+OK\r\n")
	}
	if t.multi {
		_, ok := t.r.prepareCache(a)
		if n == "UNWATCH" && len(a) == 1 {
			ok = true
		}
		if !ok {
			t.dirty = true
			return []byte("-ERR unsupported transaction command, options or key\r\n")
		}
		size := len(encodeCommand(cacheStrings(a)...))
		if len(t.queue) >= t.r.s.cfg.Limits.MaxElements || size > t.r.s.cfg.MaxBatchBytes-t.bytes {
			t.dirty = true
			return []byte("-ERR transaction buffer limit exceeded\r\n")
		}
		t.bytes += size
		t.queue = append(t.queue, a)
		return []byte("+QUEUED\r\n")
	}
	return t.r.execute(parent, a)
}

type cachePrepared struct {
	kind         string
	one          cacheCommand
	cross        cacheCross
	keys, writes [][]byte
	script       string
}

func cacheStrings(a [][]byte) []string {
	out := make([]string, len(a))
	for i, v := range a {
		out[i] = string(v)
	}
	return out
}

func (r *cacheRuntime) prepareCache(a [][]byte) (cachePrepared, bool) {
	var p cachePrepared
	if len(a) == 0 {
		return p, false
	}
	n := strings.ToUpper(string(a[0]))
	if (n == "PING" && (len(a) == 1 || len(a) == 2)) || (n == "ECHO" && len(a) == 2) {
		p.kind = "local"
		return p, true
	}
	if c, ok := classifyCacheCommand(a); ok {
		p.kind = "single"
		p.one = c
		p.keys = a[1:2]
		if c.write {
			p.writes = p.keys
		}
		return p, true
	}
	if keys, ok := cacheMultiKeys(n, a); ok {
		p.kind = "multi"
		p.keys = keys
		if n != "MGET" && n != "EXISTS" {
			p.writes = keys
		}
		return p, true
	}
	if c, ok := classifyCacheCross(a); ok {
		p.kind = "cross"
		p.cross = c
		p.keys = c.keys
		p.writes = c.writes
		return p, true
	}
	if n == "EVAL" || n == "EVALSHA" || n == "EVAL_RO" || n == "EVALSHA_RO" {
		p, ok := r.prepareScript(a)
		if !ok && (n == "EVALSHA" || n == "EVALSHA_RO") && len(a) >= 3 {
			// NOSCRIPT is an execution-time error, not a queue-time error.
			count, e := strconv.Atoi(string(a[2]))
			if e == nil && count >= 0 && count <= len(a)-3 {
				for _, key := range a[3 : 3+count] {
					if !cacheBusinessKey(key) {
						return cachePrepared{}, false
					}
				}
				return cachePrepared{kind: "noscript", keys: a[3 : 3+count]}, true
			}
		}
		return p, ok
	}
	return p, false
}

func (r *cacheRuntime) preparedCold(a [][]byte, p cachePrepared, token string) []string {
	if p.kind == "noscript" {
		return []string{"EVAL", `return redis.error_reply('NOSCRIPT No matching script. Please use EVAL.')`, "0"}
	}
	if p.kind == "single" {
		out := []string{"EVAL", cacheColdScript, "2", r.guard, string(a[1]), token, p.one.name, "normal"}
		if p.one.exactInteger {
			out[7] = "integer"
		}
		return append(out, cacheStrings(a[2:])...)
	}
	if p.kind == "script" {
		return r.userScriptArgs(a, p, token)
	}
	if p.kind == "local" {
		return cacheStrings(a)
	}
	if p.kind == "multi" && (strings.EqualFold(string(a[0]), "DEL") || strings.EqualFold(string(a[0]), "UNLINK")) {
		unique := [][]byte{a[0]}
		seen := map[string]bool{}
		for _, k := range p.keys {
			if !seen[string(k)] {
				unique = append(unique, k)
				seen[string(k)] = true
			}
		}
		a = unique
	}
	return r.crossColdArgs(a, cacheCross{keys: p.keys}, token)
}

// Normalize per-command results inside EXEC exactly as the non-transaction
// path does; in particular SET's internal applied envelope is not public.
func cachePreparedReply(a [][]byte, p cachePrepared, b []byte) ([]byte, bool) {
	if len(b) == 0 {
		return b, false
	}
	n := strings.ToUpper(string(a[0]))
	if p.kind == "cross" {
		return cacheCrossReply(a, b), true
	}
	if p.kind != "single" {
		if p.kind == "local" && n == "PING" && len(a) == 1 {
			if v, ok := frameString(b); ok && v == "PONG" {
				b = []byte("+PONG\r\n")
			}
		}
		if n == "MSET" {
			if v, ok := frameString(b); ok && v == "OK" {
				b = []byte("+OK\r\n")
			}
		}
		return b, !(n == "MSETNX" && string(b) == ":0\r\n")
	}
	if n == "SET" && b[0] != '-' {
		parts, e := frameParts(b)
		if e != nil || len(parts) != 2 || (string(parts[1]) != ":0\r\n" && string(parts[1]) != ":1\r\n") {
			return nil, false
		}
		b = parts[0]
		if string(parts[1]) == ":0\r\n" {
			return b, false
		}
	}
	if p.one.exactInteger && b[0] == '$' {
		v, ok := frameString(b)
		if !ok {
			return nil, false
		}
		b = []byte(":" + v + "\r\n")
	}
	if (n == "SET" && !cacheSetGet(a)) || n == "SETEX" || n == "PSETEX" || n == "HMSET" || n == "LSET" || n == "LTRIM" {
		if v, ok := frameString(b); ok && v == "OK" {
			b = []byte("+OK\r\n")
		}
	}
	if n == "TYPE" {
		if v, ok := frameString(b); ok {
			b = []byte("+" + v + "\r\n")
		}
	}
	b = cacheReply(p.one, a, b)
	if (n == "SETNX" || n == "HSETNX" || n == "EXPIRE" || n == "PEXPIRE" || n == "EXPIREAT" || n == "PEXPIREAT" || n == "PERSIST") && string(b) == ":0\r\n" {
		return b, false
	}
	return b, true
}

func (r *cacheRuntime) finishPrepared(ctx context.Context, cold, hot *cacheConn, a [][]byte, p cachePrepared, b []byte) {
	if len(p.writes) == 0 {
		return
	}
	keys := []string{}
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
	if p.kind == "single" {
		if p.one.name == "DEL" || p.one.name == "UNLINK" || p.one.name == "GETDEL" {
			r.invalidateKeys(ctx, hot, keys)
		} else {
			r.updateHot(ctx, cold, hot, p.one, a, b)
		}
		return
	}
	if p.kind == "cross" {
		// A STORE followed by another operation in the same transaction must
		// not refill final cold state and then apply the later mutation twice.
		n := strings.ToUpper(string(a[0]))
		if strings.HasSuffix(n, "STORE") {
			r.invalidateKeys(ctx, hot, keys)
			return
		}
		r.finishCross(ctx, cold, hot, a, p.cross, b)
		return
	}
	n := strings.ToUpper(string(a[0]))
	if p.kind == "multi" && (n == "MSET" || n == "MSETNX") {
		for i := 1; i < len(a); i += 2 {
			if r.coldOnly(a[i]) {
				continue
			}
			r.updateHot(ctx, cold, hot, cacheCommand{name: "SET", write: true}, [][]byte{[]byte("SET"), a[i], a[i+1]}, []byte("+OK\r\n"))
		}
		return
	}
	r.invalidateKeys(ctx, hot, keys)
}

func (t *cacheSession) exec(parent context.Context) []byte {
	r := t.r
	ctx, cancel := context.WithTimeout(parent, r.s.cfg.RequestTimeout)
	defer cancel()
	plans := make([]cachePrepared, len(t.queue))
	var keys [][]byte
	for key := range t.watchExpiry {
		keys = append(keys, []byte(key))
	}
	for i, a := range t.queue {
		if r.shards != nil && shardCursorOrBlocking(a) {
			return []byte("-EXECABORT sharded cursor/blocking execution is not enabled\r\n")
		}
		if strings.EqualFold(string(a[0]), "UNWATCH") {
			plans[i].kind = "local"
			continue
		}
		p, ok := r.prepareCache(a)
		if !ok {
			return []byte("-EXECABORT queued command no longer available\r\n")
		}
		plans[i] = p
		keys = append(keys, p.keys...)
	}
	var release func()
	var e error
	if r.shards != nil {
		ctx, release, e = r.shards.admit(ctx, keys, t.watchRevision)
	} else {
		release, e = r.acquireKeys(ctx, keys)
	}
	if e != nil {
		if errors.Is(e, errShardWatchChanged) {
			return []byte("*-1\r\n")
		}
		if r.shards != nil {
			return shardRuntimeError(e)
		}
		return []byte("-TRYAGAIN transaction coordination timeout; not executed\r\n")
	}
	defer release()
	if r.shards != nil && t.cold != nil && t.cold.shard != shardAdmissionFrom(ctx).node {
		return []byte("*-1\r\n")
	}
	token, e := r.ensureEpoch(ctx)
	if e != nil {
		return []byte("-TRYAGAIN cold recovery unavailable; transaction not executed\r\n")
	}
	r.recoverHot(ctx)
	hot, done := r.leaseHot(ctx)
	defer done()
	if t.cold == nil {
		t.cold, e = r.connect(ctx, false)
		if e != nil {
			return []byte("-TRYAGAIN cold unavailable; transaction not executed\r\n")
		}
	}
	// Owner WATCH and verification close the gap between checking the epoch
	// and native EXEC. A delayed EXEC after recovery CAS is aborted by cold.
	b, e := t.cold.call(ctx, "WATCH", r.guard)
	if e != nil || string(b) != "+OK\r\n" {
		return []byte("-TRYAGAIN WATCH unavailable; transaction not executed\r\n")
	}
	b, e = t.cold.call(ctx, "GET", r.guard)
	v, ok := frameString(b)
	if e != nil || !ok || v != token {
		return []byte("-TRYAGAIN owner changed; transaction not executed\r\n")
	}
	b, e = t.cold.call(ctx, "MULTI")
	if e != nil || string(b) != "+OK\r\n" {
		return []byte("-TRYAGAIN MULTI unavailable; transaction not executed\r\n")
	}
	// One guarded body checks all watched expiry deadlines before any command
	// runs. Per-command deadline checks would allow a partially committed EXEC.
	op := []string{"EVAL", cacheTransactionScript, "1", r.guard, token, strconv.Itoa(len(t.watchExpiry))}
	for _, expiry := range t.watchExpiry {
		op = append(op, strconv.FormatInt(expiry, 10))
	}
	op = append(op, strconv.Itoa(len(plans)))
	for i, a := range t.queue {
		command := r.preparedCold(a, plans[i], token)
		if strings.EqualFold(string(a[0]), "UNWATCH") {
			command = []string{"ECHO", "OK"}
		}
		op = append(op, strconv.Itoa(len(command)))
		op = append(op, command...)
	}
	if len(encodeCommand(op...)) > r.s.cfg.Limits.MaxBytes {
		return []byte("-EXECABORT expanded transaction exceeds protocol budget; not executed\r\n")
	}
	b, e = t.cold.call(ctx, op...)
	if e != nil || string(b) != "+QUEUED\r\n" {
		return []byte("-EXECABORT cold rejected transaction; not executed\r\n")
	}
	b, e = t.cold.call(ctx, "EXEC")
	if e != nil {
		r.uncertainEpoch(token)
		r.s.uncertain.Inc()
		return []byte("-ERR transaction result unknown; EXEC was not retried\r\n")
	}
	if string(b) == "*-1\r\n" || string(b) == "$-1\r\n" {
		return []byte("*-1\r\n")
	}
	parts, e := frameParts(b)
	if e != nil || len(parts) != 1 {
		r.uncertainEpoch(token)
		return []byte("-ERR invalid transaction confirmation; EXEC was not retried\r\n")
	}
	envelope, e := frameParts(parts[0])
	if e != nil || len(envelope) < 1 {
		r.uncertainEpoch(token)
		return []byte("-ERR invalid transaction envelope\r\n")
	}
	if len(envelope) == 1 && string(envelope[0]) == ":0\r\n" {
		return []byte("*-1\r\n")
	}
	if len(envelope) != 2 || string(envelope[0]) != ":1\r\n" {
		r.uncertainEpoch(token)
		return []byte("-ERR invalid transaction gate\r\n")
	}
	parts, e = frameParts(envelope[1])
	if e != nil || len(parts) != len(plans) {
		r.uncertainEpoch(token)
		return []byte("-ERR invalid transaction results\r\n")
	}
	for i, a := range t.queue {
		if plans[i].kind == "script" {
			parts[i] = r.finishUserScript(ctx, t.cold, hot, a, plans[i], parts[i])
			continue
		}
		var applied bool
		parts[i], applied = cachePreparedReply(a, plans[i], parts[i])
		if len(parts[i]) == 0 {
			r.uncertainEpoch(token)
			return []byte("-ERR invalid transaction confirmation; EXEC was not retried\r\n")
		}
		if strings.EqualFold(string(a[0]), "UNWATCH") {
			parts[i] = []byte("+OK\r\n")
		}
		if applied {
			r.finishPrepared(ctx, t.cold, hot, a, plans[i], parts[i])
		}
	}
	return cacheArray(parts)
}

const cacheTransactionScript = `
if redis.replicate_commands then redis.replicate_commands() end
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cache owner changed; transaction not executed') end
local now=redis.call('TIME');now=tonumber(now[1])*1000+math.floor(tonumber(now[2])/1000)
local watches=tonumber(ARGV[2]);local pos=3
for i=1,watches do
 local expiry=tonumber(ARGV[pos]);pos=pos+1
 if expiry>=0 and now>=expiry then return {0} end
end
local count=tonumber(ARGV[pos]);pos=pos+1
local results={}
for i=1,count do
 local size=tonumber(ARGV[pos]);pos=pos+1
 local a={};for j=1,size do a[j]=ARGV[pos];pos=pos+1 end
 local result
 if a[1]=='EVAL' then
  local f,e=loadstring(a[2]);if not f then result=redis.error_reply(e)
  else
   local keys,args={},{};local n=tonumber(a[3])
   for j=1,n do keys[j]=a[3+j] end
   for j=4+n,#a do args[#args+1]=a[j] end
   -- Only trusted, generated wrappers use this environment. User source is
   -- separately compiled into the restricted environment in cacheUserScript.
   local env={KEYS=keys,ARGV=args};setmetatable(env,{__index=_G});setfenv(f,env)
   local ok;ok,result=pcall(f)
   if not ok then result=redis.error_reply(tostring(result)) end
  end
 else result=redis.pcall(unpack(a)) end
 if result==nil then result=false end
 results[i]=result
end
return {1,results}`
