package gateway

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"strings"
)

func (r *cacheRuntime) prepareScript(a [][]byte) (cachePrepared, bool) {
	p := cachePrepared{kind: "script"}
	if len(a) < 3 {
		return p, false
	}
	n, e := strconv.Atoi(string(a[2]))
	if e != nil || n < 0 || n > len(a)-3 {
		return p, false
	}
	p.keys = a[3 : 3+n]
	for _, k := range p.keys {
		if !cacheBusinessKey(k) {
			return p, false
		}
	}
	if !strings.HasSuffix(strings.ToUpper(string(a[0])), "_RO") {
		p.writes = p.keys
	}
	p.script = string(a[1])
	if strings.Contains(strings.ToUpper(string(a[0])), "SHA") {
		r.scriptsMu.Lock()
		var found bool
		p.script, found = r.scripts[strings.ToLower(string(a[1]))]
		r.scriptsMu.Unlock()
		if !found {
			return p, false
		}
	}
	// Lua binary chunks must not reach loadstring. The sandbox accepts source
	// only; loading bytecode bypasses the Lua source safety assumptions.
	if strings.HasPrefix(p.script, "\x1b") {
		return p, false
	}
	return p, true
}

func cacheScriptSHA(source string) string {
	sum := sha1.Sum([]byte(source))
	return hex.EncodeToString(sum[:])
}

func (r *cacheRuntime) rememberScript(source string) bool {
	r.scriptsMu.Lock()
	defer r.scriptsMu.Unlock()
	sha := cacheScriptSHA(source)
	if _, ok := r.scripts[sha]; ok {
		return true
	}
	if len(source) > r.s.cfg.MaxBatchBytes-r.scriptBytes {
		return false
	}
	if r.scripts == nil {
		r.scripts = make(map[string]string)
	}
	r.scripts[sha] = source
	r.scriptBytes += len(source)
	return true
}

func (r *cacheRuntime) scriptCommand(parent context.Context, a [][]byte) []byte {
	if len(a) < 2 {
		return []byte("-ERR SCRIPT requires a subcommand\r\n")
	}
	switch strings.ToUpper(string(a[1])) {
	case "EXISTS":
		if len(a) < 3 {
			return []byte("-ERR SCRIPT EXISTS requires digests\r\n")
		}
		r.scriptsMu.Lock()
		defer r.scriptsMu.Unlock()
		out := make([][]byte, 0, len(a)-2)
		for _, sha := range a[2:] {
			_, ok := r.scripts[strings.ToLower(string(sha))]
			if ok {
				out = append(out, []byte(":1\r\n"))
			} else {
				out = append(out, []byte(":0\r\n"))
			}
		}
		return cacheArray(out)
	case "FLUSH":
		if len(a) > 3 || (len(a) == 3 && !cacheFlags(a[2:], "SYNC", "ASYNC")) {
			return []byte("-ERR invalid SCRIPT FLUSH options\r\n")
		}
		r.scriptsMu.Lock()
		r.scripts = nil
		r.scriptBytes = 0
		r.scriptsMu.Unlock()
		return []byte("+OK\r\n")
	case "LOAD":
		if len(a) != 3 || strings.HasPrefix(string(a[2]), "\x1b") {
			return []byte("-ERR SCRIPT LOAD requires Lua source\r\n")
		}
		ctx, cancel := context.WithTimeout(parent, r.s.cfg.RequestTimeout)
		defer cancel()
		cold, e := r.connect(ctx, false)
		if e != nil {
			return []byte("-TRYAGAIN cold backend unavailable\r\n")
		}
		defer cold.Close()
		b, e := cold.call(ctx, "EVAL", `local f,e=loadstring(ARGV[1]); if not f then return redis.error_reply(e) end; return 1`, "0", string(a[2]))
		if e != nil {
			return []byte("-ERR script compilation unavailable\r\n")
		}
		if string(b) != ":1\r\n" {
			return b
		}
		if !r.rememberScript(string(a[2])) {
			return []byte("-ERR Gateway script cache budget exceeded; use SCRIPT FLUSH\r\n")
		}
		return cacheBulk(cacheScriptSHA(string(a[2])))
	default:
		return []byte("-ERR unsupported SCRIPT subcommand\r\n")
	}
}

func cacheBulk(s string) []byte { return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n") }

func (r *cacheRuntime) userScriptArgs(a [][]byte, p cachePrepared, token string) []string {
	script := cacheUserScript
	if r.shards != nil {
		script = cacheShardedUserScript
	}
	out := []string{"EVAL", script, strconv.Itoa(len(p.keys) + 1), r.guard}
	for _, k := range p.keys {
		out = append(out, string(k))
	}
	ro := "0"
	if strings.HasSuffix(strings.ToUpper(string(a[0])), "_RO") {
		ro = "1"
	}
	out = append(out, token, p.script, ro, strconv.Itoa(r.s.cfg.Limits.MaxElements))
	out = append(out, cacheStrings(a[3+len(p.keys):])...)
	return out
}

func (r *cacheRuntime) executeScript(parent context.Context, a [][]byte) []byte {
	p, ok := r.prepareScript(a)
	if !ok {
		if len(a) > 1 && strings.Contains(strings.ToUpper(string(a[0])), "SHA") {
			return []byte("-NOSCRIPT No matching script. Please use EVAL.\r\n")
		}
		return []byte("-ERR invalid script, declared keys or arguments\r\n")
	}
	ctx, cancel := context.WithTimeout(parent, r.s.cfg.RequestTimeout)
	defer cancel()
	release, e := r.acquireKeys(ctx, p.keys)
	if e != nil {
		return []byte("-TRYAGAIN script coordination timeout; not executed\r\n")
	}
	defer release()
	token, e := r.ensureEpoch(ctx)
	if e != nil {
		return []byte("-TRYAGAIN cold recovery unavailable; script not executed\r\n")
	}
	r.recoverHot(ctx)
	hot, done := r.leaseHot(ctx)
	defer done()
	cold, e := r.connect(ctx, false)
	if e != nil {
		return []byte("-TRYAGAIN cold unavailable; script not executed\r\n")
	}
	defer cold.Close()
	b, e := cold.call(ctx, r.userScriptArgs(a, p, token)...)
	if e != nil {
		r.uncertainEpoch(token)
		r.s.uncertain.Inc()
		return []byte("-ERR script result unknown; script was not retried\r\n")
	}
	return r.finishUserScript(ctx, cold, hot, a, p, b)
}

func (r *cacheRuntime) finishUserScript(ctx context.Context, cold, hot *cacheConn, a [][]byte, p cachePrepared, b []byte) []byte {
	// A native error can occur after writes (OOM, response budget, etc.).
	// Never assume absence of a transcript proves absence of mutations.
	fail := func() {
		keys := []string{}
		for _, k := range p.writes {
			if !r.coldOnly(k) {
				keys = append(keys, string(k))
			}
		}
		if len(keys) > 0 {
			if hot == nil {
				r.hotFailed()
			} else {
				r.invalidateKeys(ctx, hot, keys)
			}
		}
	}
	if len(b) == 0 || b[0] == '-' {
		fail()
		return b
	}
	parts, e := frameParts(b)
	if e != nil || len(parts) != 3 {
		fail()
		return []byte("-ERR invalid script confirmation; script was not retried\r\n")
	}
	// The wrapper only returns an envelope after successful compilation.
	r.rememberScript(p.script)
	effects, e := frameParts(parts[2])
	if e != nil {
		fail()
		return []byte("-ERR invalid script effects\r\n")
	}
	for _, effect := range effects {
		fields, e := frameParts(effect)
		if e != nil || len(fields) != 2 {
			fail()
			return []byte("-ERR invalid script effect\r\n")
		}
		op, e := frameParts(fields[0])
		if e != nil {
			fail()
			return []byte("-ERR invalid script command\r\n")
		}
		args := make([][]byte, len(op))
		for i, v := range op {
			x, ok := frameString(v)
			if !ok {
				fail()
				return []byte("-ERR invalid script operand\r\n")
			}
			args[i] = []byte(x)
		}
		plan, ok := r.prepareCache(args)
		if !ok || plan.kind == "script" {
			fail()
			return []byte("-ERR unsupported script effect\r\n")
		}
		result, applied := cachePreparedReply(args, plan, fields[1])
		if len(result) == 0 {
			fail()
			return []byte("-ERR invalid script mutation confirmation\r\n")
		}
		if applied {
			r.finishPrepared(ctx, cold, hot, args, plan, result)
		}
	}
	if string(parts[0]) == ":0\r\n" {
		message, ok := frameString(parts[1])
		if !ok {
			fail()
			return []byte("-ERR invalid script failure\r\n")
		}
		message = strings.NewReplacer("\r", " ", "\n", " ").Replace(message)
		return []byte("-ERR " + message + "\r\n")
	}
	if string(parts[0]) != ":1\r\n" {
		fail()
		return []byte("-ERR invalid script completion\r\n")
	}
	encoded, ok := frameString(parts[1])
	if !ok {
		fail()
		return []byte("-ERR invalid script reply encoding\r\n")
	}
	reader, _ := NewReader(strings.NewReader(encoded), r.s.cfg.Limits)
	reply, e := reader.ReadFrame()
	if e != nil || len(reply) != len(encoded) {
		fail()
		return []byte("-ERR script reply exceeds protocol limits\r\n")
	}
	return reply
}

// A separate chunk shadows EVERY engine global with a sandbox capability (or
// nil), including _G/getfenv/loadstring. No engine global or wrapper upvalue is
// passed to user code. Prefixing top-level bindings, rather than interpolating
// source into a function body, prevents a closing `end` escaping the sandbox.
// Native Kvrocks/Redis still runs the whole script atomically. The transcript
// records actual cold effects, never re-executes user code on hot Redis.
const cacheUserScript = "local shardCursorsBlocked=false\n" + cacheUserScriptBody
const cacheShardedUserScript = "local shardCursorsBlocked=true\n" + cacheUserScriptBody
const cacheUserScriptBody = `
if redis.replicate_commands then redis.replicate_commands() end
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cache owner changed; script not executed') end
local user,compile_error=loadstring(ARGV[2])
if not user then return redis.error_reply(compile_error) end
local raw=redis
local keys,argv,declared={},{},{}
for i=2,#KEYS do keys[#keys+1]=KEYS[i];declared[KEYS[i]]=true end
for i=5,#ARGV do argv[#argv+1]=ARGV[i] end
local effects={}
local reads,writes={},{}
for n in string.gmatch('GET GETRANGE SUBSTR STRLEN HGET HMGET HGETALL HLEN HKEYS HVALS HEXISTS HSTRLEN HRANDFIELD HSCAN LLEN LINDEX LRANGE LPOS SMEMBERS SCARD SISMEMBER SMISMEMBER SRANDMEMBER SSCAN ZCARD ZSCORE ZMSCORE ZRANK ZREVRANK ZCOUNT ZLEXCOUNT ZRANGE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZRANDMEMBER ZSCAN TTL PTTL TYPE EXPIRETIME PEXPIRETIME','%S+') do reads[n]=true end
for n in string.gmatch('SET SETNX SETEX PSETEX GETSET GETDEL GETEX INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND SETRANGE HSET HMSET HSETNX HDEL HINCRBY HINCRBYFLOAT LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LSET LTRIM LREM LINSERT SADD SREM SPOP ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST','%S+') do writes[n]=true end
local count=0
local function snapshot(v)
 if type(v)~='table' then return v end
 local out={};for k,x in pairs(v) do out[k]=snapshot(x) end;return out
end
local function native(a)
 if shardCursorsBlocked and (a[1]=='HSCAN' or a[1]=='SSCAN' or a[1]=='ZSCAN') then return raw.error_reply('ERR sharded object cursor is not enabled') end
 if (a[1]=='EXPIRE' or a[1]=='PEXPIRE' or a[1]=='EXPIREAT' or a[1]=='PEXPIREAT') and #a>3 then
  local ARGV={'',a[1],'normal',a[3]}
  for i=4,#a do ARGV[#ARGV+1]=a[i] end
  local KEYS={'',a[2]}
` + cacheConditionalExpireScript + `
 end
 if a[1]=='DEL' or a[1]=='UNLINK' then
  local seen,out={},{a[1]}
  for i=2,#a do if not seen[a[i]] then out[#out+1]=a[i];seen[a[i]]=true end end
  a=out
 end
 return raw.pcall(unpack(a))
end
local function invoke(...)
 local a={...}
 for i=1,#a do
  if type(a[i])~='string' and type(a[i])~='number' then return {err='ERR Lua redis arguments must be strings or integers'} end
  a[i]=tostring(a[i])
 end
 if #a==0 then return {err='ERR missing command'} end
 local n=string.upper(a[1]);a[1]=n
 local used={};local write=writes[n] or false
 if reads[n] or writes[n] then used={a[2]}
 elseif n=='MGET' or n=='EXISTS' or n=='DEL' or n=='UNLINK' or n=='SUNION' or n=='SINTER' or n=='SDIFF' or n=='SUNIONSTORE' or n=='SINTERSTORE' or n=='SDIFFSTORE' then
  for i=2,#a do used[#used+1]=a[i] end
  write=n=='DEL' or n=='UNLINK' or string.sub(n,-5)=='STORE'
 elseif n=='MSET' or n=='MSETNX' then
  for i=2,#a,2 do used[#used+1]=a[i] end;write=true
 elseif n=='RENAME' or n=='RENAMENX' or n=='LMOVE' or n=='RPOPLPUSH' or n=='SMOVE' or n=='ZRANGESTORE' then used={a[2],a[3]};write=true
 elseif n=='ZUNION' or n=='ZINTER' or n=='ZDIFF' or n=='ZUNIONSTORE' or n=='ZINTERSTORE' or n=='ZDIFFSTORE' or n=='SINTERCARD' or n=='ZINTERCARD' then
  local start=2;if string.sub(n,-5)=='STORE' then start=3;used[1]=a[2];write=true end
  local num=tonumber(a[start]);if not num or num<1 or num%1~=0 or num>#a-start then return {err='ERR invalid number of keys'} end
  for i=start+1,start+num do used[#used+1]=a[i] end
 elseif n~='PING' and n~='ECHO' and n~='TIME' then return {err='ERR command not permitted in cache Lua'} end
 for _,key in ipairs(used) do if not declared[key] then return {err='ERR script accessed a key not declared in KEYS'} end end
 if write and ARGV[3]=='1' then return {err='ERR Write commands are not allowed from read-only scripts.'} end
 count=count+1;if count>tonumber(ARGV[4]) then return {err='ERR script command budget exceeded'} end
 -- Reserve a conservative effect before the first possible mutation. If a
 -- later normalization/readback fails and user code catches the error, the
 -- Gateway must still invalidate this key rather than trust a missing entry.
 local effect_index
 if write then effect_index=#effects+1;effects[effect_index]={a,{err='ERR script mutation confirmation incomplete'}} end
 local result=native(a)
 local confirmation=result
 if write then
  if type(result)~='table' or not result.err then
   if n=='INCRBYFLOAT' then raw.call('SET',a[2],result,'KEEPTTL')
   elseif n=='HINCRBYFLOAT' then raw.call('HSET',a[2],a[3],result) end
   if n=='INCR' or n=='DECR' or n=='INCRBY' or n=='DECRBY' then confirmation=raw.call('GET',a[2])
   elseif n=='HINCRBY' then confirmation=raw.call('HGET',a[2],a[3])
   elseif n=='SET' then
    local nx,xx,get=false,false,false;local i=4
    while i<=#a do local opt=string.upper(a[i]);if opt=='NX' then nx=true elseif opt=='XX' then xx=true elseif opt=='GET' then get=true elseif opt=='EX' or opt=='PX' or opt=='EXAT' or opt=='PXAT' then i=i+1 end;i=i+1 end
    local applied=result~=false
    if get then applied=(not nx and not xx) or (nx and result==false) or (xx and result~=false) end
    confirmation={result,applied and 1 or 0}
   end
  end
  effects[effect_index]={a,snapshot(confirmation)}
 end
 -- Restore native Redis Lua status tables on Kvrocks (which returns some
 -- statuses as strings); SET GET is a bulk value even when that value is OK.
 local setget=false
 if n=='SET' then local i=4;while i<=#a do local o=string.upper(a[i]);if o=='GET' then setget=true elseif o=='EX' or o=='PX' or o=='EXAT' or o=='PXAT' then i=i+1 end;i=i+1 end end
 if type(result)=='string' and ((result=='OK' and ((n=='SET' and not setget) or n=='MSET' or n=='SETEX' or n=='PSETEX' or n=='HMSET' or n=='LSET' or n=='LTRIM' or n=='RENAME')) or n=='TYPE') then result={ok=result} end
 if (n=='SRANDMEMBER' or n=='HRANDFIELD' or n=='ZRANDMEMBER') and #a==2 and type(result)=='table' and not result.err then result=result[1] or false end
 if n=='LPOS' and type(result)=='table' and not result.err then for i=1,#result do result[i]=tonumber(result[i]) end end
 if (n=='ZRANK' or n=='ZREVRANK') and type(result)=='table' and result[1] then result[1]=tonumber(result[1]) end
 return result
end
local facade={}
facade.pcall=invoke
facade.call=function(...) local result=invoke(...);if type(result)=='table' and result.err then error(result.err,2) end;return result end
facade.status_reply=function(x)return {ok=x}end
facade.error_reply=function(x)return {err=x}end
facade.sha1hex=raw.sha1hex
facade.replicate_commands=function()return true end
local env={KEYS=keys,ARGV=argv,redis=facade,assert=assert,error=error,ipairs=ipairs,next=next,pairs=pairs,pcall=pcall,select=select,tonumber=tonumber,tostring=tostring,type=type,unpack=unpack,xpcall=xpcall,setmetatable=setmetatable,rawget=rawget,rawset=rawset,rawequal=rawequal}
-- The string metatable points to the process-global string library, not its
-- sandbox copy. Never expose it (or another engine-owned primitive metatable).
env.getmetatable=function(v)if type(v)=='table' then return getmetatable(v) end;return nil end
local function copy(t)local x={};for k,v in pairs(t) do x[k]=v end;return x end
env.string=copy(string);env.table=copy(table);env.math=copy(math);env.cjson=copy(cjson);env.cmsgpack=copy(cmsgpack);env.bit=copy(bit);env._G=env
-- Redis security releases remove setfenv/getfenv. Bind all global names as
-- locals in a separate chunk instead. Globals not in the capability map are
-- nil; _G itself is the private sandbox table. No raw values cross this boundary.
local names,seen={},{}
local function bindname(k)
 if type(k)=='string' and string.match(k,'^[%a_][%w_]*$') and not seen[k] then
  seen[k]=true;names[#names+1]=k
 end
end
for k in pairs(_G) do bindname(k) end
for k in pairs(env) do bindname(k) end
-- Explicitly shadow dangerous globals even when an engine hides them in _G.
for k in string.gmatch('getfenv setfenv loadstring load loadfile dofile require module package debug io os collectgarbage newproxy','%S+') do bindname(k) end
if #names>128 then return raw.error_reply('ERR unsupported Lua global environment') end
table.sort(names)
local values={};for i,k in ipairs(names) do values[i]=env[k] end
user,compile_error=loadstring('local '..table.concat(names,',')..'=...\n'..ARGV[2])
if not user then return raw.error_reply(compile_error) end
local ok,result=pcall(user,unpack(values,1,#names))
if result==nil then result=false end
if not ok then result=tostring(result) end
-- Kvrocks loses status-table tags when they are nested in a Lua reply. Encode
-- the public result explicitly; Go validates it against normal RESP limits.
local function encode(v,depth)
 if depth>32 then error('script reply nesting limit') end
 local t=type(v)
 if t=='nil' or v==false then return '$-1\r\n' end
 if v==true then return ':1\r\n' end
 if t=='string' then return '$'..#v..'\r\n'..v..'\r\n' end
 if t=='number' then
  if v~=v or v>=9223372036854775808 or v< -9223372036854775808 then error('Lua number exceeds RESP integer range') end
  local integer=math.floor(v);if v<0 then integer=math.ceil(v) end
  if integer==0 then integer=0 end
  return ':'..string.format('%.0f',integer)..'\r\n'
 end
 if t=='table' then
  if type(v.err)=='string' then return '-'..string.gsub(v.err,'[\r\n]',' ')..'\r\n' end
  if type(v.ok)=='string' then return '+'..string.gsub(v.ok,'[\r\n]',' ')..'\r\n' end
  local out={};local i=1
  while rawget(v,i)~=nil do
   if i>tonumber(ARGV[4]) then error('script reply element limit') end
   out[i]=encode(rawget(v,i),depth+1);i=i+1
  end
  return '*'..#out..'\r\n'..table.concat(out)
 end
 return '$-1\r\n'
end
if ok then
 local encoded;ok,encoded=pcall(encode,result,0)
 result=encoded
end
return {ok and 1 or 0,result,effects}`
