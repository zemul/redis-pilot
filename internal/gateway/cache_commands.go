package gateway

import "strings"

type cacheCommand struct {
	name                           string
	write, cacheRead, exactInteger bool
}

// Single-key classifier. Explicit batch commands are dispatched separately to
// executeMulti, and blocking queues to block. No arbitrary pass-through.
func classifyCacheCommand(args [][]byte) (cacheCommand, bool) {
	if len(args) < 2 || len(args[1]) > 4096 || strings.HasPrefix(string(args[1]), cachePrefix) {
		return cacheCommand{}, false
	}
	n := strings.ToUpper(string(args[0]))
	c := cacheCommand{name: n}
	l := len(args)
	switch n {
	case "GET", "STRLEN", "HGETALL", "HLEN", "HKEYS", "HVALS", "LLEN", "SMEMBERS", "SCARD", "ZCARD", "TYPE", "EXISTS":
		c.cacheRead = true
		return c, l == 2
	case "HGET", "HEXISTS", "HSTRLEN", "LINDEX", "SISMEMBER", "ZSCORE":
		c.cacheRead = true
		return c, l == 3
	case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT":
		c.write = true
		return c, l >= 3 && cacheFlags(args[3:], "NX", "XX", "GT", "LT")
	case "PERSIST":
		c.write = true
		return c, l == 2
	case "HMGET", "SMISMEMBER", "ZMSCORE":
		c.cacheRead = true
		return c, l >= 3
	case "LRANGE", "ZCOUNT", "ZLEXCOUNT", "GETRANGE", "SUBSTR":
		c.cacheRead = true
		return c, l == 4
	case "ZRANGE", "ZREVRANGE":
		c.cacheRead = true
		return c, l >= 4 && cacheRangeOptions(args[4:], n == "ZRANGE", true)
	case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZRANGEBYLEX", "ZREVRANGEBYLEX":
		c.cacheRead = true
		return c, l >= 4 && cacheRangeOptions(args[4:], false, !strings.HasSuffix(n, "LEX"))
	case "ZRANK", "ZREVRANK":
		c.cacheRead = true
		return c, l == 3 || (l == 4 && strings.EqualFold(string(args[3]), "WITHSCORE"))
	case "SRANDMEMBER", "HRANDFIELD", "ZRANDMEMBER":
		c.cacheRead = true
		flag := "WITHVALUES"
		if n == "ZRANDMEMBER" {
			flag = "WITHSCORES"
		}
		return c, l == 2 || l == 3 || (n != "SRANDMEMBER" && l == 4 && strings.EqualFold(string(args[3]), flag))
	case "LPOS":
		c.cacheRead = true
		return c, l >= 3 && cachePairs(args[3:], "RANK", "COUNT", "MAXLEN")
	case "HSCAN", "SSCAN", "ZSCAN":
		// Cursor encodings belong to one backend. Never route a subsequent page
		// to hot, even if another request warmed or evicted the object.
		return c, l >= 3 && cachePairs(args[3:], "MATCH", "COUNT")
	case "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME":
		return c, l == 2 // authoritative expiration only
	case "SET":
		c.write = true
		return c, validCacheSet(args)
	case "SETEX", "PSETEX", "SETRANGE", "HSETNX", "HINCRBYFLOAT":
		c.write = true
		return c, l == 4
	case "SETNX", "GETSET", "INCRBYFLOAT":
		c.write = true
		return c, l == 3
	case "GETDEL":
		c.write = true
		return c, l == 2
	case "GETEX":
		c.write = true
		return c, l == 2 || (l == 3 && strings.EqualFold(string(args[2]), "PERSIST")) || (l == 4 && cacheFlags(args[2:3], "EX", "PX", "EXAT", "PXAT"))
	case "DEL", "UNLINK":
		c.write = true
		return c, l == 2
	case "INCR", "DECR":
		c.write = true
		c.exactInteger = true
		return c, l == 2
	case "INCRBY", "DECRBY":
		c.write = true
		c.exactInteger = true
		return c, l == 3
	case "APPEND":
		c.write = true
		return c, l == 3
	case "HSET", "HMSET":
		c.write = true
		return c, l >= 4 && l%2 == 0
	case "HDEL", "SADD", "SREM", "ZREM":
		c.write = true
		return c, l >= 3
	case "HINCRBY":
		c.write = true
		c.exactInteger = true
		return c, l == 4
	case "LPUSH", "RPUSH", "LPUSHX", "RPUSHX":
		c.write = true
		return c, l >= 3
	case "LPOP", "RPOP", "SPOP", "ZPOPMIN", "ZPOPMAX":
		c.write = true
		return c, l == 2 || l == 3
	case "LINSERT":
		c.write = true
		return c, l == 5 && cacheFlags(args[2:3], "BEFORE", "AFTER")
	case "LSET", "LTRIM", "LREM", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX":
		c.write = true
		return c, l == 4
	case "ZADD":
		c.write = true
		i := cacheZAddStart(args)
		return c, i < l && l-i >= 2 && (l-i)%2 == 0
	case "ZINCRBY":
		c.write = true
		return c, l == 4
	}
	return c, false
}

const cachePrefix = "__pilot_cache_v1__:"

func validCacheSet(args [][]byte) bool {
	if len(args) < 3 {
		return false
	}
	condition, expiration, get := false, false, false
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX", "XX":
			if condition {
				return false
			}
			condition = true
		case "KEEPTTL":
			if expiration {
				return false
			}
			expiration = true
		case "EX", "PX", "EXAT", "PXAT":
			if expiration || i+1 >= len(args) {
				return false
			}
			expiration = true
			i++ // Numeric validation belongs to the cold engine.
		case "GET":
			if get {
				return false
			}
			get = true
		default:
			return false
		}
	}
	return true
}

const cacheClaimScript = `
local old = redis.call('GET', KEYS[1])
if old == ARGV[2] then return 1 end
if (old or '') ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2])
return 1`

const cacheColdScript = `
if redis.replicate_commands then redis.replicate_commands() end
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('TRYAGAIN cache owner changed; command not executed') end
local a={ARGV[2],KEYS[2]}
for i=4,#ARGV do a[#a+1]=ARGV[i] end
` + cacheConditionalExpireScript + `
local result=redis.pcall(unpack(a))
if type(result)=='table' and result.err then return result end
-- Kvrocks 2.15 persists float increments with a different formatting path
-- than its reply. Persist that authoritative reply atomically, so later reads
-- and incremental hot updates observe the same bytes. Do not calculate again.
if ARGV[2]=='INCRBYFLOAT' then
 redis.call('SET',KEYS[2],result,'KEEPTTL')
elseif ARGV[2]=='HINCRBYFLOAT' then
 redis.call('HSET',KEYS[2],ARGV[4],result)
end
if ARGV[2]=='SET' then
 local get,nx,xx=false,false,false
 local i=5
 while i<=#ARGV do
  local flag=string.upper(ARGV[i])
  if flag=='GET' then get=true elseif flag=='NX' then nx=true elseif flag=='XX' then xx=true end
  if flag=='EX' or flag=='PX' or flag=='EXAT' or flag=='PXAT' then i=i+1 end
  i=i+1
 end
 -- Interpret the actual SET result, not an EXISTS sampled before a key can
 -- expire. GET+NX nil means a successful creation; GET+XX nil is a no-op.
 local applied=result~=false
 if get then applied=(not nx and not xx) or (nx and result==false) or (xx and result~=false) end
 return {result,applied and 1 or 0}
end
if ARGV[3]=='integer' then
 if ARGV[2]=='HINCRBY' then return redis.call('HGET',KEYS[2],ARGV[4]) end
 return redis.call('GET',KEYS[2])
end
return result`

// The Gateway's same-key lock protects the original business key throughout
// loading and publication. Clean partial writes on error; transport uncertainty
// disables the entire hot layer until server-side fencing and flush succeed.
const cachePublishScript = `
redis.replicate_commands()
redis.call('DEL',KEYS[1])
` + cacheBuildBody

const cacheAppendScript = `
redis.replicate_commands()
if redis.call('EXISTS',KEYS[1])==0 then return 0 end
` + cacheBuildBody

const cacheBuildBody = `
local now=redis.call('TIME')
if tonumber(now[1])*1000+math.floor(tonumber(now[2])/1000)>=tonumber(ARGV[2]) then return 0 end
local cmd=ARGV[1]
local a={cmd,KEYS[1]}
for i=3,#ARGV do a[#a+1]=ARGV[i] end
local result=redis.pcall(unpack(a))
if type(result)=='table' and result.err then redis.call('DEL',KEYS[1]); return result end
local expiry=redis.pcall('PEXPIREAT',KEYS[1],ARGV[2])
if type(expiry)=='table' and expiry.err then redis.call('DEL',KEYS[1]); return expiry end
return 1`

const cacheReadScript = `
if redis.call('EXISTS',KEYS[1])==0 then return {0} end
local a={ARGV[1],KEYS[1]}
for i=2,#ARGV do a[#a+1]=ARGV[i] end
return {1,redis.pcall(unpack(a))}`
