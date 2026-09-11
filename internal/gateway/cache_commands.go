package gateway

import (
	"strconv"
	"strings"
)

type cacheCommand struct {
	name                           string
	write, cacheRead, exactInteger bool
}

// No arbitrary pass-through: every accepted business command has one key.
// Unlisted SET/ZADD options, multi-key, scripting and blocking commands are
// deliberately rejected until their entire consistency path is implemented.
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
	case "HGET", "HEXISTS", "LINDEX", "SISMEMBER", "ZSCORE", "ZRANK", "ZREVRANK":
		c.cacheRead = true
		return c, l == 3
	case "EXPIRE", "PEXPIRE":
		c.write = true
		return c, l == 3
	case "PERSIST":
		c.write = true
		return c, l == 2
	case "HMGET":
		c.cacheRead = true
		return c, l >= 3
	case "LRANGE", "ZCOUNT":
		c.cacheRead = true
		return c, l == 4
	case "ZRANGE", "ZREVRANGE":
		c.cacheRead = true
		return c, l == 4 || (l == 5 && strings.EqualFold(string(args[4]), "WITHSCORES"))
	case "TTL", "PTTL":
		return c, l == 2 // authoritative expiration only
	case "SET":
		c.write = true
		return c, validCacheSet(args)
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
	case "HSET":
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
	case "LPOP", "RPOP", "SPOP":
		c.write = true
		return c, l == 2 // count variants deferred
	case "LSET", "LTRIM", "LREM":
		c.write = true
		return c, l == 4
	case "ZADD":
		c.write = true
		for i := 2; i < l; i += 2 {
			if _, err := strconv.ParseFloat(string(args[i]), 64); err != nil {
				return c, false
			}
		}
		return c, l >= 4 && l%2 == 0 // numeric score validation remains in Kvrocks
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
	condition, expiration := false, false
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
		case "EX", "PX":
			if expiration || i+1 >= len(args) {
				return false
			}
			expiration = true
			i++ // Numeric validation belongs to the cold engine.
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
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('TRYAGAIN cache owner changed; command not executed') end
local a={ARGV[2],KEYS[2]}
for i=4,#ARGV do a[#a+1]=ARGV[i] end
local result=redis.pcall(unpack(a))
if type(result)=='table' and result.err then return result end
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
