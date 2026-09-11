package gateway

import (
	"context"
	"strconv"
)

// Incremental effects only. Never fetch/rebuild an existing whole object and
// never recreate an evicted collection from a partial mutation.
func (r *cacheRuntime) updateHot(ctx context.Context, cold, hot *cacheConn, cmd cacheCommand, args [][]byte, result []byte) {
	key := string(args[1])
	expiry, err := r.cacheExpiry(ctx, cold, hot, key)
	if err != nil || expiry == 0 {
		r.invalidateHot(ctx, hot, key)
		return
	}
	op := cmd.name
	values := make([]string, 0, len(args))
	for _, v := range args[2:] {
		values = append(values, string(v))
	}
	create := "0"
	expected := ""
	check := "0"
	switch cmd.name {
	case "SET":
		op = "SET"
		values = []string{string(args[2])}
		create = "1"
	case "INCR", "DECR", "INCRBY", "DECRBY":
		v, ok := frameString(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		op = "SET"
		values = []string{v}
	case "HINCRBY":
		v, ok := frameString(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		op = "HSET"
		values = []string{string(args[2]), v}
	case "ZINCRBY":
		v, ok := frameString(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		op = "ZADD"
		values = []string{v, string(args[3])}
	case "SPOP":
		v, ok := frameString(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		op = "SREM"
		values = []string{v}
	case "LPOP", "RPOP":
		v, ok := frameString(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		check = "1"
		expected = v
	case "EXPIRE", "PEXPIRE", "PERSIST":
		op = ""
		values = nil
	}
	a := []string{"EVAL", cacheUpdateScript, "1", key, op, strconv.FormatInt(expiry, 10), create, check, expected}
	a = append(a, values...)
	b, e := hot.call(ctx, a...)
	if e != nil || (string(b) != ":1\r\n" && string(b) != ":0\r\n") {
		// A transport failure may hide a later mutation: readiness must be revoked,
		// not just DEL via another connection while the old script remains pending.
		r.hotFailed()
		return
	}
	if string(b) == ":1\r\n" {
		r.events.WithLabelValues("updated").Inc()
	} else {
		r.events.WithLabelValues("update_miss").Inc()
	}
}

const cacheUpdateScript = `
redis.replicate_commands()
if ARGV[3]~='1' and redis.call('EXISTS',KEYS[1])==0 then return 0 end
local now=redis.call('TIME')
if tonumber(now[1])*1000+math.floor(tonumber(now[2])/1000)>=tonumber(ARGV[2]) then redis.call('DEL',KEYS[1]); return 0 end
if ARGV[1]~='' then
 local a={ARGV[1],KEYS[1]}
 for i=6,#ARGV do a[#a+1]=ARGV[i] end
 local result=redis.pcall(unpack(a))
 if type(result)=='table' and result.err then redis.call('DEL',KEYS[1]); return result end
 if ARGV[4]=='1' and result~=ARGV[5] then redis.call('DEL',KEYS[1]); return 0 end
end
local expiry=redis.pcall('PEXPIREAT',KEYS[1],ARGV[2])
if type(expiry)=='table' and expiry.err then redis.call('DEL',KEYS[1]); return expiry end
return 1`

const cacheVerifyScript = `
if redis.call('EXISTS',KEYS[1])==0 then return 0 end
if redis.call(ARGV[1],KEYS[1])~=tonumber(ARGV[2]) then redis.call('DEL',KEYS[1]); return 0 end
return 1`
