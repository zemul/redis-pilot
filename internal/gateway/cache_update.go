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
	case "SET", "SETNX", "GETSET":
		op = "SET"
		values = []string{string(args[2])}
		create = "1"
	case "SETEX", "PSETEX":
		op = "SET"
		values = []string{string(args[3])}
		create = "1"
	case "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT":
		v, ok := frameString(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		op = "SET"
		values = []string{v}
	case "HMSET", "HSETNX":
		op = "HSET"
	case "HINCRBY", "HINCRBYFLOAT":
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
	case "ZADD":
		// NX/XX/GT/LT/INCR must be resolved by cold, not evaluated again
		// against an independently expiring cache. Read only affected scores.
		values = nil
		for i := cacheZAddStart(args); i < len(args); i += 2 {
			b, e := cold.call(ctx, "ZSCORE", key, string(args[i+1]))
			if e == nil && string(b) == "$-1\r\n" {
				// XX can intentionally leave a requested new member absent.
				continue
			}
			score, ok := frameString(b)
			if e != nil || !ok {
				r.invalidateHot(ctx, hot, key)
				return
			}
			values = append(values, score, string(args[i+1]))
		}
		if len(values) == 0 {
			op = ""
		}
	case "SPOP":
		var ok bool
		values, ok = cacheResultValues(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		op = "SREM"
		if len(values) == 0 {
			op = ""
		}
	case "ZPOPMIN", "ZPOPMAX":
		members, ok := cacheResultValues(result)
		if !ok || len(members)%2 != 0 {
			r.invalidateHot(ctx, hot, key)
			return
		}
		op = "ZREM"
		values = nil
		for i := 0; i < len(members); i += 2 {
			values = append(values, members[i])
		}
		if len(values) == 0 {
			op = ""
		}
	case "LPOP", "RPOP":
		v, ok := cacheResultValues(result)
		if !ok {
			r.invalidateHot(ctx, hot, key)
			return
		}
		if len(args) == 2 {
			if len(v) != 1 {
				r.invalidateHot(ctx, hot, key)
				return
			}
			check, expected = "1", v[0]
		} else {
			check, expected = "2", string(result)
		}
	case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "GETEX":
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
local function encodeArray(actual)
 if type(actual)~='table' then return '' end
 local out={'*'..#actual..'\r\n'}
 for i=1,#actual do
  if type(actual[i])~='string' then return '' end
  out[#out+1]='$'..#actual[i]..'\r\n'..actual[i]..'\r\n'
 end
 return table.concat(out)
end
if ARGV[3]~='1' and redis.call('EXISTS',KEYS[1])==0 then return 0 end
local now=redis.call('TIME')
if tonumber(now[1])*1000+math.floor(tonumber(now[2])/1000)>=tonumber(ARGV[2]) then redis.call('DEL',KEYS[1]); return 0 end
if ARGV[1]~='' then
 local a={ARGV[1],KEYS[1]}
 for i=6,#ARGV do a[#a+1]=ARGV[i] end
 local result=redis.pcall(unpack(a))
 if type(result)=='table' and result.err then redis.call('DEL',KEYS[1]); return result end
 if ARGV[4]=='1' and result~=ARGV[5] then redis.call('DEL',KEYS[1]); return 0 end
 if ARGV[4]=='2' and encodeArray(result)~=ARGV[5] then redis.call('DEL',KEYS[1]); return 0 end
end
local expiry=redis.pcall('PEXPIREAT',KEYS[1],ARGV[2])
if type(expiry)=='table' and expiry.err then redis.call('DEL',KEYS[1]); return expiry end
return 1`

func cacheResultValues(b []byte) ([]string, bool) {
	if len(b) > 0 && b[0] == '*' {
		v, err := cacheValues(b)
		return v, err == nil
	}
	v, ok := frameString(b)
	return []string{v}, ok
}

const cacheVerifyScript = `
if redis.call('EXISTS',KEYS[1])==0 then return 0 end
if redis.call(ARGV[1],KEYS[1])~=tonumber(ARGV[2]) then redis.call('DEL',KEYS[1]); return 0 end
return 1`
