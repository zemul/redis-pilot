package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

type shardAdmissionKey struct{}
type shardKeyGuard struct{ key, binding string }

// Admissions are request-scoped and immutable except for live. The outer
// operation holds the existing key locks through cold execution AND all hot
// work. Nested legacy handlers may reuse only keys already in this lease.
type shardAdmission struct {
	runtime  *cacheRuntime
	node     *cacheShardNode
	keys     []shardKeyGuard
	revision uint64
	live     atomic.Bool
	retiring bool
}

func shardAdmissionFrom(ctx context.Context) *shardAdmission {
	a, _ := ctx.Value(shardAdmissionKey{}).(*shardAdmission)
	return a
}

func (a *shardAdmission) reenter(r *cacheRuntime, keys [][]byte) (func(), error) {
	if a.runtime != r || !a.live.Load() {
		return nil, errors.New("expired shard admission")
	}
	for _, key := range keys {
		found := false
		for _, held := range a.keys {
			if held.key == string(key) {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("command escaped shard key admission")
		}
	}
	return func() {}, nil
}

// Keep original KEYS/ARGV exactly intact for all existing scripts, including
// their #KEYS loops and the user-Lua sandbox. Extra guard keys are declared to
// the engine but not exposed to the original script's local KEYS/ARGV.
func (a *shardAdmission) wrap(args []string) ([]string, error) {
	if !a.live.Load() {
		return nil, errors.New("expired shard admission")
	}
	if len(args) == 0 {
		return nil, errors.New("empty shard command")
	}
	if !strings.EqualFold(args[0], "EVAL") {
		keys := []string{}
		switch strings.ToUpper(args[0]) {
		case "TIME", "MULTI", "EXEC", "DISCARD", "UNWATCH":
		case "WATCH":
			keys = args[1:]
		default:
			if len(args) > 1 {
				keys = args[1:2]
			}
		}
		for _, key := range keys {
			if key != a.runtime.guard {
				if _, err := a.reenter(a.runtime, [][]byte{[]byte(key)}); err != nil {
					return nil, err
				}
			}
		}
		// Only read/connection commands used internally by cache handlers.
		// Native blocking writes and unguarded mutations cannot use this path.
		switch strings.ToUpper(args[0]) {
		case "GET", "TYPE", "PTTL", "PEXPIRETIME", "TIME", "STRLEN", "HLEN", "SCARD", "ZCARD", "LLEN", "GETRANGE", "LRANGE", "HSCAN", "SSCAN", "ZSCAN", "ZSCORE", "WATCH", "UNWATCH", "MULTI", "EXEC", "DISCARD":
			return args, nil
		default:
			return nil, errors.New("unguarded command on sharded cold connection")
		}
	}
	if len(args) < 3 {
		return nil, errors.New("invalid shard script")
	}
	n, err := strconv.Atoi(args[2])
	if err != nil || n < 0 || n > len(args)-3 {
		return nil, errors.New("invalid shard script keys")
	}
	for _, key := range args[3 : 3+n] {
		if key != a.runtime.guard && key != shardBlockingRegistry {
			if _, err := a.reenter(a.runtime, [][]byte{[]byte(key)}); err != nil {
				return nil, err
			}
		}
	}
	argv := len(args) - 3 - n
	// Numeric constants below are internal counts, never interpolated keys or
	// user values. All business keys/bindings remain binary-safe RESP arguments.
	script := fmt.Sprintf(`
local originalKeys,originalArgs={},{}
for i=1,%d do originalKeys[i]=KEYS[i] end
for i=1,%d do originalArgs[i]=ARGV[i] end
for i=1,%d do
 local key=KEYS[%d+(i-1)*2+1]
 local marker=KEYS[%d+(i-1)*2+2]
 local binding=ARGV[%d+i]
 if binding=='' then
  if redis.call('EXISTS',marker)~=0 then return redis.error_reply('TRYAGAIN shard admission changed') end
 elseif not (%t and redis.call('EXISTS',marker)==0) and (redis.call('HGET',marker,'binding')~=binding or redis.call('HGET',marker,'key')~=key or redis.call('HGET',marker,'phase')~='serving') then
  return redis.error_reply('TRYAGAIN shard admission changed')
 end
end
local function execute(KEYS,ARGV)
`, n, argv, len(a.keys), n, n, argv, a.retiring) + args[1] + "\nend\nreturn execute(originalKeys,originalArgs)"
	out := []string{"EVAL", script, strconv.Itoa(n + 2*len(a.keys))}
	out = append(out, args[3:3+n]...)
	for _, k := range a.keys {
		out = append(out, k.key, fmt.Sprintf("%smigration:%x", cachePrefix, sha256.Sum256([]byte(k.key))))
	}
	out = append(out, args[3+n:]...)
	for _, k := range a.keys {
		out = append(out, k.binding)
	}
	return out, nil
}
