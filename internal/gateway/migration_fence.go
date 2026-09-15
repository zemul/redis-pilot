package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

// These primitives are NOT installed in the live cache path. Before using
// them, every business operation (including Lua, EXEC and hot reads) must use
// migration-aware admission and the appropriate backend guard. A marker alone
// cannot intercept commands sent by an unmodified caller or a blocking pop.
type migrationCaller interface {
	call(context.Context, ...string) ([]byte, error)
}

type migrationFence struct {
	c                                     migrationCaller
	ownerKey, owner, marker, key, binding string
	node                                  sharding.Node
}

func newMigrationFence(c migrationCaller, node sharding.Node, ownerKey, owner string, h sharding.Handoff) (*migrationFence, error) {
	if c == nil || owner == "" || ownerKey != cachePrefix+"owner" || h.TaskID == "" || h.Sequence <= 0 || !h.Placement.Moves() || !cacheBusinessKey(h.Key) || (node != h.Placement.Source && node != h.Placement.Target) {
		return nil, errors.New("invalid migration fence identity")
	}
	return &migrationFence{c: c, node: node, ownerKey: ownerKey, owner: owner,
		marker: fmt.Sprintf("%smigration:%x", cachePrefix, sha256.Sum256(h.Key)),
		key:    string(h.Key), binding: h.TaskID + ":" + strconv.FormatInt(h.Sequence, 10)}, nil
}

func (f *migrationFence) run(ctx context.Context, script string, args ...string) ([]byte, error) {
	a := []string{"EVAL", migrationOwnerGuard + script, "3", f.ownerKey, f.marker, f.key, f.owner, f.binding}
	a = append(a, args...)
	b, err := f.c.call(ctx, a...)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || b[0] == '-' {
		return nil, fmt.Errorf("migration backend rejected operation: %s", b)
	}
	return b, nil
}

func (f *migrationFence) ack(ctx context.Context, script string, args ...string) error {
	b, err := f.run(ctx, script, args...)
	if err == nil && string(b) != ":1\r\n" {
		err = errors.New("migration backend acknowledgement missing")
	}
	return err
}

func (f *migrationFence) freeze(ctx context.Context) error { return f.ack(ctx, migrationFreezeScript) }

// Begin uses a fresh attempt and an observed previous attempt, never a blind
// overwrite. An identical late Begin must NOT delete chunks already copied.
func (f *migrationFence) begin(ctx context.Context, previous, attempt string) error {
	if attempt == "" || previous == attempt {
		return errors.New("migration requires a fresh copy attempt")
	}
	return f.ack(ctx, migrationBeginScript, previous, attempt)
}

func (f *migrationFence) append(ctx context.Context, attempt string, chunk int64, op string, values ...string) error {
	if attempt == "" || chunk < 0 || chunk >= 1<<53-1 {
		return errors.New("invalid migration chunk")
	}
	switch op {
	case "SETRANGE":
		if len(values) != 2 {
			return errors.New("invalid string chunk")
		}
	case "HSET", "ZADD":
		if len(values) == 0 || len(values)%2 != 0 {
			return errors.New("invalid pair chunk")
		}
	case "RPUSH", "SADD":
		if len(values) == 0 {
			return errors.New("empty migration chunk")
		}
	default:
		return errors.New("unsupported migration copy operation")
	}
	a := []string{attempt, strconv.FormatInt(chunk, 10), strconv.FormatInt(chunk+1, 10), op}
	a = append(a, values...)
	return f.ack(ctx, migrationAppendScript, a...)
}

// Seal follows a complete, frozen-source copy. Cardinality is only an extra
// sanity check, NOT the sole proof of content integrity. Finite expiries use a
// shared absolute clock; cross-host clock admission still belongs to runtime.
// A negative expiry is -1 (persistent) or -2 (confirmed absent/expired source).
func (f *migrationFence) seal(ctx context.Context, attempt, kind string, chunks, count, expiry int64) error {
	counts := map[string]string{"string": "STRLEN", "hash": "HLEN", "list": "LLEN", "set": "SCARD", "zset": "ZCARD", "none": "EXISTS"}
	cmd, ok := counts[kind]
	if !ok || attempt == "" || chunks < 0 || chunks > 1<<53-1 || count < 0 || count > 1<<53-1 || expiry < -2 || expiry == 0 || expiry > 1<<53-1 || (kind == "none") != (expiry == -2) {
		return errors.New("invalid migration seal")
	}
	return f.ack(ctx, migrationSealScript, attempt, strconv.FormatInt(chunks, 10), kind, cmd, strconv.FormatInt(count, 10), strconv.FormatInt(expiry, 10))
}

func (f *migrationFence) activate(ctx context.Context, attempt string) error {
	return f.ack(ctx, migrationActivateScript, attempt)
}

func (f *migrationFence) clean(ctx context.Context) error { return f.ack(ctx, migrationCleanScript) }

// All script KEYS are declared for Kvrocks strict-key-accessing mode. No
// per-key copy step deletes its control marker; task finalization does that
// only after publishing target routing and revoking pre-publication owners.
const migrationOwnerGuard = `
if redis.replicate_commands then redis.replicate_commands() end
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN migration owner changed') end
local function bound()
 return redis.call('HGET',KEYS[2],'binding')==ARGV[2] and redis.call('HGET',KEYS[2],'key')==KEYS[3]
end
local function phase() return redis.call('HGET',KEYS[2],'phase') end
local function reject() return redis.error_reply('TRYAGAIN migration state changed') end
`

const migrationFreezeScript = `
if redis.call('EXISTS',KEYS[2])==1 then
 if bound() and phase()=='frozen' then return 1 end
 return reject()
end
redis.call('HSET',KEYS[2],'binding',ARGV[2],'key',KEYS[3],'phase','frozen')
return 1`

const migrationBeginScript = `
if redis.call('EXISTS',KEYS[2])==1 then
 if not bound() then return reject() end
 local p=phase()
 if p~='copying' and p~='ready' and p~='resetting' and p~='applying' and p~='sealing' then return reject() end
 local attempt=redis.call('HGET',KEYS[2],'attempt')
 if attempt==ARGV[4] then
  if p=='copying' or p=='ready' then return 1 end
  return reject()
 end
 if attempt~=ARGV[3] then return reject() end
elseif ARGV[3]~='' then return reject() end
-- Mark BEFORE mutation. Runtime/script errors are not Redis rollbacks.
redis.call('HSET',KEYS[2],'binding',ARGV[2],'key',KEYS[3],'attempt',ARGV[4],'phase','resetting','chunk','0')
redis.call('DEL',KEYS[3])
redis.call('HSET',KEYS[2],'phase','copying')
return 1`

const migrationAppendScript = `
if not bound() or phase()~='copying' or redis.call('HGET',KEYS[2],'attempt')~=ARGV[3] or redis.call('HGET',KEYS[2],'chunk')~=ARGV[4] then return reject() end
local allowed={SETRANGE=true,HSET=true,RPUSH=true,SADD=true,ZADD=true}
if not allowed[ARGV[6]] then return reject() end
local a={ARGV[6],KEYS[3]};for i=7,#ARGV do a[#a+1]=ARGV[i] end
redis.call('HSET',KEYS[2],'phase','applying')
local result=redis.pcall(unpack(a))
if type(result)=='table' and result.err then return result end
redis.call('HSET',KEYS[2],'chunk',ARGV[5],'phase','copying')
return 1`

const migrationSealScript = `
if not bound() or phase()~='copying' or redis.call('HGET',KEYS[2],'attempt')~=ARGV[3] or redis.call('HGET',KEYS[2],'chunk')~=ARGV[4] then return reject() end
redis.call('HSET',KEYS[2],'phase','sealing')
if ARGV[8]=='-2' then redis.call('DEL',KEYS[3])
else
 local kind=redis.call('TYPE',KEYS[3]);if type(kind)=='table' then kind=kind.ok end
 if kind~=ARGV[5] or tostring(redis.call(ARGV[6],KEYS[3]))~=ARGV[7] then return redis.error_reply('ERR incomplete migration copy') end
 if ARGV[8]=='-1' then redis.call('PERSIST',KEYS[3]) else redis.call('PEXPIREAT',KEYS[3],ARGV[8]) end
end
redis.call('HSET',KEYS[2],'phase','ready')
return 1`

const migrationActivateScript = `
if not bound() or redis.call('HGET',KEYS[2],'attempt')~=ARGV[3] then return reject() end
if phase()=='serving' then return 1 end
if phase()~='ready' then return reject() end
redis.call('HSET',KEYS[2],'phase','serving')
return 1`

const migrationCleanScript = `
if not bound() then return reject() end
if phase()=='cleaned' then return 1 end
if phase()~='frozen' and phase()~='cleaning' then return reject() end
redis.call('HSET',KEYS[2],'phase','cleaning')
redis.call('DEL',KEYS[3])
redis.call('HSET',KEYS[2],'phase','cleaned')
return 1`

// Prefix for future business wrappers: ARGV[3] is empty for an unmoved key,
// otherwise the caller requires this task's "serving" target. Old requests
// carrying empty admission cannot run after a source/target marker is installed.
// This prefix does NOT yet replace the live single-/multi-key/Lua/EXEC guards.
const migrationBusinessGuard = `
if ARGV[3]=='' then
 if redis.call('EXISTS',KEYS[2])~=0 then return reject() end
elseif ARGV[3]~='serving' or not bound() or phase()~='serving' then return reject() end
`
