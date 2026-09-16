package gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"strconv"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

type migrationSnapshot struct {
	kind          string
	count, expiry int64
}

func (f *migrationFence) snapshot(ctx context.Context) (migrationSnapshot, error) {
	b, err := f.run(ctx, migrationSnapshotScript)
	if err != nil {
		return migrationSnapshot{}, err
	}
	p, err := cacheValues(b)
	if err != nil || len(p) != 3 {
		return migrationSnapshot{}, errors.New("invalid migration snapshot")
	}
	n, e1 := strconv.ParseInt(p[1], 10, 64)
	expiry, e2 := strconv.ParseInt(p[2], 10, 64)
	if e1 != nil || e2 != nil || n < 0 || n > 1<<53-1 || expiry < -2 || expiry == 0 || expiry > 1<<53-1 {
		return migrationSnapshot{}, errors.New("invalid migration snapshot bounds")
	}
	switch p[0] {
	case "string", "hash", "list", "set", "zset", "none":
	default:
		return migrationSnapshot{}, errors.New("unsupported migration type")
	}
	if (p[0] == "none") != (expiry == -2) || (p[0] == "none" && n != 0) {
		return migrationSnapshot{}, errors.New("inconsistent migration snapshot")
	}
	return migrationSnapshot{p[0], n, expiry}, nil
}

func (f *migrationFence) copyRead(ctx context.Context, cmd string, args ...string) ([]byte, bool, error) {
	a := append([]string{cmd}, args...)
	b, err := f.run(ctx, migrationReadScript, a...)
	if err != nil {
		return nil, false, err
	}
	p, err := frameParts(b)
	if err != nil || len(p) == 0 {
		return nil, false, errors.New("invalid migration read")
	}
	if string(p[0]) == ":0\r\n" && len(p) == 1 {
		return nil, false, nil
	}
	if string(p[0]) != ":1\r\n" || len(p) != 2 || len(p[1]) == 0 || p[1][0] == '-' {
		return nil, false, errors.New("migration read failed")
	}
	return p[1], true, nil
}

func (f *migrationFence) state(ctx context.Context) (phase, attempt string, err error) {
	b, err := f.run(ctx, migrationStateScript)
	if err != nil {
		return "", "", err
	}
	p, err := cacheValues(b)
	if err != nil || len(p) != 2 {
		return "", "", errors.New("invalid migration state")
	}
	return p[0], p[1], nil
}

// copyMigrationKey is a development primitive, not a live request handler.
// Caller holds shared per-key admission, and ALL writers must use backend
// guards (native blocking writers must have been independently drained).
// Backend ACK durability/replication policy must be validated before using this
// to retire authoritative source data; these primitives do not fsync Kvrocks.
// A prepared record allows restarting a partial copy; committed records NEVER
// permit copying over target data again. Each invocation starts a fresh attempt.
// Finite TTL preserves absolute expiration; cross-host clocks are checked before
// freezing and before committing, in addition to the operational NTP requirement.
func copyMigrationKey(ctx context.Context, j *sharding.Journal, h sharding.Handoff, source, target *migrationFence) (string, error) {
	if err := checkMigrationPair(h, source, target); err != nil {
		return "", err
	}
	if err := checkMigrationClocks(ctx, source, target); err != nil {
		return "", err
	}
	current, found, err := j.Lookup(ctx, h.Key)
	if err != nil {
		return "", err
	}
	if !found || current.TaskID != h.TaskID || current.Sequence != h.Sequence || current.Placement != h.Placement || current.State != sharding.Prepared {
		return "", errors.New("copy requires a prepared handoff")
	}
	if err := source.freeze(ctx); err != nil {
		return "", err
	}
	phase, previous, err := target.state(ctx)
	if err != nil {
		return "", err
	}
	if phase == "serving" {
		return "", errors.New("prepared journal conflicts with serving target")
	}
	snapshot, err := source.snapshot(ctx)
	if err != nil {
		return "", err
	}
	attempt := rand.Text()
	if err := target.begin(ctx, previous, attempt); err != nil {
		return "", err
	}
	var chunks int64
	appendChunk := func(op string, values ...string) error {
		if err := target.append(ctx, attempt, chunks, op, values...); err != nil {
			return err
		}
		chunks++
		return nil
	}
	absent := snapshot.kind == "none"
	switch snapshot.kind {
	case "none":
	case "string":
		size := int64(64 * 1024)
		for _, caller := range []migrationCaller{source.c, target.c} {
			if b, ok := caller.(interface{ migrationFrameBudget() int }); ok && b.migrationFrameBudget() > 0 {
				// Leave room for RESP nesting, fencing script and key arguments.
				if budget := int64(b.migrationFrameBudget() / 4); budget < size {
					size = budget
				}
			}
		}
		if size < 1 {
			return "", errors.New("migration frame budget too small")
		}
		for off := int64(0); off < snapshot.count || (off == 0 && snapshot.count == 0); off += size {
			b, exists, err := source.copyRead(ctx, "GETRANGE", strconv.FormatInt(off, 10), strconv.FormatInt(off+size-1, 10))
			if err != nil {
				return "", err
			}
			if !exists {
				absent = true
				break
			}
			value, ok := frameString(b)
			want := snapshot.count - off
			if want > size {
				want = size
			}
			if !ok || int64(len(value)) != want {
				return "", errors.New("source string changed during copy")
			}
			// SETRANGE with an empty payload does not create an empty String.
			if snapshot.count == 0 {
				if err := target.ack(ctx, migrationEmptyStringScript, attempt); err != nil {
					return "", err
				}
				chunks++
			} else if err := appendChunk("SETRANGE", strconv.FormatInt(off, 10), value); err != nil {
				return "", err
			}
			if snapshot.count == 0 {
				break
			}
		}
	case "list":
		for off := int64(0); off < snapshot.count; off += 128 {
			b, exists, err := source.copyRead(ctx, "LRANGE", strconv.FormatInt(off, 10), strconv.FormatInt(off+127, 10))
			if err != nil {
				return "", err
			}
			if !exists {
				absent = true
				break
			}
			values, err := cacheValues(b)
			want := snapshot.count - off
			if want > 128 {
				want = 128
			}
			if err != nil || int64(len(values)) != want {
				return "", errors.New("source list changed during copy")
			}
			if err := appendChunk("RPUSH", values...); err != nil {
				return "", err
			}
		}
	case "hash", "set", "zset":
		scan := map[string]string{"hash": "HSCAN", "set": "SSCAN", "zset": "ZSCAN"}[snapshot.kind]
		op := map[string]string{"hash": "HSET", "set": "SADD", "zset": "ZADD"}[snapshot.kind]
		cursor := "0"
		for {
			b, exists, err := source.copyRead(ctx, scan, cursor, "COUNT", "128")
			if err != nil {
				return "", err
			}
			if !exists {
				absent = true
				break
			}
			parts, err := frameParts(b)
			if err != nil || len(parts) != 2 {
				return "", errors.New("invalid migration scan")
			}
			var ok bool
			cursor, ok = frameString(parts[0])
			if _, err := strconv.ParseUint(cursor, 10, 64); !ok || err != nil {
				return "", errors.New("invalid migration scan cursor")
			}
			values, err := cacheValues(parts[1])
			if err != nil {
				return "", err
			}
			if snapshot.kind != "set" && len(values)%2 != 0 {
				return "", errors.New("invalid migration scan pairs")
			}
			if snapshot.kind == "zset" {
				for i := 0; i < len(values); i += 2 {
					values[i], values[i+1] = values[i+1], values[i]
				}
			}
			// COUNT is not a response bound. Reader enforces frame budgets;
			// split outgoing Lua argument lists even if an engine returns more.
			for len(values) > 0 {
				n := len(values)
				if n > 256 {
					n = 256
				}
				if err := appendChunk(op, values[:n]...); err != nil {
					return "", err
				}
				values = values[n:]
			}
			if cursor == "0" {
				break
			}
		}
	default:
		return "", errors.New("unsupported migration object type")
	}
	end, err := source.snapshot(ctx)
	if err != nil {
		return "", err
	}
	if absent && end.kind != "none" {
		return "", errors.New("source reappeared despite migration freeze")
	}
	if absent || end.kind == "none" {
		end = migrationSnapshot{"none", 0, -2}
	} else {
		if end.kind != snapshot.kind || end.count != snapshot.count || end.expiry != snapshot.expiry {
			return "", errors.New("source changed despite migration freeze")
		}
		// Do not restart relative TTL after a long copy.
		if snapshot.expiry > 0 {
			end.expiry = snapshot.expiry
		}
	}
	if err := target.seal(ctx, attempt, end.kind, chunks, end.count, end.expiry); err != nil {
		return "", err
	}
	return attempt, nil
}

func checkMigrationPair(h sharding.Handoff, source, target *migrationFence) error {
	binding := h.TaskID + ":" + strconv.FormatInt(h.Sequence, 10)
	if source == nil || target == nil || source == target || source.node != h.Placement.Source || target.node != h.Placement.Target || source.key != string(h.Key) || target.key != string(h.Key) || source.binding != binding || target.binding != binding {
		return errors.New("migration endpoints do not match handoff")
	}
	return nil
}

// finishMigrationKey may resume after an uncertain journal/activation/cleanup
// ACK. Once committed, it only activates/cleans; it never re-copies the source.
// Source cleanup failure leaves the journal committed and target serving.
func finishMigrationKey(ctx context.Context, j *sharding.Journal, h sharding.Handoff, source, target *migrationFence, attempt string) error {
	if err := checkMigrationPair(h, source, target); err != nil {
		return err
	}
	current, found, err := j.Lookup(ctx, h.Key)
	if err != nil {
		return err
	}
	if !found || current.TaskID != h.TaskID || current.Sequence != h.Sequence || current.Placement != h.Placement {
		return errors.New("missing or mismatched handoff")
	}
	phase, currentAttempt, err := target.state(ctx)
	if err != nil {
		return err
	}
	if currentAttempt != attempt || (phase != "ready" && phase != "serving") {
		return errors.New("target copy not ready")
	}
	if current.State == sharding.Prepared {
		if err := checkMigrationClocks(ctx, source, target); err != nil {
			return err
		}
		if phase != "ready" {
			return errors.New("target serving before journal commit")
		}
		sourcePhase, _, err := source.state(ctx)
		if err != nil {
			return err
		}
		if sourcePhase != "frozen" {
			return errors.New("source is not frozen")
		}
		if err := j.Commit(ctx, current); err != nil {
			return err
		}
	}
	if err := target.activate(ctx, attempt); err != nil {
		return err
	}
	if current.State == sharding.Cleaned {
		return nil
	}
	if err := source.clean(ctx); err != nil {
		return err
	}
	return j.MarkCleaned(ctx, current)
}

const migrationStateScript = `
if redis.call('EXISTS',KEYS[2])==0 then return {'',''} end
if not bound() then return reject() end
return {phase(),redis.call('HGET',KEYS[2],'attempt') or ''}`

const migrationSnapshotScript = `
if not bound() or phase()~='frozen' then return reject() end
local kind=redis.call('TYPE',KEYS[3]);if type(kind)=='table' then kind=kind.ok end
if kind=='none' then return {'none','0','-2'} end
local counts={string='STRLEN',hash='HLEN',list='LLEN',set='SCARD',zset='ZCARD'}
if not counts[kind] then return redis.error_reply('ERR unsupported migration type') end
local count=redis.call(counts[kind],KEYS[3])
local expiry=redis.call('PEXPIRETIME',KEYS[3])
if expiry==-2 then return {'none','0','-2'} end
return {kind,string.format('%.0f',count),string.format('%.0f',expiry)}`

const migrationReadScript = `
if not bound() or phase()~='frozen' then return reject() end
local allowed={GETRANGE=true,LRANGE=true,HSCAN=true,SSCAN=true,ZSCAN=true}
if not allowed[ARGV[3]] then return reject() end
if redis.call('EXISTS',KEYS[3])==0 then return {0} end
local a={ARGV[3],KEYS[3]};for i=4,#ARGV do a[#a+1]=ARGV[i] end
return {1,redis.call(unpack(a))}`

const migrationEmptyStringScript = `
if not bound() or phase()~='copying' or redis.call('HGET',KEYS[2],'attempt')~=ARGV[3] or redis.call('HGET',KEYS[2],'chunk')~='0' then return reject() end
redis.call('HSET',KEYS[2],'phase','applying')
redis.call('SET',KEYS[3],'')
redis.call('HSET',KEYS[2],'phase','copying','chunk','1')
return 1`
