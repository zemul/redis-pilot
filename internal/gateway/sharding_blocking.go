package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

const shardBlockingRegistry = cachePrefix + "blocking"
const shardBlockingCapacity = 256

// Only outstanding native blocking connections are registered, not all keys.
// The cold-side record precedes sending BLPOP and survives a Gateway crash.
// [][]byte JSON encoding preserves invalid UTF-8/binary keys via base64.
type shardBlockRecord struct {
	Version  int      `json:"version"`
	ClientID string   `json:"client_id"`
	RunID    string   `json:"run_id,omitempty"` // Read-only compatibility with v1 records; never proves termination.
	Name     string   `json:"name"`
	Keys     [][]byte `json:"keys"`
}

type shardBlockingActive struct {
	mu       sync.Mutex
	sessions map[string]bool
}

func (b *shardBlockingActive) set(node, field string, active bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sessions == nil {
		b.sessions = map[string]bool{}
	}
	if active {
		b.sessions[node+"/"+field] = true
	} else {
		delete(b.sessions, node+"/"+field)
	}
}
func (b *shardBlockingActive) has(node, field string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sessions[node+"/"+field]
}

func (s *cacheShards) block(parent context.Context, args [][]byte) []byte {
	if len(args) < 3 || len(args) > 18 {
		return []byte("-ERR blocking pop requires 1..16 keys and timeout\r\n")
	}
	keys := args[1 : len(args)-1]
	for _, key := range keys {
		if !cacheBusinessKey(key) || !s.r.coldOnly(key) {
			return []byte("-ERR blocking keys must belong to configured cold_only_prefixes\r\n")
		}
	}
	seconds, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil || math.IsNaN(seconds) || seconds <= 0 || seconds > 60 {
		return []byte("-ERR blocking timeout must be in (0,60] seconds\r\n")
	}
	select {
	case s.r.blocking <- struct{}{}:
		defer func() { <-s.r.blocking }()
	default:
		return []byte("-TRYAGAIN blocking connection limit reached; command not sent\r\n")
	}
	setup, cancel := context.WithTimeout(parent, s.r.s.cfg.RequestTimeout)
	defer cancel()
	ctx, release, err := s.admit(setup, keys, 0)
	if err != nil {
		return shardRuntimeError(err)
	}
	// Drop ordinary key/topology locks BEFORE waiting, otherwise producers
	// cannot push. The backend registration then protects migration instead.
	defer func() {
		if release != nil {
			release()
		}
	}()
	a := shardAdmissionFrom(ctx)
	n := a.node
	token, err := s.ensure(ctx, n)
	if err != nil {
		return shardRuntimeError(err)
	}
	c, err := s.raw(ctx, n)
	if err != nil {
		return shardRuntimeError(err)
	}
	defer c.Close()
	field := rand.Text()
	record := shardBlockRecord{Version: 2, Name: "pilot-block-" + field, Keys: keys}
	b, err := c.call(ctx, "CLIENT", "SETNAME", record.Name)
	if err != nil || string(b) != "+OK\r\n" {
		return shardRuntimeError(errors.New("blocking identity unavailable"))
	}
	b, err = c.call(ctx, "CLIENT", "ID")
	record.ClientID, _ = frameString(b)
	if err != nil || !shardClientID(record.ClientID) {
		return shardRuntimeError(errors.New("blocking client ID unavailable"))
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return shardRuntimeError(err)
	}
	s.blocking.set(n.node.ID, field, true)
	defer s.blocking.set(n.node.ID, field, false)
	c.shard = n
	b, err = c.call(ctx, "EVAL", shardBlockRegisterScript, "2", s.r.guard, shardBlockingRegistry, token, field, string(payload), strconv.Itoa(shardBlockingCapacity))
	c.shard = nil
	release()
	release = nil
	if err != nil || string(b) != ":1\r\n" {
		// A lost registration reply must never cause a native pop to be sent.
		c.Close()
		s.finishBlocking(parent, n, field, string(payload), record)
		return []byte("-TRYAGAIN blocking registration unconfirmed; pop not sent\r\n")
	}
	wait := time.Duration(seconds*float64(time.Second)) + s.r.s.cfg.RequestTimeout
	callCtx, stop := context.WithTimeout(parent, wait)
	c.cfg.RequestTimeout = wait
	b, err = c.call(callCtx, cacheStrings(args)...)
	stop()
	c.Close()
	// Cleanup failure leaves the durable record for the probe/migration path.
	// A known pop result remains usable; cleanup is not a business replay.
	s.finishBlocking(parent, n, field, string(payload), record)
	if err != nil {
		s.r.s.uncertain.Inc()
		s.r.events.WithLabelValues("blocking_unknown").Inc()
		return []byte("-ERR blocking result unknown or interrupted by migration; command was not retried\r\n")
	}
	s.r.events.WithLabelValues("blocking_completed").Inc()
	if string(b) == "$-1\r\n" {
		return []byte("*-1\r\n")
	}
	return b
}

func (s *cacheShards) finishBlocking(parent context.Context, n *cacheShardNode, field, payload string, record shardBlockRecord) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), s.r.s.cfg.RequestTimeout)
	defer cancel()
	if err := s.retireBlocking(ctx, n, field, payload, record); err != nil {
		s.r.events.WithLabelValues("blocking_drain_pending").Inc()
	}
}

// Callers that drain a key hold its normal admission lock. Orphan maintenance
// instead skips locally active sessions, including those not yet registered.
func (s *cacheShards) drainBlocking(ctx context.Context, n *cacheShardNode, key []byte, orphans bool) error {
	// The persisted cold-only policy is immutable. Other business keys cannot
	// have native blocking writers, so ordinary migration needs no registry IO.
	if key != nil && !s.r.coldOnly(key) {
		return nil
	}
	token, err := s.ensure(ctx, n)
	if err != nil {
		return err
	}
	c, err := s.raw(ctx, n)
	if err != nil {
		return err
	}
	defer c.Close()
	cursor := "0"
	for {
		b, err := c.call(ctx, "EVAL", shardBlockScanScript, "2", s.r.guard, shardBlockingRegistry, token, cursor)
		if err != nil {
			return err
		}
		parts, err := frameParts(b)
		if err != nil || len(parts) != 2 {
			return errors.New("blocking registry unavailable")
		}
		var ok bool
		cursor, ok = frameString(parts[0])
		if !ok || cursor == "" || len(cursor) > 1024 {
			return errors.New("invalid blocking registry cursor")
		}
		items, err := frameParts(parts[1])
		if err != nil || len(items)%2 != 0 {
			return errors.New("invalid blocking registry page")
		}
		for i := 0; i < len(items); i += 2 {
			field, fieldOK := frameString(items[i])
			payload, payloadOK := frameString(items[i+1])
			if !fieldOK || !payloadOK {
				return errors.New("invalid blocking record")
			}
			record, err := decodeShardBlock(field, payload)
			if err != nil {
				return err
			}
			if orphans && s.blocking.has(n.node.ID, field) {
				continue
			}
			matches := key == nil
			for _, k := range record.Keys {
				matches = matches || bytes.Equal(k, key)
			}
			if matches {
				if err := s.retireBlocking(ctx, n, field, payload, record); err != nil {
					return err
				}
			}
		}
		if cursor == "0" {
			return nil
		}
	}
}

func decodeShardBlock(field, payload string) (shardBlockRecord, error) {
	var r shardBlockRecord
	if len(field) != 26 || len(payload) > 100*1024 {
		return r, errors.New("invalid blocking record size")
	}
	for _, c := range field {
		if !(c >= 'A' && c <= 'Z') && !(c >= '2' && c <= '7') {
			return r, errors.New("invalid blocking session name")
		}
	}
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return r, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return r, errors.New("trailing blocking record data")
	}
	validVersion := (r.Version == 1 && r.RunID != "" && len(r.RunID) <= 128) || (r.Version == 2 && r.RunID == "")
	if !validVersion || !shardClientID(r.ClientID) || r.Name != "pilot-block-"+field || len(r.Keys) < 1 || len(r.Keys) > 16 {
		return r, errors.New("invalid blocking identity")
	}
	for _, key := range r.Keys {
		if !cacheBusinessKey(key) {
			return r, errors.New("invalid blocking key")
		}
	}
	return r, nil
}

func shardClientID(id string) bool {
	n, err := strconv.ParseUint(id, 10, 64)
	return err == nil && n != 0 && strconv.FormatUint(n, 10) == id
}

// Kvrocks 2.15 marks connections for asynchronous close on CLIENT KILL. Poll
// CLIENT LIST until the named connection is actually absent; neither a count
// of 1 nor a later count of 0 alone proves termination. Match the random name
// before using an ID, which may be reused after restart. LIST/KILL/confirmation
// use one direct, non-reconnecting connection: a backend restart interrupts the
// attempt instead of letting a stale ID reach a new process. Do not short-circuit
// this proof using a run_id, PID or estimated start time, including for v1 records.
func (s *cacheShards) retireBlocking(ctx context.Context, n *cacheShardNode, field, payload string, record shardBlockRecord) error {
	token, err := s.ensure(ctx, n)
	if err != nil {
		return err
	}
	c, err := s.raw(ctx, n)
	if err != nil {
		return err
	}
	defer c.Close()
	for {
		b, err := c.call(ctx, "CLIENT", "LIST")
		if err != nil {
			return err
		}
		ids, err := shardNamedClients(b, record.Name)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		if len(ids) != 1 || ids[0] != record.ClientID {
			return errors.New("blocking client identity changed")
		}
		b, err = c.call(ctx, "CLIENT", "KILL", "ID", ids[0], "SKIPME", "yes")
		if err != nil || (string(b) != ":0\r\n" && string(b) != ":1\r\n") {
			return errors.New("blocking disconnect unconfirmed")
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	b, err := c.call(ctx, "EVAL", shardBlockForgetScript, "2", s.r.guard, shardBlockingRegistry, token, field, payload)
	if err != nil || string(b) != ":1\r\n" {
		return errors.New("blocking registry cleanup unconfirmed")
	}
	return nil
}

func shardNamedClients(b []byte, name string) ([]string, error) {
	text, ok := frameString(b)
	if !ok {
		return nil, errors.New("client list unavailable")
	}
	var ids []string
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		id, clientName, named := "", "", false
		for _, field := range strings.Fields(line) {
			if strings.HasPrefix(field, "id=") {
				id = strings.TrimPrefix(field, "id=")
			}
			if strings.HasPrefix(field, "name=") {
				clientName, named = strings.TrimPrefix(field, "name="), true
			}
		}
		if !shardClientID(id) || !named {
			return nil, errors.New("invalid client identity listing")
		}
		if clientName == name {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

const shardBlockRegisterScript = `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cold owner changed') end
if redis.call('HEXISTS',KEYS[2],ARGV[2])~=0 then return redis.error_reply('TRYAGAIN blocking identity reused') end
if redis.call('HLEN',KEYS[2])>=tonumber(ARGV[4]) then return redis.error_reply('TRYAGAIN blocking registry full') end
redis.call('HSET',KEYS[2],ARGV[2],ARGV[3])
return 1`

const shardBlockScanScript = `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cold owner changed') end
return redis.call('HSCAN',KEYS[2],ARGV[2],'COUNT',16)`

const shardBlockForgetScript = `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cold owner changed') end
local v=redis.call('HGET',KEYS[2],ARGV[2])
if v and v~=ARGV[3] then return redis.error_reply('TRYAGAIN blocking identity changed') end
redis.call('HDEL',KEYS[2],ARGV[2])
return 1`
