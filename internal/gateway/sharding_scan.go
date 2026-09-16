package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

const shardCursorCapacity = 1024
const shardCursorTTL = 5 * time.Minute

// Client cursors remain uint64 decimal strings. Only bounded traversal state,
// never business values or a key directory, is retained. Recently returned
// cursors can be replayed (read reply loss); expiry/eviction/restart requires 0.
type shardCursor struct {
	fingerprint                    [32]byte
	revision                       uint64
	index                          int
	node, backend, owner, identity string
	expires                        time.Time
}
type shardCursorStore struct {
	mu      sync.Mutex
	entries map[uint64]shardCursor
}

func (s *shardCursorStore) get(id uint64, now time.Time) (shardCursor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.entries[id]
	return v, ok && now.Before(v.expires)
}

func (s *shardCursorStore) discard(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, id)
}

func (s *shardCursorStore) put(v shardCursor, now time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(v.backend) > 1024 {
		return "", errors.New("backend cursor exceeds budget")
	}
	if s.entries == nil {
		s.entries = map[uint64]shardCursor{}
	}
	var oldest uint64
	var deadline time.Time
	for id, entry := range s.entries {
		if !now.Before(entry.expires) {
			delete(s.entries, id)
			continue
		}
		if oldest == 0 || entry.expires.Before(deadline) {
			oldest, deadline = id, entry.expires
		}
	}
	if len(s.entries) >= shardCursorCapacity {
		delete(s.entries, oldest)
	}
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		id := binary.BigEndian.Uint64(b[:])
		if _, exists := s.entries[id]; id == 0 || exists {
			continue
		}
		v.expires = now.Add(shardCursorTTL)
		s.entries[id] = v
		return strconv.FormatUint(id, 10), nil
	}
}

func shardScanError() []byte {
	return []byte("-TRYAGAIN scan cursor unavailable or placement changed; restart with cursor 0\r\n")
}

func shardScanFingerprint(args [][]byte, cursor int) [32]byte {
	// RESP length framing keeps binary keys/options unambiguous.
	a := make([]string, len(args))
	for i := range args {
		a[i] = string(args[i])
	}
	a[0], a[cursor] = strings.ToUpper(a[0]), ""
	return sha256.Sum256(encodeCommand(a...))
}

// Caller holds the topology lease for the whole page. Object cursors also
// hold normal key admission; global pages filter each key under the same lock
// used by migration, but never migrate a key merely because SCAN saw it.
func (s *cacheShards) scanLocked(ctx context.Context, args [][]byte) []byte {
	name := strings.ToUpper(string(args[0]))
	global := name == "SCAN"
	pos := 2
	if global {
		pos = 1
	}
	if _, ok := s.r.prepareCache(args); !ok || len(args) <= pos {
		return []byte("-ERR invalid scan arguments\r\n")
	}
	id, err := strconv.ParseUint(string(args[pos]), 10, 64)
	if err != nil {
		return shardScanError()
	}
	fp := shardScanFingerprint(args, pos)
	v := shardCursor{fingerprint: fp, revision: s.revisionLocked(), backend: "0"}
	if id != 0 {
		var ok bool
		v, ok = s.cursors.get(id, time.Now())
		if !ok || v.fingerprint != fp || v.revision != s.revisionLocked() {
			return shardScanError()
		}
	}
	completed := false
	defer func() {
		// An uncertain backend read must not leave a reusable cursor. A
		// successfully returned page can still be replayed after reply loss.
		if id != 0 && !completed {
			s.cursors.discard(id)
		}
	}()
	var n *cacheShardNode
	var nodes []sharding.Node
	if global {
		nodes = s.topology.Config().Nodes
		if s.change != nil {
			nodes = s.change.TraversalNodes()
		}
		if v.index < 0 || v.index >= len(nodes) {
			return shardScanError()
		}
		n = s.node(nodes[v.index].ID)
	} else {
		routed, release, err := s.admitLocked(ctx, args[1:2])
		if err != nil {
			return shardScanError()
		}
		defer release()
		ctx = routed
		n = shardAdmissionFrom(ctx).node
	}
	token, err := s.ensure(ctx, n)
	if err != nil {
		return shardScanError()
	}
	c, err := s.raw(ctx, n)
	if err != nil {
		return shardScanError()
	}
	defer c.Close()
	identity, err := shardScanBackendIdentity(ctx, c)
	if err != nil {
		return shardScanError()
	}
	if v.node != "" && (v.node != n.node.ID || v.owner != token || v.identity != identity) {
		return shardScanError()
	}
	a := make([]string, len(args))
	for i := range args {
		a[i] = string(args[i])
	}
	a[0], a[pos] = name, v.backend
	var reply []byte
	if global {
		command := []string{"EVAL", shardGlobalScanScript, "1", s.r.guard, token}
		command = append(command, a[1:]...)
		reply, err = c.call(ctx, command...)
	} else {
		// Reuse the existing fenced single-key read. No cache lookup/fill.
		c.shard = n
		command := []string{"EVAL", shardObjectScanScript, "2", s.r.guard, a[1], token, name}
		command = append(command, a[2:]...)
		reply, err = c.call(ctx, command...)
	}
	if err != nil {
		return shardScanError()
	}
	if len(reply) != 0 && reply[0] == '-' {
		return reply
	}
	parts, err := frameParts(reply)
	if err != nil || len(parts) != 2 {
		return shardScanError()
	}
	backend, ok := frameString(parts[0])
	if !ok || backend == "" || len(backend) > 1024 {
		return shardScanError()
	}
	items, err := frameParts(parts[1])
	if err != nil {
		return shardScanError()
	}
	if global {
		filtered := make([][]byte, 0, len(items))
		for _, item := range items {
			key, ok := frameString(item)
			if !ok {
				return shardScanError()
			}
			if !cacheBusinessKey([]byte(key)) {
				continue
			}
			visible, err := s.scanVisible(ctx, n.node, []byte(key))
			if err != nil {
				return shardScanError()
			}
			if visible {
				filtered = append(filtered, item)
			}
		}
		parts[1] = cacheArray(filtered)
	} else if name != "SSCAN" && len(items)%2 != 0 {
		return shardScanError()
	}
	v.backend, v.node, v.owner, v.identity = backend, n.node.ID, token, identity
	if global && backend == "0" {
		v.index++
		v.node, v.owner, v.identity = "", "", ""
	}
	next := "0"
	if backend != "0" || (global && v.index < len(nodes)) {
		next, err = s.cursors.put(v, time.Now())
		if err != nil {
			return shardScanError()
		}
	}
	result := cacheArray([][]byte{cacheBulk(next), parts[1]})
	if len(result) > s.r.s.cfg.Limits.MaxBytes {
		return shardScanError()
	}
	completed = true
	return result
}

// This is a read-cursor restart hint, NEVER a migration fence or proof that old
// writers have ended. A PID reused by a restart in the same second can collide.
// INFO, TIME and the page use the same non-reconnecting connection; no connection
// is retained between pages. A clock discontinuity may require restarting SCAN.
func shardScanBackendIdentity(ctx context.Context, c *cacheConn) (string, error) {
	for attempt := 0; attempt < 3; attempt++ {
		info, err := c.call(ctx, "INFO", "server")
		if err != nil {
			return "", err
		}
		fields, err := shardScanInfoFields(info)
		if err != nil {
			return "", err
		}
		if run, exists := fields["run_id"]; exists {
			if run == "" || len(run) > 128 || strings.ContainsAny(run, " \t\r\n") {
				return "", errors.New("invalid scan run ID")
			}
			return "run:" + run, nil
		}
		pid, pidOK := shardScanNumber(fields["process_id"])
		micros, timeOK := shardScanNumber(fields["server_time_usec"])
		uptime, uptimeOK := shardScanNumber(fields["uptime_in_seconds"])
		seconds := micros / 1000000
		if !pidOK || !timeOK || !uptimeOK || pid == 0 || seconds == 0 || uptime >= seconds {
			return "", errors.New("scan startup information unavailable")
		}
		// Kvrocks reads server_time_usec before the seconds used for uptime.
		// A subsequent TIME in that same server second brackets the uptime
		// sample. If we crossed a second, resample instead of tolerating a
		// drifting +/-1 identity. No dependency on the Gateway wall clock.
		b, err := c.call(ctx, "TIME")
		if err != nil {
			return "", err
		}
		parts, err := frameParts(b)
		if err != nil || len(parts) != 2 {
			return "", errors.New("invalid scan time response")
		}
		secText, _ := frameString(parts[0])
		usecText, _ := frameString(parts[1])
		sec, secOK := shardScanNumber(secText)
		usec, usecOK := shardScanNumber(usecText)
		if !secOK || !usecOK || sec == 0 || usec >= 1000000 {
			return "", errors.New("invalid scan time fields")
		}
		if sec == seconds && usec >= micros%1000000 {
			return "start:" + strconv.FormatInt(pid, 10) + ":" + strconv.FormatInt(seconds-uptime, 10), nil
		}
	}
	return "", errors.New("scan startup sample crossed server clock boundary")
}

func shardScanNumber(v string) (int64, bool) {
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil && n >= 0 && strconv.FormatInt(n, 10) == v
}

func shardScanInfoFields(info []byte) (map[string]string, error) {
	v, ok := frameString(info)
	if !ok {
		return nil, errors.New("invalid scan server info")
	}
	fields := make(map[string]string, 4)
	for _, line := range strings.Split(v, "\n") {
		key, value, found := strings.Cut(strings.TrimSuffix(line, "\r"), ":")
		if !found || (key != "run_id" && key != "process_id" && key != "server_time_usec" && key != "uptime_in_seconds") {
			continue
		}
		if _, exists := fields[key]; exists {
			return nil, errors.New("duplicate scan identity field")
		}
		fields[key] = value
	}
	return fields, nil
}

func (s *cacheShards) scanVisible(ctx context.Context, node sharding.Node, key []byte) (bool, error) {
	release, err := s.r.acquire(ctx, key)
	if err != nil {
		return false, err
	}
	defer release()
	if s.change == nil {
		return s.topology.Owner(key) == node, nil
	}
	p := s.change.DesiredPlacement(key)
	if !p.Moves() {
		return p.Source == node, nil
	}
	h, found, err := s.journal.Lookup(ctx, key)
	if err != nil {
		return false, err
	}
	if found && h.State != sharding.Prepared {
		return p.Target == node, nil
	}
	return p.Source == node, nil
}

const shardGlobalScanScript = `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cold owner changed') end
return redis.call('SCAN',unpack(ARGV,2))`

const shardObjectScanScript = `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('TRYAGAIN cold owner changed') end
return redis.call(ARGV[2],KEYS[2],unpack(ARGV,3))`
