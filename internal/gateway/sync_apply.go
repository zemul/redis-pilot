package gateway

// G6a-4 isolated prototype, intentionally not reachable from Service/config.
// Reconstructs the ENTIRE bounded expected state on resume and compares it
// with DB0 before replay. This is not the large-dataset production protocol.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

var errSyncTargetDrift = errors.New("cold state does not match durable journal checkpoint; explicit recovery required")
var errSyncApplyUncertain = errors.New("cold commit outcome uncertain; reconnect and validate full state before replay")

type syncApplyCheckpoint struct {
	Version  int          `json:"version"`
	Position syncPosition `json:"position"`
	Chain    string       `json:"chain"`
}

func (p syncApplyCheckpoint) encode() string { b, _ := json.Marshal(p); return string(b) }

type syncApplyStep struct {
	checkpoint syncApplyCheckpoint
	command    []string // nil means a supported replication control frame
}

func checkSyncState(state map[string]string) error {
	if len(state) > syncStateKeys {
		return errSyncUnsupported
	}
	n := 0
	for k, v := range state {
		if strings.HasPrefix(k, syncMetadataPrefix) {
			return errSyncUnsupported
		}
		n += len(k) + len(v)
		if n > syncStateBytes {
			return errSyncUnsupported
		}
	}
	return nil
}

func advanceSyncState(state map[string]string, cmd []string) error {
	if len(cmd) == 0 {
		return nil
	}
	switch cmd[0] {
	case "SET":
		state[cmd[1]] = cmd[2]
	case "DEL":
		delete(state, cmd[1])
	case "INCR":
		var n int64
		if value, exists := state[cmd[1]]; exists {
			var err error
			n, err = strconv.ParseInt(value, 10, 64)
			if err != nil || strconv.FormatInt(n, 10) != value {
				return errSyncUnsupported
			}
		}
		if n == math.MaxInt64 {
			return errSyncUnsupported
		}
		state[cmd[1]] = strconv.FormatInt(n+1, 10)
	default:
		return errSyncUnsupported
	}
	return checkSyncState(state)
}

func planSyncPrefix(records []syncRecord) (map[string]string, []syncApplyStep, error) {
	if len(records) == 0 {
		return nil, nil, errSyncJournalCorrupt
	}
	initial, err := decodeSyncRDB(records[0].Data)
	if err != nil {
		return nil, nil, err
	}
	model := make(map[string]string, len(initial))
	for k, v := range initial {
		model[k] = v
	}
	var steps []syncApplyStep
	previous := &syncJournal{}
	chain := ""
	for _, record := range records {
		if err := previous.check(record); err != nil {
			return nil, nil, err
		}
		previous.position = record.Position
		encoded, _ := json.Marshal(record)
		h := sha256.New()
		h.Write([]byte(chain))
		h.Write(encoded)
		chain = fmt.Sprintf("%x", h.Sum(nil))
		step := syncApplyStep{checkpoint: syncApplyCheckpoint{1, record.Position, chain}}
		if record.Kind == "command" {
			r, _ := NewReader(bytes.NewReader(record.Data), DefaultConfig().Limits)
			raw, args, err := r.ReadCommand()
			if err != nil || len(raw) != len(record.Data) || len(args) == 0 {
				return nil, nil, errSyncUnsupported
			}
			cmd := make([]string, len(args))
			for i, arg := range args {
				cmd[i] = string(arg)
			}
			cmd[0] = strings.ToUpper(cmd[0])
			switch cmd[0] {
			case "SET":
				if len(cmd) != 3 {
					return nil, nil, errSyncUnsupported
				}
				step.command = cmd
			case "INCR", "DEL":
				if len(cmd) != 2 {
					return nil, nil, errSyncUnsupported
				}
				step.command = cmd
			case "PING":
				if len(cmd) != 1 {
					return nil, nil, errSyncUnsupported
				}
			case "SELECT":
				if len(cmd) != 2 || cmd[1] != "0" {
					return nil, nil, errSyncUnsupported
				}
			case "REPLCONF":
				if len(cmd) != 3 || strings.ToUpper(cmd[1]) != "GETACK" || cmd[2] != "*" {
					return nil, nil, errSyncUnsupported
				}
			default:
				return nil, nil, errSyncUnsupported // includes MULTI/EXEC, expiry, scripts
			}
			if len(step.command) > 0 && strings.HasPrefix(cmd[1], syncMetadataPrefix) {
				return nil, nil, errSyncUnsupported
			}
			if err := advanceSyncState(model, step.command); err != nil {
				return nil, nil, err
			}
		}
		steps = append(steps, step)
	}
	return initial, steps, nil
}

type syncColdClient struct {
	conn            net.Conn
	reader          *Reader
	timeout         time.Duration
	taskID          string
	evictionReceipt bool // isolated confirmation path only, never a service option
}

func (c *syncColdClient) request(args ...string) ([]byte, error) {
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	if err := writeAll(c.conn, encodeCommand(args...)); err != nil {
		return nil, err
	}
	return c.reader.ReadFrame()
}

func (c *syncColdClient) expect(want string, args ...string) error {
	raw, err := c.request(args...)
	if err != nil {
		return err
	}
	if string(raw) != want {
		return fmt.Errorf("%w: unexpected %s response", errSyncTargetDrift, args[0])
	}
	return nil
}

func (c *syncColdClient) get(key string) (string, bool, error) {
	raw, err := c.request("GET", key)
	if err != nil {
		return "", false, err
	}
	if string(raw) == "$-1\r\n" {
		return "", false, nil
	}
	header, body, ok := strings.Cut(string(raw), "\r\n")
	if !ok || !strings.HasPrefix(header, "$") {
		return "", false, errSyncTargetDrift
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil || n < 0 || n > len(body) || len(body)-n != 2 || body[n:] != "\r\n" {
		return "", false, errSyncTargetDrift
	}
	return body[:n], true, nil
}

func (c *syncColdClient) checkKey(state map[string]string, key string) error {
	want, exists := state[key]
	got, found, err := c.get(key)
	if err != nil {
		return err
	}
	if got != want || found != exists {
		return errSyncTargetDrift
	}
	ttl := ":-2\r\n"
	if exists {
		ttl = ":-1\r\n"
	}
	return c.expect(ttl, "PTTL", key)
}

func (c *syncColdClient) checkState(state map[string]string, cp syncApplyCheckpoint) error {
	keys, err := c.keys()
	if err != nil {
		return err
	}
	if len(keys) != len(state)+1 || !keys[syncCheckpointKey] {
		return errSyncTargetDrift
	}
	for key := range state {
		if !keys[key] {
			return errSyncTargetDrift
		}
	}
	if err := c.expect(":-1\r\n", "PTTL", syncCheckpointKey); err != nil {
		return err
	}
	if got, found, err := c.get(syncCheckpointKey); err != nil || !found || got != cp.encode() {
		return errSyncTargetDrift
	}
	for key := range state {
		if err := c.checkKey(state, key); err != nil {
			return err
		}
	}
	return nil
}

// Kvrocks DBSIZE is cached; it must never authorize empty-target import or
// prove current state equality. Enumerate with bounded SCAN instead. Target
// exclusivity is still required: SCAN is not a concurrent snapshot/fence.
func (c *syncColdClient) keys() (map[string]bool, error) {
	count, bytes := c.metadataScanBudget()
	return c.scanKeys("", syncStateKeys+count, syncStateBytes+bytes)
}

func (c *syncColdClient) scanKeys(pattern string, maxKeys, maxKeyBytes int) (map[string]bool, error) {
	keys := make(map[string]bool)
	cursor := "0"
	seen := make(map[string]bool)
	keyBytes := 0
	for page := 0; page < 512; page++ {
		if seen[cursor] {
			return nil, errSyncTargetDrift
		}
		seen[cursor] = true
		args := []string{"SCAN", cursor, "COUNT", "64"}
		if pattern != "" {
			args = append(args, "MATCH", pattern)
		}
		raw, err := c.request(args...)
		if err != nil {
			return nil, err
		}
		r, _ := NewReader(bytes.NewReader(raw), DefaultConfig().Limits)
		bulk := func() ([]byte, error) {
			marker, err := r.take()
			if err != nil || marker != '$' {
				return nil, errSyncTargetDrift
			}
			return r.bulk(false)
		}
		b, err := r.take()
		if err != nil || b != '*' {
			return nil, errSyncTargetDrift
		}
		n, err := r.count()
		if err != nil || n != 2 {
			return nil, errSyncTargetDrift
		}
		next, err := bulk()
		if err != nil || len(next) == 0 {
			return nil, errSyncTargetDrift
		}
		b, err = r.take()
		if err != nil || b != '*' {
			return nil, errSyncTargetDrift
		}
		n, err = r.count()
		if err != nil || n < 0 || n > int64(maxKeys) {
			return nil, errSyncTargetDrift
		}
		for i := int64(0); i < n; i++ {
			key, err := bulk()
			if err != nil || key == nil {
				return nil, errSyncTargetDrift
			}
			if c.rejectForeignMetadata(string(key)) {
				return nil, errSyncTargetDrift
			}
			if pattern != "" && !strings.HasPrefix(string(key), syncMetadataPrefix) {
				return nil, errSyncTargetDrift
			}
			if !keys[string(key)] {
				keyBytes += len(key)
			}
			keys[string(key)] = true
			if len(keys) > maxKeys || keyBytes > maxKeyBytes {
				return nil, errSyncTargetDrift
			}
		}
		if len(r.raw) != len(raw) {
			return nil, errSyncTargetDrift
		}
		cursor = string(next)
		if cursor == "0" {
			return keys, nil
		}
	}
	return nil, errSyncTargetDrift
}

type syncColdTarget struct {
	address, passwordFile string
	timeout               time.Duration
	taskID                string
	evictionReceipt       bool
	testDial              func(context.Context, string) (net.Conn, error) // fault injection only; never configured by a service
}

func (target syncColdTarget) open(ctx context.Context) (*syncColdClient, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if target.taskID != "" {
		if err := ValidateIsolatedSyncTaskID(target.taskID); err != nil {
			return nil, nil, err
		}
	}
	host, port, err := net.SplitHostPort(target.address)
	ip := net.ParseIP(host)
	p, pe := strconv.Atoi(port)
	if err != nil || pe != nil || p < 1 || p > 65535 || ip == nil || !ip.IsLoopback() || target.timeout <= 0 || target.timeout > 30*time.Second {
		return nil, nil, errors.New("cold prototype requires bounded loopback target")
	}
	password, err := syncReadPassword(target.passwordFile)
	if err != nil {
		return nil, nil, err
	}
	var conn net.Conn
	if target.testDial != nil {
		conn, err = target.testDial(ctx, target.address)
	} else {
		conn, err = (&net.Dialer{Timeout: target.timeout}).DialContext(ctx, "tcp", target.address)
	}
	if err != nil {
		if classified := syncTransportFailure(ctx, errSyncColdConnect, "connect", err); classified != nil {
			return nil, nil, classified
		}
		return nil, nil, errors.New("cannot connect to cold prototype")
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	closeClient := func() { stop(); conn.Close() }
	r, _ := NewReader(conn, DefaultConfig().Limits)
	c := &syncColdClient{conn: conn, reader: r, timeout: target.timeout, taskID: target.taskID, evictionReceipt: target.evictionReceipt}
	fail := func(operation string, cause error) (*syncColdClient, func(), error) {
		closeClient()
		if classified := syncTransportFailure(ctx, errSyncColdConnect, operation, cause); classified != nil {
			return nil, nil, classified
		}
		return nil, nil, errors.New("cold prototype requires writable standalone Kvrocks 2.15.0 with synchronous WAL")
	}
	if password != "" {
		if err := c.expect("+OK\r\n", "AUTH", password); err != nil {
			return fail("authentication", err)
		}
	}
	info, err := c.request("INFO", "all")
	if err != nil {
		return fail("server information", err)
	}
	if !standaloneMaster(info) || !strings.Contains(string(info), "\r\nkvrocks_version:2.15.0\r\n") {
		return fail("server information", nil)
	}
	for _, setting := range []struct{ key, value string }{{"rocksdb.write_options.sync", "yes"}, {"rocksdb.write_options.disable_wal", "no"}} {
		want := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(setting.key), setting.key, len(setting.value), setting.value)
		if err := c.expect(want, "CONFIG", "GET", setting.key); err != nil {
			return fail("durability configuration", err)
		}
	}
	if c.taskID != "" {
		if err := c.checkMetadataOwnership(); err != nil {
			closeClient()
			return nil, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		closeClient()
		return nil, nil, err
	}
	return c, closeClient, nil
}

// apply performs one bounded pass. No automatic network retry. Each invocation
// reconstructs and validates the full committed state before doing any writes.
// Sole ownership of the target DB0 is a required external test precondition;
// WATCH is a conflict detector here, not a distributed ownership/fencing lease.
func (target syncColdTarget) apply(ctx context.Context, journal *syncJournal) (syncPosition, error) {
	var confirmed syncPosition
	if target.taskID != "" {
		return confirmed, errors.New("legacy v1 apply does not support named sync tasks")
	}
	records, err := journal.applicationPrefix()
	if err != nil {
		return confirmed, err
	}
	state, steps, err := planSyncPrefix(records) // reject whole unsupported prefix before contacting cold
	if err != nil {
		return confirmed, err
	}
	c, closeClient, err := target.open(ctx)
	if err != nil {
		return confirmed, err
	}
	defer closeClient()
	saved, found, err := c.get(syncCheckpointKey)
	if err != nil {
		return confirmed, err
	}
	index := 0
	if !found {
		existingKeys, err := c.keys()
		if err != nil {
			return confirmed, err
		}
		if len(existingKeys) != 0 {
			return confirmed, errSyncTargetDrift
		}
		// Tiny initial snapshot and checkpoint use one native MSET, not a
		// sequence of overwrites. Never clear or replace an existing DB.
		args := []string{"MSET", syncCheckpointKey, steps[0].checkpoint.encode()}
		keys := make([]string, 0, len(state))
		for k := range state {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			args = append(args, k, state[k])
		}
		if err := c.expect("+OK\r\n", args...); err != nil {
			return confirmed, errSyncApplyUncertain
		}
	} else {
		index = -1
		for i, step := range steps {
			if step.checkpoint.encode() == saved {
				index = i
				break
			}
		}
		if index < 0 {
			return confirmed, errSyncHistoryGap
		}
		for _, step := range steps[1 : index+1] {
			if err := advanceSyncState(state, step.command); err != nil {
				return confirmed, err
			}
		}
	}
	if err := c.checkState(state, steps[index].checkpoint); err != nil {
		return confirmed, err
	}
	confirmed = steps[index].checkpoint.Position
	for i := index + 1; i < len(steps); i++ {
		step := steps[i]
		watch := []string{"WATCH", syncCheckpointKey}
		if len(step.command) > 0 {
			watch = append(watch, step.command[1])
		}
		if err := c.expect("+OK\r\n", watch...); err != nil {
			return confirmed, err
		}
		got, exists, err := c.get(syncCheckpointKey)
		if err != nil || !exists || got != steps[i-1].checkpoint.encode() {
			return confirmed, errSyncTargetDrift
		}
		reply := ""
		if len(step.command) > 0 {
			key := step.command[1]
			if err := c.checkKey(state, key); err != nil {
				return confirmed, err
			}
			_, existed := state[key]
			if err := advanceSyncState(state, step.command); err != nil {
				return confirmed, err
			}
			switch step.command[0] {
			case "SET":
				reply = "+OK\r\n"
			case "DEL":
				if existed {
					reply = ":1\r\n"
				} else {
					reply = ":0\r\n"
				}
			case "INCR":
				reply = ":" + state[key] + "\r\n"
			}
		}
		if err := c.expect("+OK\r\n", "MULTI"); err != nil {
			return confirmed, err
		}
		if len(step.command) > 0 {
			if err := c.expect("+QUEUED\r\n", step.command...); err != nil {
				return confirmed, err
			}
		}
		if err := c.expect("+QUEUED\r\n", "SET", syncCheckpointKey, step.checkpoint.encode()); err != nil {
			return confirmed, err
		}
		want := "*1\r\n+OK\r\n"
		if reply != "" {
			want = "*2\r\n" + reply + "+OK\r\n"
		}
		if err := c.expect(want, "EXEC"); err != nil {
			return confirmed, errSyncApplyUncertain
		}
		if len(step.command) > 0 {
			if err := c.checkKey(state, step.command[1]); err != nil {
				return confirmed, err
			}
		}
		got, exists, err = c.get(syncCheckpointKey)
		if err != nil || !exists || got != step.checkpoint.encode() {
			return confirmed, errSyncApplyUncertain
		}
		confirmed = step.checkpoint.Position
	}
	return confirmed, nil
}
