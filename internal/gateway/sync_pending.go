package gateway

// G6a-5: one durable pending record, no full cold-state reconstruction on
// resume. Still isolated/experimental: no Service wiring, multi-owner fencing,
// TTL, segmented journal/seek index, or large snapshot publication.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const syncPendingCPKey = syncMetadataPrefix + "{isolated}:v2-checkpoint"
const syncPendingKey = syncMetadataPrefix + "{isolated}:v2-pending"
const syncAnchorKey = syncMetadataPrefix + "{isolated}:v2-anchor"
const syncPendingValueLimit = 64 << 10
const syncPendingEncodedLimit = 512 << 10

var errSyncPendingUncertain = errors.New("pending sync outcome uncertain; reopen and reconcile pending record")

// Byte slices are essential: JSON strings would replace invalid UTF-8.
type syncPendingValue struct {
	Exists bool   `json:"exists"`
	Data   []byte `json:"data"`
}

type syncPending struct {
	Version  int                 `json:"version"`
	Previous syncApplyCheckpoint `json:"previous"`
	Next     syncApplyCheckpoint `json:"next"`
	Record   syncRecord          `json:"record"`
	Before   syncPendingValue    `json:"before"`
	After    syncPendingValue    `json:"after"`
}

func (p syncPending) encode() string { b, _ := json.Marshal(p); return string(b) }

func syncPendingCommand(raw []byte) ([]string, error) {
	r, _ := NewReader(bytes.NewReader(raw), DefaultConfig().Limits)
	frame, args, err := r.ReadCommand()
	if err != nil || len(frame) != len(raw) || len(args) == 0 {
		return nil, errSyncUnsupported
	}
	cmd := make([]string, len(args))
	for i, arg := range args {
		cmd[i] = string(arg)
	}
	cmd[0] = strings.ToUpper(cmd[0])
	switch cmd[0] {
	case "SET":
		if len(cmd) != 3 {
			return nil, errSyncUnsupported
		}
	case "DEL", "INCR":
		if len(cmd) != 2 {
			return nil, errSyncUnsupported
		}
	case "SELECT":
		if len(cmd) == 2 && cmd[1] == "0" {
			return nil, nil
		}
		return nil, errSyncUnsupported
	case "PING":
		if len(cmd) == 1 {
			return nil, nil
		}
		return nil, errSyncUnsupported
	case "REPLCONF":
		if len(cmd) == 3 && strings.ToUpper(cmd[1]) == "GETACK" && cmd[2] == "*" {
			return nil, nil
		}
		return nil, errSyncUnsupported
	default:
		return nil, errSyncUnsupported
	}
	if strings.HasPrefix(cmd[1], syncMetadataPrefix) || len(cmd[1]) > syncPendingValueLimit {
		return nil, errSyncUnsupported
	}
	if len(cmd) == 3 && len(cmd[2]) > syncPendingValueLimit {
		return nil, errSyncUnsupported
	}
	return cmd, nil
}

func syncPendingAfter(cmd []string, before syncPendingValue) (syncPendingValue, error) {
	if len(before.Data) > syncPendingValueLimit || (!before.Exists && len(before.Data) != 0) {
		return syncPendingValue{}, errSyncUnsupported
	}
	if len(cmd) == 0 {
		if before.Exists || len(before.Data) != 0 {
			return syncPendingValue{}, errSyncTargetDrift
		}
		return syncPendingValue{}, nil
	}
	switch cmd[0] {
	case "SET":
		return syncPendingValue{true, []byte(cmd[2])}, nil
	case "DEL":
		return syncPendingValue{}, nil
	case "INCR":
		var n int64
		if before.Exists {
			var err error
			n, err = strconv.ParseInt(string(before.Data), 10, 64)
			if err != nil || strconv.FormatInt(n, 10) != string(before.Data) {
				return syncPendingValue{}, errSyncTargetDrift
			}
		}
		if n == math.MaxInt64 {
			return syncPendingValue{}, errSyncTargetDrift
		}
		return syncPendingValue{true, []byte(strconv.FormatInt(n+1, 10))}, nil
	}
	return syncPendingValue{}, errSyncUnsupported
}

func nextPendingCheckpoint(prev syncApplyCheckpoint, record syncRecord) syncApplyCheckpoint {
	b, _ := json.Marshal(record)
	h := sha256.New()
	h.Write([]byte(prev.Chain))
	h.Write(b)
	return syncApplyCheckpoint{2, record.Position, fmt.Sprintf("%x", h.Sum(nil))}
}

func validatePending(p syncPending) error {
	if (p.Version != 1 && p.Version != 2) || p.Previous.Version != 2 || p.Next.Version != 2 || p.Record.Kind != "command" || len(p.encode()) > syncPendingEncodedLimit {
		return errSyncTargetDrift
	}
	check := syncJournal{position: p.Previous.Position}
	if check.check(p.Record) != nil || nextPendingCheckpoint(p.Previous, p.Record) != p.Next {
		return errSyncTargetDrift
	}
	cmd, err := pendingRecordCommand(p)
	if err != nil {
		return err
	}
	after, err := syncPendingAfter(cmd, p.Before)
	if err != nil {
		return err
	}
	if after.Exists != p.After.Exists || !bytes.Equal(after.Data, p.After.Data) {
		return errSyncTargetDrift
	}
	return nil
}

// Version 2 is only a completed, no-business-effect preparation marker.
// Normal source parsing still rejects it; only the finish transaction creates it.
func pendingRecordCommand(p syncPending) ([]string, error) {
	if p.Version == 1 {
		return syncPendingCommand(p.Record.Data)
	}
	if p.Version != 2 || p.Before.Exists || len(p.Before.Data) != 0 || p.After.Exists || len(p.After.Data) != 0 {
		return nil, errSyncUnsupported
	}
	r, _ := NewReader(bytes.NewReader(p.Record.Data), DefaultConfig().Limits)
	frame, args, err := r.ReadCommand()
	if err != nil || len(frame) != len(p.Record.Data) || len(args) != 3 {
		return nil, errSyncUnsupported
	}
	var intent evictionIntent
	payload, ok := strings.CutPrefix(string(args[2]), "prepare:")
	if !ok || decodeConfirmation(strings.NewReader(payload), &intent) != nil || !intent.valid() || intent.History != p.Record.Position.History || !bytes.Equal(p.Record.Data, intent.prepareFrame()) {
		return nil, errSyncUnsupported
	}
	return nil, nil
}

// An immutable-length view of durable capture. A duplicated fd keeps it alive
// if the journal is closed, while the original append offset is unaffected.
// Only one record is decoded at a time; verification is still O(log length).
type syncPendingView struct {
	f    *os.File
	size int64
	end  syncPosition
}

func (j *syncJournal) pendingView() (*syncPendingView, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.poisoned || j.position.History == "" {
		return nil, errSyncJournalCorrupt
	}
	fd, err := unix.FcntlInt(j.f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return &syncPendingView{os.NewFile(uintptr(fd), j.f.Name()), j.size, j.position}, nil
}

func (v *syncPendingView) walk(fn func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint, int64) error) error {
	r := io.NewSectionReader(v.f, int64(len(syncJournalMagic)), v.size-int64(len(syncJournalMagic)))
	check := syncJournal{}
	previous := syncApplyCheckpoint{}
	for {
		record, err := readSyncRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := check.check(record); err != nil {
			return err
		}
		check.position = record.Position
		next := nextPendingCheckpoint(previous, record)
		at, _ := r.Seek(0, io.SeekCurrent)
		if err := fn(record, previous, next, at+int64(len(syncJournalMagic))); err != nil {
			return err
		}
		previous = next
	}
	if check.position != v.end {
		return errSyncJournalCorrupt
	}
	return nil
}

type syncPendingMeta struct {
	checkpoint syncApplyCheckpoint
	anchor     string
	pending    *syncPending
}

func (c *syncColdClient) pendingMeta() (syncPendingMeta, error) {
	var m syncPendingMeta
	values := make([]string, 3)
	found := make([]bool, 3)
	for i, key := range []string{c.pendingCheckpointKey(), c.anchorKey(), c.pendingKey()} {
		var err error
		values[i], found[i], err = c.get(key)
		if err != nil {
			return m, err
		}
		ttl := ":-2\r\n"
		if found[i] {
			ttl = ":-1\r\n"
		}
		if err := c.expect(ttl, "PTTL", key); err != nil {
			return m, err
		}
	}
	if !found[0] {
		if found[1] || found[2] {
			return m, errSyncTargetDrift
		}
		return m, nil
	}
	if !found[1] || json.Unmarshal([]byte(values[0]), &m.checkpoint) != nil || m.checkpoint.Version != 2 || m.checkpoint.encode() != values[0] {
		return m, errSyncTargetDrift
	}
	m.anchor = values[1]
	if found[2] {
		p := &syncPending{}
		if len(values[2]) > syncPendingEncodedLimit || json.Unmarshal([]byte(values[2]), p) != nil || p.encode() != values[2] || validatePending(*p) != nil {
			return m, errSyncTargetDrift
		}
		if p.Version == 2 && (!c.evictionReceipt || c.taskID == "") {
			return m, errSyncTargetDrift
		}
		m.pending = p
	}
	return m, nil
}

func samePendingMeta(a, b syncPendingMeta) bool {
	if a.checkpoint != b.checkpoint || a.anchor != b.anchor || (a.pending == nil) != (b.pending == nil) {
		return false
	}
	return a.pending == nil || a.pending.encode() == b.pending.encode()
}

func (c *syncColdClient) pendingValue(cmd []string) (syncPendingValue, error) {
	if len(cmd) == 0 {
		return syncPendingValue{}, nil
	}
	value, exists, err := c.get(cmd[1])
	if err != nil {
		return syncPendingValue{}, err
	}
	if len(value) > syncPendingValueLimit {
		return syncPendingValue{}, errSyncUnsupported
	}
	ttl := ":-2\r\n"
	if exists {
		ttl = ":-1\r\n"
	}
	if err := c.expect(ttl, "PTTL", cmd[1]); err != nil {
		return syncPendingValue{}, err
	}
	if !exists {
		return syncPendingValue{}, nil
	}
	return syncPendingValue{true, []byte(value)}, nil
}

func equalPendingValue(a, b syncPendingValue) bool {
	return a.Exists == b.Exists && bytes.Equal(a.Data, b.Data)
}

func (c *syncColdClient) watchPending(cmd []string) error {
	args := []string{"WATCH", c.pendingCheckpointKey(), c.pendingKey(), c.anchorKey()}
	if len(cmd) > 0 {
		args = append(args, cmd[1])
	}
	return c.expect("+OK\r\n", args...)
}

func (c *syncColdClient) pendingTxn(commands [][]string, reply string) error {
	if err := c.expect("+OK\r\n", "MULTI"); err != nil {
		return err
	}
	for _, cmd := range commands {
		if err := c.expect("+QUEUED\r\n", cmd...); err != nil {
			return err
		}
	}
	if err := c.expect(reply, "EXEC"); err != nil {
		return errSyncPendingUncertain
	}
	return nil
}

// Never trust only the checkpoint: an unresolved step must have the matching
// before/after state. Control records carry the same intent protocol.
func (c *syncColdClient) reconcilePending(meta syncPendingMeta) (syncPendingMeta, error) {
	if meta.pending == nil {
		return meta, nil
	}
	p := *meta.pending
	cmd, err := pendingRecordCommand(p)
	if err != nil {
		return meta, err
	}
	if err := c.watchPending(cmd); err != nil {
		return meta, err
	}
	actual, err := c.pendingMeta()
	if err != nil {
		return meta, err
	}
	if !samePendingMeta(actual, meta) {
		return meta, errSyncTargetDrift
	}
	value, err := c.pendingValue(cmd)
	if err != nil {
		return meta, err
	}
	if meta.checkpoint == p.Next {
		if !equalPendingValue(value, p.After) {
			return meta, errSyncTargetDrift
		}
		if err := c.expect("+OK\r\n", "UNWATCH"); err != nil {
			return meta, err
		}
		return meta, nil
	}
	if meta.checkpoint != p.Previous || !equalPendingValue(value, p.Before) {
		return meta, errSyncTargetDrift
	}
	commands := [][]string{}
	reply := "*1\r\n+OK\r\n"
	if len(cmd) > 0 {
		commands = append(commands, cmd)
		result := "+OK\r\n"
		switch cmd[0] {
		case "INCR":
			result = ":" + string(p.After.Data) + "\r\n"
		case "DEL":
			result = ":0\r\n"
			if p.Before.Exists {
				result = ":1\r\n"
			}
		}
		reply = "*2\r\n" + result + "+OK\r\n"
	}
	commands = append(commands, []string{"SET", c.pendingCheckpointKey(), p.Next.encode()})
	if err := c.pendingTxn(commands, reply); err != nil {
		return meta, err
	}
	meta.checkpoint = p.Next
	actual, err = c.pendingMeta()
	if err != nil || !samePendingMeta(actual, meta) {
		return meta, errSyncPendingUncertain
	}
	value, err = c.pendingValue(cmd)
	if err != nil {
		return meta, err
	}
	if !equalPendingValue(value, p.After) {
		return meta, errSyncTargetDrift
	}
	return meta, nil
}

func (c *syncColdClient) preparePending(meta syncPendingMeta, r syncRecord, next syncApplyCheckpoint) (syncPendingMeta, error) {
	cmd, err := syncPendingCommand(r.Data)
	if err != nil {
		return meta, err
	}
	if err := c.watchPending(cmd); err != nil {
		return meta, err
	}
	actual, err := c.pendingMeta()
	if err != nil {
		return meta, err
	}
	if !samePendingMeta(actual, meta) {
		return meta, errSyncTargetDrift
	}
	before, err := c.pendingValue(cmd)
	if err != nil {
		return meta, err
	}
	after, err := syncPendingAfter(cmd, before)
	if err != nil {
		return meta, err
	}
	p := syncPending{1, meta.checkpoint, next, r, before, after}
	if err := validatePending(p); err != nil {
		return meta, err
	}
	encoded := p.encode()
	if len(encodeCommand("SET", c.pendingKey(), encoded)) > DefaultConfig().Limits.MaxBytes {
		return meta, errSyncUnsupported
	}
	if err := c.pendingTxn([][]string{{"SET", c.pendingKey(), encoded}}, "*1\r\n+OK\r\n"); err != nil {
		return meta, err
	}
	meta.pending = &p
	return meta, nil
}

// applyPending avoids whole-cold-state verification. It relies on an exclusive
// target: unrelated external drift is NOT detected as it was in G6a-4.
func (target syncColdTarget) applyPending(ctx context.Context, journal *syncJournal) (syncPosition, error) {
	var confirmed syncPosition
	if target.taskID != "" {
		return confirmed, errors.New("unbound single-file pending apply does not support named sync tasks")
	}
	v, err := journal.pendingView()
	if err != nil {
		return confirmed, err
	}
	defer v.f.Close()
	var initial map[string]string
	var base syncApplyCheckpoint
	err = v.walk(func(r syncRecord, prev, next syncApplyCheckpoint, at int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.Kind == "snapshot" {
			var err error
			initial, err = decodeSyncRDB(r.Data)
			base = next
			if err != nil {
				return err
			}
			for k, value := range initial {
				if len(k) > syncPendingValueLimit || len(value) > syncPendingValueLimit {
					return errSyncUnsupported
				}
			}
			return nil
		}
		_, err := syncPendingCommand(r.Data)
		return err
	})
	if err != nil {
		return confirmed, err
	}
	c, closeClient, err := target.open(ctx)
	if err != nil {
		return confirmed, err
	}
	defer closeClient()
	if err := c.watchPending(nil); err != nil {
		return confirmed, err
	}
	meta, err := c.pendingMeta()
	if err != nil {
		return confirmed, err
	}
	if meta.checkpoint.Version == 0 {
		keys, err := c.keys()
		if err != nil {
			return confirmed, err
		}
		if len(keys) != 0 {
			return confirmed, errSyncTargetDrift
		}
		args := []string{"MSET", c.pendingCheckpointKey(), base.encode(), c.anchorKey(), base.encode()}
		for key, value := range initial {
			args = append(args, key, value)
		}
		if err := c.pendingTxn([][]string{args}, "*1\r\n+OK\r\n"); err != nil {
			return confirmed, err
		}
		meta, err = c.pendingMeta()
		if err != nil {
			return confirmed, err
		}
	} else {
		if err := c.expect("+OK\r\n", "UNWATCH"); err != nil {
			return confirmed, err
		}
	}
	initial = nil // do not retain or reconstruct the complete cold state
	if meta.anchor != base.encode() {
		return confirmed, errSyncHistoryGap
	}
	if meta.pending == nil && meta.checkpoint != base {
		return confirmed, errSyncTargetDrift
	}
	checkpointFound, pendingFound := false, meta.pending == nil
	var afterCheckpoint int64
	err = v.walk(func(r syncRecord, prev, next syncApplyCheckpoint, at int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if next == meta.checkpoint {
			checkpointFound = true
			afterCheckpoint = at
		}
		if meta.pending != nil && next == meta.pending.Next && prev == meta.pending.Previous && bytes.Equal(r.Data, meta.pending.Record.Data) && r.Position == meta.pending.Record.Position {
			pendingFound = true
		}
		return nil
	})
	if err != nil {
		return confirmed, err
	}
	if !checkpointFound || !pendingFound {
		return confirmed, errSyncHistoryGap
	}
	oldCheckpoint := meta.checkpoint
	meta, err = c.reconcilePending(meta)
	if err != nil {
		return confirmed, err
	}
	confirmed = meta.checkpoint.Position
	// Stream from the old checkpoint boundary. A recovered pending item may
	// already be committed; it is validated and skipped by its exact token.
	r := io.NewSectionReader(v.f, afterCheckpoint, v.size-afterCheckpoint)
	previous := oldCheckpoint
	for {
		if err := ctx.Err(); err != nil {
			return confirmed, err
		}
		record, err := readSyncRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return confirmed, err
		}
		check := syncJournal{position: previous.Position}
		if err := check.check(record); err != nil {
			return confirmed, err
		}
		next := nextPendingCheckpoint(previous, record)
		previous = next
		if next == meta.checkpoint {
			continue
		}
		if meta.pending != nil && meta.checkpoint != meta.pending.Next {
			return confirmed, errSyncTargetDrift
		}
		meta, err = c.preparePending(meta, record, next)
		if err != nil {
			return confirmed, err
		}
		meta, err = c.reconcilePending(meta)
		if err != nil {
			return confirmed, err
		}
		confirmed = meta.checkpoint.Position
	}
	if confirmed != v.end {
		return confirmed, errSyncHistoryGap
	}
	return confirmed, nil
}
