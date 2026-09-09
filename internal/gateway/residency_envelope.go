package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
)

// Residency transactions are consumed as ONE control pending item. Never
// advance the cold checkpoint into a transaction, or replay its hot UNLINK.
const residencyPrefix = syncMetadataPrefix + "{g6b-probe}:residency:"

func residencyKey(task string, key []byte) string {
	hash := sha256.Sum256(key)
	return residencyPrefix + task + ":" + hex.EncodeToString(hash[:])
}

func (i evictionIntent) residencyKey() string { return residencyKey(i.TaskID, i.Key) }

type residencyEnvelope struct {
	intent evictionIntent
	warm   bool
	frames [][]byte
}

func parseResidencyEnvelope(raw []byte) (residencyEnvelope, error) {
	var e residencyEnvelope
	r, _ := NewReader(bytes.NewReader(raw), DefaultConfig().Limits)
	var args [][][]byte
	for {
		frame, argv, err := r.ReadCommand()
		if err == io.EOF {
			break
		}
		if err != nil || len(args) >= 5 {
			return e, errSyncUnsupported
		}
		e.frames = append(e.frames, bytes.Clone(frame))
		args = append(args, argv)
	}
	if len(args) == 5 && bytes.Equal(e.frames[1], encodeCommand("SELECT", "0")) {
		args = append(args[:1], args[2:]...)
	}
	if len(args) != 4 || len(args[0]) != 1 || string(args[0][0]) != "MULTI" ||
		len(args[3]) != 1 || string(args[3][0]) != "EXEC" {
		return e, errSyncUnsupported
	}
	marker := args[1]
	if len(args[2]) == 3 && strings.HasPrefix(string(args[2][1]), residencyPrefix) {
		e.warm, marker = true, args[2]
	}
	if len(marker) != 3 || string(marker[0]) != "SET" {
		return e, errSyncUnsupported
	}
	prefix := "cold:"
	if e.warm {
		prefix = "hot:"
	}
	payload, ok := strings.CutPrefix(string(marker[2]), prefix)
	if !ok || decodeConfirmation(strings.NewReader(payload), &e.intent) != nil ||
		!e.intent.valid() || e.intent.Version != 2 || e.intent.encode() != payload ||
		string(marker[1]) != e.intent.residencyKey() {
		return e, errSyncUnsupported
	}
	var want [][]byte
	if e.warm {
		if len(args[1]) != 3 {
			return e, errSyncUnsupported
		}
		hash := sha256.Sum256(args[1][2])
		if hex.EncodeToString(hash[:]) != e.intent.ValueSHA256 || len(args[1][2]) > syncPendingValueLimit {
			return e, errSyncUnsupported
		}
		want = [][]byte{encodeCommand("MULTI"), encodeCommand("SET", string(e.intent.Key), string(args[1][2])), encodeCommand("SET", e.intent.residencyKey(), "hot:"+payload), encodeCommand("EXEC")}
	} else {
		want = [][]byte{encodeCommand("MULTI"), encodeCommand("SET", e.intent.residencyKey(), "cold:"+payload), encodeCommand("UNLINK", string(e.intent.Key)), encodeCommand("EXEC")}
	}
	if len(e.frames) == 5 {
		want = append([][]byte{want[0], encodeCommand("SELECT", "0")}, want[1:]...)
	}
	if !bytes.Equal(raw, bytes.Join(want, nil)) {
		return e, errSyncUnsupported
	}
	return e, nil
}

func residencyNext(previous syncApplyCheckpoint, record syncRecord) (syncApplyCheckpoint, error) {
	e, err := parseResidencyEnvelope(record.Data)
	if err != nil || e.intent.History != previous.Position.History {
		return previous, errSyncUnsupported
	}
	for _, frame := range e.frames {
		pos := previous.Position
		pos.Offset += int64(len(frame))
		r := syncRecord{Kind: "command", Position: pos, Data: frame}
		check := syncJournal{position: previous.Position}
		if check.check(r) != nil {
			return previous, errSyncHistoryGap
		}
		previous = nextPendingCheckpoint(previous, r)
	}
	if previous.Position != record.Position {
		return previous, errSyncHistoryGap
	}
	return previous, nil
}

// An incomplete tail is held for the next capture cycle, not treated as a
// completed operation. Memory is bounded to one small residency transaction.
func (v *syncSegmentView) walkResidency(ctx context.Context, from, end syncApplyCheckpoint, fn func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error) (syncApplyCheckpoint, error) {
	complete := from
	var group []byte
	var start syncApplyCheckpoint
	err := v.walkThrough(from, end, func(r syncRecord, prev, next syncApplyCheckpoint) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if group == nil && !bytes.Equal(r.Data, encodeCommand("MULTI")) {
			if err := fn(r, prev, next); err != nil {
				return err
			}
			complete = next
			return nil
		}
		if group == nil {
			start = prev
			group = make([]byte, 0, 1024)
		}
		if len(group)+len(r.Data) > 256<<10 {
			return errSyncUnsupported
		}
		group = append(group, r.Data...)
		if !bytes.Equal(r.Data, encodeCommand("EXEC")) {
			return nil
		}
		r.Data = group
		proved, err := residencyNext(start, r)
		if err != nil || proved != next {
			return errSyncUnsupported
		}
		if err := fn(r, start, next); err != nil {
			return err
		}
		group, complete = nil, next
		return nil
	})
	return complete, err
}

func residencyRecord(raw []byte) bool { return bytes.HasPrefix(raw, encodeCommand("MULTI")) }

func (v *syncSegmentView) proveResidencyPending(p syncPending) error {
	var raw []byte
	err := v.walkThrough(p.Previous, p.Next, func(r syncRecord, _, _ syncApplyCheckpoint) error {
		if len(raw)+len(r.Data) > len(p.Record.Data) {
			return errSyncHistoryGap
		}
		raw = append(raw, r.Data...)
		return nil
	})
	if err != nil || !bytes.Equal(raw, p.Record.Data) {
		return errSyncHistoryGap
	}
	return nil
}

var errResidencyIncomplete = errors.New("residency transaction not fully captured")

func (c *syncColdClient) prepareResidencyPending(meta syncPendingMeta, r syncRecord, next syncApplyCheckpoint) (syncPendingMeta, error) {
	e, err := parseResidencyEnvelope(r.Data)
	if err != nil || !c.evictionReceipt || c.taskID != e.intent.TaskID {
		return meta, errSyncUnsupported
	}
	if err := c.expect("+OK\r\n", "WATCH", c.pendingCheckpointKey(), c.pendingKey(), c.anchorKey(), c.evictionReceiptKey(), string(e.intent.Key)); err != nil {
		return meta, err
	}
	actual, err := c.pendingMeta()
	if err != nil || !samePendingMeta(actual, meta) {
		return meta, errSyncTargetDrift
	}
	value, err := c.pendingValue([]string{"GET", string(e.intent.Key)})
	hash := sha256.Sum256(value.Data)
	if err != nil || !value.Exists || hex.EncodeToString(hash[:]) != e.intent.ValueSHA256 {
		return meta, errSyncTargetDrift
	}
	if !e.warm {
		raw, exists, err := c.get(c.evictionReceiptKey())
		ack, ok := parseFinishedColdReceipt(raw)
		if err != nil || !exists || !ok || ack.IntentSHA256 != evictionHash(e.intent) ||
			!sameSyncLineage(meta.checkpoint, ack.Prepared) || meta.checkpoint.Position.Offset < ack.Prepared.Position.Offset {
			return meta, errEvictionConfirmation
		}
	}
	p := syncPending{Version: 3, Previous: meta.checkpoint, Next: next, Record: r}
	if err := validatePending(p); err != nil {
		return meta, err
	}
	// Commit pending and checkpoint together: the group has NO cold business
	// effect. A lost reply is recovered by the ordinary pending proof path.
	if err := c.pendingTxn([][]string{{"MSET", c.pendingKey(), p.encode(), c.pendingCheckpointKey(), next.encode()}}, "*1\r\n+OK\r\n"); err != nil {
		return meta, err
	}
	meta.pending, meta.checkpoint = &p, next
	return meta, nil
}
