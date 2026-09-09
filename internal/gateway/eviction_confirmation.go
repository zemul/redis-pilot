package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

var errEvictionConfirmation = errors.New("cold eviction confirmation does not match operation")
var errEvictionConfirmUncertain = errors.New("cold confirmation outcome uncertain; reconnect and revalidate")

// Receipt contains its own barrier checkpoint; ordinary replay remains at
// Previous. It must not cross this marker until a residency protocol exists.
type evictionColdReceipt struct {
	Version        int                 `json:"version"`
	IdentitySHA256 string              `json:"identity_sha256"`
	IntentSHA256   string              `json:"intent_sha256"`
	Previous       syncApplyCheckpoint `json:"previous"`
	Prepared       syncApplyCheckpoint `json:"prepared"`
}

func evictionHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (r evictionColdReceipt) encode() string { b, _ := json.Marshal(r); return string(b) }

func validEvictionReceipt(s evictionOperationState, r evictionColdReceipt) bool {
	return s.Prepare != nil && r.Version == 1 && r.IdentitySHA256 == evictionHash(s.Identity) &&
		r.IntentSHA256 == evictionHash(s.Intent) && validSyncCheckpoint(r.Previous) &&
		r.Previous.Position == s.Prepare.Before && r.Prepared == nextPendingCheckpoint(r.Previous, s.Prepare.Record)
}

// Checked traversal with an exact stopping checkpoint. No raw offset alone
// defines the stop; the underlying segment reader verifies the record chain.
func (v *syncSegmentView) walkThrough(from, end syncApplyCheckpoint, fn func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint) error) error {
	if !validSyncCheckpoint(end) || !sameSyncLineage(from, end) || from.Position.Offset > end.Position.Offset || end.Position.Offset > v.end.Position.Offset {
		return errSyncHistoryGap
	}
	stop := errors.New("bounded walk complete")
	reached := from == end
	err := v.walkFrom(from, func(r syncRecord, previous, next syncApplyCheckpoint) error {
		if previous == end {
			return stop
		}
		if next.Position.Offset > end.Position.Offset {
			return errSyncHistoryGap
		}
		if err := fn(r, previous, next); err != nil {
			return err
		}
		if next == end {
			reached = true
		}
		return nil
	})
	if err != nil && !errors.Is(err, stop) {
		return err
	}
	if !reached {
		return errSyncHistoryGap
	}
	return nil
}
func (v *syncSegmentView) preflightThrough(ctx context.Context, from, end syncApplyCheckpoint) error {
	complete, err := v.walkResidency(ctx, from, end, func(r syncRecord, _, _ syncApplyCheckpoint) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if residencyRecord(r.Data) {
			_, err := parseResidencyEnvelope(r.Data)
			return err
		}
		_, err := syncPendingCommand(r.Data)
		return err
	})
	if err == nil && complete != end {
		return errResidencyIncomplete
	}
	return err
}

func (v *syncSegmentView) proveEvictionPreparation(s evictionOperationState) (evictionColdReceipt, error) {
	var receipt evictionColdReceipt
	if s.Prepare == nil || !validEvictionPreparation(s, *s.Prepare) {
		return receipt, errEvictionConfirmation
	}
	stop := errors.New("preparation located")
	err := v.walkFrom(v.floor, func(r syncRecord, prev, next syncApplyCheckpoint) error {
		if next.Position.Offset < s.Prepare.Record.Position.Offset {
			return nil
		}
		if prev.Position != s.Prepare.Before || r.Position != s.Prepare.Record.Position || r.Kind != "command" || !bytes.Equal(r.Data, s.Prepare.Record.Data) {
			return errEvictionConfirmation
		}
		receipt = evictionColdReceipt{1, evictionHash(s.Identity), evictionHash(s.Intent), prev, next}
		return stop
	})
	if !errors.Is(err, stop) {
		if err == nil {
			err = errEvictionConfirmation
		}
		return evictionColdReceipt{}, err
	}
	return receipt, nil
}

// confirmEviction serializes one local operation and its source apply/GC lane.
// The harness and explicitly selected isolated confirmation endpoint call it;
// normal Service/replay never does. It returns a confirmation, NEVER authority
// to dispatch a hot deletion.
func (target syncColdTarget) confirmEviction(ctx context.Context, journal *syncSegments, op *evictionOperation) (evictionColdReceipt, error) {
	var zero evictionColdReceipt
	if op == nil || journal == nil {
		return zero, errEvictionConfirmation
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.closed {
		return zero, errEvictionOperationClosed
	}
	if op.poisoned {
		return zero, errEvictionOperationIO
	}
	s := copyEvictionOperationState(op.state)
	if target.taskID == "" || target.taskID != s.Identity.TaskID || syncCanonicalEndpoint(target.address) != s.Identity.Cold || s.Prepare == nil {
		return zero, errEvictionConfirmation
	}
	if err := target.checkJournalIdentity(journal); err != nil {
		return zero, err
	}
	journal.applyMu.Lock()
	defer journal.applyMu.Unlock()
	v, err := journal.pendingView()
	if err != nil {
		return zero, err
	}
	defer v.close()
	ack, err := v.proveEvictionPreparation(s)
	if err != nil {
		return zero, err
	}
	if s.Receipt != nil && *s.Receipt != ack {
		return zero, errEvictionConfirmation
	}
	target.evictionReceipt = true
	if _, err := target.applySegmentView(ctx, v, ack.Previous); err != nil {
		return zero, err
	}
	c, closeClient, err := target.open(ctx)
	if err != nil {
		return zero, err
	}
	defer closeClient()
	if err := c.confirmEvictionReceipt(ctx, v, s, ack); err != nil {
		return zero, err
	}
	// A lost local fsync also means no receipt is returned to the caller.
	if err := op.recordReceiptLocked(ack); err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return ack, nil
}

func (c *syncColdClient) confirmEvictionReceipt(ctx context.Context, v *syncSegmentView, s evictionOperationState, ack evictionColdReceipt) error {
	if !c.evictionReceipt || c.taskID != s.Identity.TaskID || !validEvictionReceipt(s, ack) {
		return errEvictionConfirmation
	}
	key := string(s.Intent.Key)
	if err := c.expect("+OK\r\n", "WATCH", c.pendingCheckpointKey(), c.pendingKey(), c.anchorKey(), key, c.evictionReceiptKey()); err != nil {
		return err
	}
	meta, err := c.pendingMeta()
	if err != nil {
		return err
	}
	if meta.checkpoint != ack.Previous || v.provePending(meta) != nil || (meta.pending != nil && meta.checkpoint != meta.pending.Next) {
		return errEvictionConfirmation
	}
	value, err := c.pendingValue([]string{"GET", key})
	if err != nil {
		return err
	}
	h := sha256.Sum256(value.Data)
	if !value.Exists || hex.EncodeToString(h[:]) != s.Intent.ValueSHA256 {
		return errEvictionConfirmation
	}
	old, exists, err := c.get(c.evictionReceiptKey())
	if err != nil {
		return err
	}
	if exists && old != ack.encode() {
		finished, ok := parseFinishedColdReceipt(old)
		if !ok || s.Receipt != nil || finished.IdentitySHA256 != ack.IdentitySHA256 || !sameSyncLineage(finished.Prepared, ack.Previous) || finished.Prepared.Position.Offset > ack.Previous.Position.Offset {
			return errEvictionConfirmation
		}
	}
	// A recovered local ACK requires its cold counterpart still to exist.
	if s.Receipt != nil && !exists {
		return errEvictionConfirmation
	}
	ttl := ":-2\r\n"
	if exists {
		ttl = ":-1\r\n"
	}
	if err := c.expect(ttl, "PTTL", c.evictionReceiptKey()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Only one side-effecting SET: data is already synchronously committed.
	// WATCH binds that data/checkpoint snapshot to this receipt commit. Rewriting
	// an identical receipt also invalidates any older queued WATCH transaction.
	if err := c.pendingTxn([][]string{{"SET", c.evictionReceiptKey(), ack.encode()}}, "*1\r\n+OK\r\n"); err != nil {
		return errEvictionConfirmUncertain
	}
	return nil
}
