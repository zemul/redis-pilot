package gateway

// G6a-6 remains an isolated prototype. The cold pending format is unchanged;
// only its local recovery evidence is now indexed, segmented and reclaimable.

import (
	"bytes"
	"context"
	"errors"
)

var errSyncProofComplete = errors.New("pending record proof complete")

func (v *syncSegmentView) checkMeta(meta syncPendingMeta) error {
	if meta.anchor != v.base.encode() || meta.checkpoint.Version != 2 {
		return errSyncHistoryGap
	}
	p := meta.checkpoint.Position
	if p.Source != v.base.Position.Source || p.History != v.base.Position.History || p.Offset < v.floor.Position.Offset {
		return errSyncHistoryGap
	}
	// A durably observed GC watermark is stronger than the retained floor:
	// even a checkpoint still inside a retained segment must not roll behind it.
	if v.confirmed.Version != 0 && (p.Offset < v.confirmed.Position.Offset || (p.Offset == v.confirmed.Position.Offset && meta.checkpoint != v.confirmed)) {
		return errSyncHistoryGap
	}
	if meta.pending == nil {
		if meta.checkpoint != v.base {
			return errSyncTargetDrift
		}
		return nil
	}
	if validatePending(*meta.pending) != nil || (meta.checkpoint != meta.pending.Previous && meta.checkpoint != meta.pending.Next) {
		return errSyncTargetDrift
	}
	return nil
}

// Prove the exact raw pending record from the indexed segment, not from an
// offset-only lookup or the cold record's self-reported checksum.
func (v *syncSegmentView) provePending(meta syncPendingMeta) error {
	if err := v.checkMeta(meta); err != nil {
		return err
	}
	if meta.pending == nil {
		return nil
	}
	p := meta.pending
	err := v.walkFrom(p.Previous, func(r syncRecord, previous, next syncApplyCheckpoint) error {
		if previous != p.Previous || next != p.Next || r.Kind != p.Record.Kind || r.Position != p.Record.Position || !bytes.Equal(r.Data, p.Record.Data) {
			return errSyncHistoryGap
		}
		return errSyncProofComplete
	})
	if errors.Is(err, errSyncProofComplete) {
		return nil
	}
	if err == nil {
		return errSyncHistoryGap
	}
	return err
}

func (v *syncSegmentView) preflight(ctx context.Context, checkpoint syncApplyCheckpoint) error {
	return v.walkFrom(checkpoint, func(r syncRecord, _, _ syncApplyCheckpoint) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := syncPendingCommand(r.Data)
		return err
	})
}

func (target syncColdTarget) applySegments(ctx context.Context, journal *syncSegments) (syncPosition, error) {
	if err := target.checkJournalIdentity(journal); err != nil {
		return syncPosition{}, err
	}
	journal.applyMu.Lock()
	defer journal.applyMu.Unlock()
	var confirmed syncPosition
	v, err := journal.pendingView()
	if err != nil {
		return confirmed, err
	}
	defer v.close()
	return target.applySegmentView(ctx, v, v.end)
}

// Caller holds journal.applyMu and owns the view. The confirmation path stops
// BEFORE its preparation marker; the ordinary applier still consumes v.end.
func (target syncColdTarget) applySegmentView(ctx context.Context, v *syncSegmentView, end syncApplyCheckpoint) (syncPosition, error) {
	var confirmed syncPosition
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
		// GC cannot turn an old full snapshot into a new bootstrap. Even when
		// no segment was deleted, a recorded confirmation prohibits reinit.
		if v.floor != v.base || v.confirmed.Version != 0 {
			return confirmed, errSyncHistoryGap
		}
		if err := v.preflightThrough(ctx, v.base, end); err != nil {
			return confirmed, err
		}
		initial, err := decodeSyncRDB(v.anchor.Data)
		if err != nil {
			return confirmed, err
		}
		args := []string{"MSET", c.pendingCheckpointKey(), v.base.encode(), c.anchorKey(), v.base.encode()}
		for key, value := range initial {
			if len(key) > syncPendingValueLimit || len(value) > syncPendingValueLimit {
				return confirmed, errSyncUnsupported
			}
			args = append(args, key, value)
		}
		keys, err := c.keys()
		if err != nil {
			return confirmed, err
		}
		if len(keys) != 0 {
			return confirmed, errSyncTargetDrift
		}
		if err := c.pendingTxn([][]string{args}, "*1\r\n+OK\r\n"); err != nil {
			return confirmed, err
		}
		meta, err = c.pendingMeta()
		if err != nil {
			return confirmed, err
		}
	} else if err := c.expect("+OK\r\n", "UNWATCH"); err != nil {
		return confirmed, err
	}
	if err := v.provePending(meta); err != nil {
		return confirmed, err
	}
	oldCheckpoint := meta.checkpoint
	if err := v.preflightThrough(ctx, oldCheckpoint, end); err != nil {
		return confirmed, err
	}
	meta, err = c.reconcilePending(meta)
	if err != nil {
		return confirmed, err
	}
	confirmed = meta.checkpoint.Position
	err = v.walkThrough(oldCheckpoint, end, func(r syncRecord, previous, next syncApplyCheckpoint) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if next == meta.checkpoint { // just reconciled the durable pending item
			return nil
		}
		if previous != meta.checkpoint || (meta.pending != nil && meta.checkpoint != meta.pending.Next) {
			return errSyncTargetDrift
		}
		var err error
		meta, err = c.preparePending(meta, r, next)
		if err != nil {
			return err
		}
		meta, err = c.reconcilePending(meta)
		if err != nil {
			return err
		}
		confirmed = meta.checkpoint.Position
		return nil
	})
	if err != nil {
		return confirmed, err
	}
	if meta.checkpoint != end {
		return confirmed, errSyncHistoryGap
	}
	return confirmed, nil
}

// Explicit GC, not an automatic background policy. Capture may append while
// this runs; cold apply/GC on this journal are serialized. The experimental
// cold DB must still be exclusively owned: WATCH is not a distributed lease.
func (target syncColdTarget) pruneSegments(ctx context.Context, journal *syncSegments) error {
	if err := target.checkJournalIdentity(journal); err != nil {
		return err
	}
	journal.applyMu.Lock()
	defer journal.applyMu.Unlock()
	v, err := journal.pendingView()
	if err != nil {
		return err
	}
	defer v.close()
	c, closeClient, err := target.open(ctx)
	if err != nil {
		return err
	}
	defer closeClient()
	meta, err := c.pendingMeta()
	if err != nil {
		return err
	}
	if err := v.provePending(meta); err != nil {
		return err
	}
	if meta.pending == nil || meta.checkpoint != meta.pending.Next {
		return errSyncPendingUncertain
	}
	cmd, err := pendingRecordCommand(*meta.pending)
	if err != nil {
		return err
	}
	if err := c.watchPending(cmd); err != nil {
		return err
	}
	actual, err := c.pendingMeta()
	if err != nil {
		return err
	}
	if !samePendingMeta(meta, actual) {
		return errSyncTargetDrift
	}
	value, err := c.pendingValue(cmd)
	if err != nil {
		return err
	}
	if !equalPendingValue(value, meta.pending.After) {
		return errSyncTargetDrift
	}
	// A read-only transaction closes the WATCH read window. Changes while
	// collecting the proof abort it; a lost reply never permits local GC.
	if err := c.pendingTxn([][]string{{"PING"}}, "*1\r\n+PONG\r\n"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return journal.prune(v, meta)
}
