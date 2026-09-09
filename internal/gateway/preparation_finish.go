package gateway

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// Completion means keep the business value HOT. It is never eviction authority.
type operationFinish struct {
	Version int    `json:"version"`
	Intent  string `json:"intent_sha256"`
	Receipt string `json:"receipt_sha256"`
}

func finishFor(s evictionOperationState) operationFinish {
	return operationFinish{1, evictionHash(s.Intent), evictionHash(s.Receipt)}
}

func (g *evictionOperation) recordFinished() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.poisoned || g.state.Receipt == nil {
		return errEvictionOperation
	}
	if g.state.Finished {
		return nil
	}
	frame, err := syncSegmentFrame(finishFor(g.state))
	if err != nil {
		return err
	}
	st, err := g.f.Stat()
	if err != nil || st.Size()+int64(len(frame)) > evictionOperationFileLimit {
		return errEvictionOperation
	}
	if err := g.persist(frame, "finish"); err != nil {
		return err
	}
	g.state.Finished = true
	return nil
}

// Binding lock must be held by caller. Replace only a proven terminal local
// operation; the old record remains intact until the new file is fsynced.
func replaceFinishedOperation(path string, identity syncTaskIdentity, intent evictionIntent) (*evictionOperation, error) {
	old, err := openEvictionOperation(path, identity, nil, nil)
	if err != nil {
		return nil, err
	}
	state, err := old.snapshot()
	defer old.close()
	if err != nil || !state.Finished || state.Intent.OperationID == intent.OperationID {
		return nil, errEvictionOperation
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".operation-next-*")
	if err != nil {
		return nil, err
	}
	name := temp.Name()
	temp.Close()
	// CreateTemp reserves the exact private path; openEvictionOperation needs
	// exclusive creation. No unresolved path or broad deletion is involved.
	if err := os.Remove(name); err != nil {
		return nil, err
	}
	defer os.Remove(name)
	next, err := openEvictionOperation(name, identity, &intent, nil)
	if err != nil {
		return nil, err
	}
	if err := os.Rename(name, path); err != nil {
		next.close()
		return nil, err
	}
	if err := next.dir.Sync(); err != nil {
		next.close()
		return nil, err
	}
	return next, nil
}

func finishedColdReceipt(ack evictionColdReceipt) string { return "hot-retained:" + ack.encode() }

func parseFinishedColdReceipt(raw string) (evictionColdReceipt, bool) {
	var ack evictionColdReceipt
	value, ok := strings.CutPrefix(raw, "hot-retained:")
	if !ok || json.Unmarshal([]byte(value), &ack) != nil || ack.encode() != value || ack.Version != 1 ||
		!validSyncCheckpoint(ack.Previous) || !validSyncCheckpoint(ack.Prepared) || !sameSyncLineage(ack.Previous, ack.Prepared) || ack.Prepared.Position.Offset <= ack.Previous.Position.Offset {
		return ack, false
	}
	return ack, true
}

// Advances exactly one already confirmed preparation record as a control-only
// pending item. One MSET atomically commits checkpoint, pending and completion;
// no business command is replayed. A lost response is reconciled on a new WATCH.
func (target syncColdTarget) finishPreparation(ctx context.Context, j *syncSegments, state evictionOperationState) error {
	if state.Receipt == nil || !validEvictionReceipt(state, *state.Receipt) || target.checkJournalIdentity(j) != nil {
		return errEvictionConfirmation
	}
	j.applyMu.Lock()
	defer j.applyMu.Unlock()
	v, err := j.pendingView()
	if err != nil {
		return err
	}
	defer v.close()
	target.evictionReceipt = true
	c, closeClient, err := target.open(ctx)
	if err != nil {
		return err
	}
	defer closeClient()
	if err := c.expect("+OK\r\n", "WATCH", c.pendingCheckpointKey(), c.pendingKey(), c.anchorKey(), c.evictionReceiptKey()); err != nil {
		return err
	}
	meta, err := c.pendingMeta()
	if err != nil || v.provePending(meta) != nil {
		return errEvictionConfirmation
	}
	ack := *state.Receipt
	raw, exists, err := c.get(c.evictionReceiptKey())
	if err != nil || !exists || c.expect(":-1\r\n", "PTTL", c.evictionReceiptKey()) != nil {
		return errEvictionConfirmation
	}
	if raw == finishedColdReceipt(ack) {
		if !sameSyncLineage(meta.checkpoint, ack.Prepared) || meta.checkpoint.Position.Offset < ack.Prepared.Position.Offset {
			return errEvictionConfirmation
		}
		// Invalidate an older WATCH/EXEC even when acknowledging the same result.
		return c.pendingTxn([][]string{{"SET", c.evictionReceiptKey(), raw}}, "*1\r\n+OK\r\n")
	}
	if state.Finished || raw != ack.encode() || meta.checkpoint != ack.Previous || (meta.pending != nil && meta.checkpoint != meta.pending.Next) {
		return errEvictionConfirmation
	}
	proved, err := v.proveEvictionPreparation(state)
	if err != nil || proved != ack {
		return errEvictionConfirmation
	}
	pending := syncPending{Version: 2, Previous: ack.Previous, Next: ack.Prepared, Record: state.Prepare.Record}
	if validatePending(pending) != nil {
		return errEvictionConfirmation
	}
	return c.pendingTxn([][]string{{"MSET", c.pendingCheckpointKey(), ack.Prepared.encode(), c.pendingKey(), pending.encode(), c.evictionReceiptKey(), finishedColdReceipt(ack)}}, "*1\r\n+OK\r\n")
}
