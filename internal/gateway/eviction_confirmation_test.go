package gateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestEvictionConfirmationBoundedWalk(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 4<<20)
	for n := 0; n < 8; n++ {
		f.append(t, 100)
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	for stop := 0; stop < len(f.points); stop++ {
		count := 0
		if err := v.walkThrough(f.points[0], f.points[stop], func(_ syncRecord, _, _ syncApplyCheckpoint) error { count++; return nil }); err != nil || count != stop {
			t.Fatal("bounded walk", stop, count, err)
		}
	}
	bad := f.points[3]
	bad.Chain = strings.Repeat("b", 64)
	if err := v.walkThrough(f.points[0], bad, func(_ syncRecord, _, _ syncApplyCheckpoint) error { return nil }); err == nil {
		t.Fatal("unproved stop accepted")
	}
	if err := v.preflightThrough(context.Background(), f.points[0], f.points[4]); err != nil {
		t.Fatal(err)
	}
}

func TestEvictionConfirmationReceiptPersistence(t *testing.T) {
	for _, stage := range []string{"normal", "receipt_before_write", "receipt_after_write", "receipt_after_sync"} {
		t.Run(stage, func(t *testing.T) {
			path, id, i, p := evictionOperationFixture(t)
			g, err := openEvictionOperation(path, id, &i, func(at string) error {
				if at == stage {
					return errors.New("fault")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.recordPreparation(p); err != nil {
				t.Fatal(err)
			}
			s, _ := g.snapshot()
			prev := syncApplyCheckpoint{Version: 2, Position: p.Before, Chain: strings.Repeat("a", 64)}
			ack := evictionColdReceipt{1, evictionHash(id), evictionHash(i), prev, nextPendingCheckpoint(prev, p.Record)}
			if !validEvictionReceipt(s, ack) {
				t.Fatal("receipt invalid")
			}
			g.mu.Lock()
			err = g.recordReceiptLocked(ack)
			g.mu.Unlock()
			if (stage == "normal" && err != nil) || (stage != "normal" && err != errEvictionOperationIO) {
				t.Fatal(stage, err)
			}
			g.close()
			g, err = openEvictionOperation(path, id, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer g.close()
			s, err = g.snapshot()
			if err != nil || (s.Receipt != nil) != (stage != "receipt_before_write") {
				t.Fatal("receipt recovery", err)
			}
			if s.Receipt != nil {
				s.Receipt.IntentSHA256 = "mutated"
				state, _ := g.snapshot()
				if *state.Receipt != ack {
					t.Fatal("snapshot alias")
				}
			}
		})
	}
}

func TestEvictionConfirmationReceiptValidation(t *testing.T) {
	path, id, i, p := evictionOperationFixture(t)
	g, err := openEvictionOperation(path, id, &i, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	g.recordPreparation(p)
	s, _ := g.snapshot()
	prev := syncApplyCheckpoint{2, p.Before, strings.Repeat("a", 64)}
	ack := evictionColdReceipt{1, evictionHash(id), evictionHash(i), prev, nextPendingCheckpoint(prev, p.Record)}
	for _, mutate := range []func(*evictionColdReceipt){
		func(r *evictionColdReceipt) { r.Version++ }, func(r *evictionColdReceipt) { r.IntentSHA256 = strings.Repeat("b", 64) },
		func(r *evictionColdReceipt) { r.IdentitySHA256 = strings.Repeat("b", 64) }, func(r *evictionColdReceipt) { r.Prepared.Position.Offset++ },
		func(r *evictionColdReceipt) { r.Previous.Position.Offset++ },
	} {
		r := ack
		mutate(&r)
		if validEvictionReceipt(s, r) {
			t.Fatal("mismatched receipt accepted")
		}
	}
	// Existing client cannot silently consume the new receipt namespace.
	c := &syncColdClient{taskID: id.TaskID}
	if c.ownsMetadataKey(c.evictionReceiptKey()) {
		t.Fatal("default namespace expanded")
	}
	c.evictionReceipt = true
	if !c.ownsMetadataKey(c.evictionReceiptKey()) {
		t.Fatal("isolated receipt not allowed")
	}
	g.mu.Lock()
	err = g.recordReceiptLocked(ack)
	g.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	g.close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, 0)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if recovered, err := openEvictionOperation(path, id, nil, nil); err == nil {
		recovered.close()
		t.Fatal("trailing fourth record accepted")
	}
}

func TestEvictionConfirmationProofRejectsForgedPrepare(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 4<<20)
	f.append(t, 100)
	i := evictionTestIntent("key", "value")
	before := f.store.durablePosition()
	r := evictionTestRecords(before, [][]byte{i.prepareFrame()})[0]
	if err := f.store.append(r); err != nil {
		t.Fatal(err)
	}
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	s := evictionOperationState{Identity: syncTaskIdentity{i.TaskID, before.Source, "127.0.0.1:23456"}, Intent: i, Prepare: &evictionPreparation{before, r}}
	ack, err := v.proveEvictionPreparation(s)
	if err != nil || !validEvictionReceipt(s, ack) {
		t.Fatal("proof", err)
	}
	s.Prepare.Record.Data = bytes.Clone(r.Data)
	s.Prepare.Record.Data[len(r.Data)-3] ^= 1
	if _, err := v.proveEvictionPreparation(s); err == nil {
		t.Fatal("forged prepare accepted")
	}
}
