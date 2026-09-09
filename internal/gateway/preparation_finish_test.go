package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func waitPreparationFinished(t *testing.T, h *hotPreparation) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.gate <- struct{}{}
		done := h.request == nil && !h.finishing
		<-h.gate
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("automatic finish did not release operation")
}

func TestPreparationAutomaticallyFinishesWithoutRequest(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "restart"}[restart], func(t *testing.T) {
			f := newHotPreparationFixture(t)
			if !restart {
				f.h.options.PreparationTimeout = 100 * time.Millisecond
			}
			req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
			if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
				t.Fatal(w.Code)
			}
			h := f.h
			f.mu.Lock()
			f.finishFailures = 1 // deadline-triggered finishing must also retry
			f.mu.Unlock()
			if restart {
				h.Close()
				s, err := NewService(h.service.cfg)
				if err != nil {
					t.Fatal(err)
				}
				s.admission = testKeyBarrierSet(t, 8, 1024, 8)
				f.o.PreparationTimeout = 100 * time.Millisecond
				h, err = openHotPreparation(s, f.o)
				if err != nil {
					t.Fatal(err)
				}
				defer h.Close()
			}
			// No /confirm or /finish: the controller supplies both actions.
			waitPreparationFinished(t, h)
			testKeyRequest(t, h.service.admission, "key").settle(true)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.value != "value" || f.increments != 0 {
				t.Fatal("automatic finish changed business data")
			}
		})
	}
}

func TestPreparationDuplicateDoesNotExtendDeadline(t *testing.T) {
	f := newHotPreparationFixture(t)
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
		t.Fatal(w.Code)
	}
	f.h.gate <- struct{}{}
	deadline := f.h.autoFinishAt
	<-f.h.gate
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
		t.Fatal(w.Code)
	}
	f.h.gate <- struct{}{}
	unchanged := deadline == f.h.autoFinishAt
	<-f.h.gate
	if !unchanged {
		t.Fatal("duplicate preparation postponed automatic completion")
	}
}

func TestFinishReleasesHotBeforeColdRecovers(t *testing.T) {
	f := newHotPreparationFixture(t)
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
		t.Fatal(w.Code)
	}
	f.mu.Lock()
	f.finishFailures = 100
	f.mu.Unlock()
	if w := hotPreparationPost(t, f.h, hotPreparationPath+"/finish", req); w.Code != 503 {
		t.Fatal(w.Code)
	}
	testKeyRequest(t, f.h.service.admission, "key").settle(true)
	f.h.gate <- struct{}{}
	if f.h.request == nil || f.h.barrier != nil || !f.h.finishing {
		t.Error("hot release lost pending cold operation")
	}
	<-f.h.gate
	f.mu.Lock()
	f.value = "new-hot-value"
	f.finishFailures = 0
	f.mu.Unlock()
	waitPreparationFinished(t, f.h)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.value != "new-hot-value" || f.evals != 1 {
		t.Fatal("finish replayed preparation after hot traffic resumed")
	}
}

func TestHotPreparationFinishMultipleOperations(t *testing.T) {
	f := newHotPreparationFixture(t)
	var old hotPreparationRequest
	for _, id := range []string{"1", "2"} {
		req := hotPreparationRequest{strings.Repeat(id, 32), []byte("key")}
		if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if old.OperationID != "" {
			if w := hotPreparationPost(t, f.h, hotPreparationPath+"/finish", old); w.Code != 503 {
				t.Fatal("old finish affected next operation", w.Code)
			}
			_, err := f.h.service.admission.enter("key")
			requireBarrierError(t, err, errKeyBarrierBusy)
		}
		if w := hotPreparationPost(t, f.h, hotPreparationPath+"/finish", req); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		waitPreparationFinished(t, f.h)
		testKeyRequest(t, f.h.service.admission, "key").settle(true)
		if w := hotPreparationPost(t, f.h, hotPreparationPath+"/finish", req); w.Code != 200 {
			t.Fatal("duplicate finish", w.Code)
		}
		if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 503 {
			t.Fatal("reused completed ID", w.Code)
		}
		old = req
	}
	requireBarrierAccounting(t, f.h.service.admission, 0, 0, 0)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.value != "value" || f.increments != 0 {
		t.Fatal("finish changed business data")
	}
}

func TestHotPreparationFinishRetryAndRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "restart"}[restart], func(t *testing.T) {
			f := newHotPreparationFixture(t)
			req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
			if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
				t.Fatal(w.Code)
			}
			f.mu.Lock()
			f.finishFailures = 1
			if restart {
				f.finishFailures = 100
			}
			f.mu.Unlock()
			if w := hotPreparationPost(t, f.h, hotPreparationPath+"/finish", req); w.Code != 503 {
				t.Fatal(w.Code)
			}
			h := f.h
			if restart {
				h.Close()
				f.mu.Lock()
				f.finishFailures = 0
				f.mu.Unlock()
				s, err := NewService(h.service.cfg)
				if err != nil {
					t.Fatal(err)
				}
				s.admission = testKeyBarrierSet(t, 8, 1024, 8)
				h, err = openHotPreparation(s, f.o)
				if err != nil {
					t.Fatal(err)
				}
				defer h.Close()
			}
			waitPreparationFinished(t, h)
			testKeyRequest(t, h.service.admission, "key").settle(true)
			if _, err := os.Stat(filepath.Join(f.dir, finishingFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("finish intent retained", err)
			}
		})
	}
}

func TestOperationFinishedRecoveryAndReplacement(t *testing.T) {
	id, intent, p := evictionOperationFixtureData()
	path := filepath.Join(confirmationPrivateDir(t), "confirmation-operation.log")
	op, err := openEvictionOperation(path, id, &intent, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := op.recordFinished(); err == nil {
		t.Fatal("finish without confirmation")
	}
	if err := op.recordPreparation(p); err != nil {
		t.Fatal(err)
	}
	prev := syncApplyCheckpoint{2, p.Before, strings.Repeat("a", 64)}
	ack := evictionColdReceipt{1, evictionHash(id), evictionHash(intent), prev, nextPendingCheckpoint(prev, p.Record)}
	op.mu.Lock()
	err = op.recordReceiptLocked(ack)
	op.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := op.recordFinished(); err != nil {
		t.Fatal(err)
	}
	op.close()
	op, err = openEvictionOperation(path, id, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	state, err := op.snapshot()
	if err != nil || !state.Finished {
		t.Fatal(err)
	}
	op.close()
	intent.OperationID = strings.Repeat("7", 32)
	op, err = replaceFinishedOperation(path, id, intent)
	if err != nil {
		t.Fatal(err)
	}
	op.close()
	op, err = openEvictionOperation(path, id, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	state, err = op.snapshot()
	if err != nil || state.Finished || state.Intent.OperationID != intent.OperationID {
		t.Fatal("replacement recovery", err)
	}
}

func TestFinishedPendingControlIsNotBusinessReplay(t *testing.T) {
	_, intent, p := evictionOperationFixtureData()
	prev := syncApplyCheckpoint{2, p.Before, strings.Repeat("a", 64)}
	pending := syncPending{Version: 2, Previous: prev, Next: nextPendingCheckpoint(prev, p.Record), Record: p.Record}
	if err := validatePending(pending); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPendingCommand(intent.prepareFrame()); err == nil {
		t.Fatal("ordinary parser accepted metadata")
	}
	pending.Before.Exists = true
	if err := validatePending(pending); err == nil {
		t.Fatal("control item acquired business effect")
	}
}

func TestFinishCanceledCallStillRecovers(t *testing.T) {
	f := newHotPreparationFixture(t)
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
		t.Fatal(w.Code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.h.gate <- struct{}{}
	err := f.h.requestFinish(ctx, req)
	<-f.h.gate
	if err == nil {
		t.Fatal("canceled request returned success")
	}
	waitPreparationFinished(t, f.h)
}

func TestFinishLocalPersistenceUncertainRecovers(t *testing.T) {
	for _, stage := range []string{"finish_before_write", "finish_after_write", "finish_after_sync"} {
		t.Run(stage, func(t *testing.T) {
			f := newHotPreparationFixture(t)
			req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
			if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
				t.Fatal(w.Code)
			}
			f.h.gate <- struct{}{}
			f.h.confirmation.op.fault = func(s string) error {
				if s == stage {
					return errors.New("injected local persistence failure")
				}
				return nil
			}
			<-f.h.gate
			if w := hotPreparationPost(t, f.h, hotPreparationPath+"/finish", req); w.Code != 503 {
				t.Fatal(w.Code)
			}
			waitPreparationFinished(t, f.h)
			testKeyRequest(t, f.h.service.admission, "key").settle(true)
		})
	}
}
