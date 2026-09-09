package gateway

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinishColdCommittedLocalMissingKvrocks(t *testing.T) {
	f := newEvictionColdFixture(t, confirmationPrivateDir(t), true)
	ctx := context.Background()
	if _, err := f.target.confirmEviction(ctx, f.j, f.op); err != nil {
		t.Fatal(err)
	}
	state, err := f.op.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Commit cold completion, but deliberately omit the local terminal frame:
	// this is the durable state left by a crash between those two writes.
	if err := f.target.finishPreparation(ctx, f.j, state); err != nil {
		t.Fatal(err)
	}
	f.op.close()
	f.j.close()
	opDir := filepath.Join(f.root, "operation")
	binding := confirmationTestBinding(state.Identity, state.Intent)
	lock, err := openConfirmationBinding(opDir, binding)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
	if err := os.Rename(filepath.Join(opDir, "intent.log"), filepath.Join(opDir, "confirmation-operation.log")); err != nil {
		t.Fatal(err)
	}
	o := IsolatedConfirmationOptions{Binding: binding,
		JournalDir: filepath.Join(f.root, "segments"), OperationDir: opDir,
		TokenFile: secretFile(t, strings.Repeat("finish-recovery-", 5)), AllowUnauthenticated: true,
		SegmentBytes: 1 << 20, JournalBytes: 4 << 20, Timeout: 3 * time.Second}
	if err := o.CheckCredentials(); err != nil {
		t.Fatal("recovery options", err)
	}
	s, err := NewLiveConfirmationServer(o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.live.active = true
	if _, err := s.drainLive(ctx, s.journal); err != nil {
		t.Fatal("resume cold-committed completion", err)
	}
	if s.live.held != nil || s.live.blocked {
		t.Fatal("completed operation still held")
	}
	op, err := openEvictionOperation(filepath.Join(opDir, "confirmation-operation.log"), state.Identity, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	recovered, err := op.snapshot()
	if err != nil || !recovered.Finished {
		t.Fatal("terminal frame not recovered", err)
	}
	kvCommitGet(t, f.hot, "counter", "42")
	kvCommitGet(t, f.cold, "counter", "42")
	kvCommitGet(t, f.cold, "after-barrier", "not-applied")
}
