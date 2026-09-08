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

func TestLiveConfirmationBoundary(t *testing.T) {
	for _, scenario := range []string{"following", "held", "malformed", "wrong_epoch", "wrong_task", "unsupported_before", "snapshot_race"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 4<<20)
			f.append(t, 40)
			before := f.store.durablePosition()
			i := evictionTestIntent("key", "value")
			i.History = before.History
			id := syncTaskIdentity{i.TaskID, before.Source, "127.0.0.1:23456"}
			binding := confirmationTestBinding(id, i)
			frame := i.prepareFrame()
			switch scenario {
			case "wrong_epoch":
				i.Epoch++
				frame = i.prepareFrame()
			case "wrong_task":
				i.TaskID = "other-task"
				frame = i.prepareFrame()
			case "malformed":
				frame = encodeCommand("SET", i.prepareKey(), "prepare:{}")
			case "unsupported_before":
				frame = encodeCommand("UNLINK", "key")
			}
			var view *syncSegmentView
			if scenario == "snapshot_race" {
				view, _ = f.store.pendingView()
				defer view.close()
			}
			if scenario != "following" {
				r := evictionTestRecords(before, [][]byte{frame})[0]
				if err := f.store.append(r); err != nil {
					t.Fatal(err)
				}
				// Unsupported suffix is retained, not executed/validated as a
				// business prefix after the first held marker.
				if scenario == "held" {
					suffix := evictionTestRecords(r.Position, [][]byte{encodeCommand("UNLINK", "key")})[0]
					if err := f.store.append(suffix); err != nil {
						t.Fatal(err)
					}
				}
			}
			if view == nil {
				view, _ = f.store.pendingView()
				defer view.close()
			}
			held, end, err := liveConfirmationBoundary(context.Background(), view, binding)
			switch scenario {
			case "following", "snapshot_race":
				if err != nil || held != nil || end != view.end {
					t.Fatal("following boundary", err)
				}
			case "held":
				if err != nil || held == nil || end.Position != before {
					t.Fatal("held boundary", err)
				}
			default:
				if err == nil {
					t.Fatal("invalid prefix accepted")
				}
			}
		})
	}
}

func TestLiveConfirmationOperationRecovery(t *testing.T) {
	id, i, p := evictionOperationFixtureData()
	dir := confirmationPrivateDir(t)
	s := &IsolatedConfirmationServer{options: IsolatedConfirmationOptions{Binding: confirmationTestBinding(id, i), OperationDir: dir}}
	state := evictionOperationState{Identity: id, Intent: i, Prepare: &p}
	op, err := s.liveOperation(&state)
	if err != nil {
		t.Fatal(err)
	}
	op.close()
	if op, err := s.liveOperation(nil); err == nil {
		if op != nil {
			op.close()
		}
		t.Fatal("missing source marker ignored")
	}
	op, err = s.liveOperation(&state)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := op.snapshot()
	op.close()
	if err != nil || restored.Prepare == nil || restored.Receipt != nil {
		t.Fatal("waiting state not recovered", err)
	}
	other := state
	other.Intent.OperationID = strings.Repeat("b", 32)
	if op, err := s.liveOperation(&other); err == nil {
		op.close()
		t.Fatal("different operation adopted")
	}
	if err := os.Chmod(filepath.Join(dir, "confirmation-operation.log"), 0644); err != nil {
		t.Fatal(err)
	}
	if op, err := s.liveOperation(&state); err == nil {
		op.close()
		t.Fatal("unsafe operation recovered")
	}
}

func TestLiveConfirmationDefaultReplayStillRejectsMarker(t *testing.T) {
	i := evictionTestIntent("key", "value")
	if _, err := syncPendingCommand(i.prepareFrame()); !errors.Is(err, errSyncUnsupported) {
		t.Fatal("normal replay whitelist expanded", err)
	}
}

// Real bounded segment writes with a simulated already-verified cold prefix.
// This exercises the runner's held fast path and resource/shutdown behavior,
// not cold commit correctness (covered by the gated process integration).
func TestLiveConfirmationRunnerHeldPressureAndStop(t *testing.T) {
	f := newSyncSegmentFixture(t, 1024, 4<<20)
	r := syncRunnerForTest()
	f.store.policy.MaxUnconfirmedBytes = 512
	f.store.policy.ResumeUnconfirmedBytes = 256
	v, err := f.store.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	previous := v.end
	v.close()
	s := &IsolatedConfirmationServer{journal: f.store, gate: make(chan struct{}, 1)}
	s.live = &confirmationLive{runner: r, active: true, ready: true, held: &evictionOperationState{}, previous: previous}
	r.confirmation = s
	r.ops.capture = func(ctx context.Context, j *syncSegments) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			before := j.durablePosition()
			record := evictionTestRecords(before, [][]byte{encodeCommand("SET", "suffix", strings.Repeat("x", 100))})[0]
			if err := j.append(record); err != nil {
				return err
			}
		}
	}
	_, done := syncRunnerStartForTest(t, r, f.store)
	syncRunnerWaitForTest(t, func() bool { return r.snapshot().State == "pressure" })
	paused := r.snapshot()
	if paused.Captured.Offset <= previous.Position.Offset || paused.Applied != previous.Position || s.live.stopping.Load() {
		t.Fatal("held lane did not capture safely")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.shutdown(ctx); err != nil {
		t.Fatal("held shutdown", err)
	}
	if err := syncRunnerResultForTest(t, done); err != nil {
		t.Fatal(err)
	}
	status := r.snapshot()
	if status.State != "stopped" || status.Applied != previous.Position || status.DrainTarget != status.Captured || status.Applied == status.Captured {
		t.Fatal("held stop misreported drained")
	}
	f.store.mu.Lock()
	floor := f.store.manifest.Floor
	f.store.mu.Unlock()
	if floor.Version != 0 && floor.Position.Offset > previous.Position.Offset {
		t.Fatal("held evidence pruned")
	}
}
