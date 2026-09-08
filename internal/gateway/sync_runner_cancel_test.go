package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestSyncDrainCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	network := &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, network, &net.OpError{Op: "write", Net: "tcp", Err: net.ErrClosed}} {
		if got := syncDrainCancellation(ctx, err); got != context.Canceled {
			t.Fatal("canceled raw network operation was not normalized", got)
		}
		if got := syncDrainCancellation(context.Background(), err); got != err {
			t.Fatal("network failure without cancellation changed", got)
		}
	}
	for _, terminal := range []error{
		errSyncPendingUncertain, errSyncApplyUncertain, errSyncTargetDrift,
		errSyncJournalCorrupt, errSyncJournalFull, errSyncUnsupported,
		errSyncHistoryGap, errSyncCapturePressure, errSyncRetryExhausted,
		errSyncPressureTimeout, errSyncRunnerBusy,
	} {
		for _, err := range []error{terminal, errors.Join(terminal, network), &net.OpError{Op: "read", Net: "tcp", Err: errors.Join(terminal, io.EOF)}} {
			if got := syncDrainCancellation(ctx, err); got != err {
				t.Fatal("terminal failure was hidden by cancellation", terminal, got)
			}
		}
	}
	for _, err := range []error{nil, errors.New("protocol failure"), fmt.Errorf("unknown outer failure: %w", network), &net.OpError{Op: "dial", Net: "tcp", Err: io.EOF}, errSyncColdConnect} {
		if got := syncDrainCancellation(ctx, err); got != err {
			t.Fatal("non-raw/unknown error changed", got)
		}
	}
}

func TestSyncRunnerDrainCancelRace(t *testing.T) {
	network := &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}
	for _, tc := range []struct {
		name         string
		result, want error
		state        string
	}{
		{"read-closed", network, context.Canceled, "stopped"},
		{"read-EOF", io.EOF, context.Canceled, "stopped"},
		{"EXEC-uncertain", errSyncPendingUncertain, errSyncPendingUncertain, "failed"},
		{"EXEC-uncertain-with-network", errors.Join(errSyncPendingUncertain, network), errSyncPendingUncertain, "failed"},
		{"journal-corrupt", errSyncJournalCorrupt, errSyncJournalCorrupt, "failed"},
		{"journal-full", errSyncJournalFull, errSyncJournalFull, "failed"},
		{"unsupported", errSyncUnsupported, errSyncUnsupported, "failed"},
		{"state-drift", errSyncTargetDrift, errSyncTargetDrift, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := syncResourceFixture(t, defaultSyncResourcePolicy())
			runner := syncRunnerForTest()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runner.ops.drain = func(context.Context, *syncSegments) (syncPosition, error) {
				cancel()
				return syncPosition{}, tc.result
			}
			err := runner.run(ctx, fixture.store)
			if !errors.Is(err, tc.want) || runner.snapshot().State != tc.state {
				t.Fatalf("got result=%v state=%s", err, runner.snapshot().State)
			}
			if tc.want != context.Canceled && errors.Is(err, context.Canceled) {
				t.Fatal("runner hid terminal drain outcome behind cancellation", err)
			}
			if runner.snapshot().SourceAttempts != 0 || runner.snapshot().ColdRetries != 0 {
				t.Fatal("cancel normalization started capture or granted cold retry")
			}
		})
	}
}
