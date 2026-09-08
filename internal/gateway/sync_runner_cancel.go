package gateway

import (
	"context"
	"errors"
	"io"
	"net"
)

// Cancellation wrappers may carry a stronger failure as well. Only the bare
// context outcomes authorize replacing the result or reporting a clean stop.
func syncOnlyCancellation(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func syncTerminalOutcome(err error) bool {
	for _, terminal := range []error{
		errSyncPendingUncertain, errSyncApplyUncertain, errSyncTargetDrift,
		errSyncJournalCorrupt, errSyncJournalFull, errSyncUnsupported, errSyncHistoryGap,
		errSyncRetryExhausted, errSyncPressureTimeout, errSyncRunnerBusy, errSyncViewLimit,
		errSyncShutdownTimeout, errSyncShutdownAborted, errSyncShutdownIncomplete,
	} {
		if errors.Is(err, terminal) {
			return true
		}
	}
	return false
}

// Normalize only cancellation-induced raw network failures at the drain
// boundary. This grants no retry permission. EXEC failures are already typed
// uncertain by pendingTxn, and those (or any other terminal marker) must remain
// visible even if cancellation simultaneously closed the connection.
func syncDrainCancellation(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil {
		return err
	}
	if syncTerminalOutcome(err) || errors.Is(err, errSyncCapturePressure) {
		return err
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return ctx.Err()
	}
	// Avoid errors.As: an unknown outer failure wrapping a network error may
	// carry stronger semantics. Preserve it rather than guessing it is safe.
	if op, ok := err.(*net.OpError); ok && (op.Op == "read" || op.Op == "write") {
		return ctx.Err()
	}
	return err
}
