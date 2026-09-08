package gateway

import (
	"context"
	"errors"
	"io"
	"net"
)

// These markers grant narrowly scoped retry authority. Source capture has not
// appended an incomplete frame, and cold open performs authentication/read-only
// checks only. They must never wrap arbitrary journal or cold apply errors.
var errSyncSourceTransport = errors.New("replication source transport interrupted")
var errSyncColdConnect = errors.New("cold connection setup interrupted")

func isSyncTransportError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

type syncTransportCause struct {
	kind      error
	operation string
	cause     error
}

// Do not expose passwords, untrusted replies, or arbitrary underlying error
// strings in user-visible messages. The cause remains inspectable via Is/As.
func (e *syncTransportCause) Error() string   { return e.kind.Error() + ": " + e.operation }
func (e *syncTransportCause) Unwrap() []error { return []error{e.kind, e.cause} }

// nil means a non-transport error: the caller must retain its fatal protocol or
// configuration error, even if cancellation raced with it. Parent cancellation
// only takes precedence over a network/ctx failure, never an existing terminal
// outcome (otherwise pause could accidentally authorize retry of a bad journal).
func syncTransportFailure(ctx context.Context, kind error, operation string, cause error) error {
	contextCause := errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded)
	if !contextCause && !isSyncTransportError(cause) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if contextCause {
		return cause
	}
	return &syncTransportCause{kind, operation, cause}
}
