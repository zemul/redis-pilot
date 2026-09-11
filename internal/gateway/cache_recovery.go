package gateway

import "context"

// Only for an exclusively owned, disposable, standalone hot Redis DB 0.
// Kill server-side connections BEFORE clearing: closing a local socket alone
// does not cancel commands already buffered on an old Redis connection.
func (r *cacheRuntime) recoverHot(ctx context.Context) {
	if r.hotReady.Load() || !r.hotMu.TryLock() {
		return
	}
	defer r.hotMu.Unlock()
	if r.hotReady.Load() {
		return
	}
	r.hotStateMu.Lock()
	failures := r.hotFailures
	r.hotStateMu.Unlock()
	c, err := r.connect(ctx, true)
	if err != nil {
		return
	}
	defer c.Close()
	b, err := c.call(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes")
	if err != nil || len(b) == 0 || b[0] != ':' {
		return
	}
	b, err = c.call(ctx, "FLUSHDB", "SYNC")
	if err != nil || string(b) != "+OK\r\n" {
		return
	}
	r.hotStateMu.Lock()
	defer r.hotStateMu.Unlock()
	if r.hotFailures != failures {
		return
	}
	r.hotReady.Store(true)
	r.events.WithLabelValues("hot_reset_completed").Inc()
}
