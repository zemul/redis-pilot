package gateway

import (
	"errors"
	"strings"
	"sync"
)

// This is a process-local admission primitive, NOT an eviction
// protocol. There is deliberately no cold acknowledgement,
// backend I/O, persistence, or method to reopen a dispatched/uncertain key.
// One set belongs to one shard and one executor. Missing entries mean idle,
// not "the key exists in Redis". Residency must be established separately.
type keyBarrierSet struct {
	mu                           sync.Mutex
	entries                      map[string]*keyBarrierEntry
	maxKeys, maxBytes, maxActive int
	keyBytes, active             int
	closed                       bool
}

type keyBarrierEntry struct {
	key       string
	active    int
	uncertain bool
	barrier   *keyBarrier
}

// Handles must be used by pointer, never copied. Completion is once-only under
// the set's mutex; an old handle cannot release a newer request or barrier.
type keyRequest struct {
	set   *keyBarrierSet
	entry *keyBarrierEntry
	done  bool
}

type keyBarrier struct {
	set        *keyBarrierSet
	entry      *keyBarrierEntry
	wake       chan struct{}
	signaled   bool
	dispatched bool
}

var (
	errKeyBarrierConfig     = errors.New("invalid key barrier budget")
	errKeyBarrierBudget     = errors.New("key barrier budget exhausted")
	errKeyBarrierKey        = errors.New("key exceeds barrier key limit")
	errKeyBarrierClosed     = errors.New("key barrier set closed")
	errKeyBarrierBusy       = errors.New("key transition in progress")
	errKeyBarrierUncertain  = errors.New("key outcome uncertain")
	errKeyBarrierStale      = errors.New("key barrier handle is no longer current")
	errKeyBarrierDraining   = errors.New("key requests still in flight")
	errKeyBarrierDispatched = errors.New("key transition may have been dispatched")
)

func newKeyBarrierSet(maxKeys, maxBytes, maxActive int) (*keyBarrierSet, error) {
	if maxKeys <= 0 || maxBytes <= 0 || maxActive <= 0 {
		return nil, errKeyBarrierConfig
	}
	return &keyBarrierSet{entries: make(map[string]*keyBarrierEntry), maxKeys: maxKeys, maxBytes: maxBytes, maxActive: maxActive}, nil
}

// entryLocked retains a separate binary-safe key copy, so a short substring
// cannot pin an arbitrarily large request buffer. Empty Redis keys are valid.
func (g *keyBarrierSet) entryLocked(key string) (*keyBarrierEntry, error) {
	if g.closed {
		return nil, errKeyBarrierClosed
	}
	if len(key) > 64<<10 {
		return nil, errKeyBarrierKey
	}
	if e := g.entries[key]; e != nil {
		if e.uncertain {
			return nil, errKeyBarrierUncertain
		}
		if e.barrier != nil {
			return nil, errKeyBarrierBusy
		}
		return e, nil
	}
	if len(g.entries) >= g.maxKeys || len(key) > g.maxBytes-g.keyBytes {
		return nil, errKeyBarrierBudget
	}
	e := &keyBarrierEntry{key: strings.Clone(key)}
	g.entries[e.key] = e
	g.keyBytes += len(e.key)
	return e, nil
}

// enter is only for requests already eligible for the hot execution path.
// Multiple ordinary requests may overlap; this is not command serialization.
func (g *keyBarrierSet) enter(key string) (*keyRequest, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, errKeyBarrierClosed
	}
	if g.active >= g.maxActive {
		return nil, errKeyBarrierBudget
	}
	e, err := g.entryLocked(key)
	if err != nil {
		return nil, err
	}
	e.active++
	g.active++
	return &keyRequest{set: g, entry: e}, nil
}

// enterMany admits one command atomically across all of its distinct keys.
// A rejected command retains no handles or budget. Duplicate arguments still
// go to Redis unchanged, but need only one admission per key for this command.
func (g *keyBarrierSet) enterMany(keys [][]byte) ([]*keyRequest, error) {
	// GET/SET/INCR are the common hot path; avoid a dedup map for one key.
	if len(keys) == 1 {
		r, err := g.enter(string(keys[0]))
		if err != nil {
			return nil, err
		}
		return []*keyRequest{r}, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, errKeyBarrierClosed
	}
	seen := make(map[string]bool)
	var admitted []*keyRequest
	rollback := func(err error) ([]*keyRequest, error) {
		for _, r := range admitted {
			r.entry.active--
			g.active--
			g.removeIdleLocked(r.entry)
		}
		return nil, err
	}
	for _, raw := range keys {
		if len(raw) > 64<<10 {
			return rollback(errKeyBarrierKey)
		}
		key := string(raw)
		if seen[key] {
			continue
		}
		if g.active >= g.maxActive {
			return rollback(errKeyBarrierBudget)
		}
		e, err := g.entryLocked(key)
		if err != nil {
			return rollback(err)
		}
		e.active++
		g.active++
		seen[e.key] = true
		admitted = append(admitted, &keyRequest{set: g, entry: e})
	}
	return admitted, nil
}

// settle(true) requires a definitive backend completion or proof that nothing
// was dispatched. Client cancellation/socket close alone is not that proof.
// An unknown ordinary outcome is not replayed and does not quarantine ordinary
// access or future reservations. Only a transition already waiting for this
// request retains uncertainty: a lost reply is not proof its drain completed.
func (r *keyRequest) settle(known bool) {
	g := r.set
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.done {
		return
	}
	r.done = true
	e := r.entry
	e.active--
	g.active--
	if !known && e.barrier != nil {
		e.uncertain = true
	}
	if e.barrier != nil && (e.active == 0 || e.uncertain) {
		e.barrier.signalLocked()
	}
	g.removeIdleLocked(e)
}

// reserve closes admission immediately, before existing requests drain. This
// prevents continuous writers from slipping between a check and the barrier.
func (g *keyBarrierSet) reserve(key string) (*keyBarrier, error) {
	return g.reserveKey(key, false)
}

// Durable operation recovery restores a possibly dispatched barrier. It never
// authorizes a new transition or deletion.
func (g *keyBarrierSet) restore(key string) (*keyBarrier, error) {
	return g.reserveKey(key, true)
}

func (g *keyBarrierSet) reserveKey(key string, restore bool) (*keyBarrier, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, errKeyBarrierClosed
	}
	e, err := g.entryLocked(key)
	if err != nil {
		return nil, err
	}
	b := &keyBarrier{set: g, entry: e, wake: make(chan struct{}), dispatched: restore}
	e.barrier = b
	if e.active == 0 {
		b.signalLocked()
	}
	return b, nil
}

// wake is notification, not permission: after waking, the caller must recheck
// via beginDispatch. Cancellation, shutdown and uncertainty also wake waiters.
func (b *keyBarrier) signalLocked() {
	if !b.signaled {
		close(b.wake)
		b.signaled = true
	}
}

func (b *keyBarrier) checkLocked() error {
	if b.set.closed {
		return errKeyBarrierClosed
	}
	if b.entry.barrier != b {
		return errKeyBarrierStale
	}
	if b.entry.uncertain {
		return errKeyBarrierUncertain
	}
	if b.dispatched {
		return errKeyBarrierDispatched
	}
	return nil
}

// beginDispatch records the point after which local cancellation is unsafe.
// It grants NO backend deletion authority. A future durable protocol must
// separately establish ownership, intent and exact cold acknowledgement.
// This foundation has no completion/unblock API after this point.
func (b *keyBarrier) beginDispatch() error {
	b.set.mu.Lock()
	defer b.set.mu.Unlock()
	if err := b.checkLocked(); err != nil {
		return err
	}
	if b.entry.active != 0 {
		return errKeyBarrierDraining
	}
	b.dispatched = true
	return nil
}

// cancel is safe only before dispatch. It cannot clear an uncertain outcome.
func (b *keyBarrier) cancel() error {
	g := b.set
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := b.checkLocked(); err != nil {
		return err
	}
	b.entry.barrier = nil
	b.signalLocked()
	g.removeIdleLocked(b.entry)
	return nil
}

func (g *keyBarrierSet) removeIdleLocked(e *keyBarrierEntry) {
	if e.active == 0 && !e.uncertain && e.barrier == nil {
		delete(g.entries, e.key)
		g.keyBytes -= len(e.key)
	}
}

// close is terminal for this set. Late completions may drain accounting, but
// never reopen admission. Constructing a new set is NOT crash recovery.
func (g *keyBarrierSet) close() {
	g.closeQuiescent()
}

// Only a newly closed set without ANY retained state can support a clean boot
// latch. Held barriers and uncertain keys must survive via reconciliation.
func (g *keyBarrierSet) closeQuiescent() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	clean := !g.closed && len(g.entries) == 0 && g.active == 0
	g.closed = true
	for _, e := range g.entries {
		if e.barrier != nil {
			e.barrier.signalLocked()
		}
	}
	return clean
}
