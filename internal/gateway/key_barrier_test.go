package gateway

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func testKeyBarrierSet(t *testing.T, keys, bytes, active int) *keyBarrierSet {
	t.Helper()
	g, err := newKeyBarrierSet(keys, bytes, active)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func testKeyRequest(t *testing.T, g *keyBarrierSet, key string) *keyRequest {
	t.Helper()
	r, err := g.enter(key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func testKeyBarrier(t *testing.T, g *keyBarrierSet, key string) *keyBarrier {
	t.Helper()
	b, err := g.reserve(key)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func requireBarrierError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

func requireBarrierWake(t *testing.T, b *keyBarrier, ready bool) {
	t.Helper()
	select {
	case <-b.wake:
		if !ready {
			t.Fatal("unexpected wake")
		}
	default:
		if ready {
			t.Fatal("missing wake")
		}
	}
}

func requireBarrierAccounting(t *testing.T, g *keyBarrierSet, keys, bytes, active int) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.entries) != keys || g.keyBytes != bytes || g.active != active {
		t.Fatalf("accounting = %d/%d/%d, want %d/%d/%d", len(g.entries), g.keyBytes, g.active, keys, bytes, active)
	}
}

func TestKeyBarrierDrainsAndIsolatesKeys(t *testing.T) {
	g := testKeyBarrierSet(t, 3, 100, 4)
	r1 := testKeyRequest(t, g, "a")
	r2 := testKeyRequest(t, g, "a")
	b := testKeyBarrier(t, g, "a")
	requireBarrierWake(t, b, false)
	requireBarrierError(t, b.beginDispatch(), errKeyBarrierDraining)
	_, err := g.enter("a")
	requireBarrierError(t, err, errKeyBarrierBusy)
	_, err = g.reserve("a")
	requireBarrierError(t, err, errKeyBarrierBusy)
	other := testKeyRequest(t, g, "b")
	other.settle(true)
	r1.settle(true)
	r1.settle(true) // A duplicate completion must not drain r2.
	requireBarrierWake(t, b, false)
	requireBarrierError(t, b.beginDispatch(), errKeyBarrierDraining)
	r2.settle(true)
	requireBarrierWake(t, b, true)
	requireBarrierError(t, b.beginDispatch(), nil)
	requireBarrierError(t, b.beginDispatch(), errKeyBarrierDispatched)
	requireBarrierError(t, b.cancel(), errKeyBarrierDispatched)
	_, err = g.enter("a")
	requireBarrierError(t, err, errKeyBarrierBusy)
	requireBarrierAccounting(t, g, 1, 1, 0)
}

func TestKeyBarrierCancelBeforeDispatchAndStaleHandles(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "draining"}[active], func(t *testing.T) {
			g := testKeyBarrierSet(t, 1, 1, 2)
			var first *keyRequest
			if active {
				first = testKeyRequest(t, g, "k")
			}
			old := testKeyBarrier(t, g, "k")
			requireBarrierError(t, old.cancel(), nil)
			requireBarrierWake(t, old, true)
			r := testKeyRequest(t, g, "k")
			current := testKeyBarrier(t, g, "k")
			requireBarrierError(t, old.cancel(), errKeyBarrierStale)
			requireBarrierError(t, old.beginDispatch(), errKeyBarrierStale)
			if first != nil {
				first.settle(true)
			}
			r.settle(true)
			requireBarrierError(t, current.cancel(), nil)
			newRequest := testKeyRequest(t, g, "k")
			r.settle(true) // The old entry may already have been freed/recreated.
			requireBarrierAccounting(t, g, 1, 1, 1)
			newRequest.settle(true)
			requireBarrierAccounting(t, g, 0, 0, 0)
		})
	}
}

func TestKeyBarrierUnknownOutcomeNeverReopens(t *testing.T) {
	for _, reserve := range []bool{false, true} {
		g := testKeyBarrierSet(t, 2, 2, 2)
		r1 := testKeyRequest(t, g, "k")
		r2 := testKeyRequest(t, g, "k")
		var b *keyBarrier
		if reserve {
			b = testKeyBarrier(t, g, "k")
		}
		r1.settle(false)
		if b != nil {
			requireBarrierWake(t, b, true)
			requireBarrierError(t, b.beginDispatch(), errKeyBarrierUncertain)
			requireBarrierError(t, b.cancel(), errKeyBarrierUncertain)
		}
		r2.settle(true)
		r1.settle(true) // A late success callback cannot clear quarantine.
		_, err := g.enter("k")
		requireBarrierError(t, err, errKeyBarrierUncertain)
		_, err = g.reserve("k")
		requireBarrierError(t, err, errKeyBarrierUncertain)
		testKeyRequest(t, g, "x").settle(true)
		requireBarrierAccounting(t, g, 1, 1, 0)
	}
}

func TestKeyBarrierBudgetsAndBinaryKeys(t *testing.T) {
	for _, limits := range [][3]int{{0, 1, 1}, {1, 0, 1}, {1, 1, 0}, {-1, 1, 1}} {
		g, err := newKeyBarrierSet(limits[0], limits[1], limits[2])
		requireBarrierError(t, err, errKeyBarrierConfig)
		if g != nil {
			t.Fatal("invalid config returned a set")
		}
	}
	g := testKeyBarrierSet(t, 2, 3, 2)
	empty := testKeyRequest(t, g, "")
	binary := testKeyRequest(t, g, "\x00\xffk")
	_, err := g.enter("")
	requireBarrierError(t, err, errKeyBarrierBudget) // Per-request cap, even same key.
	_, err = g.reserve("x")
	requireBarrierError(t, err, errKeyBarrierBudget)
	empty.settle(true)
	_, err = g.reserve("x")
	requireBarrierError(t, err, errKeyBarrierBudget) // Byte cap, despite a free key slot.
	binary.settle(true)
	testKeyRequest(t, g, "abc").settle(true)
	requireBarrierAccounting(t, g, 0, 0, 0)
	_, err = g.reserve(strings.Repeat("x", (64<<10)+1))
	requireBarrierError(t, err, errKeyBarrierKey)
	requireBarrierAccounting(t, g, 0, 0, 0)
	// Empty keys still consume an entry; uncertainty is never evicted for space.
	g = testKeyBarrierSet(t, 1, 10, 2)
	testKeyRequest(t, g, "").settle(false)
	_, err = g.reserve("x")
	requireBarrierError(t, err, errKeyBarrierBudget)
	requireBarrierAccounting(t, g, 1, 0, 0)
}

func TestKeyBarrierCloseWakesButDoesNotAuthorize(t *testing.T) {
	g := testKeyBarrierSet(t, 3, 10, 3)
	r := testKeyRequest(t, g, "a")
	b := testKeyBarrier(t, g, "a")
	idle := testKeyBarrier(t, g, "b")
	g.close()
	g.close()
	requireBarrierWake(t, b, true)
	requireBarrierWake(t, idle, true)
	r.settle(true)
	requireBarrierError(t, b.beginDispatch(), errKeyBarrierClosed)
	requireBarrierError(t, idle.cancel(), errKeyBarrierClosed)
	_, err := g.enter("c")
	requireBarrierError(t, err, errKeyBarrierClosed)
	_, err = g.reserve("c")
	requireBarrierError(t, err, errKeyBarrierClosed)
	requireBarrierAccounting(t, g, 2, 2, 0)
}

func TestKeyBarrierConcurrentAdmissionAndCancellation(t *testing.T) {
	g := testKeyBarrierSet(t, 1, 1, 32)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r, err := g.enter("k")
				if err == nil {
					r.settle(true)
					r.settle(true)
				} else if err != errKeyBarrierBusy && err != errKeyBarrierBudget {
					t.Error(err)
				}
				b, err := g.reserve("k")
				if err == nil {
					if _, err := g.enter("k"); err != errKeyBarrierBusy && err != errKeyBarrierBudget {
						t.Errorf("admitted while reserved: %v", err)
					}
					if err := b.cancel(); err != nil {
						t.Error(err)
					}
				} else if err != errKeyBarrierBusy {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	requireBarrierAccounting(t, g, 0, 0, 0)
}

func TestKeyBarrierConcurrentCancelAndDispatch(t *testing.T) {
	for i := 0; i < 100; i++ {
		g := testKeyBarrierSet(t, 1, 1, 1)
		b := testKeyBarrier(t, g, "k")
		start := make(chan struct{})
		canceled, dispatched := make(chan error, 1), make(chan error, 1)
		go func() { <-start; canceled <- b.cancel() }()
		go func() { <-start; dispatched <- b.beginDispatch() }()
		close(start)
		c, d := <-canceled, <-dispatched
		switch {
		case c == nil:
			requireBarrierError(t, d, errKeyBarrierStale)
			testKeyRequest(t, g, "k").settle(true)
		case d == nil:
			requireBarrierError(t, c, errKeyBarrierDispatched)
			_, err := g.enter("k")
			requireBarrierError(t, err, errKeyBarrierBusy)
		default:
			t.Fatalf("neither operation won: %v / %v", c, d)
		}
	}
}

func TestKeyBarrierConcurrentDuplicateCompletionAndClose(t *testing.T) {
	g := testKeyBarrierSet(t, 1, 1, 1)
	r := testKeyRequest(t, g, "k")
	b := testKeyBarrier(t, g, "k")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.settle(false)
			g.close()
		}()
	}
	wg.Wait()
	requireBarrierWake(t, b, true)
	requireBarrierError(t, b.beginDispatch(), errKeyBarrierClosed)
	requireBarrierAccounting(t, g, 1, 1, 0)
	if !b.entry.uncertain {
		t.Fatal("close or duplicate callback cleared uncertainty")
	}
}

func TestKeyBarrierKeyLengthBoundary(t *testing.T) {
	g := testKeyBarrierSet(t, 1, 64<<10, 1)
	key := strings.Repeat("\x00", 64<<10)
	r := testKeyRequest(t, g, key)
	b := testKeyBarrier(t, g, key)
	r.settle(true)
	requireBarrierError(t, b.cancel(), nil)
	requireBarrierAccounting(t, g, 0, 0, 0)
	_, err := g.enter(key + "x")
	requireBarrierError(t, err, errKeyBarrierKey)
	requireBarrierAccounting(t, g, 0, 0, 0)
}
