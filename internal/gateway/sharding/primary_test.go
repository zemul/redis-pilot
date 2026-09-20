package sharding

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func primaryDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func primaryFixture() []Primary {
	return []Primary{
		{ShardID: "s1", Group: "group-1", Instance: "a", Address: "127.0.0.1:6379", Generation: 1},
		{ShardID: "s2", Group: "group-2", Instance: "b", Address: "127.0.0.1:6380", Generation: 1},
	}
}

func TestPrimarySwitchRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	db := primaryDB(t, path)
	s, err := CreatePrimaryStore(ctx, db, primaryFixture())
	if err != nil {
		t.Fatal(err)
	}
	old := primaryFixture()[0]
	next := old
	next.Instance, next.Address, next.Generation = "a-replica", "127.0.0.1:6381", 2
	if err = s.Complete(ctx, next); !errors.Is(err, ErrPrimaryConflict) {
		t.Fatal("unprepared completion", err)
	}
	for i := 0; i < 2; i++ {
		if err = s.Prepare(ctx, old, next); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = primaryDB(t, path)
	s, err = OpenPrimaryStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Snapshot(ctx)
	if err != nil || v[0].Current != old || v[0].Pending == nil || *v[0].Pending != next {
		t.Fatalf("lost pending intent: %+v %v", v, err)
	}
	if _, err := s.Resolve(ctx, old.ShardID); !errors.Is(err, ErrPrimaryPending) {
		t.Fatal("pending shard routable", err)
	}
	if p, err := s.Resolve(ctx, "s2"); err != nil || p != primaryFixture()[1] {
		t.Fatal("unrelated shard blocked", p, err)
	}
	third := next
	third.Generation++
	if err = s.Prepare(ctx, old, third); !errors.Is(err, ErrPrimaryConflict) {
		t.Fatal("overwrote pending intent", err)
	}
	if err = s.Complete(ctx, third); !errors.Is(err, ErrPrimaryConflict) {
		t.Fatal("wrong completion accepted", err)
	}
	for i := 0; i < 2; i++ {
		if err = s.Complete(ctx, next); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Prepare(ctx, old, next); err != nil {
		t.Fatal("completed replay", err)
	}
	if err = s.Prepare(ctx, next, old); !errors.Is(err, ErrPrimaryConflict) {
		t.Fatal("rollback accepted", err)
	}
	if _, err = CreatePrimaryStore(ctx, db, primaryFixture()); err == nil {
		t.Fatal("reinitialized existing state")
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenPrimaryStore(ctx, primaryDB(t, path))
	if err != nil {
		t.Fatal(err)
	}
	v, err = s.Snapshot(ctx)
	if err != nil || v[0].Current != next || v[0].Pending != nil {
		t.Fatal("completion not durable", v, err)
	}
	if p, err := s.Resolve(ctx, next.ShardID); err != nil || p != next {
		t.Fatal(p, err)
	}
	// Returning to a former master is a NEW generation, not replaying old state.
	back := old
	back.Generation = 3
	if err = s.Prepare(ctx, next, back); err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, back); err != nil {
		t.Fatal(err)
	}
	if err = s.Prepare(ctx, old, next); !errors.Is(err, ErrPrimaryConflict) {
		t.Fatal("late notification accepted", err)
	}
}

func TestPrimaryValidationAndConflict(t *testing.T) {
	ctx := context.Background()
	for name, mutate := range map[string]func(*Primary){
		"zero-generation": func(p *Primary) { p.Generation = 0 },
		"different-shard": func(p *Primary) { p.ShardID = "other" },
		"different-group": func(p *Primary) { p.Group = "other" },
		"public-address":  func(p *Primary) { p.Address = "8.8.8.8:6379" },
		"hostname":        func(p *Primary) { p.Address = "localhost:6379" },
		"mapped-address":  func(p *Primary) { p.Address = "[::ffff:127.0.0.1]:6381" },
		"peer-address":    func(p *Primary) { p.Address = "127.0.0.1:6380" },
		"peer-instance":   func(p *Primary) { p.Instance = "b" },
		"same-generation": func(p *Primary) { p.Generation = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			s, err := CreatePrimaryStore(ctx, primaryDB(t, filepath.Join(t.TempDir(), "runtime.db")), primaryFixture())
			if err != nil {
				t.Fatal(err)
			}
			old := primaryFixture()[0]
			next := old
			next.Generation, next.Instance, next.Address = 2, "replica", "127.0.0.1:6381"
			mutate(&next)
			if err = s.Prepare(ctx, old, next); !errors.Is(err, ErrPrimaryConflict) {
				t.Fatal(err)
			}
			v, err := s.Snapshot(ctx)
			if err != nil || v[0].Current != old || v[0].Pending != nil {
				t.Fatal("failed switch mutated state", v, err)
			}
		})
	}
}

func TestPrimaryConcurrentAddressReservation(t *testing.T) {
	ctx := context.Background()
	s, err := CreatePrimaryStore(ctx, primaryDB(t, filepath.Join(t.TempDir(), "runtime.db")), primaryFixture())
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, old := range primaryFixture() {
		wg.Add(1)
		go func(old Primary) {
			defer wg.Done()
			<-start
			next := old
			next.Generation, next.Instance, next.Address = 2, old.Instance+"-replica", "127.0.0.1:6400"
			errs <- s.Prepare(ctx, old, next)
		}(old)
	}
	close(start)
	wg.Wait()
	close(errs)
	success, conflict := 0, 0
	for err := range errs {
		if err == nil {
			success++
		} else if errors.Is(err, ErrPrimaryConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal(success, conflict)
	}
	if _, err = s.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPrimaryMissingAndCorruptState(t *testing.T) {
	ctx := context.Background()
	db := primaryDB(t, filepath.Join(t.TempDir(), "runtime.db"))
	if _, err := OpenPrimaryStore(ctx, db); err == nil {
		t.Fatal("missing table accepted")
	}
	if _, err := CreatePrimaryStore(ctx, db, primaryFixture()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE shard_primaries SET body='{}' WHERE shard_id='s1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPrimaryStore(ctx, db); err == nil {
		t.Fatal("corrupt state accepted")
	}
}

func TestPrimaryCanceledUpdateAndPendingPeer(t *testing.T) {
	ctx := context.Background()
	s, err := CreatePrimaryStore(ctx, primaryDB(t, filepath.Join(t.TempDir(), "runtime.db")), primaryFixture())
	if err != nil {
		t.Fatal(err)
	}
	old := primaryFixture()[0]
	next := old
	next.Generation, next.Instance, next.Address = 2, "replica-a", "127.0.0.1:6400"
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Prepare(canceled, old, next); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p, err := s.Resolve(ctx, old.ShardID); err != nil || p != old {
		t.Fatal("canceled update changed state", p, err)
	}
	if err := s.Prepare(ctx, old, next); err != nil {
		t.Fatal(err)
	}
	peer := primaryFixture()[1]
	peerNext := peer
	peerNext.Generation, peerNext.Instance, peerNext.Address = 2, "replica-b", next.Address
	if err := s.Prepare(ctx, peer, peerNext); !errors.Is(err, ErrPrimaryConflict) {
		t.Fatal("pending address reused", err)
	}
	if p, err := s.Resolve(ctx, peer.ShardID); err != nil || p != peer {
		t.Fatal("conflict changed peer", p, err)
	}
}

func TestPrimarySwitchDoesNotRehash(t *testing.T) {
	a := testTopology(t, 3, 1)
	c := a.Config()
	c.Nodes[0].Address = "127.0.0.1:18000"
	b, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if a.Owner(key).ID != b.Owner(key).ID {
			t.Fatal("primary address changed key placement")
		}
	}
}
