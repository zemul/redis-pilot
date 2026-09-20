package sharding

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/netip"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var ErrPrimaryConflict = errors.New("shard primary generation or identity conflict")
var ErrPrimaryPending = errors.New("shard primary switch requires reconciliation")

// Primary is separate from HRW placement. Generation is issued by the control
// plane, NOT Redis run_id, uptime or a Sentinel event arrival timestamp.
type Primary apitypes.ShardPrimary

type PrimaryState struct {
	Current Primary  `json:"current"`
	Pending *Primary `json:"pending,omitempty"`
}

func ValidatePrimary(p Primary) error {
	if !p.valid() {
		return ErrPrimaryConflict
	}
	return nil
}

// Retarget is legal only while no new-primary business admission has happened.
// Runtime retains its barrier across retries; an intermediate failed primary
// must not have to become reachable before a newer election can be accepted.
func (s *PrimaryStore) Retarget(ctx context.Context, pending, next Primary) error {
	if !primarySuccessor(pending, next) {
		return ErrPrimaryConflict
	}
	return s.update(ctx, pending.ShardID, func(v *PrimaryState) error {
		if v.Pending == nil || *v.Pending != pending || !primarySuccessor(v.Current, next) {
			return ErrPrimaryConflict
		}
		v.Pending = &next
		return nil
	})
}

func (p Primary) valid() bool {
	a, err := netip.ParseAddrPort(p.Address)
	return validID(p.ShardID) && validID(p.Group) && validID(p.Instance) && p.Generation > 0 && p.Replicas >= 0 && p.Replicas <= 2 &&
		err == nil && a.Port() != 0 && a.Addr().Zone() == "" &&
		(a.Addr().IsLoopback() || a.Addr().IsPrivate()) &&
		netip.AddrPortFrom(a.Addr().Unmap(), a.Port()).String() == p.Address
}

func primarySuccessor(old, next Primary) bool {
	return old.valid() && next.valid() && old.ShardID == next.ShardID && old.Group == next.Group &&
		next.Generation > old.Generation &&
		(old.Instance != next.Instance || old.Address == next.Address)
}

// PrimaryStore persists the endpoint switch intent, not permission to serve a
// new backend. The caller owns the private WAL/FULL SQLite DB and process lock.
// No network calls, replication checks, election or write retries happen here.
type PrimaryStore struct{ db *sql.DB }

// ReconcileMembers changes only membership, never an existing primary. The
// runtime must validate membership against its durable topology/task first.
// An unresolved switch cannot be silently removed by an older snapshot.
func (s *PrimaryStore) ReconcileMembers(ctx context.Context, wanted []Primary) error {
	if len(wanted) == 0 || len(wanted) > 2*MaxNodes {
		return ErrPrimaryConflict
	}
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err = c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer c.ExecContext(context.Background(), "ROLLBACK")
	rows, err := c.QueryContext(ctx, `SELECT body FROM shard_primaries`)
	if err != nil {
		return err
	}
	old := map[string]PrimaryState{}
	for rows.Next() {
		var b string
		if err = rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		v, e := decodePrimary(b)
		if e != nil {
			rows.Close()
			return e
		}
		old[v.Current.ShardID] = v
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	next := map[string]PrimaryState{}
	groups, instances, addresses := map[string]bool{}, map[string]string{}, map[string]string{}
	for _, p := range wanted {
		if !p.valid() {
			return ErrPrimaryConflict
		}
		if _, ok := next[p.ShardID]; ok {
			return ErrPrimaryConflict
		}
		v, ok := old[p.ShardID]
		if ok {
			if v.Current.Group != p.Group {
				return ErrPrimaryConflict
			}
		} else {
			v.Current = p
		}
		if groups[v.Current.Group] {
			return ErrPrimaryConflict
		}
		groups[v.Current.Group] = true
		ps := []Primary{v.Current}
		if v.Pending != nil {
			ps = append(ps, *v.Pending)
		}
		for _, a := range ps {
			if (instances[a.Instance] != "" && instances[a.Instance] != p.ShardID) || (addresses[a.Address] != "" && addresses[a.Address] != p.ShardID) {
				return ErrPrimaryConflict
			}
			instances[a.Instance], addresses[a.Address] = p.ShardID, p.ShardID
		}
		next[p.ShardID] = v
	}
	for id, v := range old {
		if _, ok := next[id]; !ok && v.Pending != nil {
			return ErrPrimaryConflict
		}
	}
	if len(next) == len(old) {
		unchanged := true
		for id := range next {
			if _, ok := old[id]; !ok {
				unchanged = false
				break
			}
		}
		if unchanged {
			return nil
		}
	}
	if _, err = c.ExecContext(ctx, `DELETE FROM shard_primaries`); err != nil {
		return err
	}
	for id, v := range next {
		b, _ := json.Marshal(v)
		if _, err = c.ExecContext(ctx, `INSERT INTO shard_primaries VALUES(?,?)`, id, string(b)); err != nil {
			return err
		}
	}
	_, err = c.ExecContext(ctx, "COMMIT")
	return err
}

// CreatePrimaryStore is explicit initialization only. Existing state must be
// opened, never rebuilt from an older Server/YAML snapshot after a restart.
func CreatePrimaryStore(ctx context.Context, db *sql.DB, primaries []Primary) (*PrimaryStore, error) {
	if len(primaries) == 0 || len(primaries) > 2*MaxNodes {
		return nil, errors.New("invalid primary count")
	}
	ids, groups, instances, addresses := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, p := range primaries {
		if !p.valid() || ids[p.ShardID] || groups[p.Group] || instances[p.Instance] || addresses[p.Address] {
			return nil, ErrPrimaryConflict
		}
		ids[p.ShardID], groups[p.Group], instances[p.Instance], addresses[p.Address] = true, true, true, true
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE shard_primaries (shard_id TEXT PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
		return nil, err
	}
	for _, p := range primaries {
		b, _ := json.Marshal(PrimaryState{Current: p})
		if _, err = tx.ExecContext(ctx, `INSERT INTO shard_primaries VALUES (?,?)`, p.ShardID, string(b)); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &PrimaryStore{db: db}, nil
}

func OpenPrimaryStore(ctx context.Context, db *sql.DB) (*PrimaryStore, error) {
	s := &PrimaryStore{db: db}
	if _, err := s.Snapshot(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func decodePrimary(b string) (PrimaryState, error) {
	var s PrimaryState
	if len(b) > 8192 {
		return s, errors.New("oversized shard primary state")
	}
	d := json.NewDecoder(bytes.NewBufferString(b))
	d.DisallowUnknownFields()
	var extra any
	if d.Decode(&s) != nil || d.Decode(&extra) != io.EOF || !s.Current.valid() ||
		(s.Pending != nil && !primarySuccessor(s.Current, *s.Pending)) {
		return s, errors.New("invalid persisted shard primary")
	}
	return s, nil
}

// Resolve refuses pending state, including after process restart. This is a
// snapshot, NOT a request lease: runtime still must hold its admission barrier.
func (s *PrimaryStore) Resolve(ctx context.Context, id string) (Primary, error) {
	var b string
	if err := s.db.QueryRowContext(ctx, `SELECT body FROM shard_primaries WHERE shard_id=?`, id).Scan(&b); err != nil {
		return Primary{}, err
	}
	v, err := decodePrimary(b)
	if err != nil {
		return Primary{}, err
	}
	if v.Current.ShardID != id {
		return Primary{}, ErrPrimaryConflict
	}
	if v.Pending != nil {
		return Primary{}, ErrPrimaryPending
	}
	return v.Current, nil
}

// Snapshot includes pending switches. Recovery must finish their barriers before
// serving the affected shard; it must not silently fall back to Current.
func (s *PrimaryStore) Snapshot(ctx context.Context) ([]PrimaryState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT shard_id,body FROM shard_primaries ORDER BY shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PrimaryState
	groups, instances, addresses := map[string]bool{}, map[string]string{}, map[string]string{}
	for rows.Next() {
		var id, b string
		if err := rows.Scan(&id, &b); err != nil {
			return nil, err
		}
		v, err := decodePrimary(b)
		if err != nil {
			return nil, err
		}
		if v.Current.ShardID != id || groups[v.Current.Group] {
			return nil, ErrPrimaryConflict
		}
		groups[v.Current.Group] = true
		ps := []Primary{v.Current}
		if v.Pending != nil {
			ps = append(ps, *v.Pending)
		}
		for _, p := range ps {
			if (instances[p.Instance] != "" && instances[p.Instance] != id) || (addresses[p.Address] != "" && addresses[p.Address] != id) {
				return nil, ErrPrimaryConflict
			}
			instances[p.Instance], addresses[p.Address] = id, id
		}
		out = append(out, v)
		if len(out) > 2*MaxNodes {
			return nil, ErrPrimaryConflict
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("empty shard primary state")
	}
	return out, nil
}

// Prepare accepts only one switch per shard. Exact replay is idempotent; another
// election must wait for reconciliation, not overwrite unresolved old work.
func (s *PrimaryStore) Prepare(ctx context.Context, old, next Primary) error {
	if !primarySuccessor(old, next) {
		return ErrPrimaryConflict
	}
	return s.update(ctx, old.ShardID, func(v *PrimaryState) error {
		if v.Current == next && v.Pending == nil {
			return nil
		}
		if v.Current != old || (v.Pending != nil && *v.Pending != next) {
			return ErrPrimaryConflict
		}
		v.Pending = &next
		return nil
	})
}

// Complete is an acknowledgement from the runtime after draining/fencing old
// requests, validating the new master, invalidating hot cache and reconciling
// migration protection. Calling this method alone performs NONE of those steps.
func (s *PrimaryStore) Complete(ctx context.Context, next Primary) error {
	if !next.valid() {
		return ErrPrimaryConflict
	}
	return s.update(ctx, next.ShardID, func(v *PrimaryState) error {
		if v.Current == next && v.Pending == nil {
			return nil
		}
		if v.Pending == nil || *v.Pending != next {
			return ErrPrimaryConflict
		}
		v.Current, v.Pending = next, nil
		return nil
	})
}

func (s *PrimaryStore) update(ctx context.Context, id string, fn func(*PrimaryState) error) error {
	// Serialize the entire snapshot validation across connections. Conditional
	// writes alone would allow two different shards to claim the same address.
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err = c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer c.ExecContext(context.Background(), "ROLLBACK")
	var raw string
	if err = c.QueryRowContext(ctx, `SELECT body FROM shard_primaries WHERE shard_id=?`, id).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPrimaryConflict
		}
		return err
	}
	v, err := decodePrimary(raw)
	if err != nil {
		return err
	}
	if v.Current.ShardID != id {
		return ErrPrimaryConflict
	}
	if err = fn(&v); err != nil {
		return err
	}
	// Validate candidate endpoints against both current and pending peers.
	rows, err := c.QueryContext(ctx, `SELECT body FROM shard_primaries WHERE shard_id<>?`, id)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b string
		if err = rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		peer, e := decodePrimary(b)
		if e != nil {
			rows.Close()
			return e
		}
		ours, theirs := []Primary{v.Current}, []Primary{peer.Current}
		if v.Pending != nil {
			ours = append(ours, *v.Pending)
		}
		if peer.Pending != nil {
			theirs = append(theirs, *peer.Pending)
		}
		for _, a := range ours {
			for _, b := range theirs {
				if a.Group == b.Group || a.Instance == b.Instance || a.Address == b.Address {
					rows.Close()
					return ErrPrimaryConflict
				}
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	b, _ := json.Marshal(v)
	if _, err = c.ExecContext(ctx, `UPDATE shard_primaries SET body=? WHERE shard_id=?`, string(b), id); err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, "COMMIT")
	return err
}
