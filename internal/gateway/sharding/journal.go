package sharding

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	_ "modernc.org/sqlite"
)

var ErrHandoffConflict = errors.New("handoff identity or state conflict")

type HandoffState string

const (
	Prepared  HandoffState = "prepared"
	Committed HandoffState = "committed"
	Cleaned   HandoffState = "cleaned"
)

// Handoff describes durable ownership, not an execution lease. The caller must
// fence backend operations before copying, committing ownership or cleaning.
// In particular, retrying Prepare does not authorize a second concurrent copy.
type Handoff struct {
	TaskID    string
	Sequence  int64
	Key       []byte
	State     HandoffState
	Placement Placement
}

// Journal is a local, disk-backed journal for ONE cold topology transition.
// It is separate from Server's pilot.db. Only one process may open it; this
// local lock does NOT fence commands already accepted by Redis/Kvrocks.
// Finalization retains task identity/target snapshots. Record reclamation is
// permitted only after publication; runtime proves backend fencing and cleanup.
type Journal struct {
	db       *sql.DB
	lock     *os.File
	taskID   string
	change   *Transition
	once     sync.Once
	closeErr error
}

// Matches binds runtime activation to the same immutable snapshots as the log.
func (j *Journal) Matches(change *Transition) bool {
	return j != nil && j.change != nil && change != nil && j.change.from.Equal(change.from) && j.change.to.Equal(change.to)
}

// CreateJournal exclusively creates a private task directory. Its parent must
// already exist on persistent local disk. Failure may leave a partial directory;
// neither Create nor Open silently replaces it. Do not use a temporary directory
// in production or automatically create a replacement for a missing active task.
func CreateJournal(ctx context.Context, dir, taskID string, from, to *Topology) (*Journal, error) {
	return openJournal(ctx, dir, taskID, from, to, true)
}

// OpenJournal requires an existing journal and exact task/topology identity.
// Missing, empty, incompatible or corrupt journals are errors, never "unmoved".
func OpenJournal(ctx context.Context, dir, taskID string, from, to *Topology) (*Journal, error) {
	return openJournal(ctx, dir, taskID, from, to, false)
}

func openJournal(ctx context.Context, dir, taskID string, from, to *Topology, create bool) (_ *Journal, err error) {
	change, err := NewTransition(from, to)
	if err != nil {
		return nil, err
	}
	if !validID(taskID) {
		return nil, errors.New("journal requires a stable task identifier")
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if create {
		if err := os.Mkdir(dir, 0700); err != nil {
			return nil, err
		}
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("journal directory must be private and not a symlink")
	}
	lockPath := filepath.Join(dir, "writer.lock")
	if info, e := os.Lstat(lockPath); e == nil && !info.Mode().IsRegular() {
		return nil, errors.New("journal lock must be a regular file")
	} else if e != nil && !os.IsNotExist(e) {
		return nil, e
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	j := &Journal{lock: lock, taskID: taskID, change: change}
	defer func() {
		if err != nil {
			j.Close()
		}
	}()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("journal already open: %w", err)
	}
	path := filepath.Join(dir, "handoffs.db")
	if create {
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		if e = f.Close(); e != nil {
			return nil, e
		}
	}
	info, err = os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("journal database must be a private regular file")
	}
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)"}}
	u.RawQuery = q.Encode()
	j.db, err = sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	// One connection serializes read-modify-write transactions without SQLite
	// WAL snapshot upgrade races; key records stay on disk, not in a Go map.
	j.db.SetMaxOpenConns(1)
	j.db.SetMaxIdleConns(1)
	if create {
		if err = j.initialize(ctx); err != nil {
			return nil, err
		}
	}
	if err = j.validate(ctx); err != nil {
		return nil, err
	}
	if create {
		// SQLite syncs database/WAL contents; persist newly created directory
		// entries too before publishing this journal to a migration controller.
		for _, p := range []string{dir, filepath.Dir(dir)} {
			f, e := os.Open(p)
			if e != nil {
				return nil, e
			}
			e = f.Sync()
			closeErr := f.Close()
			if e != nil {
				return nil, e
			}
			if closeErr != nil {
				return nil, closeErr
			}
		}
	}
	return j, nil
}

func (j *Journal) initialize(ctx context.Context) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, stmt := range []string{
		`CREATE TABLE task (id INTEGER PRIMARY KEY CHECK(id=1), task_id TEXT NOT NULL, source BLOB NOT NULL, target BLOB NOT NULL)`,
		`CREATE TABLE handoff (sequence INTEGER PRIMARY KEY AUTOINCREMENT, key BLOB NOT NULL UNIQUE, state TEXT NOT NULL CHECK(state IN ('prepared','committed','cleaned')))`,
		`PRAGMA user_version=1`,
	} {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	from, _ := j.change.from.MarshalJSON()
	to, _ := j.change.to.MarshalJSON()
	if _, err = tx.ExecContext(ctx, `INSERT INTO task VALUES(1,?,?,?)`, j.taskID, from, to); err != nil {
		return err
	}
	return tx.Commit()
}

func (j *Journal) validate(ctx context.Context) error {
	var version, synchronous int
	var mode, task string
	var source, target []byte
	for _, check := range []struct {
		query string
		dest  any
	}{
		{"PRAGMA user_version", &version}, {"PRAGMA synchronous", &synchronous}, {"PRAGMA journal_mode", &mode},
	} {
		if err := j.db.QueryRowContext(ctx, check.query).Scan(check.dest); err != nil {
			return err
		}
	}
	if (version < 1 || version > 3) || synchronous != 2 || mode != "wal" {
		return errors.New("incompatible journal format or durability settings")
	}
	if err := j.db.QueryRowContext(ctx, `SELECT task_id,source,target FROM task WHERE id=1`).Scan(&task, &source, &target); err != nil {
		return err
	}
	from, err := Decode(bytes.NewReader(source))
	if err != nil {
		return err
	}
	to, err := Decode(bytes.NewReader(target))
	if err != nil {
		return err
	}
	if task != j.taskID || !from.Equal(j.change.from) || !to.Equal(j.change.to) {
		return errors.New("journal task or topology mismatch")
	}
	// Validate the record schema without scanning a potentially large journal.
	rows, err := j.db.QueryContext(ctx, `SELECT sequence,key,state FROM handoff LIMIT 0`)
	if err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if version == 1 {
		if err := j.upgradeWorkerSchema(ctx); err != nil {
			return err
		}
	}
	if _, err = j.WorkerProgress(ctx); err != nil {
		return err
	}
	if version < 3 {
		if err = j.upgradeFinalSchema(ctx); err != nil {
			return err
		}
	}
	_, err = j.FinalPhase(ctx)
	return err
}

func (j *Journal) Close() error {
	j.once.Do(func() {
		if j.db != nil {
			j.closeErr = j.db.Close()
		}
		if j.lock != nil {
			if err := j.lock.Close(); j.closeErr == nil {
				j.closeErr = err
			}
		}
	})
	return j.closeErr
}

// Prepare durably creates a record only for a key that changes ownership.
// Repeated scans/requests return the existing record, including committed or
// cleaned records. Empty and binary keys are valid; nil means the empty key.
func (j *Journal) Prepare(ctx context.Context, key []byte) (Handoff, error) {
	if len(key) > 4096 {
		return Handoff{}, errors.New("handoff key exceeds gateway key limit")
	}
	if !j.change.DesiredPlacement(key).Moves() {
		return Handoff{}, errors.New("key does not change ownership")
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Handoff{}, err
	}
	defer tx.Rollback()
	// Avoid advancing AUTOINCREMENT or rewriting WAL for a duplicate scan.
	var phase FinalPhase
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM finalization WHERE id=1`).Scan(&phase); err != nil {
		return Handoff{}, err
	}
	if phase != FinalActive {
		return Handoff{}, errors.New("published task cannot prepare handoffs")
	}
	h, err := j.scan(tx.QueryRowContext(ctx, `SELECT sequence,key,state FROM handoff WHERE key=?`, binaryKey(key)))
	if err == nil {
		if err = tx.Commit(); err != nil {
			return Handoff{}, err
		}
		return h, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Handoff{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO handoff(key,state) VALUES(?,'prepared')`, binaryKey(key)); err != nil {
		return Handoff{}, err
	}
	h, err = j.scan(tx.QueryRowContext(ctx, `SELECT sequence,key,state FROM handoff WHERE key=?`, binaryKey(key)))
	if err != nil {
		return Handoff{}, err
	}
	if err = tx.Commit(); err != nil {
		return Handoff{}, err
	}
	return h, nil
}

type rowScanner interface{ Scan(...any) error }

func (j *Journal) scan(row rowScanner) (Handoff, error) {
	h := Handoff{TaskID: j.taskID}
	if err := row.Scan(&h.Sequence, &h.Key, &h.State); err != nil {
		return Handoff{}, err
	}
	h.Placement = j.change.DesiredPlacement(h.Key)
	if h.Sequence <= 0 || len(h.Key) > 4096 || !h.Placement.Moves() || (h.State != Prepared && h.State != Committed && h.State != Cleaned) {
		return Handoff{}, errors.New("invalid persisted handoff")
	}
	return h, nil
}

func binaryKey(key []byte) []byte {
	if key == nil {
		return []byte{}
	}
	return key
}

// Lookup distinguishes an absent record from every storage error. A false
// found value is usable only when err == nil.
func (j *Journal) Lookup(ctx context.Context, key []byte) (h Handoff, found bool, err error) {
	h, err = j.scan(j.db.QueryRowContext(ctx, `SELECT sequence,key,state FROM handoff WHERE key=?`, binaryKey(key)))
	if errors.Is(err, sql.ErrNoRows) {
		return Handoff{}, false, nil
	}
	return h, err == nil, err
}

// Commit records the point after which ALL access is to the target, including
// when the target is absent. Call only after backend fencing and full copy (or
// confirmed empty-source handoff). This method itself does no backend I/O.
func (j *Journal) Commit(ctx context.Context, h Handoff) error {
	return j.advance(ctx, h, Prepared, Committed)
}

// MarkCleaned acknowledges safe source cleanup without deleting the ownership
// record. It must never be called just because a delete request was sent.
func (j *Journal) MarkCleaned(ctx context.Context, h Handoff) error {
	return j.advance(ctx, h, Committed, Cleaned)
}

func (j *Journal) advance(ctx context.Context, h Handoff, before, after HandoffState) error {
	if h.TaskID != j.taskID || h.Sequence <= 0 || h.Placement != j.change.DesiredPlacement(h.Key) {
		return ErrHandoffConflict
	}
	// An exact duplicate ACK is idempotent; skipping/backtracking a phase or
	// carrying an identity from another key/task is not. SQLite commits this
	// update with synchronous=FULL before a successful return.
	r, err := j.db.ExecContext(ctx, `UPDATE handoff SET state=? WHERE sequence=? AND key=? AND state IN (?,?)`, after, h.Sequence, binaryKey(h.Key), before, after)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrHandoffConflict
	}
	return nil
}

type Location struct {
	Node         Node
	NeedsHandoff bool
	State        HandoffState // empty means no record, not a storage failure
}

// Locate resolves journal ownership, not backend health or request admission.
// Changed but uncommitted keys MUST complete/recover handoff before executing
// a normal request. A committed target miss must never fall back to the source.
func (j *Journal) Locate(ctx context.Context, key []byte) (Location, error) {
	h, found, err := j.Lookup(ctx, key)
	if err != nil {
		return Location{}, err
	}
	p := j.change.DesiredPlacement(key)
	if found && (h.State == Committed || h.State == Cleaned) {
		return Location{Node: p.Target, State: h.State}, nil
	}
	return Location{Node: p.Source, NeedsHandoff: p.Moves(), State: h.State}, nil
}

// Page walks durable records using an opaque monotonic sequence, not OFFSET or
// an in-memory key map. Includes cleaned records. It is NOT a snapshot or proof
// of task completion; concurrent new prepares require controller coordination.
// Recovery must inspect state again under key coordination before acting.
func (j *Journal) Page(ctx context.Context, after int64, limit int) ([]Handoff, error) {
	if after < 0 || limit < 1 || limit > 1000 {
		return nil, errors.New("invalid journal page bounds")
	}
	rows, err := j.db.QueryContext(ctx, `SELECT sequence,key,state FROM handoff WHERE sequence>? ORDER BY sequence LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Handoff
	for rows.Next() {
		h, err := j.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
