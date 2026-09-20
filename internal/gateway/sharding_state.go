package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Runtime state is local to one effective Gateway, not a distributed lease.
// The manifest references a separately durable journal; its final phase, not
// YAML or an in-memory worker cursor, determines the routing after a restart.
type shardManifest struct {
	PrimaryTracking bool            `json:"primary_tracking,omitempty"`
	ColdEngine      string          `json:"cold_engine,omitempty"`
	Version         int             `json:"version"`
	GatewayID       string          `json:"gateway_id"`
	ServiceID       string          `json:"service_id"`
	Hot             string          `json:"hot"`
	Source          sharding.Config `json:"source"`
	Task            *shardTaskRef   `json:"task,omitempty"`
}

type shardTaskRef struct {
	ID     string          `json:"id"`
	Target sharding.Config `json:"target"`
}

type shardDiskState struct {
	primaries *sharding.PrimaryStore
	dir       string
	db        *sql.DB
	lock      *os.File
	manifest  shardManifest
	journal   *sharding.Journal // owned even after the runtime finishes retirement
}

func privateShardDir(path string) error {
	f, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !f.IsDir() || f.Mode().Perm()&0077 != 0 {
		return errors.New("sharding state directory must be private and not a symlink")
	}
	return nil
}

func (s *shardDiskState) close() {
	if s.journal != nil {
		s.journal.Close()
		s.journal = nil
	}
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
	if s.lock != nil {
		s.lock.Close()
		s.lock = nil
	}
}

func openShardDisk(ctx context.Context, c Config, create bool) (_ *shardDiskState, err error) {
	dir := c.Cache.Sharding.StateDir
	if create {
		if err := privateShardDir(filepath.Dir(dir)); err != nil {
			return nil, err
		}
		if err := os.Mkdir(dir, 0700); err != nil {
			return nil, err
		}
	}
	if err := privateShardDir(dir); err != nil {
		return nil, err
	}
	s := &shardDiskState{dir: dir}
	defer func() {
		if err != nil {
			s.close()
		}
	}()
	s.lock, err = os.OpenFile(filepath.Join(dir, "runtime.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	info, err := s.lock.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("invalid sharding state lock")
	}
	if err = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.New("sharding state is already in use")
	}
	path := filepath.Join(dir, "runtime.db")
	if create {
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		if e = f.Close(); e != nil {
			return nil, e
		}
		if e = os.Mkdir(filepath.Join(dir, "tasks"), 0700); e != nil {
			return nil, e
		}
	}
	info, err = os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("runtime database must be a private regular file")
	}
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)"}}.Encode()
	s.db, err = sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	s.db.SetMaxOpenConns(1)
	if create {
		t, e := c.Cache.Sharding.registry()
		if e != nil {
			return nil, e
		}
		s.manifest = shardManifest{Version: 1, GatewayID: c.GatewayID, ServiceID: c.ServiceID, Hot: c.RedisAddress, Source: t.Config()}
		s.manifest.ColdEngine = c.Cache.ColdEngine
		b, e := json.Marshal(s.manifest)
		if e != nil {
			return nil, e
		}
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return nil, e
		}
		defer tx.Rollback()
		if _, e = tx.ExecContext(ctx, `CREATE TABLE runtime (id INTEGER PRIMARY KEY CHECK(id=1), manifest BLOB NOT NULL)`); e != nil {
			return nil, e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO runtime VALUES(1,?)`, b); e != nil {
			return nil, e
		}
		if _, e = tx.ExecContext(ctx, `PRAGMA user_version=1`); e != nil {
			return nil, e
		}
		if e = tx.Commit(); e != nil {
			return nil, e
		}
		for _, p := range []string{filepath.Join(dir, "tasks"), dir, filepath.Dir(dir)} {
			f, e := os.Open(p)
			if e != nil {
				return nil, e
			}
			e = f.Sync()
			ce := f.Close()
			if e != nil {
				return nil, e
			}
			if ce != nil {
				return nil, ce
			}
		}
	}
	var version int
	if err = s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, errors.New("unsupported sharding runtime state version")
	}
	var size int
	if err = s.db.QueryRowContext(ctx, `SELECT length(manifest) FROM runtime WHERE id=1`).Scan(&size); err != nil {
		return nil, err
	}
	if size < 1 || size > 1<<20 {
		return nil, errors.New("invalid sharding manifest size")
	}
	var b []byte
	if err = s.db.QueryRowContext(ctx, `SELECT manifest FROM runtime WHERE id=1`).Scan(&b); err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&s.manifest); err != nil {
		return nil, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("invalid trailing sharding manifest")
	}
	m := s.manifest
	if m.Version != 1 || m.GatewayID != c.GatewayID || m.ServiceID != c.ServiceID || m.Hot != c.RedisAddress || (CacheConfig{ColdEngine: m.ColdEngine}).coldEngine() != c.Cache.coldEngine() {
		return nil, errors.New("sharding state identity differs from configuration")
	}
	if m.PrimaryTracking {
		s.primaries, err = sharding.OpenPrimaryStore(ctx, s.db)
		if err != nil {
			return nil, err
		}
	}
	from, err := sharding.New(m.Source)
	if err != nil {
		return nil, err
	}
	if m.Task != nil {
		to, e := sharding.New(m.Task.Target)
		if e != nil {
			return nil, e
		}
		p, e := s.taskPath(m.Task.ID)
		if e != nil {
			return nil, e
		}
		s.journal, err = sharding.OpenJournal(ctx, p, m.Task.ID, from, to)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *shardDiskState) taskPath(id string) (string, error) {
	// IDs are not paths, including the otherwise valid topology IDs '.'/'..'.
	if len(id) == 0 || len(id) > 128 || id == "." || id == ".." {
		return "", errors.New("invalid migration task identifier")
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return "", errors.New("invalid migration task identifier")
		}
	}
	parent := filepath.Join(s.dir, "tasks")
	if err := privateShardDir(parent); err != nil {
		return "", err
	}
	return filepath.Join(parent, id), nil
}

// bindTask persists control-plane assignments under the runtime publication lock.
// A journal must already be durably created; persisting this reference precedes
// beginTransition or any copy. An uncertain SQLite result requires restart,
// never admission of a second task.
func (s *shardDiskState) bindTask(ctx context.Context, id string, to *sharding.Topology) error {
	from, err := sharding.New(s.manifest.Source)
	if err != nil {
		return err
	}
	if s.journal != nil {
		phase, err := s.journal.FinalPhase(ctx)
		if err != nil {
			return err
		}
		if phase != sharding.FinalComplete {
			return errors.New("previous migration is not complete")
		}
		if s.manifest.Task.ID == id {
			return errors.New("migration task identifier cannot be reused")
		}
		from = s.journal.Target()
	}
	p, err := s.taskPath(id)
	if err != nil {
		return err
	}
	j, err := sharding.OpenJournal(ctx, p, id, from, to)
	if err != nil {
		return err
	}
	m := s.manifest
	m.Source, m.Task = from.Config(), &shardTaskRef{ID: id, Target: to.Config()}
	b, err := json.Marshal(m)
	if err == nil {
		_, err = s.db.ExecContext(ctx, `UPDATE runtime SET manifest=? WHERE id=1`, b)
	}
	if err != nil {
		j.Close()
		return err
	}
	if s.journal != nil {
		s.journal.Close()
	}
	s.journal, s.manifest = j, m
	return nil
}

// InitializeShardState is an explicit, offline operation for NEW, empty cold
// backends. It never clears backend data or overwrites an existing directory.
func (s *Service) InitializeShardState(ctx context.Context) error {
	if s.cache == nil || s.cfg.Cache.Sharding == nil {
		return errors.New("initialization requires cache.sharding")
	}
	if s.serving.Load() {
		return errors.New("cannot initialize a serving gateway")
	}
	endpoints, err := s.cache.shardEndpoints()
	if err != nil {
		return err
	}
	var primaries []sharding.Primary
	probe := func(overrides map[string]string) error {
		for _, e := range endpoints {
			address := e.node.Address
			if overrides[e.node.ID] != "" {
				address = overrides[e.node.ID]
			}
			if s.cache.cfg.coldEngine() == "redis" {
				probe := &cacheShards{r: s.cache}
				if err := probe.preflightCold(ctx, &cacheShardNode{node: e.node, address: address, password: e.password}, false); err != nil {
					return err
				}
			}
			c, err := s.cache.connectAddress(ctx, false, address, e.password)
			if err != nil {
				return err
			}
			b, err := c.call(ctx, "DBSIZE")
			c.Close()
			if err != nil {
				return err
			}
			if string(b) != ":0\r\n" {
				return errShardInitializationNonempty
			}
		}
		return nil
	}
	if err := probe(nil); err != nil {
		if !s.cfg.ControlPlane.ManageShards || errors.Is(err, errShardInitializationNonempty) {
			return err
		}
		// A primary may fail before the first Gateway deployment. Resolve its
		// replacement from the authenticated control plane, without changing HRW
		// identity or ever accepting nonempty databases as a fresh installation.
		var c apitypes.ColdCluster
		path := "/api/v1/gateway/control/" + url.PathEscape(s.cfg.GatewayID) + "/clusters/" + url.PathEscape(s.cfg.ControlPlane.ClusterID)
		if err := s.shardControlRequest(ctx, "GET", path, nil, &c); err != nil {
			return err
		}
		want, e := coldTaskTopology(c.Revision, c.Shards)
		configured, _ := s.cfg.Cache.Sharding.registry()
		if e != nil || !want.Equal(configured) || c.Task != nil || c.ID != s.cfg.ServiceID || c.GatewayID != s.cfg.GatewayID || c.NodeID != s.cfg.ControlPlane.NodeID || c.HotAddress != s.cfg.RedisAddress || c.StorageEngine() != s.cfg.Cache.coldEngine() || c.UsesCache() != s.cfg.Cache.enabled() || len(c.Primaries) != len(endpoints) {
			return errors.New("initial primary assignment mismatch")
		}
		overrides := map[string]string{}
		for _, v := range c.Primaries {
			p := sharding.Primary(v)
			if sharding.ValidatePrimary(p) != nil || !s.cache.cfg.allowsBackend(p.Address) || p.Address == s.cfg.RedisAddress || overrides[p.ShardID] != "" {
				return errors.New("invalid initial primary")
			}
			overrides[p.ShardID] = p.Address
			primaries = append(primaries, p)
		}
		for _, e := range endpoints {
			if overrides[e.node.ID] == "" {
				return errors.New("initial primary missing")
			}
		}
		if err := probe(overrides); err != nil {
			return err
		}
	}
	state, err := openShardDisk(ctx, s.cfg, true)
	if err != nil {
		return err
	}
	defer state.close()
	if len(primaries) > 0 {
		if _, err := sharding.CreatePrimaryStore(ctx, state.db, primaries); err != nil {
			return err
		}
		state.manifest.PrimaryTracking = true
		b, err := json.Marshal(state.manifest)
		if err != nil {
			return err
		}
		_, err = state.db.ExecContext(ctx, `UPDATE runtime SET manifest=? WHERE id=1`, b)
		return err
	}
	return nil
}

var errShardInitializationNonempty = errors.New("initialization requires empty cold backends; refusing existing data")

func (r *cacheRuntime) restoreSharding(ctx context.Context) error {
	state, err := openShardDisk(ctx, r.s.cfg, false)
	if err != nil {
		return err
	}
	r.shardState = state
	from, err := sharding.New(state.manifest.Source)
	if err != nil {
		return err
	}
	endpoints, err := r.shardEndpoints()
	if err != nil {
		return err
	}
	if state.journal != nil {
		phase, err := state.journal.FinalPhase(ctx)
		if err != nil {
			return err
		}
		if phase == sharding.FinalComplete {
			// Removed cold nodes may already be offline or absent from config.
			from = state.journal.Target()
		}
	}
	if r.s.cfg.ControlPlane.ManageShards {
		// Managed addresses are trusted only from our durable validated task,
		// never fetched from Server during restart. Ignore obsolete YAML nodes.
		byID := map[string]sharding.Node{}
		for _, n := range from.Config().Nodes {
			byID[n.ID] = n
		}
		if state.journal != nil {
			for _, n := range state.journal.Target().Config().Nodes {
				byID[n.ID] = n
			}
		}
		endpoints = nil
		for _, n := range byID {
			if !r.cfg.allowsBackend(n.Address) {
				return errors.New("invalid durable managed endpoint")
			}
			endpoints = append(endpoints, cacheShardEndpoint{node: n})
		}
	}
	r.shards, err = newCacheShards(r, from, endpoints)
	if err != nil {
		return err
	}
	if err := r.restorePrimaries(ctx); err != nil {
		return err
	}
	if state.journal != nil && !from.Equal(state.journal.Target()) {
		if err := r.shards.beginTransition(state.journal.Target(), state.journal); err != nil {
			return err
		}
	}
	o := r.cfg.Sharding.Migration.options()
	r.shards.workerConfig = &o
	return nil
}
