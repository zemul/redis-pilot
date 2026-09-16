package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func coldTaskTopology(rev uint64, nodes []apitypes.ColdShard) (*sharding.Topology, error) {
	c := sharding.Config{Algorithm: sharding.Algorithm, Revision: rev}
	for _, n := range nodes {
		c.Nodes = append(c.Nodes, sharding.Node{ID: n.ID, Address: n.Address})
	}
	return sharding.New(c)
}

// Existing per-Gateway machine credential, not a Redis password. A control
// outage does not stop serving or an already durable local migration worker.
func (s *Service) shardControlRequest(ctx context.Context, method, path string, body any, dst any) error {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.cfg.ControlPlane.ServerEndpoint, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.reporter.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.reporter.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("control plane rejected request")
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return errors.New("invalid control plane response")
	}
	var envelope struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
	}
	if json.Unmarshal(b, &envelope) != nil || !envelope.Success {
		return errors.New("invalid control plane response")
	}
	if dst != nil {
		return json.Unmarshal(envelope.Data, dst)
	}
	return nil
}

func (s *Service) runShardControl(ctx context.Context) {
	for ctx.Err() == nil {
		if err := s.shardControlOnce(ctx); err != nil {
			s.cache.events.WithLabelValues("control_retry").Inc()
		}
		if migrationWait(ctx, 2*time.Second) != nil {
			return
		}
	}
}

func (s *Service) shardControlOnce(ctx context.Context) error {
	path := "/api/v1/gateway/control/" + url.PathEscape(s.cfg.GatewayID) + "/clusters/" + url.PathEscape(s.cfg.ControlPlane.ClusterID)
	var c apitypes.ColdCluster
	if err := s.shardControlRequest(ctx, "GET", path, nil, &c); err != nil {
		return err
	}
	if c.ID != s.cfg.ServiceID || c.GatewayID != s.cfg.GatewayID || c.NodeID != s.cfg.ControlPlane.NodeID || c.HotAddress != s.cfg.RedisAddress {
		return errors.New("cluster placement mismatch")
	}
	if c.Task == nil {
		return nil
	}
	if c.Task.ClusterID != c.ID {
		return errors.New("task cluster mismatch")
	}
	r := s.acceptColdTask(ctx, c.Task)
	return s.shardControlRequest(ctx, "POST", path+"/report", r, nil)
}

func (s *Service) acceptColdTask(ctx context.Context, t *apitypes.ColdTask) apitypes.ColdTaskReport {
	report := apitypes.ColdTaskReport{TaskID: t.ID, ToRevision: t.ToRevision, Phase: "pending"}
	fail := func(code string) apitypes.ColdTaskReport { report.ErrorCode = code; return report }
	from, err := coldTaskTopology(t.FromRevision, t.Source)
	if err != nil {
		return fail("task_conflict")
	}
	to, err := coldTaskTopology(t.ToRevision, t.Target)
	if err != nil {
		return fail("task_conflict")
	}
	if _, err = sharding.NewTransition(from, to); err != nil {
		return fail("task_conflict")
	}
	r := s.cache
	disk := r.shardState
	shards := r.shards
	if disk == nil || shards == nil {
		return fail("state_unavailable")
	}
	// One polling goroutine owns disk.manifest. The worker owns only its
	// journal and runtime topology; topology lock serializes task activation.
	shards.mu.Lock()
	locked := true
	defer func() {
		if locked {
			shards.mu.Unlock()
		}
	}()
	if disk.manifest.Task != nil && disk.manifest.Task.ID == t.ID {
		source, _ := sharding.New(disk.manifest.Source)
		if !source.Equal(from) || !disk.journal.Target().Equal(to) {
			return fail("task_conflict")
		}
		phase, err := disk.journal.FinalPhase(ctx)
		if err != nil {
			return fail("state_unavailable")
		}
		if phase != sharding.FinalComplete && shards.journal == nil {
			if err := shards.beginTransitionLocked(to, disk.journal); err != nil {
				return fail("state_unavailable")
			}
		}
		report.Phase = string(phase)
		p, err := disk.journal.WorkerProgress(ctx)
		if err != nil {
			return fail("state_unavailable")
		}
		report.Pages, report.Attempts = p.Pages, p.Attempts
		if p.LastError != "" && phase == sharding.FinalActive {
			report.ErrorCode = "migration_retry"
		}
		return report
	}
	if shards.change != nil || shards.retiring != nil || shards.journal != nil || !shards.topology.Equal(from) {
		return fail("task_conflict")
	}
	if shards.validateTopology(to) != nil {
		return fail("waiting_configuration")
	}
	if s.backendPassword != "" {
		return fail("waiting_configuration")
	}
	for _, n := range shards.nodes {
		if n.password != "" {
			return fail("waiting_configuration")
		}
	}
	// Preflight may wait on an unavailable target. It must not hold the
	// admission barrier for unaffected ordinary traffic.
	shards.mu.Unlock()
	locked = false
	budget := s.cfg.workspaceBytes()
	if !s.acquireWorkspace(budget) {
		return fail("state_unavailable")
	}
	defer s.releaseWorkspace(budget)
	step, cancel := context.WithTimeout(ctx, s.cfg.Cache.Sharding.Migration.options().StepTimeout)
	defer cancel()
	old := map[string]bool{}
	for _, n := range from.Config().Nodes {
		old[n.ID] = true
	}
	change, _ := sharding.NewTransition(from, to)
	checked := map[string]bool{}
	for _, n := range change.TraversalNodes() {
		if checked[n.ID] {
			continue
		}
		checked[n.ID] = true
		if err := shards.preflightCold(step, shards.nodes[n.ID], !old[n.ID]); err != nil {
			return fail("preflight_failed")
		}
	}
	shards.mu.Lock()
	locked = true
	if shards.change != nil || shards.retiring != nil || shards.journal != nil || !shards.topology.Equal(from) {
		return fail("task_conflict")
	}
	p, err := disk.taskPath(t.ID)
	if err != nil {
		return fail("task_conflict")
	}
	if _, err = os.Lstat(p); os.IsNotExist(err) {
		j, e := sharding.CreateJournal(step, p, t.ID, from, to)
		if e != nil {
			return fail("state_unavailable")
		}
		j.Close()
	} else if err != nil {
		return fail("state_unavailable")
	}
	if err := disk.bindTask(step, t.ID, to); err != nil {
		return fail("state_unavailable")
	}
	if err := shards.beginTransitionLocked(to, disk.journal); err != nil {
		return fail("state_unavailable")
	}
	report.Phase = "active"
	return report
}

// Read-only preflight. Never silently turn on persistence or clear a target.
// The full-copy protocol preserves absolute TTL, so public tasks remain local.
func (s *cacheShards) preflightCold(ctx context.Context, n *cacheShardNode, empty bool) error {
	c, err := s.raw(ctx, n)
	if err != nil {
		return err
	}
	defer c.Close()
	b, err := c.call(ctx, "INFO", "all")
	if err != nil {
		return err
	}
	info, ok := frameString(b)
	if !ok || !strings.Contains(info, "kvrocks_version:") || !strings.Contains(info, "role:master") || !strings.Contains(info, "cluster_enabled:0") {
		return errors.New("requires standalone primary Kvrocks")
	}
	for name, want := range map[string]string{"rocksdb.write_options.sync": "yes", "rocksdb.write_options.disable_wal": "no"} {
		b, err := c.call(ctx, "CONFIG", "GET", name)
		if err != nil {
			return err
		}
		parts, err := frameParts(b)
		if err != nil || len(parts) != 2 {
			return errors.New("persistence preflight unavailable")
		}
		v, ok := frameString(parts[1])
		if !ok || v != want {
			return errors.New("cold durability precondition not met")
		}
	}
	if !empty {
		return nil
	}
	cursor := "0"
	for {
		b, err := c.call(ctx, "SCAN", cursor, "COUNT", "128")
		if err != nil {
			return err
		}
		parts, err := frameParts(b)
		if err != nil || len(parts) != 2 {
			return errors.New("target scan failed")
		}
		cursor, ok = frameString(parts[0])
		if !ok {
			return errors.New("invalid target cursor")
		}
		keys, err := frameParts(parts[1])
		if err != nil {
			return err
		}
		for _, k := range keys {
			key, ok := frameString(k)
			if !ok || (key != s.r.guard && key != cachePrefix+"cold-only-policy") {
				return errors.New("new target is not empty")
			}
		}
		if cursor == "0" {
			return nil
		}
	}
}
