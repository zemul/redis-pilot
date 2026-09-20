package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"sort"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var ErrColdInvalid = errors.New("invalid cold cluster request")
var ErrColdConflict = errors.New("cold cluster state conflict")
var ErrColdNotFound = errors.New("cold cluster not found")

func coldInvalid(reason string) error { return fmt.Errorf("%w: %s", ErrColdInvalid, reason) }

func coldResource(tx *sql.Tx, name, node, engine string, storage ...bool) (string, error) {
	var host, role, kind, actualEngine, desired, lock, group, proxy string
	var port, count int
	err := tx.QueryRow(`SELECT i.server,i.role,i.port,i.desired_state,i.lock,i.grp,g.type,g.engine,g.proxy FROM instances i JOIN instance_groups g ON i.grp=g.name WHERE i.name=?`, name).Scan(&host, &role, &port, &desired, &lock, &group, &kind, &actualEngine, &proxy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", coldInvalid("instance not found")
	}
	if err != nil {
		return "", err
	}
	replicated := len(storage) > 0 && storage[0] && kind == "replication"
	if actualEngine != engine || (!replicated && (kind != "standalone" || role != "master")) || desired != "running" || port < 1 || port > 65535 || (lock != "null" && lock != "{}" && lock != "") {
		return "", coldInvalid("requires an unlocked running standalone instance with the correct engine")
	}
	var p *apitypes.ProxyConfig
	if json.Unmarshal([]byte(proxy), &p) != nil || (p != nil && (p.AutoPort != 0 || p.MasterPort != 0)) {
		return "", coldInvalid("backend group must not expose a direct Proxy listener")
	}
	if err := tx.QueryRow(`SELECT count(*) FROM instances WHERE grp=?`, group).Scan(&count); err != nil {
		return "", err
	}
	if !replicated && count != 1 {
		return "", coldInvalid("backend group must contain exactly one instance")
	}
	if replicated {
		var invalid int
		if err := tx.QueryRow(`SELECT count(*) FROM instances i JOIN instance_groups g ON i.grp=g.name WHERE i.grp=? AND (i.profile!=? OR i.desired_state!='running' OR COALESCE(json_extract(i.lock,'$.HeldBy'),'')!='' OR (i.name=g.current_master AND (i.role!='master' OR i.replica_of!='')) OR (i.name!=g.current_master AND (i.role!='replica' OR i.replica_of!=g.current_master)))`, group, engine+"-storage").Scan(&invalid); err != nil {
			return "", err
		}
		if invalid != 0 || count < 2 || count > 3 {
			return "", coldInvalid("storage group requires one primary and 1..2 verified dedicated replicas")
		}
		var masters, hosts int
		if err := tx.QueryRow(`SELECT count(*) FROM instances i JOIN instance_groups g ON g.name=i.grp WHERE g.name=? AND i.name=g.current_master AND i.role='master'`, group).Scan(&masters); err != nil {
			return "", err
		}
		if err := tx.QueryRow(`SELECT count(DISTINCT s.endpoint) FROM instances i JOIN servers s ON s.name=i.server WHERE i.grp=?`, group).Scan(&hosts); err != nil {
			return "", err
		}
		if masters != 1 || hosts != count {
			return "", coldInvalid("replicated storage requires a current primary and distinct hosts")
		}
	}
	ip := netip.MustParseAddr("127.0.0.1")
	if host != node {
		var endpoint string
		if err := tx.QueryRow(`SELECT endpoint FROM servers WHERE name=?`, host).Scan(&endpoint); err != nil {
			return "", coldInvalid("backend node not found")
		}
		var err error
		ip, err = netip.ParseAddr(endpoint)
		ip = ip.Unmap()
		if err != nil || !ip.IsPrivate() || ip.Zone() != "" {
			return "", coldInvalid("remote backend node requires a literal private IP endpoint")
		}
	}
	return netip.AddrPortFrom(ip, uint16(port)).String(), nil
}

func resolveColdShards(tx *sql.Tx, nodes []apitypes.ColdShard, host, engine string) ([]apitypes.ColdShard, error) {
	if len(nodes) < 1 || len(nodes) > 128 {
		return nil, coldInvalid("requires 1..128 cold shards")
	}
	out := append([]apitypes.ColdShard(nil), nodes...)
	ids, names, addrs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	groups := map[string]bool{}
	for i := range out {
		n := &out[i]
		if !gatewayDraftName.MatchString(n.ID) || n.Instance == "" || ids[n.ID] || names[n.Instance] {
			return nil, coldInvalid("duplicate or invalid shard/instance identity")
		}
		a, err := coldResource(tx, n.Instance, host, engine, true)
		if err != nil {
			return nil, err
		}
		var group string
		if err := tx.QueryRow(`SELECT grp FROM instances WHERE name=?`, n.Instance).Scan(&group); err != nil {
			return nil, err
		}
		if groups[group] {
			return nil, coldInvalid("a storage group cannot belong to multiple shards")
		}
		groups[group] = true
		if (n.Address != "" && n.Address != a) || addrs[a] {
			return nil, coldInvalid("backend address mismatch or duplicate")
		}
		n.Address = a
		ids[n.ID], names[n.Instance], addrs[a] = true, true, true
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func loadColdCluster(q interface{ QueryRow(string, ...any) *sql.Row }, id string) (*apitypes.ColdCluster, error) {
	var raw string
	if err := q.QueryRow(`SELECT body FROM cold_clusters WHERE id=?`, id).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrColdNotFound
		}
		return nil, err
	}
	var c apitypes.ColdCluster
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, err
	}
	var taskRaw string
	err := q.QueryRow(`SELECT body FROM cold_tasks WHERE cluster_id=? ORDER BY target_revision DESC LIMIT 1`, id).Scan(&taskRaw)
	if err == nil {
		c.Task = &apitypes.ColdTask{}
		err = json.Unmarshal([]byte(taskRaw), c.Task)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	v := apitypes.ColdIngress{ClusterID: c.ID, NodeID: c.NodeID}
	err = q.QueryRow(`SELECT address,port,password!='',ready FROM cold_ingress WHERE cluster_id=?`, id).Scan(&v.GatewayAddress, &v.Port, &v.HasPassword, &v.Ready)
	if err == nil {
		c.Ingress = &v
	} else if err != sql.ErrNoRows {
		return nil, err
	}
	return &c, nil
}
func (d *DB) GetColdCluster(id string) (*apitypes.ColdCluster, error) {
	return loadColdCluster(d.sql, id)
}
func (d *DB) ListColdClusters() ([]apitypes.ColdCluster, error) {
	rows, err := d.sql.Query(`SELECT id FROM cold_clusters ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]apitypes.ColdCluster, 0, len(ids))
	for _, id := range ids {
		c, err := d.GetColdCluster(id)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, nil
}

func saveColdCluster(tx *sql.Tx, c *apitypes.ColdCluster) error {
	v := *c
	v.Task = nil
	v.Ingress = nil
	v.Primaries = nil
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE cold_clusters SET body=? WHERE id=?`, raw, c.ID)
	return err
}
func saveColdTask(tx *sql.Tx, t *apitypes.ColdTask) error {
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO cold_tasks(cluster_id,id,target_revision,body) VALUES(?,?,?,?) ON CONFLICT(cluster_id,id) DO UPDATE SET body=excluded.body`, t.ClusterID, t.ID, t.ToRevision, raw)
	return err
}
func reserveCold(tx *sql.Tx, cluster, instance string) error {
	if instance == "" {
		return nil
	}
	rows, err := tx.Query(`SELECT name FROM instances WHERE grp=(SELECT grp FROM instances WHERE name=?)`, instance)
	if err != nil {
		return err
	}
	var members []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		members = append(members, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, name := range members {
		if err := reserveColdInstance(tx, cluster, name); err != nil {
			return err
		}
	}
	return nil
}

func reserveColdInstance(tx *sql.Tx, cluster, instance string) error {
	if instance == "" {
		return nil
	}
	var owner string
	err := tx.QueryRow(`SELECT cluster_id FROM cold_resources WHERE instance=?`, instance).Scan(&owner)
	if err == nil {
		if owner == cluster {
			return nil
		}
		return ErrColdConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.Exec(`INSERT INTO cold_resources(instance,cluster_id) VALUES(?,?)`, instance, cluster)
	return err
}

func (d *DB) CreateColdCluster(spec apitypes.ColdClusterSpec) (*apitypes.ColdCluster, error) {
	return d.createColdCluster(spec, "")
}

func (d *DB) createColdCluster(spec apitypes.ColdClusterSpec, buildTaskID string) (*apitypes.ColdCluster, error) {
	if !spec.ValidWritePolicy() {
		return nil, coldInvalid("invalid cache write policy")
	}
	if spec.Engine == "" {
		spec.Engine = "kvrocks"
	}
	if !spec.ValidEngine() {
		return nil, coldInvalid("requires Redis without cache, or Kvrocks with optional cache")
	}
	if !spec.UsesCache() && spec.HotInstance != "" {
		return nil, coldInvalid("disabled cache must not reference a hot instance")
	}
	if !gatewayDraftName.MatchString(spec.ID) || !gatewayDraftName.MatchString(spec.GatewayID) || spec.NodeID == "" {
		return nil, coldInvalid("invalid cluster, Gateway or node identity")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE cold_clusters SET body=body WHERE 0`); err != nil {
		return nil, err
	}
	if err := checkClusterBuildTx(tx, spec, buildTaskID); err != nil {
		return nil, err
	}
	var used int
	if err := tx.QueryRow(`SELECT count(*) FROM cold_clusters WHERE id=? OR gateway_id=?`, spec.ID, spec.GatewayID).Scan(&used); err != nil {
		return nil, err
	}
	if used != 0 {
		return nil, ErrColdConflict
	}
	if err := tx.QueryRow(`SELECT count(*) FROM servers WHERE name=?`, spec.NodeID).Scan(&used); err != nil {
		return nil, err
	}
	if used != 1 {
		return nil, coldInvalid("Gateway node not found")
	}
	hot := ""
	if spec.UsesCache() {
		hot, err = coldResource(tx, spec.HotInstance, spec.NodeID, "redis")
		if err != nil {
			return nil, err
		}
	}
	nodes, err := resolveColdShards(tx, spec.Shards, spec.NodeID, spec.StorageEngine())
	if err != nil {
		return nil, err
	}
	spec.Shards = nodes
	for _, n := range nodes {
		if n.Address == hot {
			return nil, coldInvalid("hot and cold endpoints must differ")
		}
	}
	c := &apitypes.ColdCluster{ColdClusterSpec: spec, HotAddress: hot, Revision: 1}
	raw, _ := json.Marshal(c)
	if _, err = tx.Exec(`INSERT INTO cold_clusters(id,gateway_id,body) VALUES(?,?,?)`, c.ID, c.GatewayID, raw); err != nil {
		return nil, err
	}
	for _, name := range append([]string{spec.HotInstance}, coldNames(nodes)...) {
		if err := reserveCold(tx, c.ID, name); err != nil {
			return nil, err
		}
	}
	if buildTaskID != "" {
		if err := finishClusterBuildTx(tx, buildTaskID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	d.bumpGeneration()
	return c, nil
}
func coldNames(nodes []apitypes.ColdShard) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Instance)
	}
	return out
}

func (d *DB) CreateColdTask(id string, req apitypes.ColdTaskRequest) (*apitypes.ColdTask, error) {
	if !gatewayDraftName.MatchString(req.ID) || req.ExpectedRevision < 1 || req.ExpectedRevision >= 1<<53 {
		return nil, coldInvalid("task ID and positive expected_revision required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE cold_clusters SET body=body WHERE 0`); err != nil {
		return nil, err
	}
	c, err := loadColdCluster(tx, id)
	if err != nil {
		return nil, err
	}
	// Retry identity is checked before resolving resources that may since have
	// been legitimately removed after completion.
	var raw string
	if err := tx.QueryRow(`SELECT body FROM cold_tasks WHERE cluster_id=? AND id=?`, id, req.ID).Scan(&raw); err == nil {
		var old apitypes.ColdTask
		if err := json.Unmarshal([]byte(raw), &old); err != nil {
			return nil, err
		}
		want := append([]apitypes.ColdShard(nil), req.Shards...)
		sort.Slice(want, func(i, j int) bool { return want[i].ID < want[j].ID })
		got := append([]apitypes.ColdShard(nil), old.Target...)
		for i := range got {
			if i < len(want) && want[i].Address == "" {
				got[i].Address = ""
			}
		}
		if old.FromRevision == req.ExpectedRevision && reflect.DeepEqual(got, want) {
			return &old, nil
		}
		return nil, ErrColdConflict
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if c.Revision != req.ExpectedRevision || (c.Task != nil && c.Task.Phase != "complete") {
		return nil, ErrColdConflict
	}
	nodes, err := resolveColdShards(tx, req.Shards, c.NodeID, c.StorageEngine())
	if err != nil {
		return nil, err
	}
	oldByID := map[string]apitypes.ColdShard{}
	oldAddr := map[string]string{}
	for _, n := range c.Shards {
		oldByID[n.ID] = n
		oldAddr[n.Address] = n.ID
	}
	changed := len(nodes) != len(c.Shards)
	for _, n := range nodes {
		if n.Address == c.HotAddress {
			return nil, coldInvalid("hot endpoint reused")
		}
		if old, ok := oldByID[n.ID]; ok {
			if old != n {
				return nil, coldInvalid("retained shard identity/address cannot change")
			}
		} else {
			changed = true
		}
		if prev, ok := oldAddr[n.Address]; ok && prev != n.ID {
			return nil, coldInvalid("old address reused under another shard ID")
		}
		if err := reserveCold(tx, id, n.Instance); err != nil {
			return nil, err
		}
	}
	if !changed {
		return nil, coldInvalid("membership is unchanged")
	}
	t := &apitypes.ColdTask{ID: req.ID, ClusterID: id, FromRevision: c.Revision, ToRevision: c.Revision + 1, Source: c.Shards, Target: nodes, Phase: "pending"}
	if err := saveColdTask(tx, t); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return t, nil
}

func coldPhase(p string) int {
	switch p {
	case "pending":
		return 0
	case "active":
		return 1
	case "published":
		return 2
	case "complete":
		return 3
	}
	return -1
}
func (d *DB) ReportColdTask(gateway, id string, r apitypes.ColdTaskReport) error {
	if coldPhase(r.Phase) < 0 || r.Pages < 0 || r.Attempts < 0 {
		return coldInvalid("invalid task progress")
	}
	switch r.ErrorCode {
	case "", "waiting_configuration", "preflight_failed", "state_unavailable", "task_conflict", "migration_retry":
	default:
		return coldInvalid("unknown error code")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Reserve the writer before reading progress: audit/task writes may bypass
	// d.mu and otherwise invalidate this WAL snapshot before its first update.
	if _, err := tx.Exec(`UPDATE cold_clusters SET body=body WHERE 0`); err != nil {
		return err
	}
	c, err := loadColdCluster(tx, id)
	if err != nil {
		return err
	}
	t := c.Task
	if c.GatewayID != gateway || t == nil || t.ID != r.TaskID || t.ToRevision != r.ToRevision {
		return ErrColdConflict
	}
	if coldPhase(r.Phase) < coldPhase(t.Phase) {
		return nil
	} // delayed report must not roll back publication
	t.Phase, t.ErrorCode = r.Phase, r.ErrorCode
	t.Pages = max(t.Pages, r.Pages)
	t.Attempts = max(t.Attempts, r.Attempts)
	if r.Phase == "complete" {
		c.Revision, c.Shards = t.ToRevision, t.Target
		if err := saveColdCluster(tx, c); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM cold_resources WHERE cluster_id=?`, id); err != nil {
			return err
		}
		for _, name := range append([]string{c.HotInstance}, coldNames(c.Shards)...) {
			if err := reserveCold(tx, id, name); err != nil {
				return err
			}
		}
	}
	if err := saveColdTask(tx, t); err != nil {
		return err
	}
	return tx.Commit()
}
