package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func createClusterBuildSchema(tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS cluster_create_requests(cluster_id TEXT PRIMARY KEY,digest TEXT NOT NULL,task_id TEXT NOT NULL UNIQUE,request TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS cluster_builds(task_id TEXT PRIMARY KEY,cluster_id TEXT NOT NULL UNIQUE,gateway_id TEXT NOT NULL UNIQUE,node_id TEXT NOT NULL,spec TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS cluster_build_members(instance TEXT PRIMARY KEY,parent_id TEXT NOT NULL,child_id TEXT NOT NULL UNIQUE)`,
		`CREATE TRIGGER IF NOT EXISTS build_instance_delete BEFORE DELETE ON instances WHEN EXISTS(SELECT 1 FROM cluster_build_members WHERE instance=OLD.name) BEGIN SELECT RAISE(ABORT,'instance belongs to cluster creation'); END`,
		`CREATE TRIGGER IF NOT EXISTS build_instance_update BEFORE UPDATE ON instances WHEN EXISTS(SELECT 1 FROM cluster_build_members WHERE instance=OLD.name) AND (NEW.grp!=OLD.grp OR NEW.server!=OLD.server OR NEW.port!=OLD.port OR NEW.memory!=OLD.memory OR NEW.cpus!=OLD.cpus OR NEW.config_overrides!=OLD.config_overrides OR NEW.desired_state!=OLD.desired_state OR (NEW.lock!=OLD.lock AND COALESCE(json_extract(NEW.lock,'$.HeldBy'),'')!='' AND COALESCE(json_extract(NEW.lock,'$.HeldBy'),'')!=COALESCE(json_extract(OLD.lock,'$.HeldBy'),''))) BEGIN SELECT RAISE(ABORT,'instance belongs to cluster creation'); END`,
		`CREATE TRIGGER IF NOT EXISTS build_foreign_claim BEFORE INSERT ON cold_resources WHEN EXISTS(SELECT 1 FROM cluster_build_members m JOIN cluster_builds b ON b.task_id=m.parent_id WHERE m.instance=NEW.instance AND b.cluster_id!=NEW.cluster_id) BEGIN SELECT RAISE(ABORT,'resource reserved by another cluster creation'); END`,
		`CREATE TRIGGER IF NOT EXISTS build_group_delete BEFORE DELETE ON instance_groups WHEN EXISTS(SELECT 1 FROM instances i JOIN cluster_build_members m ON i.name=m.instance WHERE i.grp=OLD.name) BEGIN SELECT RAISE(ABORT,'group belongs to cluster creation'); END`,
		`CREATE TRIGGER IF NOT EXISTS build_group_update BEFORE UPDATE ON instance_groups WHEN EXISTS(SELECT 1 FROM instances i JOIN cluster_build_members m ON i.name=m.instance WHERE i.grp=OLD.name) AND (NEW.engine_version!=OLD.engine_version OR NEW.password!=OLD.password) BEGIN SELECT RAISE(ABORT,'group belongs to cluster creation'); END`,
		`CREATE TRIGGER IF NOT EXISTS build_node_delete BEFORE DELETE ON servers WHEN EXISTS(SELECT 1 FROM cluster_builds b JOIN tasks t ON t.id=b.task_id WHERE t.status!='success' AND (b.node_id=OLD.name OR EXISTS(SELECT 1 FROM cluster_build_members m JOIN instances i ON m.instance=i.name WHERE m.parent_id=b.task_id AND i.server=OLD.name))) BEGIN SELECT RAISE(ABORT,'node belongs to cluster creation'); END`,
		`CREATE TRIGGER IF NOT EXISTS build_node_update BEFORE UPDATE ON servers WHEN (NEW.endpoint!=OLD.endpoint OR NEW.agent_port!=OLD.agent_port) AND EXISTS(SELECT 1 FROM cluster_builds b JOIN tasks t ON t.id=b.task_id WHERE t.status!='success' AND (b.node_id=OLD.name OR EXISTS(SELECT 1 FROM cluster_build_members m JOIN instances i ON m.instance=i.name WHERE m.parent_id=b.task_id AND i.server=OLD.name))) BEGIN SELECT RAISE(ABORT,'node belongs to cluster creation'); END`,
	} {
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// CreateClusterBuild joins already-reserved child creation tasks. It never
// adopts an arbitrary existing instance just because its name happens to match.
func (d *DB) CreateClusterBuild(spec apitypes.ColdClusterSpec, children []string, task Task) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := createClusterBuildTx(tx, spec, children, task); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	d.bumpGeneration()
	return nil
}

func createClusterBuildTx(tx *sql.Tx, spec apitypes.ColdClusterSpec, children []string, task Task) error {
	if !spec.ValidWritePolicy() {
		return coldInvalid("invalid cache write policy")
	}
	if spec.Engine == "" {
		spec.Engine = "kvrocks"
	}
	if !spec.ValidEngine() || !gatewayDraftName.MatchString(spec.ID) || !gatewayDraftName.MatchString(spec.GatewayID) || spec.NodeID == "" || len(spec.Shards) < 1 || len(spec.Shards) > 128 || task.ID == "" {
		return coldInvalid("invalid cluster creation plan")
	}
	wants := map[string]string{}
	ids := map[string]bool{}
	if spec.UsesCache() {
		if spec.HotInstance == "" {
			return coldInvalid("missing hot instance")
		}
		wants[spec.HotInstance] = "redis-cache"
	} else if spec.HotInstance != "" {
		return coldInvalid("disabled cache has hot instance")
	}
	for _, shard := range spec.Shards {
		if !gatewayDraftName.MatchString(shard.ID) || ids[shard.ID] || shard.Instance == "" || wants[shard.Instance] != "" || shard.Address != "" {
			return coldInvalid("invalid or duplicate shard")
		}
		ids[shard.ID] = true
		wants[shard.Instance] = spec.StorageEngine() + "-storage"
	}
	if len(children) != len(wants) {
		return coldInvalid("one child creation task is required for each resource")
	}
	var count int
	if err := tx.QueryRow(`SELECT count(*) FROM servers WHERE name=?`, spec.NodeID).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return coldInvalid("Gateway node not found")
	}
	if err := tx.QueryRow(`SELECT count(*) FROM cold_clusters WHERE id=? OR gateway_id=?`, spec.ID, spec.GatewayID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return ErrColdConflict
	}
	raw, _ := json.Marshal(spec)
	if _, err := tx.Exec(`INSERT INTO cluster_builds(task_id,cluster_id,gateway_id,node_id,spec) VALUES(?,?,?,?,?)`, task.ID, spec.ID, spec.GatewayID, spec.NodeID, string(raw)); err != nil {
		return ErrColdConflict
	}
	for _, id := range children {
		var name, profile, status, result string
		err := tx.QueryRow(`SELECT i.name,i.profile,t.status,t.result FROM tasks t JOIN profile_provenance p ON p.task_id=t.id JOIN instances i ON i.name=p.instance WHERE t.id=? AND t.kind='profile.create'`, id).Scan(&name, &profile, &status, &result)
		if err != nil || wants[name] != profile || (status != "pending" && status != "running" && status != "success") {
			return coldInvalid("child task does not match current resource ownership")
		}
		if status == "success" {
			var proof struct {
				Ready bool `json:"database_ready"`
			}
			if json.Unmarshal([]byte(result), &proof) != nil || !proof.Ready {
				return coldInvalid("child lacks database readiness verification")
			}
		}
		if err := tx.QueryRow(`SELECT count(*) FROM cold_resources WHERE instance=?`, name).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return ErrColdConflict
		}
		if _, err := tx.Exec(`INSERT INTO cluster_build_members(instance,parent_id,child_id) VALUES(?,?,?)`, name, task.ID, id); err != nil {
			return ErrColdConflict
		}
		delete(wants, name)
	}
	task.Kind, task.Status, task.Phase, task.GroupName, task.Payload = "cluster.provision", "pending", "waiting_resources", spec.ID, string(raw)
	if err := insertTask(task, tx.Exec); err != nil {
		return err
	}
	return nil
}

func (d *DB) ClusterBuildProgress(id string) (ready, total int, err error) {
	err = d.sql.QueryRow(`SELECT count(*),COALESCE(sum(CASE WHEN t.status='success' AND json_valid(t.result) AND json_extract(t.result,'$.database_ready')=1 THEN 1 ELSE 0 END),0) FROM cluster_build_members m JOIN tasks t ON t.id=m.child_id WHERE m.parent_id=?`, id).Scan(&total, &ready)
	return
}

func (d *DB) ClusterBuildChildren(id string) ([]Task, error) {
	rows, err := d.sql.Query(`SELECT child_id FROM cluster_build_members WHERE parent_id=? ORDER BY instance`, id)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var child string
		if err := rows.Scan(&child); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, child)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var tasks []Task
	for _, child := range ids {
		t, err := d.GetTask(child)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, *t)
	}
	return tasks, nil
}

func (d *DB) FinishClusterBuild(id string) (*apitypes.ColdCluster, error) {
	var raw string
	if err := d.sql.QueryRow(`SELECT spec FROM cluster_builds WHERE task_id=?`, id).Scan(&raw); err != nil {
		return nil, err
	}
	var spec apitypes.ColdClusterSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return nil, err
	}
	return d.createColdCluster(spec, id)
}

func finishClusterBuildTx(tx *sql.Tx, id string) error {
	now := time.Now().Format(time.RFC3339)
	if _, err := tx.Exec(`UPDATE tasks SET status='success',phase='cluster_registered',message='Resources verified and cluster registered; Gateway not yet deployed',finished_at=?,updated_at=? WHERE id=?`, now, now, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM cluster_build_members WHERE parent_id=?`, id); err != nil {
		return err
	}
	return nil
}

func checkClusterBuildTx(tx *sql.Tx, spec apitypes.ColdClusterSpec, id string) error {
	var reserved string
	err := tx.QueryRow(`SELECT task_id FROM cluster_builds WHERE cluster_id=? OR gateway_id=?`, spec.ID, spec.GatewayID).Scan(&reserved)
	if err == sql.ErrNoRows {
		if id != "" {
			return ErrColdConflict
		}
		return nil
	}
	if err != nil {
		return err
	}
	if reserved != id || id == "" {
		return ErrColdConflict
	}
	var total, ready int
	if err := tx.QueryRow(`SELECT count(*),COALESCE(sum(CASE WHEN t.status='success' AND json_valid(t.result) AND json_extract(t.result,'$.database_ready')=1 THEN 1 ELSE 0 END),0) FROM cluster_build_members m JOIN tasks t ON m.child_id=t.id WHERE m.parent_id=?`, id).Scan(&total, &ready); err != nil {
		return err
	}
	if total == 0 || total != ready {
		return fmt.Errorf("cluster creation children are not all ready")
	}
	return nil
}
