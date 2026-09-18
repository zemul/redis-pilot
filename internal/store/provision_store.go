package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func createProvisionSchema(tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS profile_provenance(instance TEXT PRIMARY KEY,task_id TEXT NOT NULL UNIQUE)`,
		`CREATE TRIGGER IF NOT EXISTS profile_provenance_delete AFTER DELETE ON instances BEGIN DELETE FROM profile_provenance WHERE instance=OLD.name; END`,
		`CREATE TRIGGER IF NOT EXISTS provision_cluster_claim BEFORE INSERT ON cold_resources WHEN EXISTS(SELECT 1 FROM tasks WHERE kind='profile.create' AND instance_name=NEW.instance AND status IN ('pending','accepted','running')) BEGIN SELECT RAISE(ABORT,'instance provisioning is not complete'); END`,
		`CREATE UNIQUE INDEX IF NOT EXISTS active_profile_create ON tasks(instance_name) WHERE kind='profile.create' AND status IN ('pending','accepted','running')`,
		`CREATE TRIGGER IF NOT EXISTS provision_instance_delete BEFORE DELETE ON instances WHEN EXISTS(SELECT 1 FROM tasks WHERE kind='profile.create' AND instance_name=OLD.name AND status IN ('pending','accepted','running')) BEGIN SELECT RAISE(ABORT,'instance has active provisioning task'); END`,
		`CREATE TRIGGER IF NOT EXISTS provision_instance_update BEFORE UPDATE ON instances WHEN EXISTS(SELECT 1 FROM tasks WHERE kind='profile.create' AND instance_name=OLD.name AND status IN ('pending','accepted','running')) AND (NEW.grp!=OLD.grp OR NEW.server!=OLD.server OR NEW.container!=OLD.container OR NEW.port!=OLD.port OR NEW.memory!=OLD.memory OR NEW.cpus!=OLD.cpus OR NEW.disk!=OLD.disk OR NEW.config_overrides!=OLD.config_overrides OR NEW.desired_state!=OLD.desired_state OR COALESCE(json_extract(NEW.lock,'$.HeldBy'),'')!=COALESCE(json_extract(OLD.lock,'$.HeldBy'),'')) BEGIN SELECT RAISE(ABORT,'instance has active provisioning task'); END`,
		`CREATE TRIGGER IF NOT EXISTS provision_group_delete BEFORE DELETE ON instance_groups WHEN EXISTS(SELECT 1 FROM tasks WHERE kind='profile.create' AND group_name=OLD.name AND status IN ('pending','accepted','running')) BEGIN SELECT RAISE(ABORT,'group has active provisioning task'); END`,
		`CREATE TRIGGER IF NOT EXISTS provision_group_update BEFORE UPDATE ON instance_groups WHEN EXISTS(SELECT 1 FROM tasks WHERE kind='profile.create' AND group_name=OLD.name AND status IN ('pending','accepted','running')) AND (NEW.engine_version!=OLD.engine_version OR NEW.password!=OLD.password) BEGIN SELECT RAISE(ABORT,'group has active provisioning task'); END`,
		`CREATE TRIGGER IF NOT EXISTS provision_server_delete BEFORE DELETE ON servers WHEN EXISTS(SELECT 1 FROM tasks WHERE kind='profile.create' AND server_name=OLD.name AND status IN ('pending','accepted','running')) BEGIN SELECT RAISE(ABORT,'server has active provisioning task'); END`,
		`CREATE TRIGGER IF NOT EXISTS provision_server_update BEFORE UPDATE ON servers WHEN EXISTS(SELECT 1 FROM tasks WHERE kind='profile.create' AND server_name=OLD.name AND status IN ('pending','accepted','running')) AND (NEW.endpoint!=OLD.endpoint OR NEW.agent_port!=OLD.agent_port) BEGIN SELECT RAISE(ABORT,'server has active provisioning task'); END`,
	} {
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// CompleteProvisionTask atomically records the verified container and releases
// the reservation. A crash cannot expose an unlocked but still active task.
func (d *DB) CompleteProvisionTask(id, containerID string) error {
	if containerID == "" {
		return fmt.Errorf("missing container identity")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status, name string
	if err := tx.QueryRow(`SELECT status,instance_name FROM tasks WHERE id=? AND kind='profile.create'`, id).Scan(&status, &name); err != nil {
		return err
	}
	if status == "success" {
		return nil
	}
	if status != "pending" && status != "running" {
		return fmt.Errorf("task is not active")
	}
	var owner string
	if err := tx.QueryRow(`SELECT COALESCE(json_extract(lock,'$.HeldBy'),'') FROM instances WHERE name=?`, name).Scan(&owner); err != nil {
		return err
	}
	if owner != id {
		return fmt.Errorf("provisioning reservation lost")
	}
	now := time.Now().Format(time.RFC3339)
	result, _ := json.Marshal(map[string]any{"container_id": containerID, "database_ready": true, "empty": true})
	if _, err := tx.Exec(`UPDATE tasks SET status='success',phase='database_ready',result=?,error='',message='Database readiness and empty keyspace verified; Gateway readiness is separate',finished_at=?,updated_at=? WHERE id=?`, string(result), now, now, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE instances SET observed_status='running',lock='null' WHERE name=?`, name); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	d.bumpGeneration()
	return nil
}
