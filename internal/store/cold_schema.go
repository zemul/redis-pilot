package store

import "database/sql"

// Database guards cover both snapshot writers and direct store methods. In
// particular operation locks are checked before handlers contact an Agent.
func createColdSchema(tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TRIGGER IF NOT EXISTS instance_profile_immutable BEFORE UPDATE ON instances WHEN NEW.profile!=OLD.profile BEGIN SELECT RAISE(ABORT,'instance profile is immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS profile_group_change BEFORE UPDATE ON instance_groups WHEN EXISTS(SELECT 1 FROM instances WHERE grp=OLD.name AND profile!='') AND (NEW.type!=OLD.type OR NEW.engine!=OLD.engine OR NEW.category!=OLD.category OR NEW.proxy!=OLD.proxy) BEGIN SELECT RAISE(ABORT,'dedicated instance purpose cannot change'); END`,
		`CREATE TRIGGER IF NOT EXISTS profile_group_member BEFORE INSERT ON instances WHEN NOT EXISTS(SELECT 1 FROM instances WHERE name=NEW.name) AND EXISTS(SELECT 1 FROM instances WHERE grp=NEW.grp AND profile!='') BEGIN SELECT RAISE(ABORT,'dedicated instance replicas are not supported'); END`,
		`CREATE TRIGGER IF NOT EXISTS profile_instance_topology BEFORE UPDATE ON instances WHEN OLD.profile!='' AND (NEW.grp!=OLD.grp OR NEW.role!=OLD.role OR NEW.replica_of!=OLD.replica_of) BEGIN SELECT RAISE(ABORT,'dedicated instance topology is immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS profile_group_member_update BEFORE UPDATE ON instances WHEN NEW.grp!=OLD.grp AND EXISTS(SELECT 1 FROM instances WHERE grp=NEW.grp AND profile!='') BEGIN SELECT RAISE(ABORT,'cannot join dedicated instance group'); END`,
		`CREATE TABLE IF NOT EXISTS cold_ingress(cluster_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,address TEXT NOT NULL,port INTEGER NOT NULL UNIQUE,password TEXT NOT NULL,ready INTEGER NOT NULL DEFAULT 0,UNIQUE(node_id,address))`,
		`CREATE TRIGGER IF NOT EXISTS cold_ingress_group_insert BEFORE INSERT ON instance_groups WHEN EXISTS(SELECT 1 FROM cold_ingress WHERE port=COALESCE(json_extract(NEW.proxy,'$.master_port'),0) OR port=COALESCE(json_extract(NEW.proxy,'$.auto_port'),0) OR NEW.name='cold:'||cluster_id) BEGIN SELECT RAISE(ABORT,'Proxy port or name reserved by cold ingress'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_ingress_group_update BEFORE UPDATE ON instance_groups WHEN EXISTS(SELECT 1 FROM cold_ingress WHERE port=COALESCE(json_extract(NEW.proxy,'$.master_port'),0) OR port=COALESCE(json_extract(NEW.proxy,'$.auto_port'),0) OR NEW.name='cold:'||cluster_id) BEGIN SELECT RAISE(ABORT,'Proxy port or name reserved by cold ingress'); END`,
		`CREATE TABLE IF NOT EXISTS cold_clusters(id TEXT PRIMARY KEY,gateway_id TEXT NOT NULL UNIQUE,body TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS cold_tasks(cluster_id TEXT NOT NULL,id TEXT NOT NULL,target_revision INTEGER NOT NULL,body TEXT NOT NULL,PRIMARY KEY(cluster_id,id),UNIQUE(cluster_id,target_revision))`,
		`CREATE TABLE IF NOT EXISTS cold_resources(instance TEXT PRIMARY KEY,cluster_id TEXT NOT NULL)`,
		`CREATE TRIGGER IF NOT EXISTS cold_gateway_node_delete BEFORE DELETE ON servers WHEN EXISTS(SELECT 1 FROM cold_clusters WHERE json_extract(body,'$.node_id')=OLD.name) BEGIN SELECT RAISE(ABORT,'node owns a cold Gateway'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_gateway_node_change BEFORE UPDATE ON servers WHEN EXISTS(SELECT 1 FROM cold_clusters WHERE json_extract(body,'$.node_id')=OLD.name) AND (NEW.name!=OLD.name OR NEW.endpoint!=OLD.endpoint) BEGIN SELECT RAISE(ABORT,'node owns a cold Gateway'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_instance_delete BEFORE DELETE ON instances WHEN EXISTS(SELECT 1 FROM cold_resources WHERE instance=OLD.name) BEGIN SELECT RAISE(ABORT,'instance is reserved by a cold cluster; finish migration before removal'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_instance_change BEFORE UPDATE ON instances WHEN EXISTS(SELECT 1 FROM cold_resources WHERE instance=OLD.name) AND
		(NEW.name!=OLD.name OR NEW.grp!=OLD.grp OR NEW.server!=OLD.server OR NEW.port!=OLD.port OR NEW.role!=OLD.role OR NEW.replica_of!=OLD.replica_of OR NEW.desired_state!=OLD.desired_state OR NEW.persistence!=OLD.persistence OR NEW.config_overrides!=OLD.config_overrides OR NEW.data_path!=OLD.data_path OR NEW.config_path!=OLD.config_path OR NEW.container!=OLD.container OR
		(NEW.lock!=OLD.lock AND COALESCE(json_extract(NEW.lock,'$.HeldBy'),'')!='' AND COALESCE(json_extract(NEW.lock,'$.Operation'),'') NOT IN ('start','backup','backup.create')))
		BEGIN SELECT RAISE(ABORT,'operation conflicts with cold cluster ownership'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_group_member BEFORE INSERT ON instances WHEN NOT EXISTS(SELECT 1 FROM instances WHERE name=NEW.name) AND EXISTS(SELECT 1 FROM instances i JOIN cold_resources r ON r.instance=i.name WHERE i.grp=NEW.grp) BEGIN SELECT RAISE(ABORT,'cold cluster replica changes are not supported'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_group_member_update BEFORE UPDATE ON instances WHEN NEW.grp!=OLD.grp AND EXISTS(SELECT 1 FROM instances i JOIN cold_resources r ON r.instance=i.name WHERE i.grp=NEW.grp) BEGIN SELECT RAISE(ABORT,'cannot join a reserved cold cluster group'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_group_delete BEFORE DELETE ON instance_groups WHEN EXISTS(SELECT 1 FROM instances i JOIN cold_resources r ON r.instance=i.name WHERE i.grp=OLD.name) BEGIN SELECT RAISE(ABORT,'group is reserved by a cold cluster'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_group_change BEFORE UPDATE ON instance_groups WHEN EXISTS(SELECT 1 FROM instances i JOIN cold_resources r ON r.instance=i.name WHERE i.grp=OLD.name) AND (NEW.name!=OLD.name OR NEW.type!=OLD.type OR NEW.engine!=OLD.engine OR NEW.engine_version!=OLD.engine_version OR NEW.proxy!=OLD.proxy) BEGIN SELECT RAISE(ABORT,'group topology or direct Proxy routing conflicts with cold cluster ownership'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_node_delete BEFORE DELETE ON servers WHEN EXISTS(SELECT 1 FROM instances i JOIN cold_resources r ON r.instance=i.name WHERE i.server=OLD.name) BEGIN SELECT RAISE(ABORT,'node is reserved by a cold cluster'); END`,
		`CREATE TRIGGER IF NOT EXISTS cold_node_change BEFORE UPDATE ON servers WHEN EXISTS(SELECT 1 FROM instances i JOIN cold_resources r ON r.instance=i.name WHERE i.server=OLD.name) AND (NEW.name!=OLD.name OR NEW.endpoint!=OLD.endpoint) BEGIN SELECT RAISE(ABORT,'node endpoint is reserved by a cold cluster'); END`,
	} {
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}
	return nil
}
