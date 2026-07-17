package store

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// ── 实例组 (instance_groups 表) ──────────────────────────────────────────────

// GetAllGroups 读取所有实例组。
func (d *DB) GetAllGroups() (map[string]*apitypes.InstanceGroupState, error) {
	rows, err := d.sql.Query(
		`SELECT name, type, engine, engine_version, category,
		        current_master, topology_status, failover_conflict, envoy, created_at, updated_at
		 FROM instance_groups`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGroups(rows)
}

// UpsertGroup 新增或更新实例组。
func (d *DB) UpsertGroup(name string, g *apitypes.InstanceGroupState) error {
	envoyJSON, _ := json.Marshal(g.Envoy)
	_, err := d.sql.Exec(`
		INSERT INTO instance_groups(name, type, engine, engine_version, category,
		    current_master, topology_status, failover_conflict, envoy, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET
			type              = excluded.type,
			engine            = excluded.engine,
			engine_version    = excluded.engine_version,
			category          = excluded.category,
			current_master    = excluded.current_master,
			topology_status   = excluded.topology_status,
			failover_conflict = excluded.failover_conflict,
			envoy             = excluded.envoy,
			created_at        = excluded.created_at,
			updated_at        = excluded.updated_at`,
		name,
		g.Type, g.Engine, g.EngineVersion, g.Category,
		g.CurrentMaster, g.TopologyStatus, boolToInt(g.FailoverConflict),
		string(envoyJSON), g.CreatedAt, g.UpdatedAt,
	)
	return err
}

// DeleteGroup 删除实例组。
func (d *DB) DeleteGroup(name string) error {
	_, err := d.sql.Exec(`DELETE FROM instance_groups WHERE name = ?`, name)
	return err
}

// ── 实例 (instances 表) ──────────────────────────────────────────────────────

// GetAllInstances 读取所有实例。
func (d *DB) GetAllInstances() (map[string]*apitypes.Instance, error) {
	rows, err := d.sql.Query(
		`SELECT name, grp, role, server, container, port, memory, cpus, disk,
		        password, config_path, data_path, backup_path,
		        persistence, config_overrides, replica_of, backup, status, lock, created_at
		 FROM instances`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanInstances(rows)
}

// UpsertInstance 新增或更新实例。
func (d *DB) UpsertInstance(name string, inst *apitypes.Instance) error {
	persistenceJSON, _ := json.Marshal(inst.Persistence)
	overridesJSON, _ := json.Marshal(inst.ConfigOverrides)
	backupJSON, _ := json.Marshal(inst.Backup)
	lockJSON, _ := json.Marshal(inst.Lock)

	_, err := d.sql.Exec(`
		INSERT INTO instances(name, grp, role, server, container, port, memory, cpus, disk,
		    password, config_path, data_path, backup_path,
		    persistence, config_overrides, replica_of, backup, status, lock, created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET
			grp              = excluded.grp,
			role             = excluded.role,
			server           = excluded.server,
			container        = excluded.container,
			port             = excluded.port,
			memory           = excluded.memory,
			cpus             = excluded.cpus,
			disk             = excluded.disk,
			password         = excluded.password,
			config_path      = excluded.config_path,
			data_path        = excluded.data_path,
			backup_path      = excluded.backup_path,
			persistence      = excluded.persistence,
			config_overrides = excluded.config_overrides,
			replica_of       = excluded.replica_of,
			backup           = excluded.backup,
			status           = excluded.status,
			lock             = excluded.lock,
			created_at       = excluded.created_at`,
		name,
		inst.Group, inst.Role, inst.Server, inst.Container,
		inst.Port, inst.Memory, inst.CPUs, inst.Disk,
		inst.Password, inst.ConfigPath, inst.DataPath, inst.BackupPath,
		string(persistenceJSON), string(overridesJSON),
		inst.ReplicaOf, string(backupJSON),
		inst.Status, string(lockJSON), inst.CreatedAt,
	)
	return err
}

// DeleteInstance 删除实例。
func (d *DB) DeleteInstance(name string) error {
	_, err := d.sql.Exec(`DELETE FROM instances WHERE name = ?`, name)
	return err
}

// GetInstance 读取单个实例，不存在时返回 nil, nil。
func (d *DB) GetInstance(name string) (*apitypes.Instance, error) {
	row := d.sql.QueryRow(
		`SELECT name, grp, role, server, container, port, memory, cpus, disk,
		        password, config_path, data_path, backup_path,
		        persistence, config_overrides, replica_of, backup, status, lock, created_at
		 FROM instances WHERE name = ?`, name)

	inst, err := scanInstance(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return inst, err
}

// WithInstances 在事务中原子执行 read → fn → write。
// fn 接收完整 InstancesState 快照，修改后批量写回。
func (d *DB) WithInstances(fn func(*apitypes.InstancesState) error) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	st, err := loadInstancesStateTx(tx)
	if err != nil {
		return err
	}

	if err := fn(st); err != nil {
		return err
	}

	// 写回 groups
	if _, err := tx.Exec(`DELETE FROM instance_groups`); err != nil {
		return err
	}
	for name, g := range st.Groups {
		envoyJSON, _ := json.Marshal(g.Envoy)
		if _, err := tx.Exec(`
			INSERT INTO instance_groups(name, type, engine, engine_version, category,
			    current_master, topology_status, failover_conflict, envoy, created_at, updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			name,
			g.Type, g.Engine, g.EngineVersion, g.Category,
			g.CurrentMaster, g.TopologyStatus, boolToInt(g.FailoverConflict),
			string(envoyJSON), g.CreatedAt, g.UpdatedAt,
		); err != nil {
			return fmt.Errorf("insert group %s: %w", name, err)
		}
	}

	// 写回 instances
	if _, err := tx.Exec(`DELETE FROM instances`); err != nil {
		return err
	}
	for name, inst := range st.Instances {
		persistenceJSON, _ := json.Marshal(inst.Persistence)
		overridesJSON, _ := json.Marshal(inst.ConfigOverrides)
		backupJSON, _ := json.Marshal(inst.Backup)
		lockJSON, _ := json.Marshal(inst.Lock)

		if _, err := tx.Exec(`
			INSERT INTO instances(name, grp, role, server, container, port, memory, cpus, disk,
			    password, config_path, data_path, backup_path,
			    persistence, config_overrides, replica_of, backup, status, lock, created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			name,
			inst.Group, inst.Role, inst.Server, inst.Container,
			inst.Port, inst.Memory, inst.CPUs, inst.Disk,
			inst.Password, inst.ConfigPath, inst.DataPath, inst.BackupPath,
			string(persistenceJSON), string(overridesJSON),
			inst.ReplicaOf, string(backupJSON),
			inst.Status, string(lockJSON), inst.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert instance %s: %w", name, err)
		}
	}
	return tx.Commit()
}

// ReadInstances 读取完整 InstancesState 快照（只读，无锁）。
func (d *DB) ReadInstances() (*apitypes.InstancesState, error) {
	return loadInstancesState(d.sql)
}

// ReadNodeState 读取完整 NodeState 快照（只读，无锁）。
func (d *DB) ReadNodeState() (*apitypes.NodeState, error) {
	servers, err := d.GetAllServers()
	if err != nil {
		return nil, err
	}
	return &apitypes.NodeState{Servers: servers}, nil
}

// WriteNodeState 覆写完整 NodeState（在事务中执行）。
func (d *DB) WriteNodeState(ns *apitypes.NodeState) error {
	return d.WithServers(func(m map[string]*apitypes.NodeServer) error {
		// 清空并重建
		for k := range m {
			delete(m, k)
		}
		for k, v := range ns.Servers {
			m[k] = v
		}
		return nil
	})
}

// ── 内部辅助 ──────────────────────────────────────────────────────────────────

func loadInstancesState(db *sql.DB) (*apitypes.InstancesState, error) {
	st := &apitypes.InstancesState{
		Groups:    make(map[string]*apitypes.InstanceGroupState),
		Instances: make(map[string]*apitypes.Instance),
	}

	// 读 groups
	gRows, err := db.Query(
		`SELECT name, type, engine, engine_version, category,
		        current_master, topology_status, failover_conflict, envoy, created_at, updated_at
		 FROM instance_groups`)
	if err != nil {
		return nil, err
	}
	groups, err := scanGroups(gRows)
	if err != nil {
		return nil, err
	}
	st.Groups = groups

	// 读 instances
	iRows, err := db.Query(
		`SELECT name, grp, role, server, container, port, memory, cpus, disk,
		        password, config_path, data_path, backup_path,
		        persistence, config_overrides, replica_of, backup, status, lock, created_at
		 FROM instances`)
	if err != nil {
		return nil, err
	}
	insts, err := scanInstances(iRows)
	if err != nil {
		return nil, err
	}
	st.Instances = insts
	return st, nil
}

func loadInstancesStateTx(tx *sql.Tx) (*apitypes.InstancesState, error) {
	st := &apitypes.InstancesState{
		Groups:    make(map[string]*apitypes.InstanceGroupState),
		Instances: make(map[string]*apitypes.Instance),
	}

	gRows, err := tx.Query(
		`SELECT name, type, engine, engine_version, category,
		        current_master, topology_status, failover_conflict, envoy, created_at, updated_at
		 FROM instance_groups`)
	if err != nil {
		return nil, err
	}
	groups, err := scanGroups(gRows)
	if err != nil {
		return nil, err
	}
	st.Groups = groups

	iRows, err := tx.Query(
		`SELECT name, grp, role, server, container, port, memory, cpus, disk,
		        password, config_path, data_path, backup_path,
		        persistence, config_overrides, replica_of, backup, status, lock, created_at
		 FROM instances`)
	if err != nil {
		return nil, err
	}
	insts, err := scanInstances(iRows)
	if err != nil {
		return nil, err
	}
	st.Instances = insts
	return st, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanInstance(row rowScanner) (*apitypes.Instance, error) {
	var name, persistenceJSON, overridesJSON, backupJSON, lockJSON string
	inst := &apitypes.Instance{}
	if err := row.Scan(
		&name,
		&inst.Group, &inst.Role, &inst.Server, &inst.Container,
		&inst.Port, &inst.Memory, &inst.CPUs, &inst.Disk,
		&inst.Password, &inst.ConfigPath, &inst.DataPath, &inst.BackupPath,
		&persistenceJSON, &overridesJSON,
		&inst.ReplicaOf, &backupJSON,
		&inst.Status, &lockJSON, &inst.CreatedAt,
	); err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(persistenceJSON), &inst.Persistence)  //nolint:errcheck
	json.Unmarshal([]byte(overridesJSON), &inst.ConfigOverrides) //nolint:errcheck
	json.Unmarshal([]byte(backupJSON), &inst.Backup)             //nolint:errcheck
	json.Unmarshal([]byte(lockJSON), &inst.Lock)                 //nolint:errcheck
	return inst, nil
}

func scanInstances(rows *sql.Rows) (map[string]*apitypes.Instance, error) {
	defer rows.Close()
	result := make(map[string]*apitypes.Instance)
	for rows.Next() {
		var name, persistenceJSON, overridesJSON, backupJSON, lockJSON string
		inst := &apitypes.Instance{}
		if err := rows.Scan(
			&name,
			&inst.Group, &inst.Role, &inst.Server, &inst.Container,
			&inst.Port, &inst.Memory, &inst.CPUs, &inst.Disk,
			&inst.Password, &inst.ConfigPath, &inst.DataPath, &inst.BackupPath,
			&persistenceJSON, &overridesJSON,
			&inst.ReplicaOf, &backupJSON,
			&inst.Status, &lockJSON, &inst.CreatedAt,
		); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(persistenceJSON), &inst.Persistence)  //nolint:errcheck
		json.Unmarshal([]byte(overridesJSON), &inst.ConfigOverrides) //nolint:errcheck
		json.Unmarshal([]byte(backupJSON), &inst.Backup)             //nolint:errcheck
		json.Unmarshal([]byte(lockJSON), &inst.Lock)                 //nolint:errcheck
		result[name] = inst
	}
	return result, rows.Err()
}

func scanGroups(rows *sql.Rows) (map[string]*apitypes.InstanceGroupState, error) {
	defer rows.Close()
	result := make(map[string]*apitypes.InstanceGroupState)
	for rows.Next() {
		var name, envoyJSON string
		var failoverConflict int
		g := &apitypes.InstanceGroupState{}
		if err := rows.Scan(
			&name,
			&g.Type, &g.Engine, &g.EngineVersion, &g.Category,
			&g.CurrentMaster, &g.TopologyStatus, &failoverConflict,
			&envoyJSON, &g.CreatedAt, &g.UpdatedAt,
		); err != nil {
			return nil, err
		}
		g.FailoverConflict = failoverConflict != 0
		json.Unmarshal([]byte(envoyJSON), &g.Envoy) //nolint:errcheck
		result[name] = g
	}
	return result, rows.Err()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
