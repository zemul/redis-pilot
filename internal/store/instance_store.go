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
		`SELECT name, type, engine, engine_version, category, access_mode,
		        current_master, topology_status, failover_conflict, proxy, created_at, updated_at
		 FROM instance_groups`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGroups(rows)
}

// UpsertGroup 新增或更新实例组。
func (d *DB) UpsertGroup(name string, g *apitypes.InstanceGroupState) error {
	proxyJSON, _ := json.Marshal(g.Proxy)
	return d.execWrite(`
		INSERT INTO instance_groups(name, type, engine, engine_version, category, access_mode,
		    current_master, topology_status, failover_conflict, proxy, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET
			type              = excluded.type,
			engine            = excluded.engine,
			engine_version    = excluded.engine_version,
			category          = excluded.category,
			access_mode       = excluded.access_mode,
			current_master    = excluded.current_master,
			topology_status   = excluded.topology_status,
			failover_conflict = excluded.failover_conflict,
			proxy             = excluded.proxy,
			created_at        = excluded.created_at,
			updated_at        = excluded.updated_at`,
		name,
		g.Type, g.Engine, g.EngineVersion, g.Category, g.AccessMode,
		g.CurrentMaster, g.TopologyStatus, boolToInt(g.FailoverConflict),
		string(proxyJSON), g.CreatedAt, g.UpdatedAt,
	)
}

// DeleteGroup 删除实例组。
func (d *DB) DeleteGroup(name string) error {
	return d.execWrite(`DELETE FROM instance_groups WHERE name = ?`, name)
}

// ── 实例 (instances 表) ──────────────────────────────────────────────────────

// GetAllInstances 读取所有实例。
func (d *DB) GetAllInstances() (map[string]*apitypes.Instance, error) {
	rows, err := d.sql.Query(
		`SELECT name, grp, role, server, container, port, memory, cpus, disk,
		        password, config_path, data_path, backup_path,
		        persistence, config_overrides, replica_of, backup, desired_state, observed_status, lock, created_at
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

	return d.execWrite(`
		INSERT INTO instances(name, grp, role, server, container, port, memory, cpus, disk,
		    password, config_path, data_path, backup_path,
		    persistence, config_overrides, replica_of, backup, desired_state, observed_status, lock, created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
			desired_state    = excluded.desired_state,
			observed_status  = excluded.observed_status,
			lock             = excluded.lock,
			created_at       = excluded.created_at`,
		name,
		inst.Group, inst.Role, inst.Server, inst.Container,
		inst.Port, inst.Memory, inst.CPUs, inst.Disk,
		inst.Password, inst.ConfigPath, inst.DataPath, inst.BackupPath,
		string(persistenceJSON), string(overridesJSON),
		inst.ReplicaOf, string(backupJSON),
		inst.DesiredState, inst.ObservedStatus, string(lockJSON), inst.CreatedAt,
	)
}

// DeleteInstance 删除实例。
func (d *DB) DeleteInstance(name string) error {
	return d.execWrite(`DELETE FROM instances WHERE name = ?`, name)
}

// GetInstance 读取单个实例，不存在时返回 nil, nil。
func (d *DB) GetInstance(name string) (*apitypes.Instance, error) {
	row := d.sql.QueryRow(
		`SELECT name, grp, role, server, container, port, memory, cpus, disk,
		        password, config_path, data_path, backup_path,
		        persistence, config_overrides, replica_of, backup, desired_state, observed_status, lock, created_at
		 FROM instances WHERE name = ?`, name)

	inst, err := scanInstance(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return inst, err
}

// WithInstances 在事务中原子执行 read → fn → write。
// fn 接收完整 InstancesState 快照，修改后增量写回（仅 upsert 变更项、delete 删除项）。
//
// 用 DB.mu 强制同一时间只有一个 WithInstances/WithServers 在执行，避免并发调用者
// 各自基于旧快照修改后互相覆盖（丢失更新），或触发 SQLITE_BUSY_SNAPSHOT 静默失败。
func (d *DB) WithInstances(fn func(*apitypes.InstancesState) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	st, err := loadInstancesStateTx(tx)
	if err != nil {
		return err
	}

	// 记录修改前的 key 集合，用于 diff
	oldGroups := make(map[string]struct{}, len(st.Groups))
	for k := range st.Groups {
		oldGroups[k] = struct{}{}
	}
	oldInstances := make(map[string]struct{}, len(st.Instances))
	for k := range st.Instances {
		oldInstances[k] = struct{}{}
	}

	if err := fn(st); err != nil {
		return err
	}

	// 增量写回 groups：upsert 当前存在的，delete 被移除的
	for name, g := range st.Groups {
		proxyJSON, _ := json.Marshal(g.Proxy)
		if _, err := tx.Exec(`
			INSERT INTO instance_groups(name, type, engine, engine_version, category, access_mode,
			    current_master, topology_status, failover_conflict, proxy, created_at, updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(name) DO UPDATE SET
				type              = excluded.type,
				engine            = excluded.engine,
				engine_version    = excluded.engine_version,
				category          = excluded.category,
				access_mode       = excluded.access_mode,
				current_master    = excluded.current_master,
				topology_status   = excluded.topology_status,
				failover_conflict = excluded.failover_conflict,
				proxy             = excluded.proxy,
				created_at        = excluded.created_at,
				updated_at        = excluded.updated_at`,
			name,
			g.Type, g.Engine, g.EngineVersion, g.Category, g.AccessMode,
			g.CurrentMaster, g.TopologyStatus, boolToInt(g.FailoverConflict),
			string(proxyJSON), g.CreatedAt, g.UpdatedAt,
		); err != nil {
			return fmt.Errorf("upsert group %s: %w", name, err)
		}
	}
	for name := range oldGroups {
		if _, exists := st.Groups[name]; !exists {
			if _, err := tx.Exec(`DELETE FROM instance_groups WHERE name = ?`, name); err != nil {
				return fmt.Errorf("delete group %s: %w", name, err)
			}
		}
	}

	// 增量写回 instances：upsert 当前存在的，delete 被移除的
	for name, inst := range st.Instances {
		persistenceJSON, _ := json.Marshal(inst.Persistence)
		overridesJSON, _ := json.Marshal(inst.ConfigOverrides)
		backupJSON, _ := json.Marshal(inst.Backup)
		lockJSON, _ := json.Marshal(inst.Lock)

		if _, err := tx.Exec(`
			INSERT INTO instances(name, grp, role, server, container, port, memory, cpus, disk,
			    password, config_path, data_path, backup_path,
			    persistence, config_overrides, replica_of, backup, desired_state, observed_status, lock, created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
				desired_state    = excluded.desired_state,
				observed_status  = excluded.observed_status,
				lock             = excluded.lock,
				created_at       = excluded.created_at`,
			name,
			inst.Group, inst.Role, inst.Server, inst.Container,
			inst.Port, inst.Memory, inst.CPUs, inst.Disk,
			inst.Password, inst.ConfigPath, inst.DataPath, inst.BackupPath,
			string(persistenceJSON), string(overridesJSON),
			inst.ReplicaOf, string(backupJSON),
			inst.DesiredState, inst.ObservedStatus, string(lockJSON), inst.CreatedAt,
		); err != nil {
			return fmt.Errorf("upsert instance %s: %w", name, err)
		}
	}
	for name := range oldInstances {
		if _, exists := st.Instances[name]; !exists {
			if _, err := tx.Exec(`DELETE FROM instances WHERE name = ?`, name); err != nil {
				return fmt.Errorf("delete instance %s: %w", name, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	d.bumpGeneration()
	return nil
}

// ReadInstances 读取完整 InstancesState 快照（只读，无锁）。
func (d *DB) ReadInstances() (*apitypes.InstancesState, error) {
	return loadInstancesState(d.sql)
}

// ReadNode 读取完整 NodeState 快照（只读，无锁）。
func (d *DB) ReadNode() (*apitypes.NodeState, error) {
	servers, err := d.GetAllServers()
	if err != nil {
		return nil, err
	}
	return &apitypes.NodeState{Servers: servers}, nil
}

// WriteNode 覆写完整 NodeState（在事务中执行）。
func (d *DB) WriteNode(ns *apitypes.NodeState) error {
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

// WithNode 在事务中原子执行 read -> fn -> write，传入 NodeState 快照。
func (d *DB) WithNode(fn func(*apitypes.NodeState) error) error {
	return d.WithServers(func(servers map[string]*apitypes.NodeServer) error {
		return fn(&apitypes.NodeState{Servers: servers})
	})
}

// WriteInstances 覆写完整 InstancesState（在事务中执行）。
func (d *DB) WriteInstances(state *apitypes.InstancesState) error {
	return d.WithInstances(func(st *apitypes.InstancesState) error {
		st.Groups = state.Groups
		st.Instances = state.Instances
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
		`SELECT name, type, engine, engine_version, category, access_mode,
		        current_master, topology_status, failover_conflict, proxy, created_at, updated_at
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
		        persistence, config_overrides, replica_of, backup, desired_state, observed_status, lock, created_at
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
		`SELECT name, type, engine, engine_version, category, access_mode,
		        current_master, topology_status, failover_conflict, proxy, created_at, updated_at
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
		        persistence, config_overrides, replica_of, backup, desired_state, observed_status, lock, created_at
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
		&inst.DesiredState, &inst.ObservedStatus, &lockJSON, &inst.CreatedAt,
	); err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(persistenceJSON), &inst.Persistence)   //nolint:errcheck
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
			&inst.DesiredState, &inst.ObservedStatus, &lockJSON, &inst.CreatedAt,
		); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(persistenceJSON), &inst.Persistence)   //nolint:errcheck
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
		var name, proxyJSON string
		var failoverConflict int
		g := &apitypes.InstanceGroupState{}
		if err := rows.Scan(
			&name,
			&g.Type, &g.Engine, &g.EngineVersion, &g.Category, &g.AccessMode,
			&g.CurrentMaster, &g.TopologyStatus, &failoverConflict,
			&proxyJSON, &g.CreatedAt, &g.UpdatedAt,
		); err != nil {
			return nil, err
		}
		g.FailoverConflict = failoverConflict != 0
		json.Unmarshal([]byte(proxyJSON), &g.Proxy) //nolint:errcheck
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
