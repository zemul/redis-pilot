package store

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// ── 节点 (servers 表) ────────────────────────────────────────────────────────

// GetAllServers 读取所有节点，返回 name→NodeServer map。
func (d *DB) GetAllServers() (map[string]*apitypes.NodeServer, error) {
	rows, err := d.sql.Query(
		`SELECT name, endpoint, agent_port, agent_token, labels, capacity, instances, status, last_heartbeat
		 FROM servers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]*apitypes.NodeServer)
	for rows.Next() {
		var name string
		var labelsJSON, capacityJSON, instancesJSON string
		srv := &apitypes.NodeServer{}
		if err := rows.Scan(
			&name, &srv.Endpoint, &srv.AgentPort, &srv.AgentToken,
			&labelsJSON, &capacityJSON, &instancesJSON,
			&srv.Status, &srv.LastHeartbeat,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(labelsJSON), &srv.Labels); err != nil {
			srv.Labels = make(map[string]string)
		}
		if err := json.Unmarshal([]byte(capacityJSON), &srv.Capacity); err != nil {
			srv.Capacity = apitypes.ResourceSpec{}
		}
		if err := json.Unmarshal([]byte(instancesJSON), &srv.Instances); err != nil {
			srv.Instances = nil
		}
		result[name] = srv
	}
	return result, rows.Err()
}

// UpsertServer 新增或更新一个节点（按 name 唯一键）。
func (d *DB) UpsertServer(name string, srv *apitypes.NodeServer) error {
	labelsJSON, _ := json.Marshal(srv.Labels)
	capacityJSON, _ := json.Marshal(srv.Capacity)
	instancesJSON, _ := json.Marshal(srv.Instances)

	_, err := d.sql.Exec(`
		INSERT INTO servers(name, endpoint, agent_port, agent_token, labels, capacity, instances, status, last_heartbeat)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET
			endpoint       = excluded.endpoint,
			agent_port     = excluded.agent_port,
			agent_token    = excluded.agent_token,
			labels         = excluded.labels,
			capacity       = excluded.capacity,
			instances      = excluded.instances,
			status         = excluded.status,
			last_heartbeat = excluded.last_heartbeat`,
		name,
		srv.Endpoint, srv.AgentPort, srv.AgentToken,
		string(labelsJSON), string(capacityJSON), string(instancesJSON),
		srv.Status, srv.LastHeartbeat,
	)
	return err
}

// DeleteServer 删除节点。
func (d *DB) DeleteServer(name string) error {
	_, err := d.sql.Exec(`DELETE FROM servers WHERE name = ?`, name)
	return err
}

// GetServer 读取单个节点，不存在时返回 nil, nil。
func (d *DB) GetServer(name string) (*apitypes.NodeServer, error) {
	row := d.sql.QueryRow(
		`SELECT endpoint, agent_port, agent_token, labels, capacity, instances, status, last_heartbeat
		 FROM servers WHERE name = ?`, name)

	var labelsJSON, capacityJSON, instancesJSON string
	srv := &apitypes.NodeServer{}
	err := row.Scan(
		&srv.Endpoint, &srv.AgentPort, &srv.AgentToken,
		&labelsJSON, &capacityJSON, &instancesJSON,
		&srv.Status, &srv.LastHeartbeat,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(labelsJSON), &srv.Labels); err != nil {
		srv.Labels = make(map[string]string)
	}
	if err := json.Unmarshal([]byte(capacityJSON), &srv.Capacity); err != nil {
		srv.Capacity = apitypes.ResourceSpec{}
	}
	if err := json.Unmarshal([]byte(instancesJSON), &srv.Instances); err != nil {
		srv.Instances = nil
	}
	return srv, nil
}

// WithServers 在事务中原子执行 read → fn → write 所有变更。
// fn 接收当前全量 map，修改后由框架批量写回。
func (d *DB) WithServers(fn func(map[string]*apitypes.NodeServer) error) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// 读全量
	servers, err := d.getAllServersInTx(tx)
	if err != nil {
		return err
	}

	if err := fn(servers); err != nil {
		return err
	}

	// 写回：先删全部再插入（节点数量极少，全量重写无性能问题）
	if _, err := tx.Exec(`DELETE FROM servers`); err != nil {
		return err
	}
	for name, srv := range servers {
		labelsJSON, _ := json.Marshal(srv.Labels)
		capacityJSON, _ := json.Marshal(srv.Capacity)
		instancesJSON, _ := json.Marshal(srv.Instances)
		if _, err := tx.Exec(`
			INSERT INTO servers(name, endpoint, agent_port, agent_token, labels, capacity, instances, status, last_heartbeat)
			VALUES(?,?,?,?,?,?,?,?,?)`,
			name,
			srv.Endpoint, srv.AgentPort, srv.AgentToken,
			string(labelsJSON), string(capacityJSON), string(instancesJSON),
			srv.Status, srv.LastHeartbeat,
		); err != nil {
			return fmt.Errorf("insert server %s: %w", name, err)
		}
	}
	return tx.Commit()
}

func (d *DB) getAllServersInTx(tx *sql.Tx) (map[string]*apitypes.NodeServer, error) {
	rows, err := tx.Query(
		`SELECT name, endpoint, agent_port, agent_token, labels, capacity, instances, status, last_heartbeat
		 FROM servers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]*apitypes.NodeServer)
	for rows.Next() {
		var name string
		var labelsJSON, capacityJSON, instancesJSON string
		srv := &apitypes.NodeServer{}
		if err := rows.Scan(
			&name, &srv.Endpoint, &srv.AgentPort, &srv.AgentToken,
			&labelsJSON, &capacityJSON, &instancesJSON,
			&srv.Status, &srv.LastHeartbeat,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(labelsJSON), &srv.Labels); err != nil {
			srv.Labels = make(map[string]string)
		}
		if err := json.Unmarshal([]byte(capacityJSON), &srv.Capacity); err != nil {
			srv.Capacity = apitypes.ResourceSpec{}
		}
		if err := json.Unmarshal([]byte(instancesJSON), &srv.Instances); err != nil {
			srv.Instances = nil
		}
		result[name] = srv
	}
	return result, rows.Err()
}
