// Package store 提供基于 SQLite 的持久化存储，替代原有的 YAML 状态文件和 JSONL 审计日志。
// 使用 modernc.org/sqlite（pure Go，无 CGO）。
package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"
)

// DB 封装 SQLite 连接，提供分域存储方法。
//
// mu 用于在应用层串行化 WithInstances/WithServers 的"读快照 → 修改 → 全量重写"过程。
// SQLite 的写锁串行化只保证单条语句/单个事务内部的原子性，不能防止两个并发事务
// 各自基于同一份旧快照做出修改后互相覆盖（丢失更新），也不能防止 WAL 模式下
// 后提交的事务因快照过期返回 SQLITE_BUSY_SNAPSHOT（_busy_timeout 不会重试这种错误）。
// 因此这里在应用层用 mu 强制同一时间只有一个 WithInstances/WithServers 在执行。
type DB struct {
	sql *sql.DB
	mu  sync.Mutex
}

// Open 打开（或创建）dataDir/pilot.db，执行建表迁移后返回。
func Open(dataDir string) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("store: mkdir %s: %w", dataDir, err)
	}
	path := filepath.Join(dataDir, "pilot.db")
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	// WAL 模式允许并发读；写锁本身由 SQLite 序列化，但跨越"读快照→改内存→写回"
	// 的 WithInstances/WithServers 需要额外的应用层锁（见 DB.mu），否则并发调用会
	// 因快照冲突（SQLITE_BUSY_SNAPSHOT）静默失败或发生丢失更新。
	// 不限制 MaxOpenConns，避免嵌套查询（如事务内读另一张表）死锁。
	db.SetMaxIdleConns(4)

	s := &DB{sql: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	return s, nil
}

// Close 关闭数据库连接。
func (d *DB) Close() error {
	return d.sql.Close()
}

// migrate 执行幂等建表。新增字段时追加 ALTER TABLE，不删除旧表。
func (d *DB) migrate() error {
	stmts := []string{
		// ── 节点 ────────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS servers (
			name            TEXT PRIMARY KEY,
			endpoint        TEXT NOT NULL,
			agent_port      INTEGER NOT NULL DEFAULT 8400,
			agent_token     TEXT NOT NULL DEFAULT '',
			labels          TEXT NOT NULL DEFAULT '{}',   -- JSON
			capacity        TEXT NOT NULL DEFAULT '{}',   -- JSON: ResourceSpec
			instances       TEXT NOT NULL DEFAULT '[]',   -- JSON: []string
			status          TEXT NOT NULL DEFAULT 'healthy',
			last_heartbeat  TEXT NOT NULL DEFAULT ''
		)`,

		// ── 实例组 ──────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS instance_groups (
			name              TEXT PRIMARY KEY,
			type              TEXT NOT NULL DEFAULT 'standalone',
			engine            TEXT NOT NULL DEFAULT 'redis',
			engine_version    TEXT NOT NULL DEFAULT '',
			category          TEXT NOT NULL DEFAULT '',
			current_master    TEXT NOT NULL DEFAULT '',
			topology_status   TEXT NOT NULL DEFAULT 'degraded',
			failover_conflict INTEGER NOT NULL DEFAULT 0,
			envoy             TEXT NOT NULL DEFAULT 'null', -- JSON: EnvoyConfig|null
			created_at        TEXT NOT NULL DEFAULT '',
			updated_at        TEXT NOT NULL DEFAULT ''
		)`,

		// ── 实例 ────────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS instances (
			name             TEXT PRIMARY KEY,
			grp              TEXT NOT NULL DEFAULT '',
			role             TEXT NOT NULL DEFAULT 'master',
			server           TEXT NOT NULL DEFAULT '',
			container        TEXT NOT NULL DEFAULT '',
			port             INTEGER NOT NULL DEFAULT 0,
			memory           TEXT NOT NULL DEFAULT '',
			cpus             REAL NOT NULL DEFAULT 0,
			disk             TEXT NOT NULL DEFAULT '',
			password         TEXT NOT NULL DEFAULT '',
			config_path      TEXT NOT NULL DEFAULT '',
			data_path        TEXT NOT NULL DEFAULT '',
			backup_path      TEXT NOT NULL DEFAULT '',
			persistence      TEXT NOT NULL DEFAULT 'null', -- JSON: Persistence|null
			config_overrides TEXT NOT NULL DEFAULT '{}',  -- JSON: map[string]string
			replica_of       TEXT NOT NULL DEFAULT '',
			backup           TEXT NOT NULL DEFAULT 'null', -- JSON: BackupConfig|null
			status           TEXT NOT NULL DEFAULT '',
			lock             TEXT NOT NULL DEFAULT 'null', -- JSON: Lock|null
			created_at       TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_instances_grp ON instances(grp)`,
		`CREATE INDEX IF NOT EXISTS idx_instances_server ON instances(server)`,
		`CREATE INDEX IF NOT EXISTS idx_instances_status ON instances(status)`,

		// ── 审计日志 ────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS audit_log (
			id           TEXT PRIMARY KEY,
			ts           TEXT NOT NULL,          -- RFC3339
			date         TEXT NOT NULL,          -- YYYYMMDD，方便按日查询
			operator     TEXT NOT NULL DEFAULT '',
			action       TEXT NOT NULL DEFAULT '',
			level        TEXT NOT NULL DEFAULT 'normal',
			target       TEXT NOT NULL DEFAULT '{}', -- JSON
			params       TEXT NOT NULL DEFAULT '{}', -- JSON
			result       TEXT NOT NULL DEFAULT '',
			duration_ms  INTEGER NOT NULL DEFAULT 0,
			detail       TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_date     ON audit_log(date)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_action   ON audit_log(action)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_level    ON audit_log(level)`,

		// ── 任务队列（预留，后续扩展异步操作） ──────────────────────────────
		`CREATE TABLE IF NOT EXISTS tasks (
			id          TEXT PRIMARY KEY,
			kind        TEXT NOT NULL DEFAULT '',
			status      TEXT NOT NULL DEFAULT 'pending', -- pending|running|done|failed
			payload     TEXT NOT NULL DEFAULT '{}',
			result      TEXT NOT NULL DEFAULT '',
			error       TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)`,

		// ── 备份记录（预留） ─────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS backup_records (
			id           TEXT PRIMARY KEY,
			instance     TEXT NOT NULL DEFAULT '',
			server       TEXT NOT NULL DEFAULT '',
			backup_path  TEXT NOT NULL DEFAULT '',
			size_bytes   INTEGER NOT NULL DEFAULT 0,
			status       TEXT NOT NULL DEFAULT '', -- success|failed
			error        TEXT NOT NULL DEFAULT '',
			started_at   TEXT NOT NULL DEFAULT '',
			finished_at  TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_backup_instance ON backup_records(instance)`,
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for _, s := range stmts {
		if _, err := tx.Exec(s); err != nil {
			return fmt.Errorf("exec DDL: %w\nSQL: %s", err, s)
		}
	}
	return tx.Commit()
}
