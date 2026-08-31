// Package store 提供基于 SQLite 的持久化存储，替代原有的 YAML 状态文件和 JSONL 审计日志。
// 使用 modernc.org/sqlite（pure Go，无 CGO）。
package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

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

	// generation 在每次拓扑状态写入成功后自增，供上层判断"自某个时刻起状态有没有动过"。
	//
	// 放在 store 层而不是各个 handler 里，是因为这里是所有状态写入的唯一收口点：
	// 全部 9 个写方法都在本包内，改动时一眼能看全。如果改成在业务代码里逐处发通知，
	// 漏掉任何一处的后果是 Proxy 的配置永久停留在旧值——一个不会报错、只会静默错下去
	// 的故障，比多几次无用的重算糟糕得多。
	//
	// 语义刻意做弱：generation 变了只说明"有写入发生"，不保证代理配置真的变了
	// （比如更新备份时间戳也会自增）。调用方据此重算并比对内容哈希，弱信号 + 强比对
	// 的组合不会漏、也不会误报。
	generation atomic.Uint64
}

// Generation 返回当前的状态代际。
func (d *DB) Generation() uint64 {
	return d.generation.Load()
}

// bumpGeneration 在写入成功后调用。
func (d *DB) bumpGeneration() {
	d.generation.Add(1)
}

// execWrite 执行一条写 SQL，成功后自增代际。
//
// 所有非事务的写操作都走这里，而不是各自 d.sql.Exec + return err：把自增和写入绑在
// 同一个函数里，新增写方法时漏掉自增的可能性就小得多。事务型写入（WithInstances /
// WithServers）在各自的 Commit 成功后自增。
func (d *DB) execWrite(query string, args ...interface{}) error {
	if _, err := d.sql.Exec(query, args...); err != nil {
		return err
	}
	d.bumpGeneration()
	return nil
}

// Open 打开（或创建）dataDir/pilot.db，执行建表迁移后返回。
//
// dataDir 和 pilot.db（以及 WAL/SHM 伴生文件）均限制为仅属主可读写（0700/0600），
// 因为 instance_groups.password 和 servers.agent_token 以明文形式存储在库中，
// 若目录或文件权限过宽，本机其他用户可直接读取数据库文件窃取凭据。
func Open(dataDir string) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("store: mkdir %s: %w", dataDir, err)
	}
	// 目录可能已存在且权限过宽（如从旧版本升级），显式收紧一次。
	if err := os.Chmod(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("store: chmod %s: %w", dataDir, err)
	}
	path := filepath.Join(dataDir, "pilot.db")
	// modernc.org/sqlite 使用 _pragma=name(value) 格式，不支持 _busy_timeout / _journal_mode
	// 等下划线参数（静默忽略，导致 busy_timeout=0 + 非 WAL 模式 -> 并发读写直接 SQLITE_BUSY）。
	//   - WAL：读不阻塞写、写不阻塞读
	//   - busy_timeout=15s：遇到锁时自动重试（覆盖 checkpoint 和短暂写冲突）
	//   - foreign_keys=ON：外键约束
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(15000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)")
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

	// pilot.db 及其 WAL/SHM 伴生文件在上面的 sql.Open/migrate 过程中才会被
	// sqlite 驱动实际创建，创建时权限受进程 umask 影响（常见为 0644），
	// 因此必须在文件确定存在之后再显式收紧为仅属主可读写。
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := restrictFilePerm(path + suffix); err != nil {
			db.Close()
			return nil, err
		}
	}
	return s, nil
}

// restrictFilePerm 将存在的文件权限收紧为 0600（仅属主可读写）。
// 文件不存在时视为无需处理（例如尚未产生 WAL/SHM 伴生文件）。
func restrictFilePerm(path string) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("store: stat %s: %w", path, err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		return fmt.Errorf("store: chmod %s: %w", path, err)
	}
	return nil
}

// Close 关闭数据库连接。
func (d *DB) Close() error {
	return d.sql.Close()
}

// Checkpoint 执行 WAL checkpoint（TRUNCATE 模式），将 WAL 内容刷入主数据库文件
// 并截断 WAL 文件为零字节，防止 WAL 无限增长。
//
// 调用时机由上层决定（如定时循环），本方法仅执行 checkpoint 本身。
// busy_timeout 已在连接参数中设置为 15s，checkpoint 遇到读锁时会自动重试。
func (d *DB) Checkpoint() error {
	_, err := d.sql.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
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
			access_mode       TEXT NOT NULL DEFAULT '',
			labels            TEXT NOT NULL DEFAULT '{}', -- JSON: map[string]string
			password          TEXT NOT NULL DEFAULT '',
			current_master    TEXT NOT NULL DEFAULT '',
			topology_status   TEXT NOT NULL DEFAULT 'degraded',
			failover_conflict INTEGER NOT NULL DEFAULT 0,
			proxy             TEXT NOT NULL DEFAULT 'null', -- JSON: ProxyConfig|null
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
			config_path      TEXT NOT NULL DEFAULT '',
			data_path        TEXT NOT NULL DEFAULT '',
			backup_path      TEXT NOT NULL DEFAULT '',
			persistence      TEXT NOT NULL DEFAULT 'null', -- JSON: Persistence|null
			config_overrides TEXT NOT NULL DEFAULT '{}',  -- JSON: map[string]string
			replica_of       TEXT NOT NULL DEFAULT '',
			backup           TEXT NOT NULL DEFAULT 'null', -- JSON: BackupConfig|null
			-- 状态拆成意图与观测两列，各自只有一类写入者。
			-- desired_state  仅用户操作可写（create/start → running, stop → stopped）
			-- observed_status 仅观测通道可写（reconcile；空串表示尚未观测）
			desired_state    TEXT NOT NULL DEFAULT 'running',
			observed_status  TEXT NOT NULL DEFAULT '',
			lock             TEXT NOT NULL DEFAULT 'null', -- JSON: Lock|null
			created_at       TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_instances_grp ON instances(grp)`,
		`CREATE INDEX IF NOT EXISTS idx_instances_server ON instances(server)`,

		// ── 审计日志 ────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS audit_log (
			id           TEXT PRIMARY KEY,
			ts           TEXT NOT NULL,          -- RFC3339
			timestamp_ms INTEGER NOT NULL DEFAULT 0,
			date         TEXT NOT NULL,          -- YYYYMMDD，方便按日查询
			operator     TEXT NOT NULL DEFAULT '',
			actor_type   TEXT NOT NULL DEFAULT 'system',
			source       TEXT NOT NULL DEFAULT 'system',
			trigger      TEXT NOT NULL DEFAULT 'automatic',
			action       TEXT NOT NULL DEFAULT '',
			phase        TEXT NOT NULL DEFAULT 'completed',
			level        TEXT NOT NULL DEFAULT 'normal',
			object_type  TEXT NOT NULL DEFAULT 'system',
			object_id    TEXT NOT NULL DEFAULT 'redis-pilot',
			group_name   TEXT NOT NULL DEFAULT '',
			instance_name TEXT NOT NULL DEFAULT '',
			correlation_id TEXT NOT NULL DEFAULT '',
			target       TEXT NOT NULL DEFAULT '{}', -- JSON
			params       TEXT NOT NULL DEFAULT '{}', -- JSON
			before_state TEXT NOT NULL DEFAULT '{}', -- JSON
			after_state  TEXT NOT NULL DEFAULT '{}', -- JSON
			result       TEXT NOT NULL DEFAULT '',
			duration_ms  INTEGER NOT NULL DEFAULT 0,
			detail       TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_date     ON audit_log(date)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_action   ON audit_log(action)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_level    ON audit_log(level)`,

		// ── 长任务状态（备份/恢复由 Server 跟踪并持久化终态） ────────────────
		`CREATE TABLE IF NOT EXISTS tasks (
			id          TEXT PRIMARY KEY,
			kind        TEXT NOT NULL DEFAULT '',
			status      TEXT NOT NULL DEFAULT 'pending', -- pending|accepted|running|success|failed|interrupted|canceled
			phase       TEXT NOT NULL DEFAULT '',
			progress    INTEGER NOT NULL DEFAULT 0,
			message     TEXT NOT NULL DEFAULT '',
			cancelable  INTEGER NOT NULL DEFAULT 0,
			cancel_requested INTEGER NOT NULL DEFAULT 0,
			payload     TEXT NOT NULL DEFAULT '{}',
			result      TEXT NOT NULL DEFAULT '',
			error       TEXT NOT NULL DEFAULT '',
			operator    TEXT NOT NULL DEFAULT 'system',
			source      TEXT NOT NULL DEFAULT 'system',
			trigger     TEXT NOT NULL DEFAULT 'automatic',
			group_name  TEXT NOT NULL DEFAULT '',
			instance_name TEXT NOT NULL DEFAULT '',
			server_name TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			started_at  TEXT NOT NULL DEFAULT '',
			finished_at TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)`,

		// ── 用户 ────────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS users (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			username    TEXT NOT NULL UNIQUE,
			password    TEXT NOT NULL,
			created_at  TEXT NOT NULL DEFAULT '',
			created_by  TEXT NOT NULL DEFAULT 'system'
		)`,

		// ── 用户 API Token ──────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS user_tokens (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			token       TEXT NOT NULL UNIQUE,
			username    TEXT NOT NULL,
			name        TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			expires_at  TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_user_tokens_username ON user_tokens(username)`,
		`CREATE INDEX IF NOT EXISTS idx_user_tokens_token ON user_tokens(token)`,

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

		// ── AI 助手会话与聊天记录（§13 持久化） ──────────────────────────────
		`CREATE TABLE IF NOT EXISTS ai_conversations (
			id          TEXT PRIMARY KEY,          -- conv-<UnixNano>
			user        TEXT NOT NULL,             -- 归属用户名（严格私有的依据）
			title       TEXT NOT NULL DEFAULT '',  -- 列表展示标题（取首条用户消息截断）
			created_at  TEXT NOT NULL,             -- RFC3339
			updated_at  TEXT NOT NULL,             -- 最后活跃时间，列表按此倒序
			deleted     INTEGER NOT NULL DEFAULT 0 -- 软删除（0/1），不物理删（倒查留痕）
		)`,
		`CREATE TABLE IF NOT EXISTS ai_messages (
			id          TEXT PRIMARY KEY,          -- msg-<UnixNano>
			conv_id     TEXT NOT NULL,             -- 所属会话 id
			seq         INTEGER NOT NULL,          -- 会话内顺序（从 1 递增）
			role        TEXT NOT NULL,             -- 'user' / 'assistant'
			content     TEXT NOT NULL DEFAULT '',  -- 正文（用户问题 or AI 回复全文）
			tool_calls  TEXT NOT NULL DEFAULT '[]',-- JSON 数组，工具调用/写操作留痕
			interrupted INTEGER NOT NULL DEFAULT 0, -- 该条 assistant 回复是否中途断开（0/1）
			created_at  TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_ai_msg_conv  ON ai_messages(conv_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_ai_conv_user ON ai_conversations(user, updated_at)`,
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

	// ── 增量迁移：instances 表从单列 status 拆分为 desired_state + observed_status ──
	// CREATE TABLE IF NOT EXISTS 不会更新已有表的 schema，需要 ALTER TABLE。
	// SQLite 的 ALTER TABLE ADD COLUMN 是幂等安全的——列已存在时报错，我们忽略即可。
	alterMigrations := []struct {
		sql     string
		postSQL string // 列添加成功后的后续操作（如数据迁移），可为空
	}{
		{
			sql:     `ALTER TABLE instances ADD COLUMN desired_state TEXT NOT NULL DEFAULT 'running'`,
			postSQL: `UPDATE instances SET desired_state = CASE WHEN status IN ('stopped','deleting') THEN status ELSE 'running' END WHERE desired_state = 'running' AND status != ''`,
		},
		{
			sql:     `ALTER TABLE instances ADD COLUMN observed_status TEXT NOT NULL DEFAULT ''`,
			postSQL: `UPDATE instances SET observed_status = status WHERE observed_status = '' AND status != ''`,
		},
		{
			sql: `ALTER TABLE instance_groups ADD COLUMN access_mode TEXT NOT NULL DEFAULT ''`,
		},
		{
			sql: `ALTER TABLE instance_groups ADD COLUMN labels TEXT NOT NULL DEFAULT '{}'`,
		},
		{
			sql:     `ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'admin'`,
			postSQL: `UPDATE users SET role = 'admin' WHERE role = ''`,
		},
		{
			sql: `ALTER TABLE user_tokens ADD COLUMN expires_at TEXT NOT NULL DEFAULT ''`,
		},
		{
			sql: `ALTER TABLE user_tokens ADD COLUMN description TEXT NOT NULL DEFAULT ''`,
		},
		{sql: `ALTER TABLE audit_log ADD COLUMN timestamp_ms INTEGER NOT NULL DEFAULT 0`},
		{sql: `ALTER TABLE audit_log ADD COLUMN actor_type TEXT NOT NULL DEFAULT 'system'`},
		{sql: `ALTER TABLE audit_log ADD COLUMN source TEXT NOT NULL DEFAULT 'system'`},
		{sql: `ALTER TABLE audit_log ADD COLUMN trigger TEXT NOT NULL DEFAULT 'automatic'`},
		{sql: `ALTER TABLE audit_log ADD COLUMN phase TEXT NOT NULL DEFAULT 'completed'`},
		{sql: `ALTER TABLE audit_log ADD COLUMN object_type TEXT NOT NULL DEFAULT 'system'`},
		{sql: `ALTER TABLE audit_log ADD COLUMN object_id TEXT NOT NULL DEFAULT 'redis-pilot'`},
		{sql: `ALTER TABLE audit_log ADD COLUMN group_name TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE audit_log ADD COLUMN instance_name TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE audit_log ADD COLUMN correlation_id TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE audit_log ADD COLUMN before_state TEXT NOT NULL DEFAULT '{}'`},
		{sql: `ALTER TABLE audit_log ADD COLUMN after_state TEXT NOT NULL DEFAULT '{}'`},
		{sql: `ALTER TABLE tasks ADD COLUMN operator TEXT NOT NULL DEFAULT 'system'`},
		{sql: `ALTER TABLE tasks ADD COLUMN source TEXT NOT NULL DEFAULT 'system'`},
		{sql: `ALTER TABLE tasks ADD COLUMN trigger TEXT NOT NULL DEFAULT 'automatic'`},
		{sql: `ALTER TABLE tasks ADD COLUMN group_name TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE tasks ADD COLUMN instance_name TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE tasks ADD COLUMN server_name TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE tasks ADD COLUMN started_at TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE tasks ADD COLUMN finished_at TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE tasks ADD COLUMN phase TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE tasks ADD COLUMN progress INTEGER NOT NULL DEFAULT 0`},
		{sql: `ALTER TABLE tasks ADD COLUMN message TEXT NOT NULL DEFAULT ''`},
		{sql: `ALTER TABLE tasks ADD COLUMN cancelable INTEGER NOT NULL DEFAULT 0`},
		{sql: `ALTER TABLE tasks ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0`},
	}
	for _, m := range alterMigrations {
		if _, err := tx.Exec(m.sql); err != nil {
			// "duplicate column name" 说明列已存在，跳过
			if !isDuplicateColumnError(err) {
				return fmt.Errorf("exec DDL: %w\nSQL: %s", err, m.sql)
			}
		} else if m.postSQL != "" {
			// 列刚加上，执行数据迁移
			if _, err := tx.Exec(m.postSQL); err != nil {
				return fmt.Errorf("exec migration: %w\nSQL: %s", err, m.postSQL)
			}
		}
	}

	// ALTER TABLE 之后才能安全创建依赖新列的索引
	postAlterIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_instances_observed_status ON instances(observed_status)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_timestamp_ms ON audit_log(timestamp_ms)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_actor_type ON audit_log(actor_type)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_source ON audit_log(source)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_result ON audit_log(result)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_group_name ON audit_log(group_name)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_instance_name ON audit_log(instance_name)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_correlation_id ON audit_log(correlation_id)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_kind ON tasks(kind)`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_instance_name ON tasks(instance_name)`,
	}
	for _, s := range postAlterIndexes {
		if _, err := tx.Exec(s); err != nil {
			return fmt.Errorf("exec DDL: %w\nSQL: %s", err, s)
		}
	}

	return tx.Commit()
}

// isDuplicateColumnError 判断 ALTER TABLE ADD COLUMN 报错是否因为列已存在。
func isDuplicateColumnError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate column name")
}
