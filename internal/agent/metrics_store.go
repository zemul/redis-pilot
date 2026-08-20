package agent

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// metricsStore 管理实例监控指标的持久化存储。
type metricsStore struct {
	db *sql.DB
}

func openMetricsStore(dataDir string) (*metricsStore, error) {
	dbPath := filepath.Join(dataDir, "metrics.db")
	db, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("open metrics db: %w", err)
	}

	// 创建表
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS instance_metrics (
			ts        INTEGER NOT NULL,
			instance  TEXT NOT NULL,
			mem_used  INTEGER NOT NULL DEFAULT 0,
			mem_max   INTEGER NOT NULL DEFAULT 0,
			ops_sec   INTEGER NOT NULL DEFAULT 0,
			hits      INTEGER NOT NULL DEFAULT 0,
			misses    INTEGER NOT NULL DEFAULT 0,
			conns     INTEGER NOT NULL DEFAULT 0,
			conns_max INTEGER NOT NULL DEFAULT 0,
			frag_ratio REAL NOT NULL DEFAULT 0,
			repl_link  INTEGER NOT NULL DEFAULT 1,
			repl_offset INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_metrics_instance_ts ON instance_metrics(instance, ts);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create metrics table: %w", err)
	}

	// 迁移：给已有表加新列（如果不存在则忽略错误）
	for _, col := range []string{
		"ALTER TABLE instance_metrics ADD COLUMN frag_ratio REAL NOT NULL DEFAULT 0",
		"ALTER TABLE instance_metrics ADD COLUMN repl_link INTEGER NOT NULL DEFAULT 1",
		"ALTER TABLE instance_metrics ADD COLUMN repl_offset INTEGER NOT NULL DEFAULT 0",
		"ALTER TABLE instance_metrics ADD COLUMN total_keys INTEGER NOT NULL DEFAULT 0",
	} {
		db.Exec(col) // 忽略 "duplicate column" 错误
	}

	return &metricsStore{db: db}, nil
}

func (ms *metricsStore) close() {
	if ms.db != nil {
		ms.db.Close()
	}
}

// record 从 INFO 输出中提取指标并存入数据库。
func (ms *metricsStore) record(instance, info string) error {
	ts := time.Now().Unix()
	memUsed := parseInfoField(info, "used_memory")
	memMax := parseInfoField(info, "maxmemory")
	opsSec := parseInfoField(info, "instantaneous_ops_per_sec")
	hits := parseInfoField(info, "keyspace_hits")
	misses := parseInfoField(info, "keyspace_misses")
	conns := parseInfoField(info, "connected_clients")
	connsMax := parseInfoFieldWithFallback(info, "maxclients", 10000)
	fragRatio := parseInfoFieldFloat(info, "mem_fragmentation_ratio")
	replLink := int64(1) // 1=up, 0=down
	if linkStatus := parseInfoString(info, "master_link_status"); linkStatus == "down" {
		replLink = 0
	}
	replOffset := parseInfoField(info, "master_repl_offset")
	totalKeys := parseKeyspaceTotal(info)

	_, err := ms.db.Exec(
		`INSERT INTO instance_metrics (ts, instance, mem_used, mem_max, ops_sec, hits, misses, conns, conns_max, frag_ratio, repl_link, repl_offset, total_keys) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ts, instance, memUsed, memMax, opsSec, hits, misses, conns, connsMax, fragRatio, replLink, replOffset, totalKeys,
	)
	return err
}

// query 返回指定实例在时间范围内的指标。
func (ms *metricsStore) query(instance string, start, end time.Time) ([]MetricPoint, error) {
	rows, err := ms.db.Query(
		`SELECT ts, mem_used, mem_max, ops_sec, hits, misses, conns, conns_max, frag_ratio, repl_link, repl_offset, total_keys FROM instance_metrics WHERE instance = ? AND ts >= ? AND ts <= ? ORDER BY ts`,
		instance, start.Unix(), end.Unix(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var points []MetricPoint
	for rows.Next() {
		var p MetricPoint
		if err := rows.Scan(&p.Ts, &p.MemUsed, &p.MemMax, &p.OpsSec, &p.Hits, &p.Misses, &p.Conns, &p.ConnsMax, &p.FragRatio, &p.ReplLink, &p.ReplOffset, &p.TotalKeys); err != nil {
			continue
		}
		points = append(points, p)
	}
	return points, nil
}

// cleanup 删除超过 retention 天的数据。
func (ms *metricsStore) cleanup(retentionDays int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays).Unix()
	_, err := ms.db.Exec(`DELETE FROM instance_metrics WHERE ts < ?`, cutoff)
	return err
}

// MetricPoint 是一个时间点的指标快照。
type MetricPoint struct {
	Ts         int64   `json:"ts"`
	MemUsed    int64   `json:"mem_used"`
	MemMax     int64   `json:"mem_max"`
	OpsSec     int64   `json:"ops_sec"`
	Hits       int64   `json:"hits"`
	Misses     int64   `json:"misses"`
	Conns      int64   `json:"conns"`
	ConnsMax   int64   `json:"conns_max"`
	FragRatio  float64 `json:"frag_ratio"`
	ReplLink   int64   `json:"repl_link"`
	ReplOffset int64   `json:"repl_offset"`
	TotalKeys  int64   `json:"total_keys"`
}

// parseKeyspaceTotal 从 INFO 的 Keyspace 部分汇总所有 db 的 keys 数量。
// 格式：db0:keys=1234,expires=100,avg_ttl=5000
func parseKeyspaceTotal(info string) int64 {
	var total int64
	lines := strings.Split(info, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "db") {
			continue
		}
		// db0:keys=1234,expires=100,avg_ttl=5000
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fields := strings.Split(parts[1], ",")
		for _, f := range fields {
			if strings.HasPrefix(f, "keys=") {
				v, _ := strconv.ParseInt(strings.TrimPrefix(f, "keys="), 10, 64)
				total += v
				break
			}
		}
	}
	return total
}

// parseInfoFieldWithFallback 解析 INFO 字段，如果解析失败或为 0 返回 fallback。
func parseInfoFieldWithFallback(info, field string, fallback int64) int64 {
	v := parseInfoField(info, field)
	if v == 0 {
		// 可能是字段不存在，也可能确实是 0，对 maxclients 来说用 fallback
		prefix := field + ":"
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, prefix) {
				val := strings.TrimPrefix(line, prefix)
				val = strings.TrimSpace(val)
				n, err := strconv.ParseInt(val, 10, 64)
				if err == nil {
					return n
				}
				return fallback
			}
		}
		return fallback
	}
	return v
}

// parseInfoFieldFloat 从 INFO 输出中解析一个浮点数字段。
func parseInfoFieldFloat(info, field string) float64 {
	prefix := field + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			val := strings.TrimPrefix(line, prefix)
			val = strings.TrimSpace(val)
			f, _ := strconv.ParseFloat(val, 64)
			return f
		}
	}
	return 0
}

// parseInfoString 从 INFO 输出中解析一个字符串字段。
func parseInfoString(info, field string) string {
	prefix := field + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}
