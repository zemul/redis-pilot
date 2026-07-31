package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ── 审计日志结构（审计日志结构体） ────────

// AuditRecord 审计日志条目。
type AuditRecord struct {
	ID         string                 `json:"id"`
	Timestamp  string                 `json:"timestamp"`
	Operator   string                 `json:"operator"`
	Action     string                 `json:"action"`
	Level      string                 `json:"level"`
	Target     map[string]interface{} `json:"target,omitempty"`
	Params     map[string]interface{} `json:"params,omitempty"`
	Result     string                 `json:"result"`
	DurationMs int64                  `json:"duration_ms,omitempty"`
	Detail     string                 `json:"detail,omitempty"`
}

// AuditFilter 查询条件。
type AuditFilter struct {
	From     string // YYYYMMDD
	To       string // YYYYMMDD
	Group    string
	Instance string
	Level    string
	Action   string
	Limit    int
}

// InsertAudit 写入一条审计记录。
func (d *DB) InsertAudit(r AuditRecord) error {
	now := time.Now()
	if r.Timestamp == "" {
		r.Timestamp = now.Format(time.RFC3339)
	}
	date := now.Format("20060102")
	// 解析 timestamp 里的日期更准确
	if t, err := time.Parse(time.RFC3339, r.Timestamp); err == nil {
		date = t.Format("20060102")
	}

	targetJSON, _ := json.Marshal(r.Target)
	paramsJSON, _ := json.Marshal(r.Params)

	_, err := d.sql.Exec(`
		INSERT OR IGNORE INTO audit_log(id, ts, date, operator, action, level, target, params, result, duration_ms, detail)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		r.ID, r.Timestamp, date,
		r.Operator, r.Action, r.Level,
		string(targetJSON), string(paramsJSON),
		r.Result, r.DurationMs, r.Detail,
	)
	return err
}

// QueryAudit 按条件查询审计日志，结果按时间倒序。
func (d *DB) QueryAudit(f AuditFilter) ([]AuditRecord, error) {
	if f.Limit <= 0 {
		f.Limit = 30
	}

	where := []string{"1=1"}
	args := []interface{}{}

	if f.From != "" {
		where = append(where, "date >= ?")
		args = append(args, f.From)
	}
	if f.To != "" {
		where = append(where, "date <= ?")
		args = append(args, f.To)
	}
	if f.Level != "" {
		where = append(where, "level = ?")
		args = append(args, f.Level)
	}
	if f.Action != "" {
		where = append(where, "action = ?")
		args = append(args, f.Action)
	}
	if f.Group != "" {
		// target 字段是 JSON，用 json_extract 查询
		where = append(where, "json_extract(target, '$.group') = ?")
		args = append(args, f.Group)
	}
	if f.Instance != "" {
		where = append(where, "json_extract(target, '$.instance') = ?")
		args = append(args, f.Instance)
	}

	query := fmt.Sprintf(`
		SELECT id, ts, operator, action, level, target, params, result, duration_ms, detail
		FROM audit_log
		WHERE %s
		ORDER BY ts DESC
		LIMIT ?`, strings.Join(where, " AND "))
	args = append(args, f.Limit)

	rows, err := d.sql.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []AuditRecord
	for rows.Next() {
		var r AuditRecord
		var targetJSON, paramsJSON string
		if err := rows.Scan(
			&r.ID, &r.Timestamp, &r.Operator, &r.Action, &r.Level,
			&targetJSON, &paramsJSON,
			&r.Result, &r.DurationMs, &r.Detail,
		); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(targetJSON), &r.Target) //nolint:errcheck
		json.Unmarshal([]byte(paramsJSON), &r.Params) //nolint:errcheck
		results = append(results, r)
	}
	return results, rows.Err()
}
