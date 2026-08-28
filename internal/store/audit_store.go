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
	ID            string                 `json:"id"`
	Timestamp     string                 `json:"timestamp"`
	TimestampMs   int64                  `json:"timestamp_ms"`
	Operator      string                 `json:"operator"`
	ActorType     string                 `json:"actor_type"`
	Source        string                 `json:"source"`
	Trigger       string                 `json:"trigger"`
	Action        string                 `json:"action"`
	Phase         string                 `json:"phase"`
	Level         string                 `json:"level"`
	ObjectType    string                 `json:"object_type"`
	ObjectID      string                 `json:"object_id"`
	GroupName     string                 `json:"group_name,omitempty"`
	InstanceName  string                 `json:"instance_name,omitempty"`
	CorrelationID string                 `json:"correlation_id,omitempty"`
	Target        map[string]interface{} `json:"target,omitempty"`
	Params        map[string]interface{} `json:"params,omitempty"`
	Before        map[string]interface{} `json:"before,omitempty"`
	After         map[string]interface{} `json:"after,omitempty"`
	Result        string                 `json:"result"`
	DurationMs    int64                  `json:"duration_ms,omitempty"`
	Detail        string                 `json:"detail,omitempty"`
}

// AuditFilter 查询条件。
type AuditFilter struct {
	From      string // YYYYMMDD
	To        string // YYYYMMDD
	Group     string
	Instance  string
	Operator  string
	ActorType string
	Source    string
	Level     string
	Action    string
	Result    string
	Query     string
	Limit     int
	Offset    int
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
	beforeJSON, _ := json.Marshal(r.Before)
	afterJSON, _ := json.Marshal(r.After)

	_, err := d.sql.Exec(`
		INSERT INTO audit_log(id, ts, timestamp_ms, date, operator, actor_type, source, trigger,
			action, phase, level, object_type, object_id, group_name, instance_name, correlation_id,
			target, params, before_state, after_state, result, duration_ms, detail)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.ID, r.Timestamp, r.TimestampMs, date,
		r.Operator, r.ActorType, r.Source, r.Trigger,
		r.Action, r.Phase, r.Level, r.ObjectType, r.ObjectID, r.GroupName, r.InstanceName, r.CorrelationID,
		string(targetJSON), string(paramsJSON), string(beforeJSON), string(afterJSON),
		r.Result, r.DurationMs, r.Detail,
	)
	return err
}

// QueryAudit 按条件查询审计日志，结果按时间倒序。
func (d *DB) QueryAudit(f AuditFilter) ([]AuditRecord, int, error) {
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
		where = append(where, "group_name = ?")
		args = append(args, f.Group)
	}
	if f.Instance != "" {
		where = append(where, "instance_name = ?")
		args = append(args, f.Instance)
	}
	if f.Operator != "" {
		where = append(where, "operator = ?")
		args = append(args, f.Operator)
	}
	if f.ActorType != "" {
		where = append(where, "actor_type = ?")
		args = append(args, f.ActorType)
	}
	if f.Source != "" {
		where = append(where, "source = ?")
		args = append(args, f.Source)
	}
	if f.Result != "" {
		where = append(where, "result = ?")
		args = append(args, f.Result)
	}
	if f.Query != "" {
		where = append(where, "(object_id LIKE ? OR group_name LIKE ? OR instance_name LIKE ? OR operator LIKE ?)")
		like := "%" + f.Query + "%"
		args = append(args, like, like, like, like)
	}

	whereCl := strings.Join(where, " AND ")

	// Count total
	var total int
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM audit_log WHERE %s`, whereCl)
	if err := d.sql.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	query := fmt.Sprintf(`
		SELECT id, ts, timestamp_ms, operator, actor_type, source, trigger, action, phase, level,
			object_type, object_id, group_name, instance_name, correlation_id,
			target, params, before_state, after_state, result, duration_ms, detail
		FROM audit_log
		WHERE %s
		ORDER BY timestamp_ms DESC, ts DESC, id DESC
		LIMIT ? OFFSET ?`, whereCl)
	args = append(args, f.Limit, f.Offset)

	rows, err := d.sql.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var results []AuditRecord
	for rows.Next() {
		var r AuditRecord
		var targetJSON, paramsJSON, beforeJSON, afterJSON string
		if err := rows.Scan(
			&r.ID, &r.Timestamp, &r.TimestampMs, &r.Operator, &r.ActorType, &r.Source, &r.Trigger,
			&r.Action, &r.Phase, &r.Level, &r.ObjectType, &r.ObjectID, &r.GroupName, &r.InstanceName, &r.CorrelationID,
			&targetJSON, &paramsJSON, &beforeJSON, &afterJSON,
			&r.Result, &r.DurationMs, &r.Detail,
		); err != nil {
			return nil, 0, err
		}
		json.Unmarshal([]byte(targetJSON), &r.Target) //nolint:errcheck
		json.Unmarshal([]byte(paramsJSON), &r.Params) //nolint:errcheck
		json.Unmarshal([]byte(beforeJSON), &r.Before) //nolint:errcheck
		json.Unmarshal([]byte(afterJSON), &r.After)   //nolint:errcheck
		results = append(results, r)
	}
	return results, total, rows.Err()
}
