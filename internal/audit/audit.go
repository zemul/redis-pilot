// Package audit 提供审计日志写入与查询，底层委托给 internal/store (SQLite)。
// 对外接口（Record、Logger、QueryFilter）与原 JSONL 版本保持一致。
package audit

import (
	"fmt"
	"sync/atomic"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

// Level 审计级别
type Level string

const (
	LevelNormal    Level = "normal"
	LevelImportant Level = "important"
	LevelCritical  Level = "critical"
)

// Record 审计日志记录（与 store.AuditRecord 字段对齐）
type Record struct {
	ID         string                 `json:"id"`
	Timestamp  string                 `json:"timestamp"`
	Operator   string                 `json:"operator"`
	Action     string                 `json:"action"`
	Level      Level                  `json:"level"`
	Target     map[string]interface{} `json:"target,omitempty"`
	Params     map[string]interface{} `json:"params,omitempty"`
	Result     string                 `json:"result"` // success | failed
	Duration   int64                  `json:"duration_ms,omitempty"`
	Detail     string                 `json:"detail,omitempty"`
}

// Logger 审计日志写入器
type Logger struct {
	db  *store.DB
	seq atomic.Int64
}

// New 创建审计日志写入器。
func New(db *store.DB) *Logger {
	return &Logger{db: db}
}

// Log 写入一条审计记录。
func (l *Logger) Log(r Record) error {
	now := time.Now()
	if r.Timestamp == "" {
		r.Timestamp = now.Format(time.RFC3339)
	}
	if r.ID == "" {
		seq := l.seq.Add(1)
		r.ID = fmt.Sprintf("audit-%s-%06d", now.Format("20060102"), seq)
	}

	return l.db.InsertAudit(store.AuditRecord{
		ID:         r.ID,
		Timestamp:  r.Timestamp,
		Operator:   r.Operator,
		Action:     r.Action,
		Level:      string(r.Level),
		Target:     r.Target,
		Params:     r.Params,
		Result:     r.Result,
		DurationMs: r.Duration,
		Detail:     r.Detail,
	})
}

// QueryFilter 审计日志查询条件
type QueryFilter struct {
	From     string // YYYYMMDD
	To       string // YYYYMMDD
	Group    string
	Instance string
	Level    string
	Action   string
	Limit    int
}

// Query 按条件查询审计日志，结果按时间倒序。
func (l *Logger) Query(f QueryFilter) ([]Record, error) {
	records, err := l.db.QueryAudit(store.AuditFilter{
		From:     f.From,
		To:       f.To,
		Group:    f.Group,
		Instance: f.Instance,
		Level:    f.Level,
		Action:   f.Action,
		Limit:    f.Limit,
	})
	if err != nil {
		return nil, err
	}

	result := make([]Record, 0, len(records))
	for _, r := range records {
		result = append(result, Record{
			ID:        r.ID,
			Timestamp: r.Timestamp,
			Operator:  r.Operator,
			Action:    r.Action,
			Level:     Level(r.Level),
			Target:    r.Target,
			Params:    r.Params,
			Result:    r.Result,
			Duration:  r.DurationMs,
			Detail:    r.Detail,
		})
	}
	return result, nil
}
