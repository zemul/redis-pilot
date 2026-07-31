// Package audit 提供审计日志写入与查询，底层委托给 internal/store (SQLite)。
package audit

import (
	"fmt"
	"sync/atomic"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

// 审计级别
const (
	LevelNormal    = "normal"
	LevelImportant = "important"
	LevelCritical  = "critical"
)

// Logger 审计日志写入器
type Logger struct {
	db  *store.DB
	seq atomic.Int64
}

// New 创建审计日志写入器。
func New(db *store.DB) *Logger {
	return &Logger{db: db}
}

// Log 写入一条审计记录（自动填充 ID 和 Timestamp）。
func (l *Logger) Log(r store.AuditRecord) error {
	now := time.Now()
	if r.Timestamp == "" {
		r.Timestamp = now.Format(time.RFC3339)
	}
	if r.ID == "" {
		seq := l.seq.Add(1)
		r.ID = fmt.Sprintf("audit-%s-%06d", now.Format("20060102"), seq)
	}
	return l.db.InsertAudit(r)
}

// Query 按条件查询审计日志，结果按时间倒序。
func (l *Logger) Query(f store.AuditFilter) ([]store.AuditRecord, error) {
	return l.db.QueryAudit(f)
}
