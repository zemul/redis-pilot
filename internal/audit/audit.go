// Package audit 提供审计日志写入与查询，底层委托给 internal/store (SQLite)。
package audit

import (
	"fmt"
	"strings"
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
	db *store.DB
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
		r.ID = fmt.Sprintf("audit-%d", now.UnixNano())
	}
	if parsed, err := time.Parse(time.RFC3339, r.Timestamp); err == nil {
		r.TimestampMs = parsed.UnixMilli()
	} else {
		r.TimestampMs = now.UnixMilli()
	}
	normalize(&r)
	return l.db.InsertAudit(r)
}

// normalize 为所有调用方补齐稳定的审计维度。旧 handler 即使只提供传统的
// operator/action/target，也不会再写出缺级别、缺结果或无法筛选目标的记录。
func normalize(r *store.AuditRecord) {
	if r.Level == "" {
		r.Level = LevelNormal
	}
	if r.Result == "" {
		r.Result = "success"
	}
	if r.Phase == "" {
		switch r.Result {
		case "accepted", "running":
			r.Phase = r.Result
		case "failed", "interrupted", "skipped":
			r.Phase = r.Result
		default:
			r.Phase = "completed"
		}
	}
	if r.Operator == "" {
		r.Operator = "system"
	}
	if r.ActorType == "" {
		switch {
		case strings.HasPrefix(r.Operator, "ai:"):
			r.ActorType = "ai"
		case r.Operator == "system" || r.Operator == "sentinel" || r.Operator == "scheduler":
			r.ActorType = "system"
		default:
			r.ActorType = "user"
		}
	}
	if r.Source == "" {
		switch {
		case r.ActorType == "ai":
			r.Source = "ai"
		case strings.HasPrefix(r.Action, "reconcile."):
			r.Source = "reconcile"
		case r.ActorType == "system" && r.Action == "topology.failover":
			r.Source = "sentinel"
		case r.ActorType == "system":
			r.Source = "system"
		default:
			r.Source = "manual"
		}
	}
	if r.Trigger == "" {
		if r.ActorType == "system" {
			r.Trigger = "automatic"
		} else {
			r.Trigger = "manual"
		}
	}
	if r.Target == nil {
		r.Target = map[string]interface{}{}
	}
	if r.GroupName == "" {
		r.GroupName = mapString(r.Target, "group", "instance_group")
	}
	if r.InstanceName == "" {
		r.InstanceName = mapString(r.Target, "instance")
	}
	if r.CorrelationID == "" {
		r.CorrelationID = mapString(r.Target, "task_id", "correlation_id")
	}
	if r.ObjectType == "" || r.ObjectID == "" {
		inferObject(r)
	}
}

func mapString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key]; ok && value != nil && fmt.Sprint(value) != "" {
			return fmt.Sprint(value)
		}
	}
	return ""
}

func inferObject(r *store.AuditRecord) {
	switch {
	case r.Action == "topology.failover" && r.GroupName != "":
		r.ObjectType, r.ObjectID = "group", r.GroupName
	case r.InstanceName != "":
		r.ObjectType, r.ObjectID = "instance", r.InstanceName
	case r.GroupName != "":
		r.ObjectType, r.ObjectID = "group", r.GroupName
	case mapString(r.Target, "node") != "":
		r.ObjectType, r.ObjectID = "node", mapString(r.Target, "node")
	case mapString(r.Target, "username") != "":
		r.ObjectType, r.ObjectID = "user", mapString(r.Target, "username")
	default:
		r.ObjectType, r.ObjectID = "system", "redis-pilot"
	}
}

// Query 按条件查询审计日志，结果按时间倒序。
func (l *Logger) Query(f store.AuditFilter) ([]store.AuditRecord, int, error) {
	return l.db.QueryAudit(f)
}
