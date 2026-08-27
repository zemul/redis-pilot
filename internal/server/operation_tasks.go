package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type operationTaskPayload struct {
	Instance      string   `json:"instance"`
	Server        string   `json:"server"`
	Operator      string   `json:"operator"`
	BackupTs      string   `json:"backup_ts,omitempty"`
	LockSessionID string   `json:"lock_session_id,omitempty"`
	LockInstances []string `json:"lock_instances,omitempty"`
}

func newOperationTaskID(kind, instance string) string {
	return fmt.Sprintf("%s-%s-%d", strings.ReplaceAll(kind, ".", "-"), instance, time.Now().UnixNano())
}

func marshalTaskPayload(p operationTaskPayload) string {
	b, _ := json.Marshal(p)
	return string(b)
}

// StartOperationTaskTracker tracks Agent jobs independently of UI/CLI polling.
func (s *Server) StartOperationTaskTracker() {
	go func() {
		s.trackOperationTasksOnce()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.trackOperationTasksOnce()
		}
	}()
}

func (s *Server) trackOperationTasksOnce() {
	tasks, err := s.db.ListActiveTasks()
	if err != nil {
		s.log.Errorf("operation task tracker: list: %v", err)
		return
	}
	for _, task := range tasks {
		s.trackOperationTask(task)
	}
}

func (s *Server) trackOperationTask(task store.Task) {
	var p operationTaskPayload
	if err := json.Unmarshal([]byte(task.Payload), &p); err != nil {
		s.finishOperationTask(task, p, "failed", "", "invalid task payload: "+err.Error())
		return
	}
	if task.Kind == "backup.restore" {
		lastRenewed, _ := time.Parse(time.RFC3339, task.UpdatedAt)
		if time.Since(lastRenewed) >= time.Minute {
			if !s.renewLockGroup(p.LockInstances, p.LockSessionID) {
				s.finishOperationTask(task, p, "failed", "", "restore operation lock was lost")
				return
			}
			_ = s.db.UpdateTaskStatus(task.ID, task.Status, task.Result, task.Error)
		}
	}
	node, err := s.db.ReadNode()
	if err != nil || node.Servers[p.Server] == nil {
		return // transient Server state/read failure; retry next tick
	}
	path := "/instance/backup/status?task_id=" + task.ID
	if task.Kind == "backup.restore" {
		path = "/instance/restore/status?task_id=" + task.ID
	}
	body, err := newAgentClient(node.Servers[p.Server]).get(path)
	if err != nil {
		if strings.Contains(err.Error(), "task not found") {
			created, _ := time.Parse(time.RFC3339, task.CreatedAt)
			if task.Status == "pending" && time.Since(created) < 30*time.Second {
				return // initiation may still be between DB insert and Agent acceptance
			}
			s.finishOperationTask(task, p, "interrupted", "", "agent no longer has the task (it may have restarted)")
		}
		return
	}
	var envelope struct {
		Success bool `json:"success"`
		Data    struct {
			Status string `json:"status"`
			Error  string `json:"error"`
			Backup string `json:"backup"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || !envelope.Success {
		return
	}
	switch envelope.Data.Status {
	case "success", "failed":
		s.finishOperationTask(task, p, envelope.Data.Status, envelope.Data.Backup, envelope.Data.Error)
	case "running":
		if task.Status != "running" {
			_ = s.db.UpdateTaskStatus(task.ID, "running", "", "")
		}
	}
}

func (s *Server) finishOperationTask(task store.Task, p operationTaskPayload, status, result, errMsg string) {
	started, _ := time.Parse(time.RFC3339, task.CreatedAt)
	level := audit.LevelNormal
	if task.Kind == "backup.restore" {
		level = audit.LevelCritical
	}
	params := map[string]interface{}{}
	if p.BackupTs != "" {
		params["backup_ts"] = p.BackupTs
	}
	// Use a deterministic audit ID and write it before the terminal transition.
	// A crash at either point is safe: the tracker retries duplicate audit insertion,
	// while a real audit failure keeps the task active for another attempt.
	auditErr := s.audit.Log(store.AuditRecord{
		ID:       "audit-task-" + task.ID,
		Operator: p.Operator, Action: task.Kind, Level: level, Result: status,
		DurationMs: time.Since(started).Milliseconds(),
		Target:     map[string]interface{}{"instance": p.Instance, "server": p.Server, "task_id": task.ID},
		Params:     params, Detail: errMsg,
	})
	if auditErr != nil && !strings.Contains(auditErr.Error(), "UNIQUE constraint failed") {
		return
	}
	changed, err := s.db.CompleteTask(task.ID, status, result, errMsg)
	if err != nil || !changed {
		return
	}
	if p.LockSessionID != "" {
		s.releaseLockGroup(p.LockInstances, p.LockSessionID)
	}
}

func (s *Server) renewLockGroup(group []string, sessionID string) bool {
	if sessionID == "" || len(group) == 0 {
		return false
	}
	return s.renewInstanceLocks(group, sessionID)
}

func (s *Server) renewInstanceLocks(group []string, sessionID string) bool {
	renewed := true
	err := s.db.WithInstances(func(instances *apitypes.InstancesState) error {
		for _, name := range group {
			inst := instances.Instances[name]
			if inst == nil || !state.RenewLock(inst, sessionID) {
				renewed = false
			}
		}
		return nil
	})
	return err == nil && renewed
}
