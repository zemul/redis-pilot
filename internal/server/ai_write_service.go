package server

import (
	"fmt"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// aiExecInstanceOp 是 AI 助手写操作的执行入口（方案 A：仅由 /api/v1/ai/execute
// 在用户二次确认后调用，绝不在对话循环内触发）。op 取 "start" 或 "stop"。
//
// 它复用与人工 CLI 写操作相同的底层能力（resolveAndLockInternal + Agent 转发 +
// 状态回写 + 审计），差异仅在于审计的 operator 记为 "ai:<username>"，以便追溯
// “AI 代某用户执行”。为安全起见，stop 保留 master 保护（不提供 force 逃生阀）。
func (s *Server) aiExecInstanceOp(name, op, operator string) error {
	start := time.Now()
	if op != "start" && op != "stop" {
		return fmt.Errorf("unsupported op: %s", op)
	}
	if name == "" {
		return fmt.Errorf("missing instance name")
	}

	// stop 的 master 保护：AI 路径永不携带 force，遇到主从组 master 直接拒绝，
	// 提示先切主。与人工 instanceStop 的默认行为一致。
	if op == "stop" {
		instances, err := s.db.ReadInstances()
		if err != nil {
			return err
		}
		if inst, ok := instances.Instances[name]; ok && inst.Role == "master" {
			if group := instances.Groups[inst.Group]; group != nil && group.Type == "replication" {
				return fmt.Errorf("停止主从组 %q 的 master %q 会移除 Sentinel 监控；请先将某个 replica 提升为主，或改用人工 CLI 强制停止", inst.Group, name)
			}
		}
	}

	inst, srv, unlock, err := s.resolveAndLockInternal(name, op)
	if err != nil {
		return err
	}
	defer unlock()

	groupState, err := s.groupForInstance(name)
	if err != nil {
		return err
	}

	auditAction := "instance." + op
	client := newAgentClient(srv)
	if _, err := client.post("/instance/"+op, map[string]string{
		"name":   name,
		"engine": groupState.Engine,
	}); err != nil {
		s.audit.Log(store.AuditRecord{
			Operator: operator,
			Action:   auditAction, Level: audit.LevelImportant, Result: "failure",
			DurationMs: time.Since(start).Milliseconds(),
			Target:     map[string]interface{}{"instance": name, "server": inst.Server, "error": err.Error()},
		})
		return err
	}

	// 成功后同步意图与观测（Agent 成功返回即为一次观测），与 instanceSimpleOp 一致。
	stateMap := map[string]struct{ desired, observed string }{
		"start": {apitypes.DesiredRunning, apitypes.ObservedRunning},
		"stop":  {apitypes.DesiredStopped, apitypes.ObservedStopped},
	}
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok := is.Instances[name]; ok {
			if sc, ok2 := stateMap[op]; ok2 {
				i.DesiredState = sc.desired
				i.ObservedStatus = sc.observed
				state.RecalculateGroupTopology(is, i.Group)
			}
		}
		return nil
	}); err != nil {
		s.log.Errorf("ai exec %s: failed to persist status for %s: %v", op, name, err)
	}

	s.audit.Log(store.AuditRecord{
		Operator: operator,
		Action:   auditAction, Level: audit.LevelImportant, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:     map[string]interface{}{"instance": name, "server": inst.Server},
	})
	return nil
}
