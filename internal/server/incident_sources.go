package server

import (
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// emitReconcileIncidents 把一轮 reconcile 的观测结果转成异常观测。
//
// 两类：
//   - node_agent_unreachable：按服务器聚合，Agent 不可达一轮一条节点级观测，
//     evidence 里列出受影响实例；恢复(可达)则提交正常观测推动去抖恢复。
//   - instance_unexpected_down：仅对可达服务器上的实例逐个提交。期望 running 却
//     stopped/missing 为异常；服务器不可达时跳过(由节点级异常覆盖，避免告警风暴)。
func (s *Server) emitReconcileIncidents(results []ReconcileResult, serverReachable map[string]bool, instances *apitypes.InstancesState) {
	if s.incidents == nil {
		return
	}

	// 节点级：Agent 可达性。
	for srv, reachable := range serverReachable {
		var affected []string
		for _, r := range results {
			if r.Server == srv {
				affected = append(affected, r.Instance)
			}
		}
		s.incidents.Observe(Observation{
			Type:       "node_agent_unreachable",
			Active:     !reachable,
			Severity:   "critical",
			Source:     "reconcile",
			ObjectType: "node",
			ObjectID:   srv,
			NodeName:   srv,
			Summary:    fmt.Sprintf("节点 %s 的 Agent 不可达", srv),
			Reason:     "reconcile 请求 Agent 失败、超时或响应无法解析",
			Evidence:   map[string]any{"affected_instances": affected},
		})
	}

	// 实例级：意外停止/丢失，仅针对可达服务器。
	for _, r := range results {
		if reachable, ok := serverReachable[r.Server]; !ok || !reachable {
			continue // 服务器不可达：本轮无可信实例观测，交给节点级异常
		}
		var group, desired string
		if inst := instances.Instances[r.Instance]; inst != nil {
			group = inst.Group
			desired = inst.DesiredState
		}
		if desired == apitypes.DesiredStopped {
			continue // 用户主动停止，不是异常；也不推动去抖
		}
		active := r.Actual == apitypes.ObservedStopped || r.Actual == apitypes.ObservedMissing
		if !active && r.Actual != apitypes.ObservedRunning {
			continue // unknown/unreachable 等既不算异常也不算恢复，跳过
		}
		s.incidents.Observe(Observation{
			Type:         "instance_unexpected_down",
			Active:       active,
			Severity:     "critical",
			Source:       "reconcile",
			ObjectType:   "instance",
			ObjectID:     r.Instance,
			InstanceName: r.Instance,
			GroupName:    group,
			NodeName:     r.Server,
			Summary:      fmt.Sprintf("实例 %s 未按预期运行", r.Instance),
			Reason:       fmt.Sprintf("期望 running，实际观测为 %s", r.Actual),
			Evidence:     map[string]any{"desired": r.Desired, "actual": r.Actual, "server": r.Server},
		})
	}
}

// emitMasterMismatch 提交组级 Master 视角一致性观测(去抖)。
//
//   - masterName：平台记录的当前 master 实例名(用于展示)。
//   - expectedAddr：平台 master 的 host:port。
//   - sentinelAddr：Sentinel 共识返回的 host:port，空表示本轮未取得共识(未知视角)。
//   - sentinelResolved：Sentinel 共识解析成的实例名(解析不到时为原始地址)。
//
// 未知视角(sentinelAddr 为空)既不判定异常也不推动恢复，直接跳过。
func (s *Server) emitMasterMismatch(group, masterName, expectedAddr, sentinelAddr, sentinelResolved string) {
	if s.incidents == nil {
		return
	}
	if sentinelAddr == "" {
		return
	}
	s.incidents.Observe(Observation{
		Type:       "master_mismatch",
		Active:     sentinelAddr != expectedAddr,
		Severity:   "critical",
		Source:     "topology",
		ObjectType: "group",
		ObjectID:   group,
		GroupName:  group,
		Summary:    fmt.Sprintf("实例组 %s 的 Master 视角不一致", group),
		Reason:     "Sentinel 共识 Master 与平台记录不一致",
		Evidence:   map[string]any{"platform_master": masterName, "sentinel_master": sentinelResolved},
	})
}

// emitFailoverConflict 提交组级故障转移冲突观测。冲突标记为一次性事实：
// true 立即记 open，false 立即结束对应异常(不走去抖)。
func (s *Server) emitFailoverConflict(group string, conflict bool) {
	if s.incidents == nil {
		return
	}
	if conflict {
		s.incidents.Report(Observation{
			Type:       "failover_conflict",
			Severity:   "critical",
			Source:     "topology",
			ObjectType: "group",
			ObjectID:   group,
			GroupName:  group,
			Summary:    fmt.Sprintf("实例组 %s 存在故障转移冲突", group),
			Reason:     "failover_conflict 标记为 true",
		})
		return
	}
	s.incidents.Resolve("failover_conflict", "group", group)
}

// emitTaskIncident 处理备份/恢复任务终态：
//   - failed/interrupted：立即记一条 open 异常(不去抖)。
//   - success：结束对应实例上同类型的 open 异常。
//   - canceled：用户主动取消，不记也不结束。
func (s *Server) emitTaskIncident(kind, instance, group, server, status, errMsg, taskID string) {
	if s.incidents == nil || instance == "" {
		return
	}
	var itype, severity string
	switch kind {
	case "backup.create":
		itype, severity = "backup_task_failed", "warning"
	case "backup.restore":
		itype, severity = "restore_task_failed", "critical"
	default:
		return // 其它任务类型(如 diagnostic.bigkeys)不记异常
	}
	switch status {
	case "failed", "interrupted":
		s.incidents.Report(Observation{
			Type:         itype,
			Severity:     severity,
			Source:       "task",
			ObjectType:   "instance",
			ObjectID:     instance,
			InstanceName: instance,
			GroupName:    group,
			NodeName:     server,
			Summary:      fmt.Sprintf("实例 %s 的 %s 任务失败", instance, kind),
			Reason:       fmt.Sprintf("任务进入 %s", status),
			Evidence:     map[string]any{"task_id": taskID, "error": truncateEvidenceText(errMsg)},
		})
	case "success":
		s.incidents.Resolve(itype, "instance", instance)
	}
}

// truncateEvidenceText 截断错误文本，避免大响应/堆栈原样入库。
func truncateEvidenceText(s string) string {
	const max = 500
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}
