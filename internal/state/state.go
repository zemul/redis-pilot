// Package state 提供状态管理辅助函数（锁、拓扑计算等纯函数）。
// 状态读写直接使用 internal/store.DB。
package state

import (
	"fmt"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// ── 锁 ────────────────────────────────────────────────────────────────────────

// TryAcquireLock 在已读取的实例上尝试获取操作锁，成功则修改 inst.Lock。
// 调用方负责在 WithInstances 内调用。
func TryAcquireLock(inst *apitypes.Instance, heldBy, operation string, timeout int) error {
	now := time.Now()
	if inst.Lock != nil {
		if inst.Lock.HeldBy == heldBy {
			return nil // 同会话可重入
		}
		acquired, err := time.Parse(time.RFC3339, inst.Lock.AcquiredAt)
		if err != nil {
			return fmt.Errorf("locked by %s (operation: %s, bad acquired_at)", inst.Lock.HeldBy, inst.Lock.Operation)
		}
		if now.Sub(acquired) < time.Duration(inst.Lock.Timeout)*time.Second {
			return fmt.Errorf("locked by %s (operation: %s)", inst.Lock.HeldBy, inst.Lock.Operation)
		}
	}
	if timeout <= 0 {
		timeout = 300
	}
	inst.Lock = &apitypes.Lock{
		HeldBy:     heldBy,
		Operation:  operation,
		AcquiredAt: now.Format(time.RFC3339),
		Timeout:    timeout,
	}
	return nil
}

// ReleaseLock 清除实例上的操作锁。调用方负责 WriteInstances。
func ReleaseLock(inst *apitypes.Instance, heldBy string) {
	if inst.Lock != nil && (inst.Lock.HeldBy == heldBy || heldBy == "") {
		inst.Lock = nil
	}
}

// RenewLock refreshes a lock lease owned by heldBy without changing its operation.
func RenewLock(inst *apitypes.Instance, heldBy string) bool {
	if inst.Lock == nil || inst.Lock.HeldBy != heldBy {
		return false
	}
	inst.Lock.AcquiredAt = time.Now().Format(time.RFC3339)
	return true
}

// ── 拓扑辅助 ──────────────────────────────────────────────────────────────────

// InstanceGroup 返回实例所属组的所有实例名（主库+所有从库）。
func InstanceGroup(instances *apitypes.InstancesState, name string) []string {
	inst := instances.Instances[name]
	if inst == nil {
		return []string{name}
	}
	if inst.Group != "" {
		group := make([]string, 0)
		for n, i := range instances.Instances {
			if i != nil && i.Group == inst.Group {
				group = append(group, n)
			}
		}
		if len(group) > 0 {
			return group
		}
	}
	return []string{name}
}

// RecalculateGroupTopology 根据实例实际状态刷新组拓扑派生字段。
func RecalculateGroupTopology(instances *apitypes.InstancesState, groupName string) {
	group := instances.Groups[groupName]
	if group == nil {
		return
	}
	hasRunningMaster := false
	hasRunningReplica := false
	for name, inst := range instances.Instances {
		if inst == nil || inst.Group != groupName {
			continue
		}
		// 组的拓扑健康度是一个观测结论，必须读 ObservedStatus 而不是意图。
		// 用意图判断会得出"配置上应该是健康的"这种毫无用处的结论。
		if name == group.CurrentMaster && inst.Role == "master" && inst.IsServing() {
			hasRunningMaster = true
		}
		if inst.Role == "replica" && inst.IsServing() {
			hasRunningReplica = true
		}
	}
	if hasRunningMaster && (group.Type != "replication" || hasRunningReplica) {
		group.TopologyStatus = "healthy"
	} else {
		group.TopologyStatus = "degraded"
	}
	group.UpdatedAt = time.Now().Format(time.RFC3339)
}
