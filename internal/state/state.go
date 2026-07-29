// Package state 提供全局状态管理，底层委托给 internal/store (SQLite)。
// 对外接口与原 YAML 版本保持一致，上层 handler 无需改动。
package state

import (
	"fmt"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Manager 封装所有状态读写，底层使用 SQLite。
type Manager struct {
	db *store.DB
}

// NewManager 打开（或创建）dataDir/pilot.db 并返回 Manager。
func NewManager(dataDir string) (*Manager, error) {
	db, err := store.Open(dataDir)
	if err != nil {
		return nil, fmt.Errorf("state: open store: %w", err)
	}
	return &Manager{db: db}, nil
}

// Close 关闭底层数据库连接。
func (m *Manager) Close() error {
	return m.db.Close()
}

// DB 返回底层 store.DB，供需要直接访问 store 的模块使用（如 audit.Logger）。
func (m *Manager) DB() *store.DB {
	return m.db
}

// ── 节点 ──────────────────────────────────────────────────────────────────────

// ReadNode 返回当前节点状态快照（只读）。
func (m *Manager) ReadNode() (*apitypes.NodeState, error) {
	return m.db.ReadNodeState()
}

// WriteNode 覆写全量节点状态。
func (m *Manager) WriteNode(state *apitypes.NodeState) error {
	return m.db.WriteNodeState(state)
}

// WithNode 在事务中原子执行 read → fn → write。
func (m *Manager) WithNode(fn func(*apitypes.NodeState) error) error {
	return m.db.WithServers(func(servers map[string]*apitypes.NodeServer) error {
		st := &apitypes.NodeState{Servers: servers}
		return fn(st)
	})
}

// ── 实例 ──────────────────────────────────────────────────────────────────────

// ReadInstances 返回当前实例状态快照（只读）。
func (m *Manager) ReadInstances() (*apitypes.InstancesState, error) {
	return m.db.ReadInstances()
}

// WriteInstances 覆写全量实例状态。
func (m *Manager) WriteInstances(state *apitypes.InstancesState) error {
	return m.db.WithInstances(func(st *apitypes.InstancesState) error {
		st.Groups = state.Groups
		st.Instances = state.Instances
		return nil
	})
}

// WithInstances 在事务中原子执行 read → fn → write。
func (m *Manager) WithInstances(fn func(*apitypes.InstancesState) error) error {
	return m.db.WithInstances(fn)
}

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
