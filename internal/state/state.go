package state

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type Manager struct {
	dataDir string
	poolMu  sync.RWMutex
	instMu  sync.RWMutex
}

func NewManager(dataDir string) *Manager {
	return &Manager{dataDir: dataDir}
}

func (m *Manager) poolStatePath() string {
	return filepath.Join(m.dataDir, "pool-state.yaml")
}

func (m *Manager) instancesStatePath() string {
	return filepath.Join(m.dataDir, "instances-state.yaml")
}

func (m *Manager) ReadPool() (*apitypes.PoolState, error) {
	m.poolMu.RLock()
	defer m.poolMu.RUnlock()

	var state apitypes.PoolState
	data, err := os.ReadFile(m.poolStatePath())
	if os.IsNotExist(err) {
		state.Servers = make(map[string]*apitypes.PoolServer)
		return &state, nil
	}
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Servers == nil {
		state.Servers = make(map[string]*apitypes.PoolServer)
	}
	return &state, nil
}

func (m *Manager) WritePool(state *apitypes.PoolState) error {
	m.poolMu.Lock()
	defer m.poolMu.Unlock()
	return writeYAML(m.poolStatePath(), state)
}

func (m *Manager) ReadInstances() (*apitypes.InstancesState, error) {
	m.instMu.RLock()
	defer m.instMu.RUnlock()

	var state apitypes.InstancesState
	data, err := os.ReadFile(m.instancesStatePath())
	if os.IsNotExist(err) {
		state.Instances = make(map[string]*apitypes.Instance)
		return &state, nil
	}
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Instances == nil {
		state.Instances = make(map[string]*apitypes.Instance)
	}
	return &state, nil
}

func (m *Manager) WriteInstances(state *apitypes.InstancesState) error {
	m.instMu.Lock()
	defer m.instMu.Unlock()
	return writeYAML(m.instancesStatePath(), state)
}

// WithInstances 在写锁保护下原子执行 read → fn → write。
// fn 可修改 InstancesState，返回 error 时不写入。
func (m *Manager) WithInstances(fn func(*apitypes.InstancesState) error) error {
	m.instMu.Lock()
	defer m.instMu.Unlock()

	var st apitypes.InstancesState
	data, err := os.ReadFile(m.instancesStatePath())
	if os.IsNotExist(err) {
		st.Instances = make(map[string]*apitypes.Instance)
	} else if err != nil {
		return err
	} else {
		if err := yaml.Unmarshal(data, &st); err != nil {
			return err
		}
		if st.Instances == nil {
			st.Instances = make(map[string]*apitypes.Instance)
		}
	}

	if err := fn(&st); err != nil {
		return err
	}
	return writeYAML(m.instancesStatePath(), &st)
}

// TryAcquireLock 在已读取的实例上尝试获取操作锁，成功则修改 inst.Lock。
// 调用方负责 ReadInstances / WriteInstances。
func TryAcquireLock(inst *apitypes.Instance, heldBy, operation string, timeout int) error {
	now := time.Now()
	if inst.Lock != nil {
		if inst.Lock.HeldBy == heldBy {
			return nil // 同会话可重入
		}
		acquired, err := time.Parse(time.RFC3339, inst.Lock.AcquiredAt)
		if err != nil {
			// 时间格式损坏，视为锁仍有效，拒绝抢占
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

// InstanceGroup 返回实例所属组的所有实例名（主库+所有从库）。
func InstanceGroup(instances *apitypes.InstancesState, name string) []string {
	inst := instances.Instances[name]
	if inst == nil {
		return []string{name}
	}
	// 找主库
	master := name
	if inst.ReplicaOf != "" {
		for n, i := range instances.Instances {
			for _, r := range i.Replicas {
				if r == name {
					master = n
					break
				}
			}
		}
	}
	// 主库 + 所有从库
	group := []string{master}
	if m := instances.Instances[master]; m != nil {
		for _, r := range m.Replicas {
			if r != master {
				group = append(group, r)
			}
		}
	}
	return group
}

func writeYAML(path string, v interface{}) error {
	data, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
