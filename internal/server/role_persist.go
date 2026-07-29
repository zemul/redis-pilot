package server

import (
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 角色落盘
//
// 问题：Sentinel 在运行时用 REPLICAOF 改变实例角色，这个变更只存在于 Redis 进程内存中。
// 磁盘配置文件里仍写着旧的 replicaof，而 podman 的 --restart on-failure:5
// （podman.go:48,63）会在进程异常退出时自动重启容器。重启后实例按旧配置文件起来，
// 角色就回退了 —— Sentinel 认为 A 是 master，A 自己却又变回了 B 的 replica。
//
// 解决思路是让配置文件的角色始终跟随 Server 记录（Server 记录又跟随 Sentinel 的决定）：
//   1. 角色变化时立即推一次配置重写（handleSentinelFailover）
//   2. reconcile 周期性比对磁盘配置与 Server 记录，不一致就补推（reconcileRoleDrift）
//
// 第 2 条是关键。只做第 1 条的话，Sentinel 事件一旦丢失（Server 重启、Server 与
// Sentinel 之间网络分区），配置文件就停在旧角色上，窗口依然存在。周期性比对让这套机制
// 收敛到期望状态，而不是依赖事件可靠投递 —— 与 Wave 0 修 Sentinel 监控用的是同一个思路。

// resolveReplicaOfAddr 把 Server 记录的 ReplicaOf（实例名）解析为 "ip:port"。
//
// 与 resolveReplicaOf 的区别：后者用于校验用户输入，会断言目标必须是 master。
// 这里刻意不做该断言 —— 角色落盘发生在故障转移前后，此刻组内角色可能正处于中间状态，
// 需要的是"把 Server 当前记录如实写进配置文件"，而不是校验这份记录是否合理。
//
// ReplicaOf 为空（即该实例是 master）时返回空串。
func resolveReplicaOfAddr(node *apitypes.NodeState, instances *apitypes.InstancesState, replicaOf string) (string, error) {
	if replicaOf == "" {
		return "", nil
	}
	master, exists := instances.Instances[replicaOf]
	if !exists || master == nil {
		return "", fmt.Errorf("replica_of instance not found: %s", replicaOf)
	}
	endpoint := nodeEndpoint(node, master.Server)
	if endpoint == "" {
		return "", fmt.Errorf("cannot resolve endpoint for server: %s", master.Server)
	}
	return fmt.Sprintf("%s:%d", endpoint, master.Port), nil
}

// resolveReplicaOfAddrFor 读取当前状态，把指定实例记录的 ReplicaOf（实例名）解析为
// "ip:port"。实例是 master 时返回空串。
func (s *Server) resolveReplicaOfAddrFor(name string) (string, error) {
	instances, err := s.state.ReadInstances()
	if err != nil {
		return "", err
	}
	inst := instances.Instances[name]
	if inst == nil {
		return "", fmt.Errorf("instance not found: %s", name)
	}
	if inst.ReplicaOf == "" {
		return "", nil
	}
	node, err := s.state.ReadNode()
	if err != nil {
		return "", err
	}
	return resolveReplicaOfAddr(node, instances, inst.ReplicaOf)
}

// pushConfigRewrite 让 Agent 把实例的磁盘配置文件重写成与 Server 记录一致，
// 不重启容器。replicaOfAddr 为空表示写成 master。
func (s *Server) pushConfigRewrite(
	srv *apitypes.NodeServer,
	group *apitypes.InstanceGroupState,
	name string,
	inst *apitypes.Instance,
	replicaOfAddr string,
) error {
	client := newAgentClient(srv)
	_, err := client.post("/instance/config", map[string]interface{}{
		"name":                name,
		"engine":              group.Engine,
		"type":                group.Type,
		"category":            group.Category,
		"config_overrides":    inst.ConfigOverrides,
		"rewrite_config_only": true,
		"password":            inst.Password,
		"memory":              inst.Memory,
		"replica_of":          replicaOfAddr,
		"port":                inst.Port,
	})
	return err
}

// persistGroupRoles 把某个复制组内所有实例的角色写进各自的磁盘配置文件。
//
// 在故障转移同步完成后调用。失败不影响故障转移本身已经生效的结果（运行时角色由 Sentinel
// 保证），只是磁盘那份还没跟上；reconcileRoleDrift 会在下一轮补推，所以这里对单个实例的
// 失败只记日志、继续处理其余实例。
func (s *Server) persistGroupRoles(groupName string) {
	instances, err := s.state.ReadInstances()
	if err != nil {
		s.log.Errorf("persist group roles: read instances: %v", err)
		return
	}
	node, err := s.state.ReadNode()
	if err != nil {
		s.log.Errorf("persist group roles: read node: %v", err)
		return
	}
	group := instances.Groups[groupName]
	if group == nil {
		return
	}

	for name, inst := range instances.Instances {
		if inst == nil || inst.Group != groupName {
			continue
		}
		srv := node.Servers[inst.Server]
		if srv == nil {
			continue
		}
		addr, err := resolveReplicaOfAddr(node, instances, inst.ReplicaOf)
		if err != nil {
			s.log.Errorf("persist role for %s: %v", name, err)
			continue
		}
		if err := s.pushConfigRewrite(srv, group, name, inst, addr); err != nil {
			s.log.Errorf("persist role for %s: %v", name, err)
			continue
		}
		s.log.Infof("persisted role to config file: instance=%s replica_of=%q", name, addr)
	}
}

// roleDrift 描述一处"磁盘配置文件里的角色"与"Server 记录的角色"不一致。
type roleDrift struct {
	Instance   string
	Server     string
	Configured string // 磁盘配置文件里的 replicaof（ip:port，空表示 master）
	Expected   string // Server 记录对应的 replicaof（ip:port，空表示 master）
}

// detectRoleDrift 比对 Agent 上报的磁盘配置角色与 Server 记录，返回不一致的实例。
//
// 只检查 replication 组：standalone 实例永远不该有 replicaof，也没有角色概念。
// configuredReplicaOf 中不存在的实例跳过 —— 配置文件缺失说明实例刚创建或已删除，
// 不是需要补推的漂移。
func detectRoleDrift(
	node *apitypes.NodeState,
	instances *apitypes.InstancesState,
	serverName string,
	instNames []string,
	configuredReplicaOf map[string]string,
) []roleDrift {
	var drifts []roleDrift
	for _, name := range instNames {
		inst := instances.Instances[name]
		if inst == nil {
			continue
		}
		group := instances.Groups[inst.Group]
		if group == nil || group.Type != "replication" {
			continue
		}
		configured, reported := configuredReplicaOf[name]
		if !reported {
			continue
		}
		expected, err := resolveReplicaOfAddr(node, instances, inst.ReplicaOf)
		if err != nil {
			// 解析不出期望地址时不能判定漂移，否则会拿一个空地址去覆盖配置文件，
			// 把一个正常的 replica 改写成 master。
			continue
		}
		if configured != expected {
			drifts = append(drifts, roleDrift{
				Instance:   name,
				Server:     serverName,
				Configured: configured,
				Expected:   expected,
			})
		}
	}
	return drifts
}

// reconcileRoleDrift 对检测到的角色漂移补推配置重写。
func (s *Server) reconcileRoleDrift(
	node *apitypes.NodeState,
	instances *apitypes.InstancesState,
	drifts []roleDrift,
) {
	for _, d := range drifts {
		inst := instances.Instances[d.Instance]
		if inst == nil {
			continue
		}
		group := instances.Groups[inst.Group]
		srv := node.Servers[inst.Server]
		if group == nil || srv == nil {
			continue
		}
		if err := s.pushConfigRewrite(srv, group, d.Instance, inst, d.Expected); err != nil {
			s.log.Errorf("reconcile role drift for %s: %v", d.Instance, err)
			continue
		}
		s.log.Infof("corrected role drift in config file: instance=%s configured=%q expected=%q",
			d.Instance, d.Configured, d.Expected)
	}
}
