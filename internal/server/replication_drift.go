package server

import (
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 运行时复制漂移
//
// 一个实例的"它在复制谁"这件事存在三份记录，三份可以互相偏离：
//
//	1. Server 记录的 Instance.ReplicaOf —— 意图，跟随 Sentinel 的决定
//	2. 磁盘配置文件里的 replicaof     —— 容器下次重启时会读到的角色
//	3. 实例进程内存里的实际复制目标   —— 此刻正在发生的事实
//
// 1↔2 的偏差由 detectRoleDrift / reconcileRoleDrift 处理（角色落盘）。1↔3 的偏差此前
// 没有任何机制发现，这是个出口缺失：制造 1↔3 偏差的入口有好几个——Sentinel 的 +switch-master
// 事件在 Server 重启或网络分区期间丢失、实例恰好在故障转移期间重启后按旧配置文件起来、
// 有人直接对容器跑了 podman/redis-cli——而 Server 侧三条自愈路径一条都不看运行时复制关系：
// runReconcile 只问容器在不在跑，reconcileSentinelOnce 只比对每组的 master 地址，
// detectRoleDrift 只比对配置文件。
//
// 结果是任何一次瞬时偏差都变成永久偏差。最典型的形态：三节点组 A/B/C，Sentinel 把
// master 从 A 切到 B，但 C 因为某个入口没收到，仍在复制 A。数据本身不会错（B→A→C 的
// 链式复制在 Redis 里是合法的），但 C 在 proxy_group 眼里 IsServing() 为真，照样进
// replica cluster 接读流量；一旦 A 停掉或 A→B 的复制链断开，C 就长期提供陈旧数据，
// 而所有监控指标都显示它健康。这类"看起来正常的错"比直接报错危险得多。
//
// 纠正动作的边界刻意收得很窄，理由是不能让 Server 变成第二个拓扑决策者：
//
//   - 只对 replica 下发 REPLICAOF，目标地址取自 Sentinel 确认过的当前 master。
//   - 绝不提升任何实例（不发 REPLICAOF NO ONE）。谁该当 master 是 Sentinel 的选举结果。
//   - Sentinel 启用时，先向 Sentinel 确认该组的 master 与 Server 记录一致，不一致就
//     什么都不做——那属于 reconcileSentinelOnce 的职责，两条路径同时动手会互相打架。
//
// 这样一来本机制只做一件事：把 Sentinel 已经做出、但在某个实例上丢失了的决定重新贴上去。

// replicaDrift 描述一处"实例进程实际在复制谁"与"Server 记录它应该复制谁"的不一致。
type replicaDrift struct {
	Instance string
	Server   string
	Group    string
	Runtime  string // 实际在复制的地址（ip:port，空表示它认为自己是 master）
	Expected string // Server 记录对应的地址（ip:port，空表示它应该是 master）
}

// detectReplicaDrift 比对 Agent 上报的运行时复制目标与 Server 记录，返回不一致的实例。
//
// 与 detectRoleDrift 保持同样的保守约定：只检查 replication 组；runtimeReplicaOf 里
// 没有的实例跳过（Agent 这一轮没探到，或实例正在 loading / 不可达，都不构成漂移证据）；
// 解析不出期望地址时跳过，否则会拿一个空地址去判定漂移，把正常 replica 当成要纠正的对象。
func detectReplicaDrift(
	node *apitypes.NodeState,
	instances *apitypes.InstancesState,
	serverName string,
	instNames []string,
	runtimeReplicaOf map[string]string,
) []replicaDrift {
	var drifts []replicaDrift
	for _, name := range instNames {
		inst := instances.Instances[name]
		if inst == nil {
			continue
		}
		group := instances.Groups[inst.Group]
		if group == nil || group.Type != "replication" {
			continue
		}
		runtime, reported := runtimeReplicaOf[name]
		if !reported {
			continue
		}
		expected, err := resolveReplicaOfAddr(node, instances, inst.ReplicaOf)
		if err != nil {
			continue
		}
		if runtime != expected {
			drifts = append(drifts, replicaDrift{
				Instance: name,
				Server:   serverName,
				Group:    inst.Group,
				Runtime:  runtime,
				Expected: expected,
			})
		}
	}
	return drifts
}

// reconcileReplicaDrift 对检测到的运行时复制漂移重新下发 REPLICAOF。
//
// 与 reconcileRoleDrift 一样，状态在编排锁内重新读取而不沿用检测时的快照：检测发生在
// 遍历各节点 Agent 的过程中，那段窗口里完成的故障转移会让旧快照里的 Expected 变成错的，
// 照旧快照下发等于把刚切好的复制关系又掰回去。
func (s *Server) reconcileReplicaDrift(drifts []replicaDrift) {
	if len(drifts) == 0 {
		return
	}
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()

	instances, err := s.db.ReadInstances()
	if err != nil {
		s.log.Errorf("reconcile replica drift: read instances: %v", err)
		return
	}
	node, err := s.db.ReadNode()
	if err != nil {
		s.log.Errorf("reconcile replica drift: read node: %v", err)
		return
	}

	// 每组只向 Sentinel 确认一次，避免同组多个实例漂移时重复查询。
	confirmed := make(map[string]bool)

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

		expected, err := resolveReplicaOfAddr(node, instances, inst.ReplicaOf)
		if err != nil {
			s.log.Errorf("reconcile replica drift for %s: resolve expected master: %v", d.Instance, err)
			continue
		}
		if expected != d.Expected {
			s.log.Infof("skipped replica drift correction for %s: topology changed since detection (detected=%q now=%q)",
				d.Instance, d.Expected, expected)
			s.auditReplicaDrift(d, "skipped",
				"topology changed between drift detection and correction; deferred to the next reconcile")
			continue
		}

		// 期望它是 master，而它实际在复制别人。这是提升/降级范畴的判断，不属于本机制：
		// 要么 Server 记录本身落后于 Sentinel（reconcileSentinelOnce 会纠正），要么
		// Sentinel 确实认为它该是 master 而它自己不知道（该由 Sentinel 提升）。
		// Server 在这里发 REPLICAOF NO ONE 就是自行提升实例，等于制造第二个决策者。
		if expected == "" {
			s.log.Errorf("replica drift needs attention: instance=%s should be master but is replicating %q; leaving promotion to sentinel",
				d.Instance, d.Runtime)
			s.auditReplicaDrift(d, "alert",
				"instance should be master but is replicating another node; promotion is sentinel's decision, not corrected here")
			continue
		}

		// Sentinel 是拓扑的唯一权威。下发之前必须确认它对该组 master 的认定与 Server
		// 记录一致，否则我们可能拿一个已经过期的地址去覆盖 Sentinel 刚配好的复制关系。
		if s.cfg.Sentinel.Enabled {
			okToApply, checked := confirmed[d.Group]
			if !checked {
				okToApply = s.sentinelAgreesOnMaster(node, instances, d.Group)
				confirmed[d.Group] = okToApply
			}
			if !okToApply {
				s.log.Infof("skipped replica drift correction for %s: sentinel and server disagree on group %s master; leaving it to sentinel reconcile",
					d.Instance, d.Group)
				s.auditReplicaDrift(d, "skipped",
					"sentinel and server records disagree on the group master; deferred to sentinel reconcile")
				continue
			}
		}

		client := newAgentClient(srv)
		if _, err := client.post("/instance/replicate", map[string]string{
			"name":       d.Instance,
			"replica_of": expected,
		}); err != nil {
			s.log.Errorf("reconcile replica drift for %s: %v", d.Instance, err)
			s.auditReplicaDrift(d, "failed", err.Error())
			continue
		}
		s.log.Infof("corrected runtime replication: instance=%s was replicating %q, now %q",
			d.Instance, d.Runtime, expected)
		s.auditReplicaDrift(d, "corrected",
			"instance was replicating a node other than the group master; re-issued REPLICAOF to the sentinel-confirmed master")
	}
}

// sentinelAgreesOnMaster 报告 Sentinel 认定的该组 master 是否与 Server 记录一致。
//
// 查询失败一律返回 false：拿不到 Sentinel 的意见时不能假定一致，宁可这一轮不纠正。
func (s *Server) sentinelAgreesOnMaster(
	node *apitypes.NodeState,
	instances *apitypes.InstancesState,
	groupName string,
) bool {
	group := instances.Groups[groupName]
	if group == nil || group.CurrentMaster == "" {
		return false
	}
	master := instances.Instances[group.CurrentMaster]
	if master == nil {
		return false
	}
	endpoint := nodeEndpoint(node, master.Server)
	if endpoint == "" {
		return false
	}
	recorded := fmt.Sprintf("%s:%d", endpoint, master.Port)

	nodes := s.selectSentinelNodes(node)
	if len(nodes) == 0 {
		return false
	}
	actual, err := s.querySentinelMaster(node, nodes, groupName)
	if err != nil {
		s.log.Errorf("replica drift: query sentinel master for group %s: %v", groupName, err)
		return false
	}
	if actual == "" {
		return false
	}
	return actual == recorded
}

// auditReplicaDrift 记录一次运行时复制漂移的处置结果。
//
// 级别是 Critical 而不是 Important：重新下发 REPLICAOF 会让目标实例丢弃自己与新 master
// 分叉的那部分数据（复制的固有语义），属于影响数据的动作，必须留下可追溯的记录。
func (s *Server) auditReplicaDrift(d replicaDrift, result, detail string) {
	s.audit.Log(store.AuditRecord{
		Operator: "system",
		Source:   "reconcile",
		Trigger:  "replica_drift",
		Action:   "reconcile.replica_drift",
		Level:    audit.LevelCritical,
		Result:   result,
		Target: map[string]interface{}{
			"instance":       d.Instance,
			"server":         d.Server,
			"instance_group": d.Group,
		},
		Params: map[string]interface{}{
			"runtime_replica_of":  d.Runtime,
			"expected_replica_of": d.Expected,
		},
		Detail: detail,
	})
}
