package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// ReconcileResult 单个实例的校验结果
type ReconcileResult struct {
	Instance string `json:"instance"`
	Server   string `json:"server"`
	Desired  string `json:"desired"` // instances-state 中的 status
	Actual   string `json:"actual"`  // 容器实际状态: running | stopped | missing
	Action   string `json:"action"`  // 采取的动作: none | updated | alert
}

func (s *Server) reconcile(c *gin.Context) {
	results, err := s.runReconcile()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, results)
}

// statusPatch 记录 reconcile 发现的单个实例状态变更
// statusPatch 是一次待应用的观测结果写入。
// 只承载观测字段——reconcile 没有资格修改意图。
type statusPatch struct {
	name        string
	newObserved string
}

// desiredMatchesObserved 报告观测结果是否符合意图。
//
//	意图 running → 只有观测到 running 才算符合
//	意图 stopped → 观测到 stopped 或 missing 都算符合（用户要它停，容器被清掉也是停）
//
// 尚未观测（空串）不算符合，但也不是异常——调用方据此产生 "creating" 语义。
func desiredMatchesObserved(desired, observed string) bool {
	switch desired {
	case apitypes.DesiredStopped:
		return observed == apitypes.ObservedStopped || observed == apitypes.ObservedMissing
	default: // DesiredRunning
		return observed == apitypes.ObservedRunning
	}
}

// runReconcile 执行状态校验，HTTP handler 和定时任务共用
func (s *Server) runReconcile() ([]ReconcileResult, error) {
	instances, err := s.db.ReadInstances()
	if err != nil {
		return nil, err
	}
	node, err := s.db.ReadNode()
	if err != nil {
		return nil, err
	}

	// 按 server 分组实例
	serverInstances := make(map[string][]string)
	for name, inst := range instances.Instances {
		serverInstances[inst.Server] = append(serverInstances[inst.Server], name)
	}

	var results []ReconcileResult
	var patches []statusPatch
	var roleDrifts []roleDrift
	var replicaDrifts []replicaDrift
	serverReachable := make(map[string]bool)

	for serverName, instNames := range serverInstances {
		srv := node.Servers[serverName]
		if srv == nil {
			// 服务器不在 node 中，所有实例标记异常
			serverReachable[serverName] = false
			for _, name := range instNames {
				results = append(results, ReconcileResult{
					Instance: name, Server: serverName,
					Desired: instances.Instances[name].DesiredState,
					Actual:  "unknown",
					Action:  "alert",
				})
			}
			continue
		}

		// 调 Agent 获取实际容器列表
		client := newAgentClient(srv)
		resp, err := client.get("/instance/list")
		if err != nil {
			// Agent 不可达，所有实例标记异常
			serverReachable[serverName] = false
			for _, name := range instNames {
				results = append(results, ReconcileResult{
					Instance: name, Server: serverName,
					Desired: instances.Instances[name].DesiredState,
					Actual:  "unreachable",
					Action:  "alert",
				})
			}
			continue
		}

		// 解析 Agent 返回的容器列表
		var apiResp apitypes.APIResponse
		if err := json.Unmarshal(resp, &apiResp); err != nil {
			// Agent 响应格式异常（网络抖动/截断/非预期响应），不能当作"容器全部消失"处理，
			// 否则会把所有实例误判为 missing 并标记为 failed。按 Agent 不可达的方式处理。
			serverReachable[serverName] = false
			for _, name := range instNames {
				results = append(results, ReconcileResult{
					Instance: name, Server: serverName,
					Desired: instances.Instances[name].DesiredState,
					Actual:  "unreachable",
					Action:  "alert",
				})
			}
			continue
		}
		containerMap := parseContainerMap(apiResp.Data)
		serverReachable[serverName] = true
		// Agent 同时上报了各实例磁盘配置文件里的角色，用于检测角色漂移。
		configuredReplicaOf := parseConfiguredReplicaOf(apiResp.Data)
		roleDrifts = append(roleDrifts,
			detectRoleDrift(node, instances, serverName, instNames, configuredReplicaOf)...)
		// 以及各实例进程此刻实际在复制谁，用于检测运行时复制漂移。
		runtimeReplicaOf := parseRuntimeReplicaOf(apiResp.Data)
		replicaDrifts = append(replicaDrifts,
			detectReplicaDrift(node, instances, serverName, instNames, runtimeReplicaOf)...)

		// 缓存运行时复制关系，供列表接口推导每组"运行时实际 master"（大盘三视角展示）。
		if len(runtimeReplicaOf) > 0 {
			s.runtimeReplMu.Lock()
			for name, addr := range runtimeReplicaOf {
				s.runtimeReplicaOf[name] = addr
			}
			s.runtimeReplMu.Unlock()
		}

		// 缓存实例内存使用
		usedMemory := parseUsedMemory(apiResp.Data)
		if len(usedMemory) > 0 {
			s.memoryUsedMu.Lock()
			for name, v := range usedMemory {
				s.memoryUsed[name] = v
			}
			s.memoryUsedMu.Unlock()
		}

		// 逐个对比
		for _, name := range instNames {
			inst := instances.Instances[name]
			containerName := inst.Container
			if containerName == "" {
				if group := instances.Groups[inst.Group]; group != nil {
					containerName = group.Engine + "-" + name
				}
			}

			actual := apitypes.ObservedMissing
			if status, exists := containerMap[containerName]; exists {
				if status {
					actual = apitypes.ObservedRunning
				} else {
					actual = apitypes.ObservedStopped
				}
			}

			result := ReconcileResult{
				Instance: name, Server: serverName,
				Desired: inst.DesiredState, Actual: actual,
			}

			// reconcile 是观测通道：只写 ObservedStatus，绝不碰 DesiredState。
			// 动作由"意图 vs 观测"的差值推导，而不是靠状态字段里塞一个
			// unexpected_stopped 这类混合语义的值。
			changed := actual != inst.ObservedStatus
			if changed {
				patches = append(patches, statusPatch{name: name, newObserved: actual})
			}

			switch {
			case !desiredMatchesObserved(inst.DesiredState, actual):
				// 意图与观测不符：没人要求它停它却停了，或者容器不见了
				result.Action = "alert"
			case changed:
				// 与意图一致，且观测记录本轮追上了现实
				result.Action = "updated"
			default:
				result.Action = "none"
			}

			results = append(results, result)
		}
	}

	// 在写锁内原子应用状态变更，避免覆盖并发写入
	if len(patches) > 0 {
		if err := s.db.WithInstances(func(st *apitypes.InstancesState) error {
			affectedGroups := map[string]struct{}{}
			for _, p := range patches {
				if inst := st.Instances[p.name]; inst != nil {
					inst.ObservedStatus = p.newObserved
					affectedGroups[inst.Group] = struct{}{}
				}
			}
			for g := range affectedGroups {
				state.RecalculateGroupTopology(st, g)
			}
			return nil
		}); err != nil {
			s.log.Errorf("reconcile: failed to persist status updates: %v", err)
		}
	}

	// 状态由 reconcileRoleDrift 在编排锁内重新读取，这里传的只是待处置清单。
	s.reconcileRoleDrift(roleDrifts)

	// 修正运行时复制漂移：实例进程此刻实际在复制谁，与 Server 记录不一致时重新下发
	// REPLICAOF。与上一步是两件不同的事--配置文件的角色要等下次重启才生效，运行时偏差
	// 是此刻正在发生的：一个实际挂在错误 master 上的 replica 在 Proxy 眼里完全健康，
	// 仍然接读流量，链路一断就长期提供陈旧数据而不报任何错。
	s.reconcileReplicaDrift(replicaDrifts)

	// Update server health based on Agent reachability.
	// Never touch "drain" status -- that's a manual operational decision.
	if len(serverReachable) > 0 {
		if err := s.db.WithNode(func(n *apitypes.NodeState) error {
			for name, reachable := range serverReachable {
				srv := n.Servers[name]
				if srv == nil || srv.Status == "drain" {
					continue
				}
				if reachable {
					srv.Status = "healthy"
					srv.LastHeartbeat = time.Now().Format(time.RFC3339)
				} else {
					srv.Status = "unhealthy"
				}
			}
			return nil
		}); err != nil {
			s.log.Errorf("reconcile: failed to update server health: %v", err)
		}
	}

	// Log state transitions to application log (not audit log -- these are
	// observations, not operations).
	for _, r := range results {
		isAlert := r.Action == "alert"
		wasAlert := s.alertInstances[r.Instance]
		if isAlert && !wasAlert {
			s.alertInstances[r.Instance] = true
			s.log.Errorf("reconcile: instance %s on %s entered alert state: desired=%s actual=%s",
				r.Instance, r.Server, r.Desired, r.Actual)
		} else if !isAlert && wasAlert {
			s.alertInstances[r.Instance] = false
			s.log.Infof("reconcile: instance %s on %s recovered: desired=%s actual=%s",
				r.Instance, r.Server, r.Desired, r.Actual)
		}
	}

	// 把本轮观测转成异常记录(去抖由 IncidentService 负责)。
	s.emitReconcileIncidents(results, serverReachable, instances)
	// 组级：replication 组是否还有可用从节点。
	s.emitReplicaAvailability(results, instances)

	return results, nil
}
// parseConfiguredReplicaOf 从 Agent /instance/list 的响应里取出各实例磁盘配置文件中的
// replicaof（归一化为 ip:port，空串表示配置为 master）。
//
// 字段缺失时返回空 map 而不是 nil 语义上的"全部为 master"——detectRoleDrift 只检查
// map 中存在的实例，所以旧版本 Agent（不上报该字段）不会被误判为全组漂移。
func parseConfiguredReplicaOf(data interface{}) map[string]string {
	return parseStringMapField(data, "configured_replica_of")
}

// parseRuntimeReplicaOf 从 Agent /instance/list 的响应里取出各实例进程此刻实际在复制的
// 地址（ip:port，空串表示它认为自己是 master）。
//
// 与 configured_replica_of 同样的约定：只有 Agent 探测成功且结果足够新的实例才会出现在
// map 里，缺失即"这一轮没有可信的运行时观测"，检测方跳过而不是当成 master。判据宁可缺失
// 也不能猜——猜错的方向是给一个正常的 replica 下发 REPLICAOF。
func parseRuntimeReplicaOf(data interface{}) map[string]string {
	return parseStringMapField(data, "runtime_replica_of")
}

// parseStringMapField 从 Agent 响应的 data 里取出一个 map[string]string 字段。
func parseStringMapField(data interface{}, field string) map[string]string {
	result := make(map[string]string)
	m, ok := data.(map[string]interface{})
	if !ok {
		return result
	}
	raw, ok := m[field].(map[string]interface{})
	if !ok {
		return result
	}
	for name, v := range raw {
		if s, ok := v.(string); ok {
			result[name] = s
		}
	}
	return result
}

// parseUsedMemory 从 Agent /instance/list 响应中提取各实例的 used_memory（字节）。
func parseUsedMemory(data interface{}) map[string]int64 {
	result := make(map[string]int64)
	m, ok := data.(map[string]interface{})
	if !ok {
		return result
	}
	raw, ok := m["used_memory"].(map[string]interface{})
	if !ok {
		return result
	}
	for name, v := range raw {
		switch n := v.(type) {
		case float64:
			result[name] = int64(n)
		case json.Number:
			if i, err := n.Int64(); err == nil {
				result[name] = i
			}
		}
	}
	return result
}

func parseContainerMap(data interface{}) map[string]bool {
	result := make(map[string]bool)
	if data == nil {
		return result
	}
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return result
	}
	containers, ok := dataMap["containers"]
	if !ok {
		return result
	}
	list, ok := containers.([]interface{})
	if !ok {
		return result
	}
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		running, _ := m["running"].(bool)
		if name != "" {
			result[name] = running
		}
	}
	return result
}
