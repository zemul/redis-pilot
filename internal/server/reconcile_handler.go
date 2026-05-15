package server

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
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
type statusPatch struct {
	name      string
	newStatus string
}

// runReconcile 执行状态校验，HTTP handler 和定时任务共用
func (s *Server) runReconcile() ([]ReconcileResult, error) {
	instances, err := s.state.ReadInstances()
	if err != nil {
		return nil, err
	}
	node, err := s.state.ReadNode()
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

	for serverName, instNames := range serverInstances {
		srv := node.Servers[serverName]
		if srv == nil {
			// 服务器不在 node 中，所有实例标记异常
			for _, name := range instNames {
				results = append(results, ReconcileResult{
					Instance: name, Server: serverName,
					Desired: instances.Instances[name].Status,
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
			for _, name := range instNames {
				results = append(results, ReconcileResult{
					Instance: name, Server: serverName,
					Desired: instances.Instances[name].Status,
					Actual:  "unreachable",
					Action:  "alert",
				})
			}
			continue
		}

		// 解析 Agent 返回的容器列表
		var apiResp apitypes.APIResponse
		json.Unmarshal(resp, &apiResp)
		containerMap := parseContainerMap(apiResp.Data)

		// 逐个对比
		for _, name := range instNames {
			inst := instances.Instances[name]
			containerName := inst.Container
			if containerName == "" {
				if group := instances.Groups[inst.Group]; group != nil {
					containerName = group.Engine + "-" + name
				}
			}

			actual := "missing"
			if status, exists := containerMap[containerName]; exists {
				if status {
					actual = "running"
				} else {
					actual = "stopped"
				}
			}

			result := ReconcileResult{
				Instance: name, Server: serverName,
				Desired: inst.Status, Actual: actual,
			}

			switch {
			case inst.Status == actual:
				result.Action = "none"

			case actual == "running" && (inst.Status == "creating" || inst.Status == "failed" || inst.Status == "unexpected_stopped"):
				patches = append(patches, statusPatch{name: name, newStatus: "running"})
				result.Action = "updated"

			case actual == "stopped" && inst.Status == "running":
				patches = append(patches, statusPatch{name: name, newStatus: "unexpected_stopped"})
				result.Action = "alert"

			case actual == "missing" && inst.Status == "running":
				patches = append(patches, statusPatch{name: name, newStatus: "failed"})
				result.Action = "alert"

			default:
				result.Action = "none"
			}

			results = append(results, result)
		}
	}

	// 在写锁内原子应用状态变更，避免覆盖并发写入
	if len(patches) > 0 {
		s.state.WithInstances(func(st *apitypes.InstancesState) error {
			affectedGroups := map[string]struct{}{}
			for _, p := range patches {
				if inst := st.Instances[p.name]; inst != nil {
					inst.Status = p.newStatus
					affectedGroups[inst.Group] = struct{}{}
				}
			}
			for g := range affectedGroups {
				state.RecalculateGroupTopology(st, g)
			}
			return nil
		})
	}

	// 记录有异常的审计日志
	for _, r := range results {
		if r.Action == "alert" || r.Action == "updated" {
			s.audit.Log(audit.Record{
				Operator: "system",
				Action:   "reconcile",
				Level:    audit.LevelImportant,
				Result:   r.Action,
				Target:   map[string]interface{}{"instance": r.Instance, "server": r.Server},
				Params:   map[string]interface{}{"desired": r.Desired, "actual": r.Actual},
			})
		}
	}

	return results, nil
}

// parseContainerMap 从 Agent 返回的 data 中提取容器名→运行状态映射
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
