package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 类型化错误：供 resolveInstance 映射到 HTTP 状态码，也供 AI service 层判断。
var (
	errInstanceNotFound = errors.New("instance not found")
	errServerNotFound   = errors.New("server not found")
)

// lookupInstance 解析实例及其所在节点，不依赖 *gin.Context。
//
// 这是 HTTP handler（resolveInstance）与 AI 助手 service 层共用的能力底座：
// 能力实现只有这一份，CLI/HTTP/AI 都是它的消费者，彼此解耦
// （见 docs/architecture/ai-assistant.md §3.1.1）。
func (s *Server) lookupInstance(name string) (*apitypes.Instance, *apitypes.NodeServer, error) {
	instances, err := s.db.ReadInstances()
	if err != nil {
		return nil, nil, err
	}
	inst, ok := instances.Instances[name]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", errInstanceNotFound, name)
	}
	node, err := s.db.ReadNode()
	if err != nil {
		return nil, nil, err
	}
	srv := node.Servers[inst.Server]
	if srv == nil {
		return nil, nil, fmt.Errorf("%w: %s", errServerNotFound, inst.Server)
	}
	return inst, srv, nil
}

// collectFromAgent 解析实例，调用其所在 Agent 的只读端点，返回原始 JSON。
// name 经 URL 转义，避免实例名中的特殊字符破坏查询串。
func (s *Server) collectFromAgent(name, path, query string) (json.RawMessage, error) {
	_, srv, err := s.lookupInstance(name)
	if err != nil {
		return nil, err
	}
	full := path + "?name=" + url.QueryEscape(name)
	if query != "" {
		full += "&" + query
	}
	out, err := newAgentClient(srv).get(full)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(out), nil
}

// collectFromAgentData 同 collectFromAgent，但提取 Agent {success,data} 响应中的
// data 字段（slowlog/bigkeys/config/metrics 等端点用）。
func (s *Server) collectFromAgentData(name, path, query string) (json.RawMessage, error) {
	raw, err := s.collectFromAgent(name, path, query)
	if err != nil {
		return nil, err
	}
	return extractAgentData(raw), nil
}

// --- AI 助手只读工具的 service 实现（MVP）---

// aiDiagnose 深度诊断：INFO + SLOWLOG + MEMORY DOCTOR，返回 Agent 原始诊断结果。
func (s *Server) aiDiagnose(name string) (json.RawMessage, error) {
	return s.collectFromAgent(name, "/instance/diagnose", "")
}

// aiStatus 实例运行状态。
func (s *Server) aiStatus(name string) (json.RawMessage, error) {
	return s.collectFromAgent(name, "/instance/status", "")
}

// aiSlowlog 慢查询日志（count 为条数，<=0 时用 Agent 默认）。
func (s *Server) aiSlowlog(name string, count int) (json.RawMessage, error) {
	query := ""
	if count > 0 {
		query = "count=" + strconv.Itoa(count)
	}
	return s.collectFromAgentData(name, "/instance/slowlog", query)
}

// aiBigkeys 大 key 扫描结果。
func (s *Server) aiBigkeys(name string) (json.RawMessage, error) {
	return s.collectFromAgentData(name, "/instance/bigkeys", "")
}

// aiConfig 实例当前运行配置。
func (s *Server) aiConfig(name string) (json.RawMessage, error) {
	return s.collectFromAgentData(name, "/instance/config", "")
}

// aiMetricsHistory 历史指标（timeRange 如 "1h"/"6h"/"24h"，空时用 Agent 默认）。
func (s *Server) aiMetricsHistory(name, timeRange string) (json.RawMessage, error) {
	query := ""
	if timeRange != "" {
		query = "range=" + url.QueryEscape(timeRange)
	}
	return s.collectFromAgentData(name, "/instance/metrics/history", query)
}

// aiListNodes 返回全部服务器节点的精简清单（不含 agent_token 等敏感字段）。
func (s *Server) aiListNodes() (json.RawMessage, error) {
	node, err := s.db.ReadNode()
	if err != nil {
		return nil, err
	}
	type item struct {
		Name          string               `json:"name"`
		Endpoint      string               `json:"endpoint"`
		Status        string               `json:"status"`
		Instances     int                  `json:"instances"`
		Labels        map[string]string    `json:"labels,omitempty"`
		Capacity      apitypes.ResourceSpec `json:"capacity"`
		Allocated     apitypes.ResourceSpec `json:"allocated"`
		LastHeartbeat string               `json:"last_heartbeat,omitempty"`
	}
	list := make([]item, 0, len(node.Servers))
	for name, srv := range node.Servers {
		list = append(list, item{
			Name:          name,
			Endpoint:      srv.Endpoint,
			Status:        srv.Status,
			Instances:     len(srv.Instances),
			Labels:        srv.Labels,
			Capacity:      srv.Capacity,
			Allocated:     srv.Allocated,
			LastHeartbeat: srv.LastHeartbeat,
		})
	}
	return json.Marshal(list)
}

// aiQueryAudit 查询最近的审计日志（limit 条，时间倒序）。
func (s *Server) aiQueryAudit(limit int) (json.RawMessage, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	records, total, err := s.audit.Query(store.AuditFilter{Limit: limit})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"records": records, "total": total})
}

// aiListInstances 返回全部实例的精简清单，供 AI 了解拓扑（不含敏感/冗余字段）。
func (s *Server) aiListInstances() (json.RawMessage, error) {
	instances, err := s.db.ReadInstances()
	if err != nil {
		return nil, err
	}
	type item struct {
		Name     string `json:"name"`
		Group    string `json:"group"`
		Role     string `json:"role"`
		Server   string `json:"server"`
		Port     int    `json:"port"`
		Memory   string `json:"memory"`
		Desired  string `json:"desired_state"`
		Observed string `json:"observed_status"`
	}
	list := make([]item, 0, len(instances.Instances))
	for name, inst := range instances.Instances {
		list = append(list, item{
			Name:     name,
			Group:    inst.Group,
			Role:     inst.Role,
			Server:   inst.Server,
			Port:     inst.Port,
			Memory:   inst.Memory,
			Desired:  inst.DesiredState,
			Observed: inst.ObservedStatus,
		})
	}
	return json.Marshal(list)
}
