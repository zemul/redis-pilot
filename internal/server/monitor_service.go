package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

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

// --- AI 助手只读工具的 service 实现（MVP）---

// aiDiagnose 深度诊断：INFO + SLOWLOG + MEMORY DOCTOR，返回 Agent 原始诊断结果。
func (s *Server) aiDiagnose(name string) (json.RawMessage, error) {
	return s.collectFromAgent(name, "/instance/diagnose", "")
}

// aiStatus 实例运行状态。
func (s *Server) aiStatus(name string) (json.RawMessage, error) {
	return s.collectFromAgent(name, "/instance/status", "")
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
