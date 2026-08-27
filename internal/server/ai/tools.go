package ai

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/openai/openai-go"
)

// toolRegistry 定义 AI 可用的工具（schema）并执行它们（绑定到 PlatformAPI）。
// MVP 只读工具：list_instances / get_instance_status / diagnose_instance，
// 外加内置 load_skill（progressive disclosure：按需展开 SKILL.md 正文）。
type toolRegistry struct {
	api    PlatformAPI
	skills []Skill
}

func makeTool(name, desc string, props map[string]any, required []string) openai.ChatCompletionToolParam {
	params := openai.FunctionParameters{"type": "object", "properties": props}
	if len(required) > 0 {
		params["required"] = required
	}
	return openai.ChatCompletionToolParam{
		Function: openai.FunctionDefinitionParam{
			Name:        name,
			Description: openai.String(desc),
			Parameters:  params,
		},
	}
}

func (r *toolRegistry) schemas() []openai.ChatCompletionToolParam {
	nameArg := map[string]any{
		"name": map[string]string{"type": "string", "description": "实例名"},
	}
	skillArg := map[string]any{
		"name": map[string]string{"type": "string", "description": "技能名"},
	}
	slowlogArg := map[string]any{
		"name":  map[string]string{"type": "string", "description": "实例名"},
		"count": map[string]string{"type": "integer", "description": "返回条数，可选，默认由服务端决定"},
	}
	metricsArg := map[string]any{
		"name":  map[string]string{"type": "string", "description": "实例名"},
		"range": map[string]string{"type": "string", "description": "时间范围，如 1h/6h/24h，可选"},
	}
	auditArg := map[string]any{
		"limit": map[string]string{"type": "integer", "description": "返回条数，可选，默认 20，最大 100"},
	}
	return []openai.ChatCompletionToolParam{
		makeTool("list_instances", "列出所有 Redis/Kvrocks 实例及其组、角色、所在节点、内存与运行状态", map[string]any{}, nil),
		makeTool("get_instance_status", "查询指定实例的运行状态", nameArg, []string{"name"}),
		makeTool("diagnose_instance", "对指定实例做深度诊断（采集 INFO、SLOWLOG、MEMORY DOCTOR）", nameArg, []string{"name"}),
		makeTool("get_slowlog", "获取指定实例的慢查询日志", slowlogArg, []string{"name"}),
		makeTool("get_bigkeys", "获取指定实例的大 key 扫描结果", nameArg, []string{"name"}),
		makeTool("get_instance_config", "获取指定实例的当前运行配置", nameArg, []string{"name"}),
		makeTool("get_metrics_history", "获取指定实例的历史监控指标（内存、连接数、QPS 等）", metricsArg, []string{"name"}),
		makeTool("list_nodes", "列出所有服务器节点及其状态、容量、已分配资源、实例数", map[string]any{}, nil),
		makeTool("query_audit", "查询最近的运维操作审计日志（谁在什么时候对哪个实例做了什么）", auditArg, nil),
		makeTool("load_skill", "加载指定运维技能的详细操作手册；当需要按某类运维流程分析或给出步骤时先调用它", skillArg, []string{"name"}),
	}
}

// exec 执行一个工具调用，返回其结果 JSON（回填给模型）。
func (r *toolRegistry) exec(ctx context.Context, name, argsJSON string) (json.RawMessage, error) {
	var args struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
		Range string `json:"range"`
		Limit int    `json:"limit"`
	}
	if argsJSON != "" {
		_ = json.Unmarshal([]byte(argsJSON), &args)
	}
	needName := func() error {
		if args.Name == "" {
			return fmt.Errorf("missing arg: name")
		}
		return nil
	}
	switch name {
	case "list_instances":
		return r.api.ListInstances(ctx)
	case "get_instance_status":
		if err := needName(); err != nil {
			return nil, err
		}
		return r.api.InstanceStatus(ctx, args.Name)
	case "diagnose_instance":
		if err := needName(); err != nil {
			return nil, err
		}
		return r.api.DiagnoseInstance(ctx, args.Name)
	case "get_slowlog":
		if err := needName(); err != nil {
			return nil, err
		}
		return r.api.GetSlowlog(ctx, args.Name, args.Count)
	case "get_bigkeys":
		if err := needName(); err != nil {
			return nil, err
		}
		return r.api.GetBigkeys(ctx, args.Name)
	case "get_instance_config":
		if err := needName(); err != nil {
			return nil, err
		}
		return r.api.GetInstanceConfig(ctx, args.Name)
	case "get_metrics_history":
		if err := needName(); err != nil {
			return nil, err
		}
		return r.api.GetMetricsHistory(ctx, args.Name, args.Range)
	case "list_nodes":
		return r.api.ListNodes(ctx)
	case "query_audit":
		return r.api.QueryAudit(ctx, args.Limit)
	case "load_skill":
		for _, sk := range r.skills {
			if sk.Name == args.Name {
				return json.Marshal(map[string]string{"skill": sk.Name, "content": sk.Body})
			}
		}
		return json.Marshal(map[string]string{"error": "skill not found: " + args.Name})
	default:
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
}
