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
	return []openai.ChatCompletionToolParam{
		makeTool("list_instances", "列出所有 Redis/Kvrocks 实例及其组、角色、所在节点、内存与运行状态", map[string]any{}, nil),
		makeTool("get_instance_status", "查询指定实例的运行状态", nameArg, []string{"name"}),
		makeTool("diagnose_instance", "对指定实例做深度诊断（采集 INFO、SLOWLOG、MEMORY DOCTOR）", nameArg, []string{"name"}),
		makeTool("load_skill", "加载指定运维技能的详细操作手册；当需要按某类运维流程分析或给出步骤时先调用它", skillArg, []string{"name"}),
	}
}

// exec 执行一个工具调用，返回其结果 JSON（回填给模型）。
func (r *toolRegistry) exec(ctx context.Context, name, argsJSON string) (json.RawMessage, error) {
	var args struct {
		Name string `json:"name"`
	}
	if argsJSON != "" {
		_ = json.Unmarshal([]byte(argsJSON), &args)
	}
	switch name {
	case "list_instances":
		return r.api.ListInstances(ctx)
	case "get_instance_status":
		if args.Name == "" {
			return nil, fmt.Errorf("missing arg: name")
		}
		return r.api.InstanceStatus(ctx, args.Name)
	case "diagnose_instance":
		if args.Name == "" {
			return nil, fmt.Errorf("missing arg: name")
		}
		return r.api.DiagnoseInstance(ctx, args.Name)
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
