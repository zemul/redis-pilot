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
	api       PlatformAPI
	skills    []Skill
	allowWrite bool
}

// writeTools 是代码级白名单：只有列在此处的工具属于写操作。
// 写操作永不在对话循环内直接执行，只会产出 confirm 事件，经用户二次确认后
// 由独立执行端点 /api/v1/ai/execute 执行（方案 A，见 §7.2）。
// 破坏性操作（delete / restore / clean-data / node remove）刻意不在此列。
//
// 注意：这里是“暴露给模型 + 触发 confirm”的白名单权威；真正的执行分发在
// server 包的 aiExecute（把工具映射到已验证的 gin handler）。两处工具名需保持一致。
var writeTools = map[string]bool{
	"create_instance":     true,
	"start_instance":      true,
	"stop_instance":       true,
	"failover":            true,
	"config_set":          true,
	"backup_exec":         true,
	"set_access_mode":     true,
	"replicate":           true,
	"backup_set_schedule": true,
}

// isWriteTool 判断工具名是否为写操作。
func isWriteTool(name string) bool { return writeTools[name] }

// IsWriteTool 是 isWriteTool 的导出版本，供执行端点做白名单校验，
// 确保 ai 包是写工具白名单的唯一来源。
func IsWriteTool(name string) bool { return isWriteTool(name) }

// writeToolSummary 由后端根据 tool+args 生成**可信**的人类可读摘要，供前端确认弹窗展示。
// 刻意不使用 LLM 生成的描述，防止模型谎报将要执行的动作。
func writeToolSummary(name, argsJSON string) string {
	var a struct {
		Name          string `json:"name"`
		Key           string `json:"key"`
		Value         string `json:"value"`
		Group         string `json:"group"`
		Category      string `json:"category"`
		Engine        string `json:"engine"`
		EngineVersion string `json:"engine_version"`
		Type          string `json:"type"`
		Memory        string `json:"memory"`
		AccessMode    string `json:"access_mode"`
		ReplicaOf     string `json:"replica_of"`
		Schedule      string `json:"schedule"`
		Retention     int    `json:"retention"`
	}
	if argsJSON != "" {
		_ = json.Unmarshal([]byte(argsJSON), &a)
	}
	target := a.Name
	if target == "" {
		target = "(未指定实例)"
	}
	switch name {
	case "create_instance":
		engine := a.Engine
		if engine == "" {
			engine = "redis"
		}
		if a.EngineVersion != "" {
			engine += " " + a.EngineVersion
		}
		typ := a.Type
		if typ == "" {
			typ = "standalone"
		}
		typeLabel := "单点"
		if typ == "replication" {
			typeLabel = "主从"
		}
		mem := a.Memory
		if mem == "" {
			mem = "(未指定内存)"
		}
		grp := a.Group
		if grp == "" {
			grp = "(未指定 group)"
		}
		return fmt.Sprintf("创建实例 %s：%s %s，内存 %s，group=%s，用途=%s", target, engine, typeLabel, mem, grp, a.Category)
	case "start_instance":
		return fmt.Sprintf("启动实例 %s", target)
	case "stop_instance":
		return fmt.Sprintf("停止实例 %s（若为主从组的 master 将被拒绝，需先切主）", target)
	case "failover":
		return fmt.Sprintf("对实例 %s 所在主从组发起故障转移（由 Sentinel 选举新主，不保证提升 %s 本身）", target, target)
	case "config_set":
		return fmt.Sprintf("修改实例 %s 配置：%s = %s（不重启；平台保留配置项会被拒绝）", target, a.Key, a.Value)
	case "backup_exec":
		return fmt.Sprintf("对实例 %s 立即执行一次备份", target)
	case "set_access_mode":
		modeLabel := a.AccessMode
		switch a.AccessMode {
		case "rw-split":
			modeLabel = "rw-split（读写分离，读流量走从库）"
		case "standard":
			modeLabel = "standard（所有流量走 master）"
		}
		return fmt.Sprintf("将 %s 所在组的访问模式设为 %s", target, modeLabel)
	case "replicate":
		src := a.ReplicaOf
		if src == "" {
			src = "(未指定主实例)"
		}
		return fmt.Sprintf("让实例 %s 作为 %s 的从库（加入其主从组并开始复制）", target, src)
	case "backup_set_schedule":
		if a.Schedule == "" {
			return fmt.Sprintf("关闭实例 %s 的自动备份计划", target)
		}
		ret := ""
		if a.Retention > 0 {
			ret = fmt.Sprintf("，保留 %d 份", a.Retention)
		}
		return fmt.Sprintf("设置实例 %s 的自动备份计划：cron=%q%s", target, a.Schedule, ret)
	default:
		return fmt.Sprintf("%s(%s)", name, target)
	}
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
	tools := []openai.ChatCompletionToolParam{
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
	// 写操作工具仅在 allowWrite（配置总闸开启 且 调用者为 admin）时才暴露给模型。
	// 即便暴露，它们也只会走 confirm 二次确认，不会在对话循环内直接执行。
	if r.allowWrite {
		configSetArg := map[string]any{
			"name":  map[string]string{"type": "string", "description": "实例名"},
			"key":   map[string]string{"type": "string", "description": "配置项名，如 maxmemory-policy；平台保留项会被拒绝"},
			"value": map[string]string{"type": "string", "description": "配置项的新值"},
		}
		createArg := map[string]any{
			"name":           map[string]string{"type": "string", "description": "实例名（唯一）"},
			"group":          map[string]string{"type": "string", "description": "逻辑组名，单点/主实例必填，如 cache、order"},
			"category":       map[string]string{"type": "string", "description": "用途：cache（缓存，可丢）或 persistent（持久化，需备份）"},
			"engine":         map[string]string{"type": "string", "description": "引擎：redis 或 kvrocks，默认 redis"},
			"engine_version": map[string]string{"type": "string", "description": "引擎版本，可选，如 redis 的 5/6.2/7；留空用平台默认"},
			"type":           map[string]string{"type": "string", "description": "拓扑：standalone（单点）或 replication（主从），默认 standalone"},
			"memory":         map[string]string{"type": "string", "description": "内存规格，如 2Gi、4Gi；必填"},
		}
		tools = append(tools,
			makeTool("create_instance", "创建一个新的 Redis/Kvrocks 实例（写操作，需用户二次确认后执行；节点由平台自动调度，无需指定）", createArg, []string{"name", "group", "category", "memory", "type"}),
			makeTool("start_instance", "启动指定实例（写操作，需用户二次确认后执行）", nameArg, []string{"name"}),
			makeTool("stop_instance", "停止指定实例（写操作，需用户二次确认后执行；主从组 master 会被拒绝）", nameArg, []string{"name"}),
			makeTool("failover", "对指定实例所在的主从组发起故障转移（写操作，需二次确认；由 Sentinel 选举新主）", nameArg, []string{"name"}),
			makeTool("config_set", "修改指定实例的单个运行配置项（写操作，需二次确认；平台保留配置项会被拒绝）", configSetArg, []string{"name", "key", "value"}),
			makeTool("backup_exec", "对指定实例立即执行一次备份（写操作，需二次确认）", nameArg, []string{"name"}),
			makeTool("set_access_mode", "设置实例所在组的访问模式：standard（全部走 master）或 rw-split（读写分离）。影响伪 Sentinel 返回给客户端的端口（写操作，需二次确认）",
				map[string]any{
					"name":        map[string]string{"type": "string", "description": "实例名或组名"},
					"access_mode": map[string]string{"type": "string", "description": "standard 或 rw-split"},
				}, []string{"name", "access_mode"}),
			makeTool("replicate", "让一个已存在的实例作为另一个实例的从库，加入其主从组并开始复制（写操作，需二次确认；用于加副本/扩容）",
				map[string]any{
					"name":       map[string]string{"type": "string", "description": "要变为从库的实例名"},
					"replica_of": map[string]string{"type": "string", "description": "作为复制源的主实例名"},
				}, []string{"name", "replica_of"}),
			makeTool("backup_set_schedule", "设置或关闭指定实例的自动备份计划（写操作，需二次确认；schedule 为 cron 表达式，留空表示关闭自动备份）",
				map[string]any{
					"name":      map[string]string{"type": "string", "description": "实例名"},
					"schedule":  map[string]string{"type": "string", "description": "cron 表达式，如 '0 2 * * *'；留空关闭自动备份"},
					"retention": map[string]string{"type": "integer", "description": "保留备份份数，可选；0 或省略表示不改动现值"},
				}, []string{"name"}),
		)
	}
	return tools
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
	// 写操作绝不在对话循环内执行；应由 Agent.Run 识别为写工具并走 confirm。
	// 这里作为最后一道防线，防止误配置导致写工具被直接执行。
	if isWriteTool(name) {
		return nil, fmt.Errorf("write tool %q must go through confirm/execute, not in-loop exec", name)
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
