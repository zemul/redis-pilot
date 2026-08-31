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
	api        PlatformAPI
	skills     []Skill
	allowWrite bool
	admin      bool // 调用者是否 admin：门控受限 K/V 浏览器的读工具（describe_key/get_key）
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
	"resize_instance":     true,
	"backup_exec":         true,
	"set_access_mode":     true,
	"replicate":           true,
	"backup_set_schedule": true,
	"scan_bigkeys":        true,
	"cancel_task":         true,
	"write_key":           true,
	"delete_key":          true,
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
		Name          string  `json:"name"`
		Key           string  `json:"key"`
		Value         string  `json:"value"`
		Group         string  `json:"group"`
		Category      string  `json:"category"`
		Engine        string  `json:"engine"`
		EngineVersion string  `json:"engine_version"`
		Type          string  `json:"type"`
		Memory        string  `json:"memory"`
		CPUs          float64 `json:"cpus"`
		Disk          string  `json:"disk"`
		AccessMode    string  `json:"access_mode"`
		ReplicaOf     string  `json:"replica_of"`
		Schedule      string  `json:"schedule"`
		Retention     int     `json:"retention"`
		Op            string  `json:"op"`
		Field         string  `json:"field"`
		Member        string  `json:"member"`
		Score         string  `json:"score"`
		Side          string  `json:"side"`
		Index         *int64  `json:"index"`
		TTLSeconds    int64   `json:"ttl_seconds"`
	}
	if argsJSON != "" {
		_ = json.Unmarshal([]byte(argsJSON), &a)
	}
	target := a.Name
	if target == "" {
		target = "(未指定实例)"
	}
	switch name {
	case "scan_bigkeys":
		return fmt.Sprintf("对实例 %s 发起异步大 Key 扫描（限速 SCAN，最长 60 秒）", target)
	case "cancel_task":
		taskID := "(未指定)"
		if argsJSON != "" {
			var t struct {
				TaskID string `json:"task_id"`
			}
			_ = json.Unmarshal([]byte(argsJSON), &t)
			if t.TaskID != "" {
				taskID = t.TaskID
			}
		}
		return fmt.Sprintf("取消任务 %s", taskID)
	case "create_instance":
		engine := a.Engine
		if engine == "" {
			engine = "redis"
		}
		if a.EngineVersion != "" {
			engine += " " + a.EngineVersion
		}
		// 创建从库：group/category/type 均继承主库，摘要聚焦"作为谁的从库"。
		if a.ReplicaOf != "" {
			mem := a.Memory
			if mem == "" {
				mem = "(未指定内存)"
			}
			cpuLabel := "默认 1"
			if a.CPUs > 0 {
				cpuLabel = fmt.Sprintf("%g", a.CPUs)
			}
			diskLabel := a.Disk
			if diskLabel == "" {
				diskLabel = "默认"
			}
			return fmt.Sprintf("创建从库 %s：作为 %s 的从库（%s，内存 %s，CPU %s，磁盘 %s，组/用途继承主库）", target, a.ReplicaOf, engine, mem, cpuLabel, diskLabel)
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
		// cpus/disk 为可选：展示用户/AI 指定的值，未指定则标注按默认，与执行端兜底一致。
		cpuLabel := "默认 1"
		if a.CPUs > 0 {
			cpuLabel = fmt.Sprintf("%g", a.CPUs)
		}
		diskLabel := a.Disk
		if diskLabel == "" {
			diskLabel = "默认"
		}
		return fmt.Sprintf("创建实例 %s：%s %s，内存 %s，CPU %s，磁盘 %s，group=%s，用途=%s", target, engine, typeLabel, mem, cpuLabel, diskLabel, grp, a.Category)
	case "start_instance":
		return fmt.Sprintf("启动实例 %s", target)
	case "stop_instance":
		return fmt.Sprintf("停止实例 %s（若为主从组的 master 将被拒绝，需先切主）", target)
	case "failover":
		return fmt.Sprintf("对实例 %s 所在主从组发起故障转移（由 Sentinel 选举新主，不保证提升 %s 本身）", target, target)
	case "config_set":
		return fmt.Sprintf("修改实例 %s 配置：%s = %s（不重启；平台保留配置项会被拒绝）", target, a.Key, a.Value)
	case "resize_instance":
		var parts []string
		if a.Memory != "" {
			parts = append(parts, "内存→"+a.Memory)
		}
		if a.CPUs > 0 {
			parts = append(parts, fmt.Sprintf("CPU→%g", a.CPUs))
		}
		if a.Disk != "" {
			parts = append(parts, "磁盘→"+a.Disk)
		}
		change := "(未指定变更)"
		if len(parts) > 0 {
			change = fmt.Sprintf("%v", parts)
		}
		return fmt.Sprintf("变配实例 %s：%s（内存/CPU 不停机在线调整，磁盘为规划配额；升配受节点容量校验，内存降配受 OOM 护栏保护）", target, change)
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
	case "write_key":
		keyLabel := a.Key
		if keyLabel == "" {
			keyLabel = "(未指定 key)"
		}
		switch a.Op {
		case "set":
			return fmt.Sprintf("在实例 %s 上 SET key %s（覆盖整个字符串值）", target, keyLabel)
		case "hset":
			return fmt.Sprintf("在实例 %s 上 HSET key %s 字段 %q（新增或覆盖该字段的值）", target, keyLabel, a.Field)
		case "push":
			side := "右(RPUSH)"
			if a.Side == "left" {
				side = "左(LPUSH)"
			}
			return fmt.Sprintf("在实例 %s 上向 list %s %s 追加一个元素", target, keyLabel, side)
		case "sadd":
			return fmt.Sprintf("在实例 %s 上 SADD 到 set %s：添加成员 %q", target, keyLabel, a.Member)
		case "zadd":
			return fmt.Sprintf("在实例 %s 上 ZADD 到 zset %s：成员 %q 分数 %s（新增或更新）", target, keyLabel, a.Member, a.Score)
		case "expire":
			if a.TTLSeconds < 0 {
				return fmt.Sprintf("在实例 %s 上 PERSIST key %s（移除过期时间，改为永久）", target, keyLabel)
			}
			return fmt.Sprintf("在实例 %s 上 EXPIRE key %s：设置过期 %d 秒", target, keyLabel, a.TTLSeconds)
		case "hdel":
			return fmt.Sprintf("在实例 %s 上 HDEL：删除 hash %s 的字段 %q", target, keyLabel, a.Field)
		case "srem":
			return fmt.Sprintf("在实例 %s 上 SREM：删除 set %s 的成员 %q", target, keyLabel, a.Member)
		case "zrem":
			return fmt.Sprintf("在实例 %s 上 ZREM：删除 zset %s 的成员 %q", target, keyLabel, a.Member)
		case "lset":
			idx := "(未指定下标)"
			if a.Index != nil {
				idx = fmt.Sprintf("%d", *a.Index)
			}
			return fmt.Sprintf("在实例 %s 上 LSET：覆盖 list %s 下标 %s 的值", target, keyLabel, idx)
		default:
			return fmt.Sprintf("在实例 %s 上对 key %s 执行受限写操作 %q", target, keyLabel, a.Op)
		}
	case "delete_key":
		keyLabel := a.Key
		if keyLabel == "" {
			keyLabel = "(未指定 key)"
		}
		return fmt.Sprintf("在实例 %s 上 DEL：删除整个 key %s（不可恢复）", target, keyLabel)
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
		makeTool("query_tasks", "查询运维任务列表（备份、恢复、大 Key 扫描等异步任务的状态和进度）",
			map[string]any{
				"status": map[string]string{"type": "string", "description": "按状态筛选：active（进行中）/ success / failed / canceled，可选"},
				"kind":   map[string]string{"type": "string", "description": "按类型筛选：backup.create / backup.restore / diagnostic.bigkeys，可选"},
				"limit":  map[string]string{"type": "integer", "description": "返回条数，可选，默认 20，最大 50"},
			}, nil),
		makeTool("get_task_detail", "查询单个运维任务的详情（含结果、错误原因、进度、目标实例/节点）",
			map[string]any{
				"task_id": map[string]string{"type": "string", "description": "任务 ID"},
			}, []string{"task_id"}),
		makeTool("load_skill", "加载指定运维技能的详细操作手册；当需要按某类运维流程分析或给出步骤时先调用它", skillArg, []string{"name"}),
	}
	// 受限 K/V 浏览器读工具：仅 admin 可见——与 HTTP handler 硬校验 admin 一致，
	// 避免非 admin 用户经 AI 读到任意 key 的值（key 值可能含敏感数据）。
	if r.admin {
		describeKeyArg := map[string]any{
			"name": map[string]string{"type": "string", "description": "实例名或从库名（读写都会路由到其所在组的当前主库）"},
			"key":  map[string]string{"type": "string", "description": "要查看的 key"},
		}
		getKeyArg := map[string]any{
			"name":   map[string]string{"type": "string", "description": "实例名或从库名（路由到当前主库）"},
			"key":    map[string]string{"type": "string", "description": "要读取的 key"},
			"cursor": map[string]string{"type": "string", "description": "hash/set/zset 分页游标，取自上一页的 next_cursor；首页留空或 0"},
			"offset": map[string]string{"type": "integer", "description": "list/string 偏移分页起点，首页 0"},
			"count":  map[string]string{"type": "integer", "description": "返回条数，可选，默认 200，最大 1000"},
		}
		tools = append(tools,
			makeTool("describe_key", "查看一个 key 的元信息（类型/大小/TTL），不拉取值。大 key 应先用它探明规模，再决定是否 get_key", describeKeyArg, []string{"name", "key"}),
			makeTool("get_key", "读取一个 key 的值（带大 value 保护与分页：string 分片、list 偏移、hash/set/zset 游标遍历，响应有字节上限）。仅 admin 可用", getKeyArg, []string{"name", "key"}),
		)
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
			"name":           map[string]string{"type": "string", "description": "实例名（唯一）。命名规范 <group>-<序号>，如 order-1、order-2"},
			"group":          map[string]string{"type": "string", "description": "逻辑组名。创建单点/主库时必填，如 cache、order；创建从库时留空，会自动继承主库所在组"},
			"category":       map[string]string{"type": "string", "description": "用途：cache（缓存，可丢）或 persistent（持久化，需备份）。创建单点/主库时必填；创建从库时留空，自动继承主库"},
			"engine":         map[string]string{"type": "string", "description": "引擎：redis 或 kvrocks，默认 redis。创建从库时须与主库一致"},
			"engine_version": map[string]string{"type": "string", "description": "引擎版本，可选，如 redis 的 5/6.2/7；留空用平台默认"},
			"type":           map[string]string{"type": "string", "description": "拓扑：standalone（单点）或 replication（主从）。创建从库时无需提供，会自动归为 replication"},
			"memory":         map[string]string{"type": "string", "description": "内存规格，如 2Gi、4Gi；必填"},
			"cpus":           map[string]string{"type": "number", "description": "CPU 核数，可选，默认 1。Redis 命令处理单线程，常规负载 1 核即可，除非用户明确要求否则不必调高；Kvrocks 是多线程存储引擎，写密集/大数据量场景可按需给 2-4 核。"},
			"disk":           map[string]string{"type": "string", "description": "磁盘容量，可选，单位必须用 Gi（如 50Gi）。Redis 为纯内存，磁盘仅用于持久化和备份，留空用默认即可；Kvrocks 是磁盘型存储，数据落盘可远超内存，应根据用户描述的数据规模设置（如预计存 100G 数据则给 100Gi 以上）。"},
			"replica_of":     map[string]string{"type": "string", "description": "创建从库时填主库实例名（如 order-1），表示把本实例作为该主库的从库加入其主从组并开始复制；group/category/type/engine_version 会自动继承主库，无需重复提供。创建单点或主库时留空。"},
			"node_selector":  map[string]string{"type": "string", "description": "标签亲和调度，格式 k=v,k=v（如 disk=ssd,zone=az-1）。只调度到标签全部匹配的节点，可选，不传则不限制。"},
		}
		tools = append(tools,
			makeTool("scan_bigkeys", "对指定实例发起异步大 Key 扫描（写操作，需用户二次确认后执行；限速 SCAN，最长 60 秒，同一实例同时只允许一个扫描任务）", nameArg, []string{"name"}),
			makeTool("cancel_task", "取消一个正在运行的可取消任务（写操作，需二次确认；当前仅大 Key 扫描支持取消，备份/恢复不支持）",
				map[string]any{
					"task_id": map[string]string{"type": "string", "description": "要取消的任务 ID"},
				}, []string{"task_id"}),
			makeTool("create_instance", "创建一个新的 Redis/Kvrocks 实例（写操作，需用户二次确认后执行；节点由平台自动调度，无需指定）。创建主从实例时分两步：先建主库（提供 group/category，type=replication），再建从库（提供 replica_of=主库名，其余自动继承）。", createArg, []string{"name", "memory"}),
			makeTool("start_instance", "启动指定实例（写操作，需用户二次确认后执行）", nameArg, []string{"name"}),
			makeTool("stop_instance", "停止指定实例（写操作，需用户二次确认后执行；主从组 master 会被拒绝）", nameArg, []string{"name"}),
			makeTool("failover", "对指定实例所在的主从组发起故障转移（写操作，需二次确认；由 Sentinel 选举新主）", nameArg, []string{"name"}),
			makeTool("config_set", "修改指定实例的单个运行配置项（写操作，需二次确认；平台保留配置项会被拒绝）", configSetArg, []string{"name", "key", "value"}),
			makeTool("resize_instance", "在线变配实例的内存/CPU/磁盘规格（写操作，需二次确认）。内存和 CPU 不停机调整容器 cgroup（内存同时调整 maxmemory）；磁盘为容量规划配额（仅元数据）。三个维度均可选，只改传入的；升配受节点剩余容量校验、内存降配受 OOM 护栏保护。用于纵向扩缩容，优先于替换式升级。",
				map[string]any{
					"name":   map[string]string{"type": "string", "description": "实例名"},
					"memory": map[string]string{"type": "string", "description": "新内存规格，可选，如 8Gi；不改则省略"},
					"cpus":   map[string]string{"type": "number", "description": "新 CPU 核数，可选，需 >0；不改则省略"},
					"disk":   map[string]string{"type": "string", "description": "新磁盘规划配额，可选，单位用 Gi 如 100Gi；不改则省略。纯规划元数据，不强制"},
				}, []string{"name"}),
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
			makeTool("write_key", "对一个 key 做受限写操作（写操作，需二次确认；读写都路由到组当前主库）。op 决定动作：set(字符串)/hset(hash 设字段)/push(list 追加)/sadd(set 加成员)/zadd(zset 加/改成员分数)/expire(设过期,ttl_seconds<0 为永久)/hdel(删 hash 字段)/srem(删 set 成员)/zrem(删 zset 成员)/lset(按下标改 list 值)。按 op 提供对应参数。",
				map[string]any{
					"name":        map[string]string{"type": "string", "description": "实例名或从库名（路由到当前主库）"},
					"key":         map[string]string{"type": "string", "description": "目标 key"},
					"op":          map[string]string{"type": "string", "description": "set/hset/push/sadd/zadd/expire/hdel/srem/zrem/lset 之一"},
					"value":       map[string]string{"type": "string", "description": "值：set/hset/push/lset 用"},
					"field":       map[string]string{"type": "string", "description": "hash 字段：hset/hdel 用"},
					"member":      map[string]string{"type": "string", "description": "成员：sadd/zadd/srem/zrem 用"},
					"score":       map[string]string{"type": "string", "description": "zset 分数（数字字符串）：zadd 用"},
					"side":        map[string]string{"type": "string", "description": "list 方向 left|right：push 用，默认 right"},
					"index":       map[string]string{"type": "integer", "description": "list 下标：lset 用"},
					"ttl_seconds": map[string]string{"type": "integer", "description": "过期秒数：expire 用，<0 表示永久(PERSIST)"},
				}, []string{"name", "key", "op"}),
			makeTool("delete_key", "删除整个 key（写操作，需二次确认；不可恢复，路由到组当前主库）。只删单个元素请改用 write_key 的 hdel/srem/zrem",
				map[string]any{
					"name": map[string]string{"type": "string", "description": "实例名或从库名（路由到当前主库）"},
					"key":  map[string]string{"type": "string", "description": "要删除的整个 key"},
				}, []string{"name", "key"}),
		)
	}
	return tools
}

// exec 执行一个工具调用，返回其结果 JSON（回填给模型）。
func (r *toolRegistry) exec(ctx context.Context, name, argsJSON string) (json.RawMessage, error) {
	var args struct {
		Name   string `json:"name"`
		Count  int    `json:"count"`
		Range  string `json:"range"`
		Limit  int    `json:"limit"`
		Status string `json:"status"`
		Kind   string `json:"kind"`
		TaskID string `json:"task_id"`
		Key    string `json:"key"`
		Cursor string `json:"cursor"`
		Offset int    `json:"offset"`
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
	case "query_tasks":
		return r.api.QueryTasks(ctx, args.Status, args.Kind, args.Limit)
	case "get_task_detail":
		if args.TaskID == "" {
			return nil, fmt.Errorf("missing arg: task_id")
		}
		return r.api.GetTaskDetail(ctx, args.TaskID)
	case "describe_key", "get_key":
		// 防御纵深：读 key 是 admin-only 能力；即便工具未暴露给非 admin，exec 再挡一道。
		if !r.admin {
			return nil, fmt.Errorf("admin role required for %s", name)
		}
		if err := needName(); err != nil {
			return nil, err
		}
		if args.Key == "" {
			return nil, fmt.Errorf("missing arg: key")
		}
		if name == "describe_key" {
			return r.api.DescribeKey(ctx, args.Name, args.Key)
		}
		cursor := args.Cursor
		if cursor == "" {
			cursor = "0"
		}
		return r.api.GetKey(ctx, args.Name, args.Key, cursor, args.Offset, args.Count)
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
