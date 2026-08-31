package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/server/ai"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

// aiPlatform 把 Server 的 service 方法适配成 ai.PlatformAPI，供 AI 进程内直调。
// 能力实现只有 monitor_service.go 一份，这里只是接口适配（见 §3.1.1 单一能力源）。
type aiPlatform struct{ s *Server }

func (p aiPlatform) ListInstances(_ context.Context) (json.RawMessage, error) {
	return p.s.aiListInstances()
}
func (p aiPlatform) InstanceStatus(_ context.Context, name string) (json.RawMessage, error) {
	return p.s.aiStatus(name)
}
func (p aiPlatform) DiagnoseInstance(_ context.Context, name string) (json.RawMessage, error) {
	return p.s.aiDiagnose(name)
}
func (p aiPlatform) GetSlowlog(_ context.Context, name string, count int) (json.RawMessage, error) {
	return p.s.aiSlowlog(name, count)
}
func (p aiPlatform) GetBigkeys(_ context.Context, name string) (json.RawMessage, error) {
	return p.s.aiBigkeys(name)
}
func (p aiPlatform) GetInstanceConfig(_ context.Context, name string) (json.RawMessage, error) {
	return p.s.aiConfig(name)
}
func (p aiPlatform) GetMetricsHistory(_ context.Context, name, timeRange string) (json.RawMessage, error) {
	return p.s.aiMetricsHistory(name, timeRange)
}
func (p aiPlatform) ListNodes(_ context.Context) (json.RawMessage, error) {
	return p.s.aiListNodes()
}
func (p aiPlatform) QueryAudit(_ context.Context, limit int) (json.RawMessage, error) {
	return p.s.aiQueryAudit(limit)
}
func (p aiPlatform) QueryTasks(_ context.Context, status, kind string, limit int) (json.RawMessage, error) {
	return p.s.aiQueryTasks(status, kind, limit)
}
func (p aiPlatform) GetTaskDetail(_ context.Context, taskID string) (json.RawMessage, error) {
	return p.s.aiGetTaskDetail(taskID)
}
func (p aiPlatform) DescribeKey(_ context.Context, name, key string) (json.RawMessage, error) {
	return p.s.keyMeta(name, key)
}
func (p aiPlatform) GetKey(_ context.Context, name, key, cursor string, offset, count int) (json.RawMessage, error) {
	return p.s.keyGet(name, key, cursor, offset, count, false)
}

// sseSink 把 Agent 的运行事件写成 SSE 帧推给前端，同时把工具调用与 token 用量
// 写入结构化日志（可观测性，见 §7.5；token 记录见 §7.3）。
type sseSink struct {
	c       *gin.Context
	flusher http.Flusher
	log     *logger.Logger
	user    string

	// 累积器：aiChat 结束后从这里取完整 assistant 文本 + tool_calls 落库（§13.5.2-b）。
	// 不侵入 ai/ 包——在各 On* 回调里顺带 append。
	sb    strings.Builder // 累积流式 token（assistant 回复全文）
	calls []any           // 累积 tool_call / write_confirm 元素
}

func (k *sseSink) send(event string, payload any) {
	data, _ := json.Marshal(payload)
	fmt.Fprintf(k.c.Writer, "event: %s\ndata: %s\n\n", event, data)
	k.flusher.Flush()
}

func (k *sseSink) OnToken(text string) {
	k.sb.WriteString(text)
	k.send("token", map[string]string{"text": text})
}
func (k *sseSink) OnToolCall(name, args string) {
	k.log.Infof("ai tool_call: user=%s tool=%s args=%s", k.user, name, args)
	k.calls = append(k.calls, map[string]any{"type": "tool_call", "tool": name, "args": args})
	k.send("tool_call", map[string]string{"name": name, "args": args})
}
func (k *sseSink) OnToolResult(name string, ok bool, errMsg string) {
	if ok {
		k.log.Infof("ai tool_result: user=%s tool=%s ok", k.user, name)
	} else {
		k.log.Infof("ai tool_result: user=%s tool=%s error=%s", k.user, name, errMsg)
	}
	// 把结果状态补写回最近一个同名 tool_call 元素的 result_summary（§13.3）。
	summary := "ok"
	if !ok {
		summary = "error: " + errMsg
	}
	for i := len(k.calls) - 1; i >= 0; i-- {
		if m, isMap := k.calls[i].(map[string]any); isMap && m["type"] == "tool_call" && m["tool"] == name {
			if _, has := m["result_summary"]; !has {
				m["result_summary"] = summary
			}
			break
		}
	}
	k.send("tool_result", map[string]any{"name": name, "ok": ok, "error": errMsg})
}
func (k *sseSink) OnUsage(promptTokens, completionTokens, totalTokens int64) {
	k.log.Infof("ai usage: user=%s prompt=%d completion=%d total=%d",
		k.user, promptTokens, completionTokens, totalTokens)
	k.send("usage", map[string]int64{
		"prompt_tokens": promptTokens, "completion_tokens": completionTokens, "total_tokens": totalTokens,
	})
}
func (k *sseSink) OnConfirm(tool, args, summary string) {
	// 写操作提议：不执行，推给前端做二次确认（方案 A，见 §7.2）。summary 由后端可信生成。
	k.log.Infof("ai confirm: user=%s tool=%s args=%s", k.user, tool, args)
	k.calls = append(k.calls, map[string]any{"type": "write_confirm", "tool": tool, "args": args, "summary": summary})
	k.send("confirm", map[string]string{"tool": tool, "args": args, "summary": summary})
}
func (k *sseSink) OnError(err error) { k.send("error", map[string]string{"message": err.Error()}) }
func (k *sseSink) OnDone()           { k.send("done", map[string]bool{"done": true}) }

// OnConversation 在新建会话时告知前端 convID，使前端能绑定新会话（§13.5.2-c）。
func (k *sseSink) OnConversation(id string) { k.send("conversation", map[string]string{"id": id}) }

// toolCallsJSON 把累积的 tool_calls 元素序列化为 JSON 字符串（落库用）。
func (k *sseSink) toolCallsJSON() string {
	if len(k.calls) == 0 {
		return "[]"
	}
	b, err := json.Marshal(k.calls)
	if err != nil {
		return "[]"
	}
	return string(b)
}

type aiChatRequest struct {
	Messages       []ai.Message `json:"messages"`
	ConversationID string       `json:"conversation_id"` // 空=新建会话
}

// aiChat 是 AI 助手的 SSE 端点（POST /api/v1/ai/chat）。
// 挂在 authed group：所有登录用户（含普通用户）均可使用只读诊断。
func (s *Server) aiChat(c *gin.Context) {
	if s.aiAgent == nil {
		fail(c, http.StatusServiceUnavailable, "AI assistant is not enabled")
		return
	}
	var req aiChatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Messages) == 0 {
		fail(c, http.StatusBadRequest, "messages is required")
		return
	}
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		fail(c, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no") // 禁止 nginx 缓冲，保证流式

	s.log.Infof("ai chat: user=%s messages=%d", operatorFrom(c), len(req.Messages))
	user := operatorFrom(c)
	sink := &sseSink{c: c, flusher: flusher, log: s.log, user: user}

	// ── 会话落库（§13.5.2-b / §13.7） ──────────────────────────────────────
	// 取最后一条 user 消息作为本轮提问正文（前端仍发完整 history，DB 只存档）。
	lastUser := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			lastUser = req.Messages[i].Content
			break
		}
	}
	convID := req.ConversationID
	if s.db != nil {
		if convID == "" {
			// 新建会话：id=conv-<UnixNano>，title=首条 user 消息截断 30 字。
			convID = fmt.Sprintf("conv-%d", time.Now().UnixNano())
			if err := s.db.CreateConversation(store.AIConversation{
				ID:    convID,
				User:  user,
				Title: convTitle(lastUser),
			}); err != nil {
				s.log.Errorf("ai chat: create conversation failed: %v", err)
			} else {
				// 新建会话时先发 conversation 事件，让前端绑定 convID。
				sink.OnConversation(convID)
			}
		}
		// 写 user 消息（seq 自增）。
		if _, err := s.db.AppendMessage(store.AIMessage{
			ID:      fmt.Sprintf("msg-%d", time.Now().UnixNano()),
			ConvID:  convID,
			Role:    "user",
			Content: lastUser,
		}); err != nil {
			s.log.Errorf("ai chat: append user message failed: %v", err)
		}
	}

	// 写操作闸门 = 配置总闸 && 调用者为 admin。为 false 时模型看不到写工具。
	allowWrite := s.cfg.AI.AllowWrite && c.GetString("auth_role") == "admin"
	// admin 门控受限 K/V 浏览器的读工具（describe_key/get_key），与 handler 硬校验 admin 一致。
	admin := c.GetString("auth_role") == "admin"
	// c.Request.Context() 在客户端断开时取消，Agent 会随之中止。
	ctx := c.Request.Context()
	s.aiAgent.Run(ctx, req.Messages, sink, allowWrite, admin)

	// ── Agent 结束后落库 assistant 消息（§13.7） ───────────────────────────
	// 正常结束 interrupted=0；ctx 被取消（客户端断开/停止）interrupted=1，content=已累积部分。
	if s.db != nil && convID != "" {
		interrupted := ctx.Err() != nil
		if _, err := s.db.AppendMessage(store.AIMessage{
			ID:          fmt.Sprintf("msg-%d", time.Now().UnixNano()),
			ConvID:      convID,
			Role:        "assistant",
			Content:     sink.sb.String(),
			ToolCalls:   sink.toolCallsJSON(),
			Interrupted: interrupted,
		}); err != nil {
			s.log.Errorf("ai chat: append assistant message failed: %v", err)
		}
	}
}

// convTitle 取首条 user 消息前 30 字作为会话标题（去换行；不足则全取）。§13.4-5。
func convTitle(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) > 30 {
		return string(r[:30])
	}
	return s
}

// aiExecuteRequest 是二次确认后执行写操作的请求体，字段来自 confirm 事件。
type aiExecuteRequest struct {
	Tool           string          `json:"tool"`
	Args           json.RawMessage `json:"args"`
	ConversationID string          `json:"conversation_id"` // 空=无会话直调，跳过落库
}

// aiExecute 执行 AI 提议、用户二次确认后的写操作（POST /api/v1/ai/execute）。
// 方案 A 的执行端点：不走 LLM，硬校验 allow_write + admin + 写工具白名单，然后
// **复用已验证的实例/备份 gin handler**执行——把审计 operator 覆写成 "ai:<user>" 后
// 构造对应请求体转发给现有 handler。这样 master 保护、配置保留字校验、failover 的
// Sentinel/直连分支等逻辑全部复用，不重复实现，也不改动关键路径。
// 模型没有任何路径能绕过用户在前端的确认点击到达这里。
func (s *Server) aiExecute(c *gin.Context) {
	if s.aiAgent == nil {
		fail(c, http.StatusServiceUnavailable, "AI assistant is not enabled")
		return
	}
	// 双保险：写操作需配置总闸开启 + 调用者为 admin。
	if !s.cfg.AI.AllowWrite {
		fail(c, http.StatusForbidden, "AI write operations are disabled")
		return
	}
	if c.GetString("auth_role") != "admin" {
		fail(c, http.StatusForbidden, "admin role required")
		return
	}
	var req aiExecuteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	// 白名单校验：只接受 ai 包定义的已知写工具。
	if !ai.IsWriteTool(req.Tool) {
		fail(c, http.StatusBadRequest, "not an allowed write tool: "+req.Tool)
		return
	}
	var a struct {
		Name          string            `json:"name"`
		Key           string            `json:"key"`
		Value         string            `json:"value"`
		Group         string            `json:"group"`
		Category      string            `json:"category"`
		Engine        string            `json:"engine"`
		EngineVersion string            `json:"engine_version"`
		Type          string            `json:"type"`
		Memory        string            `json:"memory"`
		CPUs          float64           `json:"cpus"`
		Disk          string            `json:"disk"`
		AccessMode    string            `json:"access_mode"`
		ReplicaOf     string            `json:"replica_of"`
		NodeSelector  map[string]string `json:"node_selector"`
		Schedule      string            `json:"schedule"`
		Retention     int               `json:"retention"`
		TaskID        string            `json:"task_id"`
		Op            string            `json:"op"`
		Field         string            `json:"field"`
		Member        string            `json:"member"`
		Score         string            `json:"score"`
		Side          string            `json:"side"`
		Index         *int64            `json:"index"`
		TTLSeconds    int64             `json:"ttl_seconds"`
	}
	if len(req.Args) > 0 {
		_ = json.Unmarshal(req.Args, &a)
	}
	if a.Name == "" && req.Tool != "cancel_task" {
		fail(c, http.StatusBadRequest, "missing arg: name")
		return
	}

	// 把工具映射到已验证的 gin handler，并构造其期望的请求体。
	var (
		handler gin.HandlerFunc
		body    map[string]any
	)
	switch req.Tool {
	case "create_instance":
		// 创建参数比其它写操作复杂：补默认值 + 校验必填，再构造 CreateInstanceRequest。
		engine := a.Engine
		if engine == "" {
			engine = "redis"
		}
		if a.Memory == "" {
			fail(c, http.StatusBadRequest, "missing arg: memory")
			return
		}
		handler = s.instanceCreate
		// cpus/disk 是可选入参：用户/AI 指定则采用，否则用与 CLI 一致的兜底。
		// cpus 默认 1；disk 按引擎默认（redis 10Gi / kvrocks 20Gi）。
		// 否则零值会导致 0C/0Gi 且调度不占容量账（见 scheduler reqCPUs>0 / reqDiskGi>0）。
		cpus := a.CPUs
		if cpus <= 0 {
			cpus = 1
		}
		disk := a.Disk
		if disk == "" {
			disk = "10Gi"
			if engine == "kvrocks" {
				disk = "20Gi"
			}
		} else if parseMemoryGi(disk) <= 0 {
			// parseMemoryGi 只认 Gi/G/Mi/M；GB/TB 等会解析成 0 导致不占磁盘账。
			fail(c, http.StatusBadRequest, "invalid disk (use Gi unit, e.g. 50Gi): "+disk)
			return
		}
		if a.ReplicaOf != "" {
			// 创建从库：group/category/type/engine_version 由 handler 从主库继承。
			// 但 CreateInstanceRequest 对 category/type 有 binding:"required"，故填占位值
			// （cache/replication）以通过绑定校验——handler 会用主库组的真实值覆盖它们。
			body = map[string]any{
				"name":       a.Name,
				"engine":     engine,
				"memory":     a.Memory,
				"cpus":       cpus,
				"disk":       disk,
				"replica_of": a.ReplicaOf,
				"category":   "cache",       // 占位，handler 用主库组 category 覆盖
				"type":       "replication", // 占位，handler 强制为 replication
			}
		} else {
			// 创建单点/主库：group 必填；category 缺省与 CLI 一致默认 cache。
			typ := a.Type
			if typ == "" {
				typ = "standalone"
			}
			category := a.Category
			if category == "" {
				category = "cache" // 对齐 CLI（--category 默认 cache）
			}
			if category != "cache" && category != "persistent" {
				fail(c, http.StatusBadRequest, "category must be cache or persistent")
				return
			}
			if a.Group == "" {
				fail(c, http.StatusBadRequest, "missing arg: group")
				return
			}
			body = map[string]any{
				"name":     a.Name,
				"group":    a.Group,
				"category": category,
				"engine":   engine,
				"type":     typ,
				"memory":   a.Memory,
				"cpus":     cpus,
				"disk":     disk,
			}
		}
		if a.EngineVersion != "" {
			body["engine_version"] = a.EngineVersion
		}
		if len(a.NodeSelector) > 0 {
			body["node_selector"] = a.NodeSelector
		}
	case "start_instance":
		handler, body = s.instanceStart, map[string]any{"name": a.Name}
	case "stop_instance":
		handler, body = s.instanceStop, map[string]any{"name": a.Name}
	case "failover":
		handler, body = s.instanceFailover, map[string]any{"name": a.Name}
	case "backup_exec":
		handler, body = s.backupExec, map[string]any{"name": a.Name}
	case "set_access_mode":
		if a.AccessMode != "rw-split" && a.AccessMode != "standard" {
			fail(c, http.StatusBadRequest, "access_mode must be rw-split or standard")
			return
		}
		handler, body = s.instanceAccessMode, map[string]any{"name": a.Name, "access_mode": a.AccessMode}
	case "replicate":
		if a.ReplicaOf == "" {
			fail(c, http.StatusBadRequest, "missing arg: replica_of")
			return
		}
		handler, body = s.instanceReplicate, map[string]any{"name": a.Name, "replica_of": a.ReplicaOf}
	case "backup_set_schedule":
		// schedule 允许为空（表示关闭自动备份），故不强制校验；retention 可选。
		handler, body = s.backupSetSchedule, map[string]any{"name": a.Name, "schedule": a.Schedule, "retention": a.Retention}
	case "config_set":
		if a.Key == "" {
			fail(c, http.StatusBadRequest, "missing arg: key")
			return
		}
		handler = s.instanceConfig
		body = map[string]any{"name": a.Name, "config_overrides": map[string]string{a.Key: a.Value}, "restart": false}
	case "resize_instance":
		// 只把用户/AI 实际指定的维度放进 body，缺省字段留空 → Server 端 ResizeInstanceRequest
		// 的指针字段为 nil，表示"不改该维度"。至少要有一个，否则 Server 返回 400。
		body = map[string]any{"name": a.Name}
		if a.Memory != "" {
			body["memory"] = a.Memory
		}
		if a.CPUs > 0 {
			body["cpus"] = a.CPUs
		}
		if a.Disk != "" {
			body["disk"] = a.Disk
		}
		if len(body) == 1 {
			fail(c, http.StatusBadRequest, "resize requires at least one of memory, cpus, disk")
			return
		}
		handler = s.instanceResize
	case "scan_bigkeys":
		handler, body = s.instanceBigkeysStart, map[string]any{"name": a.Name}
	case "cancel_task":
		// cancel_task 的参数是 task_id 而不是 name；路由是 /tasks/:id/cancel。
		// 复用 taskCancel handler，需要把 task_id 设置到 URL param。
		taskID := a.TaskID
		if taskID == "" {
			fail(c, http.StatusBadRequest, "missing arg: task_id")
			return
		}
		c.Params = append(c.Params, gin.Param{Key: "id", Value: taskID})
		handler, body = s.taskCancel, map[string]any{}
	case "write_key":
		if a.Key == "" {
			fail(c, http.StatusBadRequest, "missing arg: key")
			return
		}
		if a.Op == "" {
			fail(c, http.StatusBadRequest, "missing arg: op")
			return
		}
		// 复用 instanceKeyWrite handler（内含 admin 硬校验、白名单、master 路由、写审计）。
		// args 按 KeyWriteArgs 结构组织；index 仅 lset 用，非空才带上。
		kvArgs := map[string]any{
			"value":       a.Value,
			"field":       a.Field,
			"member":      a.Member,
			"score":       a.Score,
			"side":        a.Side,
			"ttl_seconds": a.TTLSeconds,
		}
		if a.Index != nil {
			kvArgs["index"] = *a.Index
		}
		handler, body = s.instanceKeyWrite, map[string]any{"name": a.Name, "key": a.Key, "op": a.Op, "args": kvArgs}
	case "delete_key":
		if a.Key == "" {
			fail(c, http.StatusBadRequest, "missing arg: key")
			return
		}
		handler, body = s.instanceKeyDelete, map[string]any{"name": a.Name, "key": a.Key}
	default:
		// 在 ai.writeTools 白名单内但此处没有执行器：属于开发期未同步，拒绝而非静默。
		fail(c, http.StatusInternalServerError, "no executor for write tool: "+req.Tool)
		return
	}

	// 审计归属：把 operator 覆写为 ai:<username>，复用的 handler 内部 operatorFrom(c)
	// 会读到它，从而落审计为“AI 代某用户执行”。role 不变（仍是 admin）。
	realUser := operatorFrom(c)
	c.Set("auth_user", "ai:"+realUser)
	s.log.Infof("ai execute: operator=ai:%s tool=%s instance=%s", realUser, req.Tool, a.Name)

	// 用构造的请求体重放给被复用的 handler；由它写响应与审计。
	raw, _ := json.Marshal(body)
	c.Request.Body = io.NopCloser(bytes.NewReader(raw))
	c.Request.ContentLength = int64(len(raw))
	c.Request.Header.Set("Content-Type", "application/json")

	// 包一层捕获 ResponseWriter，拿到执行结果（task_id）用于会话落库（§13.5.2-d）。
	cap := &captureWriter{ResponseWriter: c.Writer}
	c.Writer = cap
	handler(c)
	c.Writer = cap.ResponseWriter // 还原

	// 执行成功（2xx）且带会话 id 时，向最近 assistant 消息 tool_calls 追加 write_execute。
	if s.db != nil && req.ConversationID != "" && cap.status >= 200 && cap.status < 300 {
		taskID := ""
		var parsed struct {
			Data struct {
				TaskID string `json:"task_id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(cap.body.Bytes(), &parsed); err == nil {
			taskID = parsed.Data.TaskID
		}
		element := map[string]any{
			"type":    "write_execute",
			"tool":    req.Tool,
			"task_id": taskID,
			"result":  "accepted",
		}
		if err := s.db.AppendToolCallToLatestAssistant(req.ConversationID, realUser, element); err != nil && err != store.ErrNotFound {
			s.log.Errorf("ai execute: append write_execute failed: %v", err)
		}
	}
}

// captureWriter 包一层 gin.ResponseWriter，透传写入的同时缓存 body 与 status，
// 供 aiExecute 从复用 handler 的响应里取出 task_id（§13.5.2-d）。
type captureWriter struct {
	gin.ResponseWriter
	body   bytes.Buffer
	status int
}

func (w *captureWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *captureWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

// ── 会话历史 REST 端点（§13.5.2-e / §13.6） ──────────────────────────────

// aiCapabilities 向前端暴露 AI 能力状态（GET /api/v1/ai/capabilities）。
// 用于前端决定是否展示写操作相关的预置引导：allow_write = 配置总闸开启 且 调用者为 admin。
// 与 aiChat/aiExecute 里的闸门计算保持一致（cfg.AI.AllowWrite && role==admin），
// 使前端展示的能力与后端实际放行的能力不产生偏差（避免引导用户点了却被拒）。
func (s *Server) aiCapabilities(c *gin.Context) {
	allowWrite := s.cfg.AI.AllowWrite && c.GetString("auth_role") == "admin"
	ok(c, map[string]bool{"allow_write": allowWrite})
}

// aiListConversations 列出当前用户未删除的会话（GET /api/v1/ai/conversations）。
func (s *Server) aiListConversations(c *gin.Context) {
	if s.db == nil {
		fail(c, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	list, err := s.db.ListConversations(operatorFrom(c))
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, list)
}

// aiGetConversation 取某会话及其全部消息（GET /api/v1/ai/conversations/:id）。
// 归属校验失败或不存在 → 404。
func (s *Server) aiGetConversation(c *gin.Context) {
	if s.db == nil {
		fail(c, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	conv, msgs, err := s.db.GetConversationMessages(c.Param("id"), operatorFrom(c))
	if err == store.ErrNotFound {
		fail(c, http.StatusNotFound, "conversation not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, gin.H{"conversation": conv, "messages": msgs})
}

// aiDeleteConversation 软删除某会话（DELETE /api/v1/ai/conversations/:id）。
// 归属校验失败或不存在 → 404。
func (s *Server) aiDeleteConversation(c *gin.Context) {
	if s.db == nil {
		fail(c, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	err := s.db.SoftDeleteConversation(c.Param("id"), operatorFrom(c))
	if err == store.ErrNotFound {
		fail(c, http.StatusNotFound, "conversation not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, gin.H{"deleted": true})
}
