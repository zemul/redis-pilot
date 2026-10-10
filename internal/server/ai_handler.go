package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

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
func (p aiPlatform) QueryIncidents(_ context.Context, status, severity, instance, group, node string, limit int) (json.RawMessage, error) {
	return p.s.aiQueryIncidents(status, severity, instance, group, node, limit)
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

type aiChatRequest struct {
	// Message 本轮用户提问。兼容旧前端：为空时取 Messages 里最后一条 user 消息。
	// 对话上下文以服务端落库的会话历史为准（不再信任前端回传的完整 history）。
	Message        string       `json:"message"`
	Messages       []ai.Message `json:"messages"`
	ConversationID string       `json:"conversation_id"` // 空=新建会话
}

// aiChat 是 AI 助手的对话端点（POST /api/v1/ai/chat）。
// 挂在 authed group：所有登录用户（含普通用户）均可使用只读诊断。
//
// 行为（§14）：落库 user 消息 → 启动与本连接解耦的后台 run → 以 SSE 订阅该 run 的事件。
// 客户端断开只结束订阅，run 继续执行并在结束时落库；客户端可经
// GET /ai/conversations/:id/stream 断点续订，显式停止走 POST /ai/conversations/:id/stop。
func (s *Server) aiChat(c *gin.Context) {
	if s.aiAgent == nil {
		fail(c, http.StatusServiceUnavailable, "AI assistant is not enabled")
		return
	}
	if s.db == nil {
		fail(c, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	var req aiChatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	question := strings.TrimSpace(req.Message)
	if question == "" {
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == "user" {
				question = strings.TrimSpace(req.Messages[i].Content)
				break
			}
		}
	}
	if question == "" {
		fail(c, http.StatusBadRequest, "message is required")
		return
	}
	user := operatorFrom(c)
	s.log.Infof("ai chat: user=%s conv=%s", user, req.ConversationID)

	convID := req.ConversationID
	var history []ai.Message
	if convID != "" {
		_, msgs, err := s.db.GetConversationMessages(convID, user)
		if err == store.ErrNotFound {
			fail(c, http.StatusNotFound, "conversation not found")
			return
		}
		if err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		if running, _ := s.aiRuns.status(convID); running {
			fail(c, http.StatusConflict, "该会话正在回复中，请等待完成或先停止")
			return
		}
		history = historyFromStore(msgs)
	} else {
		// 新建会话：id=conv-<UnixNano>，title=首条 user 消息截断 30 字。
		convID = fmt.Sprintf("conv-%d", time.Now().UnixNano())
		if err := s.db.CreateConversation(store.AIConversation{
			ID:    convID,
			User:  user,
			Title: convTitle(question),
		}); err != nil {
			fail(c, http.StatusInternalServerError, "create conversation: "+err.Error())
			return
		}
	}
	if _, err := s.db.AppendMessage(store.AIMessage{
		ID:      fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		ConvID:  convID,
		Role:    "user",
		Content: question,
	}); err != nil {
		s.log.Errorf("ai chat: append user message failed: %v", err)
	}
	history = append(history, ai.Message{Role: "user", Content: question})

	// 用户主动发问：重置多步写操作自动续接链计数。
	s.aiRuns.resetDepth(convID)
	// 写操作闸门 = 配置总闸 && 调用者为 admin。为 false 时模型看不到写工具。
	admin := c.GetString("auth_role") == "admin"
	allowWrite := s.cfg.AI.AllowWrite && admin
	r, err := s.startAIRun(convID, user, history, allowWrite, admin)
	if err == errAIRunBusy {
		fail(c, http.StatusConflict, "该会话正在回复中，请等待完成或先停止")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	streamAIRun(c, r, 0)
}

// startAIRun 以后台 run 执行一轮 Agent 对话；结束时落库 assistant 消息（§13.7）：
// 正常结束 interrupted=0；被停止/超时 interrupted=1，content=已累积部分。
func (s *Server) startAIRun(convID, user string, history []ai.Message, allowWrite, admin bool) (*aiRun, error) {
	timeout, _ := time.ParseDuration(s.cfg.AI.Timeout)
	return s.aiRuns.start(convID, user, s.log,
		func(ctx context.Context, r *aiRun) {
			s.aiAgent.Run(ctx, history, &aiRunSink{r: r, log: s.log, timeout: timeout}, allowWrite, admin)
		},
		func(r *aiRun, interrupted bool) {
			if s.db == nil {
				return
			}
			if _, err := s.db.AppendMessage(store.AIMessage{
				ID:          fmt.Sprintf("msg-%d", time.Now().UnixNano()),
				ConvID:      convID,
				Role:        "assistant",
				Content:     r.sb.String(),
				ToolCalls:   r.toolCallsJSON(),
				Interrupted: interrupted,
			}); err != nil {
				s.log.Errorf("ai run: append assistant message failed: %v", err)
			}
		},
	)
}

// historyFromStore 把落库消息转成发给 Agent 的对话上下文。
// 空 content 的消息（如只提议了写操作的 assistant 回复）跳过，避免 LLM 接口拒绝空消息。
func historyFromStore(msgs []store.AIMessage) []ai.Message {
	out := make([]ai.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Role != "user" && m.Role != "assistant" {
			continue
		}
		if strings.TrimSpace(m.Content) == "" {
			continue
		}
		out = append(out, ai.Message{Role: m.Role, Content: m.Content})
	}
	return out
}

// aiAutoContinuePrefix 标记后端自动续接注入的 user 消息；前端加载历史时据此隐藏。
const aiAutoContinuePrefix = "[系统提示]"

// continueAfterExecute 在写操作执行成功后由后端自动发起下一轮 run，让 AI 接续多步意图的
// 下一步（如主从创建：主库建好 → 自动续 → AI 提议建从库）。放在后端而非前端，
// 保证执行成功后即使用户刷新/切走页面，多步链也不会断。
func (s *Server) continueAfterExecute(convID, user, summary string, allowWrite bool) {
	if s.db == nil || s.aiAgent == nil {
		return
	}
	if !s.aiRuns.nextDepth(convID) {
		s.log.Infof("ai execute: auto-continue limit reached: conv=%s", convID)
		return
	}
	followup := aiAutoContinuePrefix + ` 上一步写操作已成功执行：` + summary + `。` +
		`如果用户最初的意图还需要后续写操作才能完成（例如“创建主从实例”在主库创建后还需创建从库并建立复制），` +
		`请继续提议下一步并简要说明；如果整个意图已经完成，请简要总结，不要再提议任何写操作。`
	if _, err := s.db.AppendMessage(store.AIMessage{
		ID:      fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		ConvID:  convID,
		Role:    "user",
		Content: followup,
	}); err != nil {
		s.log.Errorf("ai execute: append followup failed: %v", err)
		return
	}
	_, msgs, err := s.db.GetConversationMessages(convID, user)
	if err != nil {
		s.log.Errorf("ai execute: load history failed: %v", err)
		return
	}
	if _, err := s.startAIRun(convID, user, historyFromStore(msgs), allowWrite, true); err != nil {
		s.log.Errorf("ai execute: auto-continue start failed: conv=%s: %v", convID, err)
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
		if a.Group == "" {
			fail(c, http.StatusBadRequest, "missing arg: group")
			return
		}
		handler, body = s.backupExec, map[string]any{"group": a.Group}
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
		if a.Group == "" {
			fail(c, http.StatusBadRequest, "missing arg: group")
			return
		}
		handler, body = s.backupSetSchedule, map[string]any{"group": a.Group, "schedule": a.Schedule, "retention": a.Retention}
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
		if err := s.db.AppendToolCallToLatestAssistant(req.ConversationID, realUser, element); err != nil {
			if err != store.ErrNotFound {
				s.log.Errorf("ai execute: append write_execute failed: %v", err)
			}
			return
		}
		// 待确认已被执行：清掉，并由后端自动续接下一轮（前端随后订阅会话 stream 即可）。
		// run 在本 handler 返回前就已登记，保证前端拿到响应后订阅一定能命中。
		s.aiRuns.clearPending(req.ConversationID)
		summary := ai.WriteToolSummary(req.Tool, string(req.Args))
		s.continueAfterExecute(req.ConversationID, realUser, summary, s.cfg.AI.AllowWrite)
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

// aiConversationItem 是会话列表项：会话元数据 + 内存中的运行状态（侧栏转圈/待确认角标用）。
type aiConversationItem struct {
	store.AIConversation
	Running        bool `json:"running"`
	PendingConfirm bool `json:"pending_confirm"`
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
	out := make([]aiConversationItem, 0, len(list))
	for _, conv := range list {
		running, pending := s.aiRuns.status(conv.ID)
		out = append(out, aiConversationItem{AIConversation: conv, Running: running, PendingConfirm: pending})
	}
	ok(c, out)
}

// aiGetConversation 取某会话及其全部消息（GET /api/v1/ai/conversations/:id）。
// 额外返回 run（运行中时：前端据此断点续订）与 pending（待确认写操作：刷新后恢复确认卡片）。
// 归属校验失败或不存在 → 404。
func (s *Server) aiGetConversation(c *gin.Context) {
	if s.db == nil {
		fail(c, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	user := operatorFrom(c)
	conv, msgs, err := s.db.GetConversationMessages(c.Param("id"), user)
	if err == store.ErrNotFound {
		fail(c, http.StatusNotFound, "conversation not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	var run any
	if r := s.aiRuns.get(conv.ID, user); r != nil && r.running() {
		run = gin.H{"id": r.id, "running": true}
	}
	var pending any
	if p := s.aiRuns.pendingFor(conv.ID); p != nil {
		pending = p
	}
	ok(c, gin.H{"conversation": conv, "messages": msgs, "run": run, "pending": pending})
}

// aiDeleteConversation 软删除某会话（DELETE /api/v1/ai/conversations/:id）。
// 同时中止其运行中的 run 并清理内存状态。归属校验失败或不存在 → 404。
func (s *Server) aiDeleteConversation(c *gin.Context) {
	if s.db == nil {
		fail(c, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	id := c.Param("id")
	err := s.db.SoftDeleteConversation(id, operatorFrom(c))
	if err == store.ErrNotFound {
		fail(c, http.StatusNotFound, "conversation not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	s.aiRuns.forget(id)
	ok(c, gin.H{"deleted": true})
}

// aiStreamConversation 断点续订会话当前/最近一次 run 的事件
// （GET /api/v1/ai/conversations/:id/stream?run=<run_id>&after=<seq>）。
// run 与当前不一致（已被新 run 取代）时从头回放；无 run → 204。
func (s *Server) aiStreamConversation(c *gin.Context) {
	r := s.aiRuns.get(c.Param("id"), operatorFrom(c))
	if r == nil {
		c.Status(http.StatusNoContent)
		return
	}
	var after int64
	if c.Query("run") == r.id {
		after, _ = strconv.ParseInt(c.Query("after"), 10, 64)
	}
	streamAIRun(c, r, after)
}

// aiStopConversation 显式停止会话正在运行的 run（POST /api/v1/ai/conversations/:id/stop）。
// 断开连接不再等于停止，只有这里会中止 run；已生成部分以 interrupted=1 落库。
func (s *Server) aiStopConversation(c *gin.Context) {
	stopped := s.aiRuns.stop(c.Param("id"), operatorFrom(c))
	ok(c, gin.H{"stopped": stopped})
}

// aiDismissConfirm 用户取消待确认的写操作（POST /api/v1/ai/conversations/:id/dismiss）。
func (s *Server) aiDismissConfirm(c *gin.Context) {
	if s.db == nil {
		fail(c, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	id := c.Param("id")
	if _, _, err := s.db.GetConversationMessages(id, operatorFrom(c)); err != nil {
		fail(c, http.StatusNotFound, "conversation not found")
		return
	}
	s.aiRuns.clearPending(id)
	ok(c, gin.H{"dismissed": true})
}
