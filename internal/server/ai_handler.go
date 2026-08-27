package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/server/ai"
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

// sseSink 把 Agent 的运行事件写成 SSE 帧推给前端，同时把工具调用与 token 用量
// 写入结构化日志（可观测性，见 §7.5；token 记录见 §7.3）。
type sseSink struct {
	c       *gin.Context
	flusher http.Flusher
	log     *logger.Logger
	user    string
}

func (k *sseSink) send(event string, payload any) {
	data, _ := json.Marshal(payload)
	fmt.Fprintf(k.c.Writer, "event: %s\ndata: %s\n\n", event, data)
	k.flusher.Flush()
}

func (k *sseSink) OnToken(text string) { k.send("token", map[string]string{"text": text}) }
func (k *sseSink) OnToolCall(name, args string) {
	k.log.Infof("ai tool_call: user=%s tool=%s args=%s", k.user, name, args)
	k.send("tool_call", map[string]string{"name": name, "args": args})
}
func (k *sseSink) OnToolResult(name string, ok bool, errMsg string) {
	if ok {
		k.log.Infof("ai tool_result: user=%s tool=%s ok", k.user, name)
	} else {
		k.log.Infof("ai tool_result: user=%s tool=%s error=%s", k.user, name, errMsg)
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
	k.send("confirm", map[string]string{"tool": tool, "args": args, "summary": summary})
}
func (k *sseSink) OnError(err error) { k.send("error", map[string]string{"message": err.Error()}) }
func (k *sseSink) OnDone()           { k.send("done", map[string]bool{"done": true}) }

type aiChatRequest struct {
	Messages []ai.Message `json:"messages"`
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
	sink := &sseSink{c: c, flusher: flusher, log: s.log, user: operatorFrom(c)}
	// 写操作闸门 = 配置总闸 && 调用者为 admin。为 false 时模型看不到写工具。
	allowWrite := s.cfg.AI.AllowWrite && c.GetString("auth_role") == "admin"
	// c.Request.Context() 在客户端断开时取消，Agent 会随之中止。
	s.aiAgent.Run(c.Request.Context(), req.Messages, sink, allowWrite)
}

// aiExecuteRequest 是二次确认后执行写操作的请求体，字段来自 confirm 事件。
type aiExecuteRequest struct {
	Tool string          `json:"tool"`
	Args json.RawMessage `json:"args"`
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
		Name  string `json:"name"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if len(req.Args) > 0 {
		_ = json.Unmarshal(req.Args, &a)
	}
	if a.Name == "" {
		fail(c, http.StatusBadRequest, "missing arg: name")
		return
	}

	// 把工具映射到已验证的 gin handler，并构造其期望的请求体。
	var (
		handler gin.HandlerFunc
		body    map[string]any
	)
	switch req.Tool {
	case "start_instance":
		handler, body = s.instanceStart, map[string]any{"name": a.Name}
	case "stop_instance":
		handler, body = s.instanceStop, map[string]any{"name": a.Name}
	case "failover":
		handler, body = s.instanceFailover, map[string]any{"name": a.Name}
	case "backup_exec":
		handler, body = s.backupExec, map[string]any{"name": a.Name}
	case "config_set":
		if a.Key == "" {
			fail(c, http.StatusBadRequest, "missing arg: key")
			return
		}
		handler = s.instanceConfig
		body = map[string]any{"name": a.Name, "config_overrides": map[string]string{a.Key: a.Value}, "restart": false}
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
	handler(c)
}
