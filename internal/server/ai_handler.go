package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

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

// sseSink 把 Agent 的运行事件写成 SSE 帧推给前端。
type sseSink struct {
	c       *gin.Context
	flusher http.Flusher
}

func (k *sseSink) send(event string, payload any) {
	data, _ := json.Marshal(payload)
	fmt.Fprintf(k.c.Writer, "event: %s\ndata: %s\n\n", event, data)
	k.flusher.Flush()
}

func (k *sseSink) OnToken(text string)          { k.send("token", map[string]string{"text": text}) }
func (k *sseSink) OnToolCall(name, args string) { k.send("tool_call", map[string]string{"name": name, "args": args}) }
func (k *sseSink) OnToolResult(name string, ok bool, errMsg string) {
	k.send("tool_result", map[string]any{"name": name, "ok": ok, "error": errMsg})
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
	sink := &sseSink{c: c, flusher: flusher}
	// c.Request.Context() 在客户端断开时取消，Agent 会随之中止。
	s.aiAgent.Run(c.Request.Context(), req.Messages, sink)
}
