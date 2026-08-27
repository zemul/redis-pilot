package ai

import (
	"context"
	"encoding/json"
)

// Message 一条对话消息（role: user / assistant）。system 消息由 Agent 内部注入。
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Sink 接收 Agent 运行过程中的流式事件；由调用方（如 SSE handler）实现。
// 所有回调在 Agent.Run 的 goroutine 内串行调用。
type Sink interface {
	OnToken(text string)                              // 助手回复的增量文本
	OnToolCall(name, args string)                     // 模型请求调用某工具
	OnToolResult(name string, ok bool, errMsg string) // 工具执行结果（不含原始数据，避免刷屏/泄敏）
	OnError(err error)                                // 出错
	OnDone()                                          // 正常结束
}

// PlatformAPI 是 AI 可调用的平台只读能力，由 Server 进程内直接实现
// （不经 HTTP 回环、不经 CLI，见 docs/architecture/ai-assistant.md §4.1）。
type PlatformAPI interface {
	ListInstances(ctx context.Context) (json.RawMessage, error)
	InstanceStatus(ctx context.Context, name string) (json.RawMessage, error)
	DiagnoseInstance(ctx context.Context, name string) (json.RawMessage, error)
	GetSlowlog(ctx context.Context, name string, count int) (json.RawMessage, error)
	GetBigkeys(ctx context.Context, name string) (json.RawMessage, error)
	GetInstanceConfig(ctx context.Context, name string) (json.RawMessage, error)
	GetMetricsHistory(ctx context.Context, name, timeRange string) (json.RawMessage, error)
	ListNodes(ctx context.Context) (json.RawMessage, error)
	QueryAudit(ctx context.Context, limit int) (json.RawMessage, error)
}
