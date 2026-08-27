package ai

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// mockPlatform 返回固定的假数据，让端到端测试不依赖真实 Agent / 实例。
type mockPlatform struct{ calls []string }

func (m *mockPlatform) ListInstances(context.Context) (json.RawMessage, error) {
	m.calls = append(m.calls, "list")
	return json.RawMessage(`[{"name":"galaxy-1","group":"cache","role":"master","server":"node-a","observed_status":"running"}]`), nil
}
func (m *mockPlatform) InstanceStatus(_ context.Context, name string) (json.RawMessage, error) {
	m.calls = append(m.calls, "status:"+name)
	return json.RawMessage(`{"name":"` + name + `","status":"running"}`), nil
}
func (m *mockPlatform) DiagnoseInstance(_ context.Context, name string) (json.RawMessage, error) {
	m.calls = append(m.calls, "diagnose:"+name)
	return json.RawMessage(`{"info":{"mem_fragmentation_ratio":1.8,"used_memory_human":"1.2G","role":"master"},"slowlog":[{"command":"KEYS *","usec":120000}]}`), nil
}
func (m *mockPlatform) GetSlowlog(_ context.Context, name string, count int) (json.RawMessage, error) {
	m.calls = append(m.calls, "slowlog:"+name)
	return json.RawMessage(`[{"command":"KEYS *","usec":120000}]`), nil
}
func (m *mockPlatform) GetBigkeys(_ context.Context, name string) (json.RawMessage, error) {
	m.calls = append(m.calls, "bigkeys:"+name)
	return json.RawMessage(`{"biggest_string":{"key":"foo","bytes":1048576}}`), nil
}
func (m *mockPlatform) GetInstanceConfig(_ context.Context, name string) (json.RawMessage, error) {
	m.calls = append(m.calls, "config:"+name)
	return json.RawMessage(`{"maxmemory":"2gb","maxmemory-policy":"allkeys-lru"}`), nil
}
func (m *mockPlatform) GetMetricsHistory(_ context.Context, name, timeRange string) (json.RawMessage, error) {
	m.calls = append(m.calls, "metrics:"+name)
	return json.RawMessage(`{"points":[]}`), nil
}
func (m *mockPlatform) ListNodes(context.Context) (json.RawMessage, error) {
	m.calls = append(m.calls, "nodes")
	return json.RawMessage(`[{"name":"node-a","endpoint":"10.0.1.10","status":"healthy","instances":1}]`), nil
}
func (m *mockPlatform) QueryAudit(_ context.Context, limit int) (json.RawMessage, error) {
	m.calls = append(m.calls, "audit")
	return json.RawMessage(`{"records":[],"total":0}`), nil
}

type captureSink struct {
	tokens    strings.Builder
	toolCalls []string
	confirms  []string
	done      bool
	err       error
}

func (s *captureSink) OnToken(t string)                    { s.tokens.WriteString(t) }
func (s *captureSink) OnToolCall(name, _ string)           { s.toolCalls = append(s.toolCalls, name) }
func (s *captureSink) OnToolResult(string, bool, string)   {}
func (s *captureSink) OnConfirm(tool, _, _ string)         { s.confirms = append(s.confirms, tool) }
func (s *captureSink) OnUsage(_, _, _ int64)               {}
func (s *captureSink) OnError(err error)                   { s.err = err }
func (s *captureSink) OnDone()                             { s.done = true }

// TestAgentEndToEnd 用真实 LLM 网关 + mock 平台跑通完整 agent loop。
// 需要 AGP_API_KEY 环境变量，否则跳过（CI 无 key 时不失败）。
func TestAgentEndToEnd(t *testing.T) {
	key := os.Getenv("AGP_API_KEY")
	if key == "" {
		t.Skip("AGP_API_KEY not set; skipping live LLM end-to-end test")
	}
	platform := &mockPlatform{}
	agent, err := NewAgent(platform, Options{
		BaseURL:   "https://agp-in.dev.ihuman.com/v1",
		APIKey:    key,
		Model:     "volcengine/deepseek-v4-flash",
		MaxTurns:  6,
		MaxTokens: 1024,
		Timeout:   90 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	sink := &captureSink{}
	agent.Run(context.Background(), []Message{
		{Role: "user", Content: "诊断实例 galaxy-1，看看内存碎片和慢查询有没有问题，给出结论和建议。"},
	}, sink, false)

	if sink.err != nil {
		t.Fatalf("agent error: %v", sink.err)
	}
	if !sink.done {
		t.Error("expected OnDone to be called")
	}
	if len(sink.toolCalls) == 0 {
		t.Fatal("expected at least one tool call, got none")
	}
	diagnosed := false
	for _, tc := range sink.toolCalls {
		if tc == "diagnose_instance" {
			diagnosed = true
		}
	}
	if !diagnosed {
		t.Errorf("expected diagnose_instance tool call, got %v", sink.toolCalls)
	}
	if sink.tokens.Len() == 0 {
		t.Error("expected assistant to produce a final answer")
	}
	t.Logf("tool calls: %v", sink.toolCalls)
	t.Logf("platform calls: %v", platform.calls)
	t.Logf("final answer:\n%s", sink.tokens.String())
}
