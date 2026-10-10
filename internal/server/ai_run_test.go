package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

// ── aiRunManager 单元测试 ───────────────────────────────────────────────

func waitRunDone(t *testing.T, r *aiRun) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for r.running() {
		if time.Now().After(deadline) {
			t.Fatal("run did not finish in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAIRunManagerResumeAfterSeq(t *testing.T) {
	m := newAIRunManager()
	release := make(chan struct{})
	r, err := m.start("c1", "alice", nil, func(ctx context.Context, r *aiRun) {
		r.emit("token", map[string]string{"text": "a"})
		r.emit("token", map[string]string{"text": "b"})
		<-release
		r.emit("token", map[string]string{"text": "c"})
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 同会话第二个 run 被拒绝（单会话单 run）。
	if _, err := m.start("c1", "alice", nil, func(context.Context, *aiRun) {}, nil); err != errAIRunBusy {
		t.Fatalf("want errAIRunBusy, got %v", err)
	}
	if running, _ := m.status("c1"); !running {
		t.Fatal("want running")
	}
	close(release)
	waitRunDone(t, r)

	evs, done, _ := r.snapshot(1) // 已收到 seq=1，从 2 开始续
	if !done {
		t.Fatal("want done")
	}
	var names []string
	for _, e := range evs {
		names = append(names, e.Name+":"+string(e.Data))
	}
	got := strings.Join(names, ",")
	if !strings.HasPrefix(got, `token:{"text":"b"},token:{"text":"c"},end:`) {
		t.Fatalf("unexpected resumed events: %s", got)
	}
	// 非本人拿不到 run。
	if m.get("c1", "bob") != nil {
		t.Fatal("other user must not see run")
	}
}

func TestAIRunManagerStopAndPending(t *testing.T) {
	m := newAIRunManager()
	var interrupted atomic.Bool
	r, _ := m.start("c1", "alice", nil, func(ctx context.Context, r *aiRun) {
		<-ctx.Done()
	}, func(r *aiRun, in bool) { interrupted.Store(in) })
	if m.stop("c1", "bob") {
		t.Fatal("other user must not stop")
	}
	if !m.stop("c1", "alice") {
		t.Fatal("want stopped")
	}
	waitRunDone(t, r)
	if !interrupted.Load() {
		t.Fatal("stopped run must finalize as interrupted")
	}
	evs, _, _ := r.snapshot(0)
	last := evs[len(evs)-1]
	if last.Name != "end" || !strings.Contains(string(last.Data), `"stopped":true`) {
		t.Fatalf("want end stopped, got %s %s", last.Name, last.Data)
	}

	// 提议写操作的 run 结束后，待确认挂到会话上；新 run 启动时被清掉。
	r2, _ := m.start("c1", "alice", nil, func(ctx context.Context, r *aiRun) {
		r.confirm = &aiPendingConfirm{Tool: "start_instance", Summary: "启动 x"}
	}, nil)
	waitRunDone(t, r2)
	if p := m.pendingFor("c1"); p == nil || p.Tool != "start_instance" {
		t.Fatalf("want pending, got %+v", p)
	}
	if _, pending := m.status("c1"); !pending {
		t.Fatal("status must report pending")
	}
	r3, _ := m.start("c1", "alice", nil, func(context.Context, *aiRun) {}, nil)
	waitRunDone(t, r3)
	if m.pendingFor("c1") != nil {
		t.Fatal("new run must clear pending")
	}
}

func TestAIRunManagerGC(t *testing.T) {
	m := newAIRunManager()
	now := time.Now()
	m.now = func() time.Time { return now }
	r, _ := m.start("c1", "alice", nil, func(ctx context.Context, r *aiRun) {
		r.confirm = &aiPendingConfirm{Tool: "start_instance"}
	}, nil)
	waitRunDone(t, r)
	if m.get("c1", "alice") == nil || m.pendingFor("c1") == nil {
		t.Fatal("finished run and pending should be retained")
	}
	now = now.Add(aiPendingTTL + time.Minute)
	r.mu.Lock()
	r.finishedAt = now.Add(-aiRunRetention - time.Second)
	r.mu.Unlock()
	if m.get("c1", "alice") != nil {
		t.Fatal("expired run should be collected")
	}
	if m.pendingFor("c1") != nil {
		t.Fatal("expired pending should be collected")
	}
}

func TestAIRunManagerDepthLimit(t *testing.T) {
	m := newAIRunManager()
	for i := 0; i < aiMaxAutoContinue; i++ {
		if !m.nextDepth("c1") {
			t.Fatalf("depth %d should be allowed", i)
		}
	}
	if m.nextDepth("c1") {
		t.Fatal("depth beyond limit must be refused")
	}
	m.resetDepth("c1")
	if !m.nextDepth("c1") {
		t.Fatal("reset should allow again")
	}
}

// ── 端到端：后台 run 与 HTTP 连接解耦 ──────────────────────────────────

// fakeLLM 模拟 OpenAI 兼容的流式 chat completion：逐个吐出 tokens，每个间隔 delay。
func fakeLLM(t *testing.T, tokens []string, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for _, tok := range tokens {
			chunk := map[string]any{
				"id": "x", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "content": tok}, "finish_reason": nil}},
			}
			b, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		end := map[string]any{
			"id": "x", "object": "chat.completion.chunk", "created": 1, "model": "m",
			"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
		}
		b, _ := json.Marshal(end)
		fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", b)
		fl.Flush()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newAITestServer(t *testing.T, llmURL string) (*Server, *httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		Port:    8080,
		DataDir: dir,
		Admin:   AdminConfig{Username: "admin", Password: "test"},
		Log:     LogConfig{Dir: dir + "/log", Stdout: false},
		AI:      AIConfig{Enabled: true, BaseURL: llmURL, APIKey: "k", Model: "m", MaxTurns: 2, Timeout: "10s"},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := s.InitAdmin(); err != nil {
		t.Fatalf("InitAdmin: %v", err)
	}
	if s.aiAgent == nil {
		t.Fatal("ai agent not initialized")
	}
	ts := httptest.NewServer(s.Router())
	t.Cleanup(func() { ts.Close(); s.db.Close() })
	return s, ts, makeTestJWT("admin", []byte("redis-pilot-default-jwt-secret"))
}

type sseEv struct {
	id, name, data string
}

// readSSE 读取 SSE 事件，直到 stop 返回 true 或流结束。
func readSSE(t *testing.T, resp *http.Response, stop func(sseEv) bool) []sseEv {
	t.Helper()
	sc := bufio.NewScanner(resp.Body)
	var out []sseEv
	var cur sseEv
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur.name != "" {
				out = append(out, cur)
				if stop != nil && stop(cur) {
					return out
				}
			}
			cur = sseEv{}
		case strings.HasPrefix(line, "id: "):
			cur.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			cur.name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			cur.data = strings.TrimPrefix(line, "data: ")
		}
	}
	return out
}

func TestAIChatSurvivesClientDisconnectAndResumes(t *testing.T) {
	llm := fakeLLM(t, []string{"你", "好", "，", "世", "界"}, 80*time.Millisecond)
	s, ts, token := newAITestServer(t, llm.URL)

	// 1) 发起对话，收到第一个 token 后立刻断开（模拟刷新/切走）。
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/v1/ai/chat",
		strings.NewReader(`{"message":"hi"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var convID, runID string
	evs := readSSE(t, resp, func(e sseEv) bool { return e.name == "token" })
	for _, e := range evs {
		if e.name == "run" {
			var raw map[string]any
			_ = json.Unmarshal([]byte(e.data), &raw)
			convID, runID = fmt.Sprint(raw["conversation_id"]), fmt.Sprint(raw["id"])
		}
	}
	lastSeq := evs[len(evs)-1].id
	cancel()
	resp.Body.Close()
	if convID == "" || runID == "" {
		t.Fatalf("missing run meta: %+v", evs)
	}

	// 2) 断开后会话仍显示 running（侧栏转圈数据源）。
	time.Sleep(30 * time.Millisecond)
	if running, _ := s.aiRuns.status(convID); !running {
		t.Fatal("run must keep running after client disconnect")
	}
	// 运行中再发消息 → 409（单会话单 run）。
	busy := doRequestWithAuth(s.Router(), http.MethodPost, "/api/v1/ai/chat", token,
		map[string]string{"message": "again", "conversation_id": convID})
	if busy.Code != http.StatusConflict {
		t.Fatalf("want 409 while running, got %d", busy.Code)
	}

	// 3) 断点续订：从 lastSeq 之后接着收，直到 end。
	sreq, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/api/v1/ai/conversations/%s/stream?run=%s&after=%s", ts.URL, convID, runID, lastSeq), nil)
	sreq.Header.Set("Authorization", "Bearer "+token)
	sresp, err := http.DefaultClient.Do(sreq)
	if err != nil {
		t.Fatal(err)
	}
	rest := readSSE(t, sresp, func(e sseEv) bool { return e.name == "end" })
	sresp.Body.Close()
	var text strings.Builder
	text.WriteString("你")
	for _, e := range rest {
		if e.name == "token" {
			var d map[string]string
			_ = json.Unmarshal([]byte(e.data), &d)
			text.WriteString(d["text"])
		}
	}
	if text.String() != "你好，世界" {
		t.Fatalf("resumed text = %q", text.String())
	}

	// 4) 落库的是完整回复，且未标记中断。
	_, msgs, err := s.db.GetConversationMessages(convID, "admin")
	if err != nil {
		t.Fatal(err)
	}
	var last store.AIMessage
	for _, m := range msgs {
		if m.Role == "assistant" {
			last = m
		}
	}
	if last.Content != "你好，世界" || last.Interrupted {
		t.Fatalf("persisted assistant = %+v", last)
	}

	// 5) 结束后会话列表不再 running；无 run 可订阅时 stream 返回 204（保留期内仍可回放）。
	list := doRequestWithAuth(s.Router(), http.MethodGet, "/api/v1/ai/conversations", token, nil)
	if !strings.Contains(list.Body.String(), `"running":false`) {
		t.Fatalf("list should report not running: %s", list.Body.String())
	}
	none := doRequestWithAuth(s.Router(), http.MethodGet, "/api/v1/ai/conversations/conv-none/stream", token, nil)
	if none.Code != http.StatusNoContent {
		t.Fatalf("want 204 for no run, got %d", none.Code)
	}
}

func TestAIChatExplicitStop(t *testing.T) {
	llm := fakeLLM(t, []string{"a", "b", "c", "d", "e", "f"}, 150*time.Millisecond)
	s, ts, token := newAITestServer(t, llm.URL)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/ai/chat", strings.NewReader(`{"message":"hi"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var convID string
	evs := readSSE(t, resp, func(e sseEv) bool { return e.name == "token" })
	for _, e := range evs {
		if e.name == "run" {
			var raw map[string]any
			_ = json.Unmarshal([]byte(e.data), &raw)
			convID = fmt.Sprint(raw["conversation_id"])
		}
	}
	stop := doRequestWithAuth(s.Router(), http.MethodPost, "/api/v1/ai/conversations/"+convID+"/stop", token, nil)
	if !strings.Contains(stop.Body.String(), `"stopped":true`) {
		t.Fatalf("stop: %s", stop.Body.String())
	}
	rest := readSSE(t, resp, func(e sseEv) bool { return e.name == "end" })
	for _, e := range rest {
		if e.name == "error" {
			t.Fatalf("explicit stop must not surface an error event: %s", e.data)
		}
	}
	if end := rest[len(rest)-1]; end.name != "end" || !strings.Contains(end.data, `"stopped":true`) {
		t.Fatalf("want end stopped, got %+v", end)
	}
	_, msgs, _ := s.db.GetConversationMessages(convID, "admin")
	if lastMsg := msgs[len(msgs)-1]; lastMsg.Role != "assistant" || !lastMsg.Interrupted {
		t.Fatalf("stopped reply must persist as interrupted: %+v", lastMsg)
	}
}

// fakeLLMWriteProposal 模拟模型提议一个写操作（start_instance）。
func fakeLLMWriteProposal(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		chunks := []map[string]any{
			{"id": "x", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []any{
					map[string]any{"index": 0, "id": "call1", "type": "function",
						"function": map[string]any{"name": "start_instance", "arguments": `{"name":"cache-1"}`}},
				}}, "finish_reason": nil}}},
			{"id": "x", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}},
		}
		for _, c := range chunks {
			b, _ := json.Marshal(c)
			fmt.Fprintf(w, "data: %s\n\n", b)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv
}

// 刷新页面后打开会话，能拿回待确认写操作（确认卡片可恢复）；dismiss 后清除。
func TestAIPendingConfirmRecoverableAfterReload(t *testing.T) {
	llm := fakeLLMWriteProposal(t)
	s, ts, token := newAITestServer(t, llm.URL)
	s.cfg.AI.AllowWrite = true

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/ai/chat", strings.NewReader(`{"message":"启动 cache-1"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	evs := readSSE(t, resp, func(e sseEv) bool { return e.name == "end" })
	resp.Body.Close()
	var convID string
	sawConfirm := false
	for _, e := range evs {
		if e.name == "run" {
			var raw map[string]any
			_ = json.Unmarshal([]byte(e.data), &raw)
			convID = fmt.Sprint(raw["conversation_id"])
		}
		if e.name == "confirm" {
			sawConfirm = true
		}
	}
	if !sawConfirm {
		t.Fatalf("expected confirm event: %+v", evs)
	}
	// end 事件在 finish 前发出，等 run 完全收尾再查。
	if r := s.aiRuns.get(convID, "admin"); r != nil {
		waitRunDone(t, r)
	}

	got := doRequestWithAuth(s.Router(), http.MethodGet, "/api/v1/ai/conversations/"+convID, token, nil)
	var body struct {
		Data struct {
			Pending *aiPendingConfirm `json:"pending"`
			Run     any               `json:"run"`
		} `json:"data"`
	}
	_ = json.Unmarshal(got.Body.Bytes(), &body)
	if body.Data.Pending == nil || body.Data.Pending.Tool != "start_instance" || body.Data.Pending.Summary == "" {
		t.Fatalf("pending not recoverable: %s", got.Body.String())
	}
	if body.Data.Run != nil {
		t.Fatalf("finished run must not be reported as running: %s", got.Body.String())
	}
	list := doRequestWithAuth(s.Router(), http.MethodGet, "/api/v1/ai/conversations", token, nil)
	if !strings.Contains(list.Body.String(), `"pending_confirm":true`) {
		t.Fatalf("list should flag pending: %s", list.Body.String())
	}

	// 落库的 tool_calls 带时间戳，并以 run_meta 收尾（折叠摘要显示耗时用）。
	_, msgs, _ := s.db.GetConversationMessages(convID, "admin")
	var calls []map[string]any
	_ = json.Unmarshal([]byte(msgs[len(msgs)-1].ToolCalls), &calls)
	if len(calls) != 2 || calls[0]["ts"] == nil || calls[1]["type"] != "run_meta" || calls[1]["started_at"] == nil || calls[1]["finished_at"] == nil {
		t.Fatalf("tool_calls missing timing: %s", msgs[len(msgs)-1].ToolCalls)
	}

	doRequestWithAuth(s.Router(), http.MethodPost, "/api/v1/ai/conversations/"+convID+"/dismiss", token, nil)
	if s.aiRuns.pendingFor(convID) != nil {
		t.Fatal("dismiss must clear pending")
	}
}
