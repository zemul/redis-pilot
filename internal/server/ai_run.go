package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
)

// ── AI 后台 run 管理（§14） ────────────────────────────────────────────────
//
// 一次 AI 回复（Agent.Run）作为 Server 进程内的后台 run 执行，与发起它的 HTTP 连接解耦：
// 客户端断开（刷新页面、切 tab、关闭标签页）不会中止 run，只有显式 stop 或总超时才会。
// run 产生的事件按序编号缓存在内存，客户端可凭 (run_id, after=最后收到的序号) 断点续订。
//
// 刻意只放内存、不落库：Server 重启/崩溃是小概率事件，重启后 run 丢失，会话里只剩已落库
// 的历史（最后一轮提问无回复），用户重问即可。

const (
	// aiRunRetention：run 结束后在内存保留的时长，供晚到的订阅者拿到结尾事件。
	aiRunRetention = 2 * time.Minute
	// aiPendingTTL：待确认写操作的有效期，超时视为自动取消，避免无人值守的提议一直挂着。
	aiPendingTTL = 30 * time.Minute
	// aiMaxAutoContinue：多步写操作后端自动续接的安全上限（单条用户意图链）。
	aiMaxAutoContinue = 25
	// aiStreamHeartbeat：SSE 心跳间隔，防止中间代理因空闲断开长连接。
	aiStreamHeartbeat = 15 * time.Second
)

var errAIRunBusy = errors.New("conversation is running")

// aiEvent 是 run 内一条已编号的事件（seq 从 1 起，单 run 内单调递增）。
type aiEvent struct {
	Seq  int64
	Name string
	Data []byte
}

// aiPendingConfirm 是 AI 提议、等待用户二次确认的写操作。挂在会话上（而非前端 state），
// 刷新页面后打开会话可恢复确认卡片。
type aiPendingConfirm struct {
	Tool      string    `json:"tool"`
	Args      string    `json:"args"`
	Summary   string    `json:"summary"`
	CreatedAt time.Time `json:"created_at"`
}

// aiRun 一次后台 AI 回复。
type aiRun struct {
	id      string
	convID  string
	user    string
	started time.Time
	cancel  context.CancelFunc

	mu         sync.Mutex
	events     []aiEvent
	notify     chan struct{} // 每追加一个事件就 close 并替换，唤醒所有订阅者
	done       bool
	stopped    bool
	finishedAt time.Time

	// 以下累积器只在 run goroutine 内访问（Sink 回调串行），结束后用于落库。
	sb      strings.Builder
	calls   []any
	confirm *aiPendingConfirm
}

func (r *aiRun) emit(name string, payload any) {
	data, _ := json.Marshal(payload)
	r.mu.Lock()
	r.events = append(r.events, aiEvent{Seq: int64(len(r.events) + 1), Name: name, Data: data})
	close(r.notify)
	r.notify = make(chan struct{})
	r.mu.Unlock()
}

// snapshot 返回 seq > after 的事件、run 是否已结束、以及下一次变化的通知 channel。
func (r *aiRun) snapshot(after int64) ([]aiEvent, bool, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if after < 0 {
		after = 0
	}
	var evs []aiEvent
	if after < int64(len(r.events)) {
		evs = append([]aiEvent(nil), r.events[after:]...)
	}
	return evs, r.done, r.notify
}

// finish 追加 end 事件并标记结束，二者在同一把锁内完成：订阅者一旦看到 end，
// running() 必然已为 false，避免前端据「仍在运行」再次订阅造成重复回放。
func (r *aiRun) finish(interrupted bool) {
	r.mu.Lock()
	data, _ := json.Marshal(map[string]bool{"interrupted": interrupted, "stopped": r.stopped})
	r.events = append(r.events, aiEvent{Seq: int64(len(r.events) + 1), Name: "end", Data: data})
	r.done = true
	r.finishedAt = time.Now()
	close(r.notify)
	r.notify = make(chan struct{})
	r.mu.Unlock()
}

func (r *aiRun) running() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.done
}

func (r *aiRun) wasStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

// aiRunManager 管理所有会话的后台 run、待确认写操作与自动续接深度。
// 约束：同一会话同一时刻只允许一个 run（多标签页可同时订阅，但不能并发发新消息）。
type aiRunManager struct {
	mu      sync.Mutex
	runs    map[string]*aiRun // convID → 最近一次 run（运行中或刚结束）
	pending map[string]*aiPendingConfirm
	depth   map[string]int // convID → 自动续接链已续轮数
	seq     int64
	now     func() time.Time
}

func newAIRunManager() *aiRunManager {
	return &aiRunManager{
		runs:    map[string]*aiRun{},
		pending: map[string]*aiPendingConfirm{},
		depth:   map[string]int{},
		now:     time.Now,
	}
}

// gcLocked 清理已结束超过保留期的 run 与过期的待确认。调用方持有 m.mu。
func (m *aiRunManager) gcLocked() {
	now := m.now()
	for id, r := range m.runs {
		r.mu.Lock()
		expired := r.done && now.Sub(r.finishedAt) > aiRunRetention
		r.mu.Unlock()
		if expired {
			delete(m.runs, id)
		}
	}
	for id, p := range m.pending {
		if now.Sub(p.CreatedAt) > aiPendingTTL {
			delete(m.pending, id)
		}
	}
}

// start 为会话启动一个后台 run。body 在独立 ctx 下执行（不随 HTTP 连接取消）；
// body 返回后调用 finalize（落库），再把待确认挂到会话上、追加 end 事件并标记结束。
// 会话已有运行中的 run 时返回 errAIRunBusy。
func (m *aiRunManager) start(convID, user string, log *logger.Logger,
	body func(ctx context.Context, r *aiRun),
	finalize func(r *aiRun, interrupted bool),
) (*aiRun, error) {
	m.mu.Lock()
	m.gcLocked()
	if old := m.runs[convID]; old != nil && old.running() {
		m.mu.Unlock()
		return nil, errAIRunBusy
	}
	m.seq++
	ctx, cancel := context.WithCancel(context.Background())
	r := &aiRun{
		id:      fmt.Sprintf("run-%d-%d", m.now().UnixNano(), m.seq),
		convID:  convID,
		user:    user,
		started: m.now(),
		cancel:  cancel,
		notify:  make(chan struct{}),
	}
	m.runs[convID] = r
	delete(m.pending, convID) // 新一轮对话取代上一个未处理的确认
	m.mu.Unlock()

	go func() {
		defer cancel()
		func() {
			defer func() {
				if p := recover(); p != nil && log != nil {
					log.Errorf("ai run panic: conv=%s run=%s: %v", convID, r.id, p)
				}
			}()
			body(ctx, r)
		}()
		interrupted := ctx.Err() != nil
		if finalize != nil {
			finalize(r, interrupted)
		}
		if r.confirm != nil && !interrupted {
			m.mu.Lock()
			p := *r.confirm
			p.CreatedAt = m.now()
			m.pending[convID] = &p
			m.mu.Unlock()
		}
		r.finish(interrupted)
	}()
	return r, nil
}

// get 返回会话最近一次 run（运行中或刚结束）；不存在或非本人返回 nil。
func (m *aiRunManager) get(convID, user string) *aiRun {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gcLocked()
	r := m.runs[convID]
	if r == nil || r.user != user {
		return nil
	}
	return r
}

// stop 显式中止会话正在运行的 run。返回是否确有 run 被中止。
func (m *aiRunManager) stop(convID, user string) bool {
	m.mu.Lock()
	r := m.runs[convID]
	m.mu.Unlock()
	if r == nil || r.user != user || !r.running() {
		return false
	}
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	r.cancel()
	return true
}

// status 返回会话是否有运行中的 run、是否有未过期的待确认写操作。
func (m *aiRunManager) status(convID string) (running, pending bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gcLocked()
	if r := m.runs[convID]; r != nil && r.running() {
		running = true
	}
	_, pending = m.pending[convID]
	return running, pending
}

// pendingFor 返回会话未过期的待确认写操作（无则 nil）。
func (m *aiRunManager) pendingFor(convID string) *aiPendingConfirm {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gcLocked()
	if p := m.pending[convID]; p != nil {
		cp := *p
		return &cp
	}
	return nil
}

func (m *aiRunManager) clearPending(convID string) {
	m.mu.Lock()
	delete(m.pending, convID)
	m.mu.Unlock()
}

// resetDepth 在用户主动发问时重置自动续接链计数。
func (m *aiRunManager) resetDepth(convID string) {
	m.mu.Lock()
	delete(m.depth, convID)
	m.mu.Unlock()
}

// nextDepth 自动续接链 +1；超过上限返回 false（不再续接）。
func (m *aiRunManager) nextDepth(convID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.depth[convID] >= aiMaxAutoContinue {
		return false
	}
	m.depth[convID]++
	return true
}

// forget 在会话删除时中止其 run 并清理全部内存状态。
func (m *aiRunManager) forget(convID string) {
	m.mu.Lock()
	r := m.runs[convID]
	delete(m.runs, convID)
	delete(m.pending, convID)
	delete(m.depth, convID)
	m.mu.Unlock()
	if r != nil && r.running() {
		r.mu.Lock()
		r.stopped = true
		r.mu.Unlock()
		r.cancel()
	}
}

// ── run 的 Sink：把 Agent 事件写入 run 缓冲，同时累积落库内容与结构化日志 ────

type aiRunSink struct {
	r       *aiRun
	log     *logger.Logger
	timeout time.Duration
}

func (k *aiRunSink) OnToken(text string) {
	k.r.sb.WriteString(text)
	k.r.emit("token", map[string]string{"text": text})
}

func (k *aiRunSink) OnToolCall(name, args string) {
	k.log.Infof("ai tool_call: user=%s conv=%s tool=%s args=%s", k.r.user, k.r.convID, name, args)
	k.r.calls = append(k.r.calls, map[string]any{"type": "tool_call", "tool": name, "args": args, "ts": time.Now().UnixMilli()})
	k.r.emit("tool_call", map[string]string{"name": name, "args": args})
}

func (k *aiRunSink) OnToolResult(name string, ok bool, errMsg string) {
	if ok {
		k.log.Infof("ai tool_result: user=%s tool=%s ok", k.r.user, name)
	} else {
		k.log.Infof("ai tool_result: user=%s tool=%s error=%s", k.r.user, name, errMsg)
	}
	// 把结果状态补写回最近一个同名 tool_call 元素的 result_summary（§13.3）。
	summary := "ok"
	if !ok {
		summary = "error: " + errMsg
	}
	for i := len(k.r.calls) - 1; i >= 0; i-- {
		if m, isMap := k.r.calls[i].(map[string]any); isMap && m["type"] == "tool_call" && m["tool"] == name {
			if _, has := m["result_summary"]; !has {
				m["result_summary"] = summary
				m["ok"] = ok
			}
			break
		}
	}
	k.r.emit("tool_result", map[string]any{"name": name, "ok": ok, "error": errMsg})
}

func (k *aiRunSink) OnUsage(promptTokens, completionTokens, totalTokens int64) {
	k.log.Infof("ai usage: user=%s prompt=%d completion=%d total=%d",
		k.r.user, promptTokens, completionTokens, totalTokens)
	k.r.emit("usage", map[string]int64{
		"prompt_tokens": promptTokens, "completion_tokens": completionTokens, "total_tokens": totalTokens,
	})
}

func (k *aiRunSink) OnConfirm(tool, args, summary string) {
	// 写操作提议：不执行，推给前端做二次确认（方案 A，见 §7.2）。summary 由后端可信生成。
	k.log.Infof("ai confirm: user=%s conv=%s tool=%s args=%s", k.r.user, k.r.convID, tool, args)
	k.r.calls = append(k.r.calls, map[string]any{"type": "write_confirm", "tool": tool, "args": args, "summary": summary, "ts": time.Now().UnixMilli()})
	k.r.confirm = &aiPendingConfirm{Tool: tool, Args: args, Summary: summary}
	k.r.emit("confirm", map[string]string{"tool": tool, "args": args, "summary": summary})
}

func (k *aiRunSink) OnError(err error) {
	// 用户显式停止导致的 ctx 取消不是错误，由 end 事件的 stopped 标记表达。
	if k.r.wasStopped() {
		return
	}
	msg := err.Error()
	if errors.Is(err, context.DeadlineExceeded) {
		msg = fmt.Sprintf("AI 回复超时（超过 %s），已中止", k.timeout)
	}
	k.log.Errorf("ai run error: user=%s conv=%s: %v", k.r.user, k.r.convID, err)
	k.r.emit("error", map[string]string{"message": msg})
}

func (k *aiRunSink) OnDone() { k.r.emit("done", map[string]bool{"done": true}) }

// toolCallsJSON 序列化本轮工具留痕用于落库。有工具调用时在末尾追加一条 run_meta
// （本轮开始/结束时间，毫秒），供前端折叠摘要显示「已处理 X 秒」（§14.6）。
func (r *aiRun) toolCallsJSON() string {
	if len(r.calls) == 0 {
		return "[]"
	}
	calls := append(append([]any(nil), r.calls...), map[string]any{
		"type":        "run_meta",
		"started_at":  r.started.UnixMilli(),
		"finished_at": time.Now().UnixMilli(),
	})
	b, err := json.Marshal(calls)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// streamAIRun 把 run 的事件以 SSE 推给客户端：先回放 seq > after 的已有事件，再实时跟随，
// 直到 run 结束（end 事件之后）或客户端断开。客户端断开只结束本次订阅，不影响 run。
func streamAIRun(c *gin.Context, r *aiRun, after int64) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		fail(c, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no") // 禁止 nginx 缓冲，保证流式
	c.Status(http.StatusOK)

	meta, _ := json.Marshal(map[string]any{"id": r.id, "conversation_id": r.convID, "started_at": r.started.UnixMilli()})
	fmt.Fprintf(c.Writer, "event: run\ndata: %s\n\n", meta)
	flusher.Flush()

	heartbeat := time.NewTicker(aiStreamHeartbeat)
	defer heartbeat.Stop()
	clientGone := c.Request.Context().Done()
	for {
		evs, done, wait := r.snapshot(after)
		for _, ev := range evs {
			fmt.Fprintf(c.Writer, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Name, ev.Data)
			after = ev.Seq
		}
		if len(evs) > 0 {
			flusher.Flush()
		}
		if done && len(evs) == 0 {
			return
		}
		if done {
			continue // 结束前可能还有尾部事件，再取一次确认已追平
		}
		select {
		case <-wait:
		case <-heartbeat.C:
			fmt.Fprint(c.Writer, ": ping\n\n")
			flusher.Flush()
		case <-clientGone:
			return
		}
	}
}
