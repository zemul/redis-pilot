package store

import (
	"encoding/json"
	"testing"
)

// TestAIConversationLifecycle 覆盖 §13.9 后端 DoD：
// 建会话 → 追加消息（seq 递增）→ 列出 → 取消息（归属校验，他人取 ErrNotFound）
// → 软删除 → 列表不含已删。
func TestAIConversationLifecycle(t *testing.T) {
	db := openTestDB(t)

	const alice, bob = "alice", "bob"
	conv := AIConversation{ID: "conv-1", User: alice, Title: "逐出策略是什么"}
	if err := db.CreateConversation(conv); err != nil {
		t.Fatalf("CreateConversation: %v", err)
	}

	// 追加消息：seq 应从 1 递增。
	seq1, err := db.AppendMessage(AIMessage{ID: "msg-1", ConvID: "conv-1", Role: "user", Content: "逐出策略是什么"})
	if err != nil {
		t.Fatalf("AppendMessage user: %v", err)
	}
	if seq1 != 1 {
		t.Errorf("first seq = %d, want 1", seq1)
	}
	seq2, err := db.AppendMessage(AIMessage{ID: "msg-2", ConvID: "conv-1", Role: "assistant", Content: "当前是 noeviction"})
	if err != nil {
		t.Fatalf("AppendMessage assistant: %v", err)
	}
	if seq2 != 2 {
		t.Errorf("second seq = %d, want 2", seq2)
	}

	// 列出：alice 有 1 条。
	list, err := db.ListConversations(alice)
	if err != nil {
		t.Fatalf("ListConversations: %v", err)
	}
	if len(list) != 1 || list[0].ID != "conv-1" {
		t.Fatalf("ListConversations = %+v, want 1 conv-1", list)
	}

	// 取消息：本人可取，消息按 seq 升序。
	gotConv, msgs, err := db.GetConversationMessages("conv-1", alice)
	if err != nil {
		t.Fatalf("GetConversationMessages(alice): %v", err)
	}
	if gotConv.ID != "conv-1" || len(msgs) != 2 {
		t.Fatalf("got conv=%s msgs=%d, want conv-1 / 2", gotConv.ID, len(msgs))
	}
	if msgs[0].Seq != 1 || msgs[1].Seq != 2 {
		t.Errorf("seq order = [%d,%d], want [1,2]", msgs[0].Seq, msgs[1].Seq)
	}

	// 归属校验：bob 取 alice 的会话 → ErrNotFound。
	if _, _, err := db.GetConversationMessages("conv-1", bob); err != ErrNotFound {
		t.Errorf("GetConversationMessages(bob) err = %v, want ErrNotFound", err)
	}

	// bob 软删除 alice 的会话 → ErrNotFound（不越权）。
	if err := db.SoftDeleteConversation("conv-1", bob); err != ErrNotFound {
		t.Errorf("SoftDeleteConversation(bob) err = %v, want ErrNotFound", err)
	}

	// alice 软删除 → 成功；列表不再包含。
	if err := db.SoftDeleteConversation("conv-1", alice); err != nil {
		t.Fatalf("SoftDeleteConversation(alice): %v", err)
	}
	list, err = db.ListConversations(alice)
	if err != nil {
		t.Fatalf("ListConversations after delete: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("ListConversations after delete = %d, want 0", len(list))
	}
	// 已删会话取消息也应 ErrNotFound。
	if _, _, err := db.GetConversationMessages("conv-1", alice); err != ErrNotFound {
		t.Errorf("GetConversationMessages after delete err = %v, want ErrNotFound", err)
	}
}

// TestAppendToolCallToLatestAssistant 覆盖 execute 落库：向最近 assistant 消息
// tool_calls 追加 write_execute 元素；找不到 assistant 消息时新建承载。
func TestAppendToolCallToLatestAssistant(t *testing.T) {
	db := openTestDB(t)
	const user = "alice"

	if err := db.CreateConversation(AIConversation{ID: "conv-1", User: user, Title: "t"}); err != nil {
		t.Fatalf("CreateConversation: %v", err)
	}
	if _, err := db.AppendMessage(AIMessage{ID: "m1", ConvID: "conv-1", Role: "user", Content: "备份 test-1"}); err != nil {
		t.Fatalf("AppendMessage user: %v", err)
	}
	if _, err := db.AppendMessage(AIMessage{ID: "m2", ConvID: "conv-1", Role: "assistant", Content: "好的", ToolCalls: `[{"type":"write_confirm","tool":"backup_exec"}]`}); err != nil {
		t.Fatalf("AppendMessage assistant: %v", err)
	}

	elem := map[string]any{"type": "write_execute", "tool": "backup_exec", "task_id": "test-1-178", "result": "accepted"}
	if err := db.AppendToolCallToLatestAssistant("conv-1", user, elem); err != nil {
		t.Fatalf("AppendToolCallToLatestAssistant: %v", err)
	}

	_, msgs, err := db.GetConversationMessages("conv-1", user)
	if err != nil {
		t.Fatalf("GetConversationMessages: %v", err)
	}
	// 找最近 assistant 消息，其 tool_calls 应含 2 个元素且末尾是 write_execute。
	var assistant *AIMessage
	for i := range msgs {
		if msgs[i].Role == "assistant" {
			assistant = &msgs[i]
		}
	}
	if assistant == nil {
		t.Fatal("no assistant message")
	}
	var calls []map[string]any
	if err := json.Unmarshal([]byte(assistant.ToolCalls), &calls); err != nil {
		t.Fatalf("unmarshal tool_calls: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("tool_calls len = %d, want 2", len(calls))
	}
	last := calls[len(calls)-1]
	if last["type"] != "write_execute" || last["task_id"] != "test-1-178" {
		t.Errorf("last element = %+v, want write_execute/test-1-178", last)
	}

	// 归属校验：他人追加 → ErrNotFound。
	if err := db.AppendToolCallToLatestAssistant("conv-1", "bob", elem); err != ErrNotFound {
		t.Errorf("AppendToolCallToLatestAssistant(bob) = %v, want ErrNotFound", err)
	}

	// 无 assistant 消息时新建承载。
	if err := db.CreateConversation(AIConversation{ID: "conv-2", User: user, Title: "t2"}); err != nil {
		t.Fatalf("CreateConversation conv-2: %v", err)
	}
	if err := db.AppendToolCallToLatestAssistant("conv-2", user, elem); err != nil {
		t.Fatalf("AppendToolCallToLatestAssistant conv-2: %v", err)
	}
	_, msgs2, err := db.GetConversationMessages("conv-2", user)
	if err != nil {
		t.Fatalf("GetConversationMessages conv-2: %v", err)
	}
	if len(msgs2) != 1 || msgs2[0].Role != "assistant" {
		t.Fatalf("conv-2 msgs = %+v, want 1 assistant carrier", msgs2)
	}
}
