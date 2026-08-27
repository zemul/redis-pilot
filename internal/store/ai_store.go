package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound 表示请求的资源不存在，或调用者无权访问（归属校验失败）。
// AI 会话查询/删除对"非本人"一律返回该错误，handler 据此回 404（不回 403，
// 避免探测他人会话是否存在，见 §13.5.2-e）。
var ErrNotFound = errors.New("store: not found")

// ── AI 助手会话与聊天记录结构（§13） ─────────────────────────────────────

// AIConversation 一次 AI 助手会话（严格私有，按 user 归属）。
type AIConversation struct {
	ID        string `json:"id"`
	User      string `json:"user"`
	Title     string `json:"title"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// AIMessage 会话内一条消息（user 提问或 assistant 回复）。
type AIMessage struct {
	ID          string `json:"id"`
	ConvID      string `json:"conv_id"`
	Seq         int    `json:"seq"`
	Role        string `json:"role"`       // user / assistant
	Content     string `json:"content"`
	ToolCalls   string `json:"tool_calls"` // 原样 JSON 字符串，前端自行解析
	Interrupted bool   `json:"interrupted"`
	CreatedAt   string `json:"created_at"`
}

// CreateConversation 创建会话（deleted=0，created_at=updated_at=now）。
func (d *DB) CreateConversation(c AIConversation) error {
	now := time.Now().Format(time.RFC3339)
	if c.CreatedAt == "" {
		c.CreatedAt = now
	}
	if c.UpdatedAt == "" {
		c.UpdatedAt = now
	}
	_, err := d.sql.Exec(`
		INSERT INTO ai_conversations(id, user, title, created_at, updated_at, deleted)
		VALUES(?,?,?,?,?,0)`,
		c.ID, c.User, c.Title, c.CreatedAt, c.UpdatedAt,
	)
	return err
}

// AppendMessage 追加一条消息。seq 在同一事务内取 max(seq)+1，避免并发错乱；
// 同时刷新所属会话的 updated_at。返回分配到的 seq。
func (d *DB) AppendMessage(m AIMessage) (int, error) {
	if m.ToolCalls == "" {
		m.ToolCalls = "[]"
	}
	if m.CreatedAt == "" {
		m.CreatedAt = time.Now().Format(time.RFC3339)
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	var seq int
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(seq),0)+1 FROM ai_messages WHERE conv_id=?`, m.ConvID,
	).Scan(&seq); err != nil {
		return 0, err
	}

	interrupted := 0
	if m.Interrupted {
		interrupted = 1
	}
	if _, err := tx.Exec(`
		INSERT INTO ai_messages(id, conv_id, seq, role, content, tool_calls, interrupted, created_at)
		VALUES(?,?,?,?,?,?,?,?)`,
		m.ID, m.ConvID, seq, m.Role, m.Content, m.ToolCalls, interrupted, m.CreatedAt,
	); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(
		`UPDATE ai_conversations SET updated_at=? WHERE id=?`, m.CreatedAt, m.ConvID,
	); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return seq, nil
}

// ListConversations 列出某用户未删除的会话，按 updated_at 倒序。
func (d *DB) ListConversations(user string) ([]AIConversation, error) {
	rows, err := d.sql.Query(`
		SELECT id, user, title, created_at, updated_at
		FROM ai_conversations
		WHERE user=? AND deleted=0
		ORDER BY updated_at DESC`, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []AIConversation{}
	for rows.Next() {
		var c AIConversation
		if err := rows.Scan(&c.ID, &c.User, &c.Title, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// getConversationOwned 读会话并校验归属；非本人或不存在（含已删除）返回 ErrNotFound。
func (d *DB) getConversationOwned(id, user string) (AIConversation, error) {
	var c AIConversation
	err := d.sql.QueryRow(`
		SELECT id, user, title, created_at, updated_at
		FROM ai_conversations
		WHERE id=? AND deleted=0`, id).
		Scan(&c.ID, &c.User, &c.Title, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return AIConversation{}, ErrNotFound
	}
	if err != nil {
		return AIConversation{}, err
	}
	if c.User != user {
		return AIConversation{}, ErrNotFound
	}
	return c, nil
}

// GetConversationMessages 取某会话全部消息（seq 升序）。
// 校验归属：conv.user != user（或不存在）返回 ErrNotFound。
func (d *DB) GetConversationMessages(id, user string) (AIConversation, []AIMessage, error) {
	conv, err := d.getConversationOwned(id, user)
	if err != nil {
		return AIConversation{}, nil, err
	}

	rows, err := d.sql.Query(`
		SELECT id, conv_id, seq, role, content, tool_calls, interrupted, created_at
		FROM ai_messages
		WHERE conv_id=?
		ORDER BY seq ASC`, id)
	if err != nil {
		return AIConversation{}, nil, err
	}
	defer rows.Close()

	msgs := []AIMessage{}
	for rows.Next() {
		var m AIMessage
		var interrupted int
		if err := rows.Scan(&m.ID, &m.ConvID, &m.Seq, &m.Role, &m.Content, &m.ToolCalls, &interrupted, &m.CreatedAt); err != nil {
			return AIConversation{}, nil, err
		}
		m.Interrupted = interrupted != 0
		msgs = append(msgs, m)
	}
	return conv, msgs, rows.Err()
}

// SoftDeleteConversation 软删除会话（deleted=1）。校验归属，非本人返回 ErrNotFound。
func (d *DB) SoftDeleteConversation(id, user string) error {
	if _, err := d.getConversationOwned(id, user); err != nil {
		return err
	}
	_, err := d.sql.Exec(
		`UPDATE ai_conversations SET deleted=1, updated_at=? WHERE id=?`,
		time.Now().Format(time.RFC3339), id,
	)
	return err
}

// AppendToolCallToLatestAssistant 给会话最近一条 assistant 消息的 tool_calls
// 追加一个元素（execute 落库用）。读出该行 tool_calls JSON → append → 写回；
// 找不到 assistant 消息则新建一条 role=assistant、空 content 的消息承载。
// 校验归属，非本人（或会话不存在）返回 ErrNotFound。
func (d *DB) AppendToolCallToLatestAssistant(convID, user string, element any) error {
	if _, err := d.getConversationOwned(convID, user); err != nil {
		return err
	}

	var (
		msgID   string
		rawJSON string
	)
	err := d.sql.QueryRow(`
		SELECT id, tool_calls FROM ai_messages
		WHERE conv_id=? AND role='assistant'
		ORDER BY seq DESC LIMIT 1`, convID).Scan(&msgID, &rawJSON)

	if err == sql.ErrNoRows {
		// 没有 assistant 消息：新建一条空 content 的承载。
		calls := []any{element}
		callsJSON, mErr := json.Marshal(calls)
		if mErr != nil {
			return mErr
		}
		_, aErr := d.AppendMessage(AIMessage{
			ID:        fmt.Sprintf("msg-%d", time.Now().UnixNano()),
			ConvID:    convID,
			Role:      "assistant",
			Content:   "",
			ToolCalls: string(callsJSON),
		})
		return aErr
	}
	if err != nil {
		return err
	}

	var calls []any
	if rawJSON == "" {
		rawJSON = "[]"
	}
	if uErr := json.Unmarshal([]byte(rawJSON), &calls); uErr != nil {
		// tool_calls 损坏时不阻断，从空数组重建。
		calls = []any{}
	}
	calls = append(calls, element)
	callsJSON, mErr := json.Marshal(calls)
	if mErr != nil {
		return mErr
	}
	_, err = d.sql.Exec(`UPDATE ai_messages SET tool_calls=? WHERE id=?`, string(callsJSON), msgID)
	return err
}
