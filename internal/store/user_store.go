package store

import (
	"database/sql"
	"fmt"
	"time"
)

// User 用户记录。
type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Password  string `json:"-"` // bcrypt hash, never serialize
	CreatedAt string `json:"created_at"`
	CreatedBy string `json:"created_by"`
}

// UserCreate 创建用户，password 应已经是 bcrypt hash。
func (d *DB) UserCreate(username, passwordHash, createdBy string) error {
	now := time.Now().Format(time.RFC3339)
	return d.execWrite(
		`INSERT INTO users (username, password, created_at, created_by) VALUES (?, ?, ?, ?)`,
		username, passwordHash, now, createdBy,
	)
}

// UserGet 按用户名查询。
func (d *DB) UserGet(username string) (*User, error) {
	row := d.sql.QueryRow(`SELECT id, username, password, created_at, created_by FROM users WHERE username = ?`, username)
	u := &User{}
	if err := row.Scan(&u.ID, &u.Username, &u.Password, &u.CreatedAt, &u.CreatedBy); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("store: user get: %w", err)
	}
	return u, nil
}

// UserList 返回所有用户（不含密码 hash）。
func (d *DB) UserList() ([]User, error) {
	rows, err := d.sql.Query(`SELECT id, username, created_at, created_by FROM users ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("store: user list: %w", err)
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username, &u.CreatedAt, &u.CreatedBy); err != nil {
			return nil, fmt.Errorf("store: user list scan: %w", err)
		}
		users = append(users, u)
	}
	return users, nil
}

// UserDelete 删除用户。
func (d *DB) UserDelete(username string) error {
	return d.execWrite(`DELETE FROM users WHERE username = ?`, username)
}

// UserCount 返回用户总数。
func (d *DB) UserCount() (int, error) {
	var count int
	if err := d.sql.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return 0, fmt.Errorf("store: user count: %w", err)
	}
	return count, nil
}

// UserUpdatePassword 更新密码。
func (d *DB) UserUpdatePassword(username, newPasswordHash string) error {
	return d.execWrite(`UPDATE users SET password = ? WHERE username = ?`, newPasswordHash, username)
}

// ── User API Tokens ─────────────────────────────────────────────────────────

// UserToken API Token 记录。
type UserToken struct {
	ID          int64  `json:"id"`
	Token       string `json:"token,omitempty"` // 仅创建时返回明文
	Username    string `json:"username"`
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
	ExpiresAt   string `json:"expires_at"` // 空字符串表示永不过期
}

// TokenCreate 创建 API Token。expiresAt 为空表示永不过期。
func (d *DB) TokenCreate(token, username, name, description, expiresAt string) error {
	now := time.Now().Format(time.RFC3339)
	return d.execWrite(
		`INSERT INTO user_tokens (token, username, name, description, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)`,
		token, username, name, description, now, expiresAt,
	)
}

// TokenLookup 按 token 值查找，返回关联的用户名。过期的 token 返回空。
func (d *DB) TokenLookup(token string) (string, error) {
	var username, expiresAt string
	err := d.sql.QueryRow(`SELECT username, expires_at FROM user_tokens WHERE token = ?`, token).Scan(&username, &expiresAt)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: token lookup: %w", err)
	}
	if expiresAt != "" {
		exp, err := time.Parse(time.RFC3339, expiresAt)
		if err == nil && time.Now().After(exp) {
			return "", nil // expired
		}
	}
	return username, nil
}

// TokenListByUser 列出某用户的所有 token。
func (d *DB) TokenListByUser(username string) ([]UserToken, error) {
	rows, err := d.sql.Query(
		`SELECT id, token, username, name, description, created_at, expires_at FROM user_tokens WHERE username = ? ORDER BY id`,
		username,
	)
	if err != nil {
		return nil, fmt.Errorf("store: token list: %w", err)
	}
	defer rows.Close()
	var tokens []UserToken
	for rows.Next() {
		var t UserToken
		if err := rows.Scan(&t.ID, &t.Token, &t.Username, &t.Name, &t.Description, &t.CreatedAt, &t.ExpiresAt); err != nil {
			return nil, fmt.Errorf("store: token list scan: %w", err)
		}
		tokens = append(tokens, t)
	}
	return tokens, nil
}

// TokenDelete 按 ID 删除 token（需属于指定用户）。
func (d *DB) TokenDelete(id int64, username string) error {
	return d.execWrite(`DELETE FROM user_tokens WHERE id = ? AND username = ?`, id, username)
}

// TokenDeleteAllByUser 删除某用户所有 token（用户被删除时调用）。
func (d *DB) TokenDeleteAllByUser(username string) error {
	return d.execWrite(`DELETE FROM user_tokens WHERE username = ?`, username)
}
