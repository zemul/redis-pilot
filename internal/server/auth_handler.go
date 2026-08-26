package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// ─── JWT helpers (HS256, minimal, no third-party dep) ───────────────────────

type jwtClaims struct {
	Username string `json:"sub"`
	Role     string `json:"role"`
	Exp      int64  `json:"exp"`
	Iat      int64  `json:"iat"`
}

func (s *Server) jwtSecret() []byte {
	if s.cfg.JWTSecret != "" {
		return []byte(s.cfg.JWTSecret)
	}
	return []byte("redis-pilot-default-jwt-secret")
}

func (s *Server) jwtSign(username, role string) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := jwtClaims{
		Username: username,
		Role:     role,
		Exp:      time.Now().Add(30 * 24 * time.Hour).Unix(),
		Iat:      time.Now().Unix(),
	}
	claimsJSON, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	msg := header + "." + payload
	mac := hmac.New(sha256.New, s.jwtSecret())
	mac.Write([]byte(msg))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return msg + "." + sig, nil
}

func (s *Server) jwtVerify(tokenStr string) (*jwtClaims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid token format")
	}
	msg := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid signature encoding")
	}
	mac := hmac.New(sha256.New, s.jwtSecret())
	mac.Write([]byte(msg))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return nil, fmt.Errorf("signature mismatch")
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid payload encoding")
	}
	var claims jwtClaims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return nil, fmt.Errorf("invalid claims")
	}
	if time.Now().Unix() > claims.Exp {
		return nil, fmt.Errorf("token expired")
	}
	return &claims, nil
}

// ─── Unified Auth Middleware ────────────────────────────────────────────────
// 接受 JWT 或 API Token，解析到用户名后存入 context。

func (s *Server) unifiedAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		auth := c.GetHeader("Authorization")
		token := strings.TrimPrefix(auth, "Bearer ")
		if token == "" || token == auth {
			token = c.Query("token")
		}
		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, apitypes.APIResponse{Error: "login required"})
			return
		}

		// 尝试 JWT
		if claims, err := s.jwtVerify(token); err == nil {
			c.Set("auth_user", claims.Username)
			c.Set("auth_role", claims.Role)
			c.Next()
			return
		}

		// 尝试 API Token
		username, err := s.db.TokenLookup(token)
		if err != nil {
			s.log.Errorf("auth token lookup error: %v", err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, apitypes.APIResponse{Error: "internal error"})
			return
		}
		if username != "" {
			// 查询用户角色
			user, err := s.db.UserGet(username)
			if err != nil || user == nil {
				c.AbortWithStatusJSON(http.StatusUnauthorized, apitypes.APIResponse{Error: "user not found"})
				return
			}
			c.Set("auth_user", username)
			c.Set("auth_role", user.Role)
			c.Next()
			return
		}

		c.AbortWithStatusJSON(http.StatusUnauthorized, apitypes.APIResponse{Error: "invalid or expired token"})
	}
}

// ─── RBAC Middleware ─────────────────────────────────────────────────────────
// adminOnly 拒绝非 admin 用户对写操作的访问。
// viewer 角色仅允许 GET 请求（只读），其他方法返回 403。

func (s *Server) adminOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		role := c.GetString("auth_role")
		if role == "admin" {
			c.Next()
			return
		}
		// viewer: 只允许 GET
		if c.Request.Method == http.MethodGet {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusForbidden, apitypes.APIResponse{Error: "admin role required"})
	}
}

// ─── Auth Handlers ──────────────────────────────────────────────────────────

type loginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func (s *Server) authLogin(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "username and password required")
		return
	}
	user, err := s.db.UserGet(req.Username)
	if err != nil {
		s.log.Errorf("auth login db error: %v", err)
		fail(c, http.StatusInternalServerError, "internal error")
		return
	}
	if user == nil {
		fail(c, http.StatusUnauthorized, "invalid username or password")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		fail(c, http.StatusUnauthorized, "invalid username or password")
		return
	}
	token, err := s.jwtSign(user.Username, user.Role)
	if err != nil {
		fail(c, http.StatusInternalServerError, "failed to generate token")
		return
	}
	ok(c, gin.H{"token": token, "username": user.Username, "role": user.Role})
}

func (s *Server) authMe(c *gin.Context) {
	username := c.GetString("auth_user")
	role := c.GetString("auth_role")
	ok(c, gin.H{"username": username, "role": role})
}

// ─── User Management ────────────────────────────────────────────────────────

type createUserRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
	Role     string `json:"role"` // admin | viewer，默认 admin
}

func (s *Server) authUserCreate(c *gin.Context) {
	var req createUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "username and password required")
		return
	}
	if len(req.Username) < 2 || len(req.Username) > 32 {
		fail(c, http.StatusBadRequest, "username must be 2-32 characters")
		return
	}
	if len(req.Password) < 4 {
		fail(c, http.StatusBadRequest, "password must be at least 4 characters")
		return
	}
	// 校验角色值
	if req.Role == "" {
		req.Role = "admin"
	}
	if req.Role != "admin" && req.Role != "viewer" {
		fail(c, http.StatusBadRequest, "role must be admin or viewer")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		fail(c, http.StatusInternalServerError, "failed to hash password")
		return
	}
	createdBy := c.GetString("auth_user")
	if err := s.db.UserCreate(req.Username, string(hash), req.Role, createdBy); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			fail(c, http.StatusConflict, "username already exists")
			return
		}
		s.log.Errorf("auth user create: %v", err)
		fail(c, http.StatusInternalServerError, "failed to create user")
		return
	}
	s.audit.Log(store.AuditRecord{Operator: createdBy, Action: "user.create", Target: map[string]interface{}{"username": req.Username, "role": req.Role}})
	ok(c, gin.H{"username": req.Username, "role": req.Role})
}

func (s *Server) authUserList(c *gin.Context) {
	users, err := s.db.UserList()
	if err != nil {
		s.log.Errorf("auth user list: %v", err)
		fail(c, http.StatusInternalServerError, "failed to list users")
		return
	}
	ok(c, users)
}

type deleteUserRequest struct {
	Username string `json:"username" binding:"required"`
}

func (s *Server) authUserDelete(c *gin.Context) {
	var req deleteUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "username required")
		return
	}
	currentUser := c.GetString("auth_user")
	if req.Username == currentUser {
		fail(c, http.StatusBadRequest, "cannot delete yourself")
		return
	}
	// 删除用户的同时删除其所有 token
	if err := s.db.TokenDeleteAllByUser(req.Username); err != nil {
		s.log.Errorf("auth user delete tokens: %v", err)
	}
	if err := s.db.UserDelete(req.Username); err != nil {
		s.log.Errorf("auth user delete: %v", err)
		fail(c, http.StatusInternalServerError, "failed to delete user")
		return
	}
	s.audit.Log(store.AuditRecord{Operator: currentUser, Action: "user.delete", Target: map[string]interface{}{"username": req.Username}})
	ok(c, gin.H{"deleted": req.Username})
}

type changePasswordRequest struct {
	OldPassword string `json:"old_password" binding:"required"`
	NewPassword string `json:"new_password" binding:"required"`
}

func (s *Server) authChangePassword(c *gin.Context) {
	var req changePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "old_password and new_password required")
		return
	}
	if len(req.NewPassword) < 4 {
		fail(c, http.StatusBadRequest, "new password must be at least 4 characters")
		return
	}
	username := c.GetString("auth_user")
	user, err := s.db.UserGet(username)
	if err != nil || user == nil {
		fail(c, http.StatusInternalServerError, "user not found")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.OldPassword)); err != nil {
		fail(c, http.StatusUnauthorized, "old password incorrect")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		fail(c, http.StatusInternalServerError, "failed to hash password")
		return
	}
	if err := s.db.UserUpdatePassword(username, string(hash)); err != nil {
		fail(c, http.StatusInternalServerError, "failed to update password")
		return
	}
	ok(c, gin.H{"message": "password changed"})
}

// ─── API Token Management ───────────────────────────────────────────────────

type createTokenRequest struct {
	Name        string `json:"name"`        // 名称
	Description string `json:"description"` // 备注
	ExpiresIn   string `json:"expires_in"`  // 有效期：7d/30d/90d/365d/never（默认 never）
}

func (s *Server) authTokenCreate(c *gin.Context) {
	var req createTokenRequest
	c.ShouldBindJSON(&req)
	if req.Name == "" {
		req.Name = "default"
	}
	username := c.GetString("auth_user")

	// 计算过期时间
	var expiresAt string
	switch req.ExpiresIn {
	case "7d":
		expiresAt = time.Now().Add(7 * 24 * time.Hour).Format(time.RFC3339)
	case "30d":
		expiresAt = time.Now().Add(30 * 24 * time.Hour).Format(time.RFC3339)
	case "90d":
		expiresAt = time.Now().Add(90 * 24 * time.Hour).Format(time.RFC3339)
	case "365d":
		expiresAt = time.Now().Add(365 * 24 * time.Hour).Format(time.RFC3339)
	default:
		expiresAt = "" // never
	}

	// 生成 32 字节随机 token，rpt_ 前缀便于识别
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		fail(c, http.StatusInternalServerError, "failed to generate token")
		return
	}
	token := "rpt_" + hex.EncodeToString(b)

	if err := s.db.TokenCreate(token, username, req.Name, req.Description, expiresAt); err != nil {
		s.log.Errorf("auth token create: %v", err)
		fail(c, http.StatusInternalServerError, "failed to create token")
		return
	}
	ok(c, gin.H{"token": token, "name": req.Name, "expires_at": expiresAt})
}

func (s *Server) authTokenList(c *gin.Context) {
	username := c.GetString("auth_user")
	tokens, err := s.db.TokenListByUser(username)
	if err != nil {
		s.log.Errorf("auth token list: %v", err)
		fail(c, http.StatusInternalServerError, "failed to list tokens")
		return
	}
	ok(c, tokens)
}

type deleteTokenRequest struct {
	ID int64 `json:"id" binding:"required"`
}

func (s *Server) authTokenDelete(c *gin.Context) {
	var req deleteTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		// 也尝试从 query 参数获取
		idStr := c.Query("id")
		if idStr == "" {
			fail(c, http.StatusBadRequest, "token id required")
			return
		}
		id, _ := strconv.ParseInt(idStr, 10, 64)
		req.ID = id
	}
	username := c.GetString("auth_user")
	if err := s.db.TokenDelete(req.ID, username); err != nil {
		s.log.Errorf("auth token delete: %v", err)
		fail(c, http.StatusInternalServerError, "failed to delete token")
		return
	}
	ok(c, gin.H{"deleted": req.ID})
}

// ─── Init Admin ─────────────────────────────────────────────────────────────

// InitAdmin 首次启动时创建初始 admin 账号（幂等：users 表非空则跳过）。
func (s *Server) InitAdmin() error {
	count, err := s.db.UserCount()
	if err != nil {
		return fmt.Errorf("init admin: count users: %w", err)
	}
	if count > 0 {
		return nil
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(s.cfg.Admin.Password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("init admin: hash password: %w", err)
	}
	if err := s.db.UserCreate(s.cfg.Admin.Username, string(hash), "admin", "system"); err != nil {
		return fmt.Errorf("init admin: create user: %w", err)
	}
	s.log.Infof("created initial admin user: %s", s.cfg.Admin.Username)
	return nil
}
