package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 受限 K/V 浏览器 HTTP handler。这些端点挂在 /instance 组下，而该组的 adminOnly()
// 中间件会放行 viewer 的 GET —— 但本功能对 viewer 完全不开放，故每个 handler 内再
// 硬校验 admin（含读 GET）。真正的能力在 key_service.go（单一能力源）。

// requireAdminRole 在 handler 内强制 admin（比组级 adminOnly 更严：GET 也拒绝 viewer）。
func requireAdminRole(c *gin.Context) bool {
	if c.GetString("auth_role") != "admin" {
		fail(c, http.StatusForbidden, "admin role required")
		return false
	}
	return true
}

// keyFail 把 service 层错误映射到 HTTP 状态码。
func keyFail(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errInstanceNotFound):
		fail(c, http.StatusNotFound, err.Error())
	case errors.Is(err, errServerNotFound):
		fail(c, http.StatusBadRequest, err.Error())
	default:
		fail(c, http.StatusBadGateway, err.Error())
	}
}

func (s *Server) instanceKeyGet(c *gin.Context) {
	if !requireAdminRole(c) {
		return
	}
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	key := c.Query("key")
	if key == "" {
		fail(c, http.StatusBadRequest, "key is required")
		return
	}
	meta := c.Query("meta") == "true"
	cursor := c.DefaultQuery("cursor", "0")
	count, _ := strconv.Atoi(c.Query("count"))
	offset, _ := strconv.Atoi(c.Query("offset"))
	data, err := s.keyGet(name, key, cursor, offset, count, meta)
	if err != nil {
		keyFail(c, err)
		return
	}
	ok(c, data)
}

func (s *Server) instanceKeyWrite(c *gin.Context) {
	if !requireAdminRole(c) {
		return
	}
	start := time.Now()
	var req apitypes.KeyWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	data, err := s.keyWrite(req.Name, req.Key, req.Op, req.Args)
	result := "success"
	if err != nil {
		result = "failed"
	}
	_ = s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c), Source: "manual", Trigger: "manual",
		Action: "key.write", Level: audit.LevelImportant, Result: result,
		DurationMs: time.Since(start).Milliseconds(),
		Target:     map[string]interface{}{"instance": req.Name, "key": req.Key, "op": req.Op},
	})
	if err != nil {
		keyFail(c, err)
		return
	}
	ok(c, data)
}

func (s *Server) instanceKeyDelete(c *gin.Context) {
	if !requireAdminRole(c) {
		return
	}
	start := time.Now()
	var req apitypes.KeyDeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	data, err := s.keyDelete(req.Name, req.Key)
	result := "success"
	if err != nil {
		result = "failed"
	}
	_ = s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c), Source: "manual", Trigger: "manual",
		Action: "key.delete", Level: audit.LevelCritical, Result: result,
		DurationMs: time.Since(start).Milliseconds(),
		Target:     map[string]interface{}{"instance": req.Name, "key": req.Key},
	})
	if err != nil {
		keyFail(c, err)
		return
	}
	ok(c, data)
}
