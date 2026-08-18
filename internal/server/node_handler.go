package server

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) nodeQuery(c *gin.Context) {
	state, err := s.db.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	instances, _ := s.db.ReadInstances()
	if instances != nil {
		for name, srv := range state.Servers {
			memGi, cpus, diskGi := computeAllocated(instances, name)
			srv.Allocated.Memory = fmt.Sprintf("%dGi", memGi)
			srv.Allocated.CPUCores = cpus
			srv.Allocated.Disk = fmt.Sprintf("%dGi", diskGi)
		}
	}

	// 剥离 AgentToken 等敏感凭证后再返回，避免通过只读查询接口泄露 Agent Token。
	views := make(map[string]*apitypes.NodeServerView, len(state.Servers))
	for name, srv := range state.Servers {
		views[name] = srv.ToView()
	}
	ok(c, gin.H{"servers": views})
}

func (s *Server) nodeAdd(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name   string               `json:"name" binding:"required"`
		Server *apitypes.NodeServer `json:"server" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	// 用 WithNode 而非 ReadNode+WriteNode：后者的"检查是否已存在"和"写入"之间没有互斥，
	// 两个并发的 node add 同名请求会双双通过存在性检查，后写的静默覆盖先写的，
	// 冲突检查形同虚设。WithNode 在数据库互斥锁内完成读-改-写。
	var conflict bool
	if err := s.db.WithNode(func(node *apitypes.NodeState) error {
		if _, exists := node.Servers[req.Name]; exists {
			conflict = true
			return errNodeConflict
		}
		node.Servers[req.Name] = req.Server
		return nil
	}); err != nil {
		if conflict {
			s.audit.Log(store.AuditRecord{
				Operator: operatorFrom(c), Action: "node.add", Level: audit.LevelImportant, Result: "failed",
				DurationMs: time.Since(start).Milliseconds(),
				Target:     map[string]interface{}{"node": req.Name},
				Detail:     "server already exists",
			})
			fail(c, http.StatusConflict, "server already exists: "+req.Name)
			return
		}
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c), Action: "node.add", Level: audit.LevelImportant, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:     map[string]interface{}{"node": req.Name},
	})
	ok(c, nil)
}

// errNodeConflict / errNodeNotFound 只用于把 WithNode 回调里的判定结果带出来，
// 让 HTTP 状态码能区分冲突、不存在和真正的存储错误。不会返回给调用方。
var (
	errNodeConflict = errors.New("node already exists")
	errNodeNotFound = errors.New("node not found")
)

func (s *Server) nodeRemove(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	// 存在性检查、实例引用检查和删除必须是一个事务。若分成 ReadInstances + WithNode，
	// instance create 可以在两步之间写入一条指向即将被删节点的记录，制造孤儿实例。
	found, used, err := s.db.RemoveServerIfUnused(req.Name)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		fail(c, http.StatusNotFound, "server not found: "+req.Name)
		return
	}
	if used {
		fail(c, http.StatusConflict, "server still has managed instances: "+req.Name)
		return
	}
	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c), Action: "node.remove", Level: audit.LevelImportant, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:     map[string]interface{}{"node": req.Name},
	})
	ok(c, nil)
}

func (s *Server) nodeUpdate(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name   string               `json:"name" binding:"required"`
		Server *apitypes.NodeServer `json:"server" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	// 同 nodeAdd 用 WithNode：避免 update 用一份过期快照把已被删除的节点写回来。
	var notFound bool
	if err := s.db.WithNode(func(node *apitypes.NodeState) error {
		if _, exists := node.Servers[req.Name]; !exists {
			notFound = true
			return errNodeNotFound
		}
		node.Servers[req.Name] = req.Server
		return nil
	}); err != nil {
		if notFound {
			fail(c, http.StatusNotFound, "server not found: "+req.Name)
			return
		}
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c), Action: "node.update", Level: audit.LevelNormal, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:     map[string]interface{}{"node": req.Name},
	})
	ok(c, nil)
}
