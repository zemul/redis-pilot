package server

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) nodeQuery(c *gin.Context) {
	state, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	instances, _ := s.state.ReadInstances()
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
	if err := s.state.WithNode(func(node *apitypes.NodeState) error {
		if _, exists := node.Servers[req.Name]; exists {
			conflict = true
			return errNodeConflict
		}
		node.Servers[req.Name] = req.Server
		return nil
	}); err != nil {
		if conflict {
			fail(c, http.StatusConflict, "server already exists: "+req.Name)
			return
		}
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

// errNodeConflict / errNodeNotFound 只用于把 WithNode 回调里的判定结果带出来，
// 让 HTTP 状态码能区分冲突、不存在和真正的存储错误。不会返回给调用方。
var (
	errNodeConflict = errors.New("node already exists")
	errNodeNotFound = errors.New("node not found")
)

func (s *Server) nodeRemove(c *gin.Context) {
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	// 同 nodeAdd 用 WithNode。这里的竞态后果更实际：并发的 node remove 与 node update
	// 若各自读到同一份快照，remove 先写、update 后写，被删掉的节点会被 update 重新写回来，
	// 表现为"删除成功但节点还在"。
	var notFound bool
	if err := s.state.WithNode(func(node *apitypes.NodeState) error {
		if _, exists := node.Servers[req.Name]; !exists {
			notFound = true
			return errNodeNotFound
		}
		delete(node.Servers, req.Name)
		return nil
	}); err != nil {
		if notFound {
			fail(c, http.StatusNotFound, "server not found: "+req.Name)
			return
		}
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (s *Server) nodeUpdate(c *gin.Context) {
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
	if err := s.state.WithNode(func(node *apitypes.NodeState) error {
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
	ok(c, nil)
}
