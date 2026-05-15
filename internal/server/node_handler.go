package server

import (
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
	ok(c, state)
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

	node, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, exists := node.Servers[req.Name]; exists {
		fail(c, http.StatusConflict, "server already exists: "+req.Name)
		return
	}
	node.Servers[req.Name] = req.Server
	if err := s.state.WriteNode(node); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (s *Server) nodeRemove(c *gin.Context) {
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	node, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, exists := node.Servers[req.Name]; !exists {
		fail(c, http.StatusNotFound, "server not found: "+req.Name)
		return
	}
	delete(node.Servers, req.Name)
	if err := s.state.WriteNode(node); err != nil {
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

	node, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, exists := node.Servers[req.Name]; !exists {
		fail(c, http.StatusNotFound, "server not found: "+req.Name)
		return
	}
	node.Servers[req.Name] = req.Server
	if err := s.state.WriteNode(node); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}
