package server

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) poolQuery(c *gin.Context) {
	state, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, state)
}

func (s *Server) poolAdd(c *gin.Context) {
	var req struct {
		Name   string                  `json:"name" binding:"required"`
		Server *apitypes.PoolServer    `json:"server" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, exists := pool.Servers[req.Name]; exists {
		fail(c, http.StatusConflict, "server already exists: "+req.Name)
		return
	}
	pool.Servers[req.Name] = req.Server
	if err := s.state.WritePool(pool); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (s *Server) poolRemove(c *gin.Context) {
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, exists := pool.Servers[req.Name]; !exists {
		fail(c, http.StatusNotFound, "server not found: "+req.Name)
		return
	}
	delete(pool.Servers, req.Name)
	if err := s.state.WritePool(pool); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (s *Server) poolUpdate(c *gin.Context) {
	var req struct {
		Name   string                `json:"name" binding:"required"`
		Server *apitypes.PoolServer  `json:"server" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, exists := pool.Servers[req.Name]; !exists {
		fail(c, http.StatusNotFound, "server not found: "+req.Name)
		return
	}
	pool.Servers[req.Name] = req.Server
	if err := s.state.WritePool(pool); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}
