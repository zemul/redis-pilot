package server

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s *Server) healthCheck(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	_, srv, err := s.resolveInstance(c, name)
	if err != nil {
		return
	}
	agent := newAgentClient(srv)
	out, err := agent.get("/instance/status?name=" + name)
	if err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	var raw json.RawMessage = out
	ok(c, raw)
}

func (s *Server) metricsCollect(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	_, srv, err := s.resolveInstance(c, name)
	if err != nil {
		return
	}
	agent := newAgentClient(srv)
	out, err := agent.get("/instance/status?name=" + name)
	if err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	var raw json.RawMessage = out
	ok(c, raw)
}
