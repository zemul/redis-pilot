package server

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

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

func (s *Server) instanceDiagnose(c *gin.Context) {
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
	out, err := agent.get("/instance/diagnose?name=" + name)
	if err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	var raw json.RawMessage = out
	ok(c, raw)
}

func (s *Server) instanceSlowlog(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	_, srv, err := s.resolveInstance(c, name)
	if err != nil {
		return
	}
	count := c.DefaultQuery("count", "50")
	agent := newAgentClient(srv)
	out, err := agent.get("/instance/slowlog?name=" + name + "&count=" + count)
	if err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	ok(c, extractAgentData(out))
}

func (s *Server) instanceBigkeys(c *gin.Context) {
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
	out, err := agent.get("/instance/bigkeys?name=" + name)
	if err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	ok(c, extractAgentData(out))
}

// extractAgentData 从 Agent 的 {"success":true,"data":{...}} 响应中提取 data 字段。
func extractAgentData(raw []byte) json.RawMessage {
	var resp struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil || resp.Data == nil {
		return raw
	}
	return resp.Data
}
