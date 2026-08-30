package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
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
	ok(c, extractAgentData(out))
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

func (s *Server) instanceBigkeysStart(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	inst, srv, err := s.resolveInstance(c, req.Name)
	if err != nil {
		return
	}
	taskID := newOperationTaskID("diagnostic.bigkeys", req.Name)
	operator := operatorFrom(c)
	payload := operationTaskPayload{Instance: req.Name, Server: inst.Server, Group: inst.Group, Operator: operator, Source: "manual", Trigger: "manual"}
	if err := s.db.CreateTask(store.Task{ID: taskID, Kind: "diagnostic.bigkeys", Status: "pending", Phase: "scanning", Message: "等待 Agent 受理", Cancelable: true,
		Payload: marshalTaskPayload(payload), Operator: operator, Source: "manual", Trigger: "manual", GroupName: inst.Group, InstanceName: req.Name, ServerName: inst.Server}); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := newAgentClient(srv).post("/instance/bigkeys", map[string]string{"name": req.Name, "task_id": taskID}); err != nil {
		_ = s.db.UpdateTaskStatus(taskID, "failed", "", err.Error())
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	_ = s.db.UpdateTaskStatus(taskID, "running", "", "")
	_ = s.audit.Log(store.AuditRecord{Operator: operator, Source: "manual", Trigger: "manual", Action: "diagnostic.bigkeys", Level: audit.LevelNormal,
		Result: "accepted", CorrelationID: taskID, DurationMs: time.Since(start).Milliseconds(),
		Target: map[string]interface{}{"instance": req.Name, "group": inst.Group, "server": inst.Server, "task_id": taskID}})
	ok(c, gin.H{"task_id": taskID})
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

func (s *Server) instanceConfigGet(c *gin.Context) {
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
	out, err := agent.get("/instance/config?name=" + name)
	if err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	ok(c, extractAgentData(out))
}

func (s *Server) instanceMetrics(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	_, srv, err := s.resolveInstance(c, name)
	if err != nil {
		return
	}
	rangeStr := c.DefaultQuery("range", "1h")
	agent := newAgentClient(srv)
	q := "/instance/metrics?name=" + name + "&range=" + rangeStr
	if s := c.Query("start"); s != "" {
		q += "&start=" + s
	}
	if e := c.Query("end"); e != "" {
		q += "&end=" + e
	}
	out, err := agent.get(q)
	if err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	ok(c, extractAgentData(out))
}
