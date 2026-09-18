package server

import (
	"encoding/json"
	"fmt"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) clusterBuildCreate(c *gin.Context) {
	var req struct {
		Spec     apitypes.ColdClusterSpec `json:"spec"`
		Children []string                 `json:"child_tasks"`
	}
	if !coldBody(c, &req) {
		return
	}
	if len(s.cfg.GatewayTokens[req.Spec.GatewayID]) < 16 {
		fail(c, 400, "configure Gateway machine credential first")
		return
	}
	id := newProvisionID()
	err := s.db.CreateClusterBuild(req.Spec, req.Children, store.Task{ID: id, Operator: operatorFrom(c), Source: "api", Trigger: "manual"})
	s.coldAudit(c, "provision", req.Spec.ID, err)
	if err != nil {
		s.coldError(c, err)
		return
	}
	c.JSON(202, apitypes.APIResponse{Success: true, Data: gin.H{"task_id": id, "cluster_id": req.Spec.ID, "status": "pending"}})
}

func (s *Server) trackClusterBuildTask(task store.Task) {
	current, err := s.db.GetTask(task.ID)
	if err != nil || current.Kind != "cluster.provision" || (current.Status != "pending" && current.Status != "running") {
		return
	}
	wait := func(reason string) { _ = s.db.UpdateTaskProgress(task.ID, "retry_wait", 0, reason, "", false, false) }
	children, err := s.db.ClusterBuildChildren(task.ID)
	if err != nil || len(children) == 0 {
		wait("Cannot read cluster child tasks")
		return
	}
	ready := 0
	for _, child := range children {
		if child.Status == "success" {
			ready++
		}
	}
	if ready != len(children) {
		_ = s.db.UpdateTaskProgress(task.ID, "waiting_resources", int64(ready), fmt.Sprintf("%d/%d resources verified", ready, len(children)), "", false, false)
		return
	}
	if err := s.db.UpdateTaskStatus(task.ID, "running", "", ""); err != nil {
		return
	}
	// Recheck after reserving all members. Completed children could have been
	// modified before adoption; old readiness receipts alone are not sufficient.
	for _, child := range children {
		if err := s.verifyClusterChild(child); err != nil {
			wait(fmt.Sprintf("%s: %v", child.InstanceName, err))
			return
		}
	}
	if _, err := s.db.FinishClusterBuild(task.ID); err != nil {
		wait("Cluster registration pending: " + err.Error())
	}
}

func (s *Server) verifyClusterChild(child store.Task) error {
	var req profileTaskPayload
	if json.Unmarshal([]byte(child.Payload), &req) != nil || req.OperationID != child.ID {
		return fmt.Errorf("invalid child payload")
	}
	var proof struct {
		ContainerID string `json:"container_id"`
	}
	if json.Unmarshal([]byte(child.Result), &proof) != nil || proof.ContainerID == "" {
		return fmt.Errorf("missing child container proof")
	}
	nodes, err := s.db.ReadNode()
	if err != nil {
		return err
	}
	node := nodes.Servers[req.Server]
	if node == nil || node.Endpoint != req.AgentEndpoint || node.AgentPort != req.AgentPort {
		return fmt.Errorf("Agent identity changed")
	}
	b, err := newAgentClient(node).post("/instance/verify-profile", req.EnsureProfileRequest)
	if err != nil {
		return err
	}
	var reply struct {
		Success bool `json:"success"`
		Data    struct {
			OperationID string `json:"operation_id"`
			ContainerID string `json:"container_id"`
			Ready       bool   `json:"database_ready"`
			Empty       bool   `json:"empty"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &reply) != nil || !reply.Success || reply.Data.OperationID != child.ID || reply.Data.ContainerID != proof.ContainerID || !reply.Data.Ready || !reply.Data.Empty {
		return fmt.Errorf("database verification not confirmed")
	}
	return nil
}
