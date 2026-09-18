package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var validProvisionName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

type profileTaskPayload struct {
	apitypes.EnsureProfileRequest
	AgentEndpoint string `json:"agent_endpoint"`
	AgentPort     int    `json:"agent_port"`
}

func newProvisionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "provision-" + hex.EncodeToString(b[:])
}

func (s *Server) runProvisionTasks() {
	s.trackProvisionTasksOnce()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.trackProvisionTasksOnce()
	}
}

func (s *Server) trackProvisionTasksOnce() {
	if !s.provisionMu.TryLock() {
		return
	}
	defer s.provisionMu.Unlock()
	tasks, err := s.db.ListActiveTasks()
	if err != nil {
		s.log.Errorf("provision tasks: %v", err)
		return
	}
	for _, task := range tasks {
		if task.Kind != "profile.create" && task.Kind != "cluster.provision" {
			continue
		}
		last, _ := time.Parse(time.RFC3339, task.UpdatedAt)
		if task.Phase == "retry_wait" && time.Since(last) < 30*time.Second {
			continue
		}
		if task.Kind == "cluster.provision" {
			s.trackClusterBuildTask(task)
		} else {
			s.trackProvisionTask(task)
		}
	}
}

func (s *Server) trackProvisionTask(task store.Task) {
	current, err := s.db.GetTask(task.ID)
	if err != nil || current.Kind != "profile.create" || (current.Status != "pending" && current.Status != "running") {
		return
	}
	task = *current
	wait := func(reason string) {
		// Keep the reservation while the outcome is unknown; never delete data
		// and never convert a transport error into a terminal failed resource.
		_ = s.db.UpdateTaskProgress(task.ID, "retry_wait", task.Progress, reason, "", false, false)
	}
	var req profileTaskPayload
	if json.Unmarshal([]byte(task.Payload), &req) != nil || req.OperationID != task.ID || req.Profile == "" || req.Name != task.InstanceName || req.Server != task.ServerName {
		wait("Invalid provisioning payload; resource remains reserved")
		return
	}
	instances, err := s.db.ReadInstances()
	if err != nil {
		wait("Cannot read reserved resources")
		return
	}
	inst := instances.Instances[req.Name]
	if inst == nil || inst.Lock == nil || inst.Lock.HeldBy != task.ID || inst.Profile != req.Profile || inst.Server != req.Server || inst.Port != req.Port || inst.DesiredState != apitypes.DesiredRunning {
		wait("Provisioning reservation no longer matches; no Agent request sent")
		return
	}
	nodes, err := s.db.ReadNode()
	if err != nil || nodes.Servers[req.Server] == nil {
		wait("Provisioning Agent unavailable")
		return
	}
	if nodes.Servers[req.Server].Endpoint != req.AgentEndpoint || nodes.Servers[req.Server].AgentPort != req.AgentPort {
		wait("Agent endpoint no longer matches the reserved host; no request sent")
		return
	}
	if err := s.db.UpdateTaskStatus(task.ID, "running", "", ""); err != nil {
		return
	}
	task.Progress++
	if err := s.db.UpdateTaskProgress(task.ID, "ensuring", task.Progress, "Checking or creating owned container", "", false, false); err != nil {
		return
	}
	body, err := newAgentClient(nodes.Servers[req.Server]).postLong("/instance/ensure-profile", req.EnsureProfileRequest)
	if err != nil {
		wait("Agent call failed; retrying with the same operation ID: " + err.Error())
		return
	}
	var reply struct {
		Success bool `json:"success"`
		Data    struct {
			OperationID string `json:"operation_id"`
			ContainerID string `json:"container_id"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &reply) != nil || !reply.Success || reply.Data.OperationID != task.ID || reply.Data.ContainerID == "" {
		wait("Agent acknowledgement missing or mismatched; will recheck")
		return
	}
	if err := s.db.UpdateTaskProgress(task.ID, "verifying_database", task.Progress, "Checking database readiness, runtime profile and empty keyspace", "", false, false); err != nil {
		return
	}
	body, err = newAgentClient(nodes.Servers[req.Server]).post("/instance/verify-profile", req.EnsureProfileRequest)
	if err != nil {
		wait("Database not ready for provisioning: " + err.Error())
		return
	}
	var verified struct {
		Success bool `json:"success"`
		Data    struct {
			OperationID string `json:"operation_id"`
			ContainerID string `json:"container_id"`
			Ready       bool   `json:"database_ready"`
			Empty       bool   `json:"empty"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &verified) != nil || !verified.Success || !verified.Data.Ready || !verified.Data.Empty || verified.Data.OperationID != task.ID || verified.Data.ContainerID != reply.Data.ContainerID {
		wait("Database verification acknowledgement missing or mismatched")
		return
	}
	err = s.audit.Log(store.AuditRecord{ID: "audit-task-" + task.ID, Operator: task.Operator, Source: task.Source, Trigger: task.Trigger, Action: task.Kind, Level: audit.LevelImportant, Result: "success", CorrelationID: task.ID, Target: map[string]any{"instance": req.Name, "server": req.Server, "task_id": task.ID}, Detail: "Container identity, database readiness, runtime profile and empty keyspace verified; Gateway readiness is separate"})
	if err != nil && !strings.Contains(err.Error(), "UNIQUE constraint failed") {
		wait("Cannot persist provisioning audit; will recheck")
		return
	}
	if err := s.db.CompleteProvisionTask(task.ID, reply.Data.ContainerID); err != nil {
		wait(fmt.Sprintf("Cannot commit provisioning result: %v", err))
		return
	}
	_ = s.db.WithInstances(func(st *apitypes.InstancesState) error { state.RecalculateGroupTopology(st, inst.Group); return nil })
}
