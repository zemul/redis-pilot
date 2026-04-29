package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) instanceList(c *gin.Context) {
	state, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, state)
}

func (s *Server) instanceStatus(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	instances, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	inst, exists := instances.Instances[name]
	if !exists {
		fail(c, http.StatusNotFound, "instance not found: "+name)
		return
	}
	ok(c, inst)
}

func (s *Server) instanceCreate(c *gin.Context) {
	start := time.Now()
	var req apitypes.CreateInstanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	// 检查实例名冲突
	instances, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if _, exists := instances.Instances[req.Name]; exists {
		fail(c, http.StatusConflict, "instance already exists: "+req.Name)
		return
	}

	// 查找目标服务器
	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if req.Server == "" {
		// 自动调度
		selected, err := selectServer(pool, instances, req.Memory, req.CPUs, req.ReplicaOf)
		if err != nil {
			fail(c, http.StatusBadRequest, "auto schedule: "+err.Error())
			return
		}
		req.Server = selected
	}
	srv, exists := pool.Servers[req.Server]
	if !exists {
		fail(c, http.StatusBadRequest, "server not found: "+req.Server)
		return
	}

	// 写入 creating 状态
	dataDir := "/data/redis/" + req.Name
	role := "standalone"
	if req.Type == "replication" {
		if req.ReplicaOf != "" {
			role = "replica"
		} else {
			role = "master"
		}
	}

	// 自动分配 Redis 端口（请求未指定时）
	if req.Port == 0 {
		p, err := allocRedisPort(s.cfg.Ports, instances, req.Server)
		if err != nil {
			fail(c, http.StatusBadRequest, err.Error())
			return
		}
		req.Port = p
	}

	// 自动分配 Envoy 端口（master/standalone 自动分配）
	var envoyConf *apitypes.EnvoyConfig
	if role == "master" || role == "standalone" {
		ec, err := allocEnvoyPorts(s.cfg.Ports, instances)
		if err != nil {
			fail(c, http.StatusBadRequest, err.Error())
			return
		}
		envoyConf = ec
	}

	instances.Instances[req.Name] = &apitypes.Instance{
		Category:        req.Category,
		Engine:          req.Engine,
		Type:            req.Type,
		Role:            role,
		Server:          req.Server,
		Container:       req.Engine + "-" + req.Name,
		Port:            req.Port,
		Memory:          req.Memory,
		CPUs:            req.CPUs,
		Password:        req.Password,
		ConfigPath:      dataDir + "/conf",
		DataPath:        dataDir + "/data",
		BackupPath:      dataDir + "/backup",
		ConfigOverrides: req.ConfigOverrides,
		ReplicaOf:       req.ReplicaOf,
		Envoy:           envoyConf,
		Status:          "creating",
		CreatedAt:       time.Now().Format(time.RFC3339),
	}
	if err := s.state.WriteInstances(instances); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	// 调用 Agent 创建
	client := newAgentClient(srv)
	if _, err := client.post("/instance/create", req); err != nil {
		instances.Instances[req.Name].Status = "failed"
		s.state.WriteInstances(instances)
		s.audit.Log(audit.Record{Action: "instance.create", Level: audit.LevelImportant, Result: "failed", Detail: err.Error(),
			Target: map[string]interface{}{"instance": req.Name, "server": req.Server}})
		fail(c, http.StatusInternalServerError, "agent create failed: "+err.Error())
		return
	}

	// 更新状态为 running
	instances.Instances[req.Name].Status = "running"
	if err := s.state.WriteInstances(instances); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	// 更新 pool-state: allocated + instances 列表
	s.updatePoolAllocated(pool, req.Server, req.Memory, req.CPUs, req.Name, true)

	s.audit.Log(audit.Record{
		Action:   "instance.create",
		Level:    audit.LevelImportant,
		Result:   "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "engine": req.Engine, "server": req.Server},
		Params:   map[string]interface{}{"memory": req.Memory, "cpus": req.CPUs, "category": req.Category},
	})

	s.log.Infof("instance created: %s on %s", req.Name, req.Server)
	s.refreshEnvoy()
	ok(c, instances.Instances[req.Name])
}

func (s *Server) instanceDelete(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name      string `json:"name" binding:"required"`
		CleanData bool   `json:"clean_data"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	instances, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	inst, exists := instances.Instances[req.Name]
	if !exists {
		fail(c, http.StatusNotFound, "instance not found: "+req.Name)
		return
	}

	// 对实例组加操作锁
	sessionID := c.GetHeader("X-Session-ID")
	group := state.InstanceGroup(instances, req.Name)
	for _, n := range group {
		if i, ok2 := instances.Instances[n]; ok2 {
			if err := state.TryAcquireLock(i, sessionID, "delete", 300); err != nil {
				fail(c, http.StatusConflict, fmt.Sprintf("instance %s: %s", n, err.Error()))
				return
			}
		}
	}
	s.state.WriteInstances(instances)
	defer func() {
		if is, err := s.state.ReadInstances(); err == nil {
			for _, n := range group {
				if i, ok2 := is.Instances[n]; ok2 {
					state.ReleaseLock(i, sessionID)
				}
			}
			s.state.WriteInstances(is)
		}
	}()

	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	srv := pool.Servers[inst.Server]
	if srv == nil {
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return
	}

	client := newAgentClient(srv)
	if _, err := client.post("/instance/delete", map[string]interface{}{
		"name":       req.Name,
		"engine":     inst.Engine,
		"clean_data": req.CleanData,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent delete failed: "+err.Error())
		return
	}

	// 释放资源
	s.updatePoolAllocated(pool, inst.Server, inst.Memory, inst.CPUs, req.Name, false)

	delete(instances.Instances, req.Name)
	if err := s.state.WriteInstances(instances); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	s.audit.Log(audit.Record{
		Action: "instance.delete", Level: audit.LevelCritical, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	s.log.Infof("instance deleted: %s", req.Name)
	s.refreshEnvoy()
	ok(c, nil)
}

func (s *Server) instanceStart(c *gin.Context) {
	s.instanceSimpleOp(c, "start", "/instance/start", "instance.start", audit.LevelNormal)
}

func (s *Server) instanceStop(c *gin.Context) {
	s.instanceSimpleOp(c, "stop", "/instance/stop", "instance.stop", audit.LevelImportant)
}

func (s *Server) instanceConfig(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name            string            `json:"name" binding:"required"`
		ConfigOverrides map[string]string `json:"config_overrides" binding:"required"`
		Restart         bool              `json:"restart"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	inst, srv, unlock, err := s.resolveAndLock(c, req.Name, "config")
	if err != nil {
		return
	}
	defer unlock()

	client := newAgentClient(srv)
	if _, err := client.post("/instance/config", map[string]interface{}{
		"name":             req.Name,
		"engine":           inst.Engine,
		"config_overrides": req.ConfigOverrides,
		"restart":          req.Restart,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent config failed: "+err.Error())
		return
	}

	// 更新 instances-state 中的 config_overrides
	instances, _ := s.state.ReadInstances()
	if i, ok2 := instances.Instances[req.Name]; ok2 {
		if i.ConfigOverrides == nil {
			i.ConfigOverrides = make(map[string]string)
		}
		for k, v := range req.ConfigOverrides {
			i.ConfigOverrides[k] = v
		}
		s.state.WriteInstances(instances)
	}

	s.audit.Log(audit.Record{
		Action: "config.update", Level: audit.LevelImportant, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
		Params:   toMap(req.ConfigOverrides),
	})

	ok(c, nil)
}

func (s *Server) instancePromote(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	inst, srv, unlock, err := s.resolveAndLock(c, req.Name, "promote")
	if err != nil {
		return
	}
	defer unlock()

	client := newAgentClient(srv)
	if _, err := client.post("/instance/promote", map[string]string{"name": req.Name}); err != nil {
		fail(c, http.StatusInternalServerError, "agent promote failed: "+err.Error())
		return
	}

	// 更新状态
	instances, _ := s.state.ReadInstances()
	if i, ok2 := instances.Instances[req.Name]; ok2 {
		i.Role = "master"
		i.ReplicaOf = ""
		s.state.WriteInstances(instances)
	}

	s.audit.Log(audit.Record{
		Action: "topology.failover", Level: audit.LevelCritical, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	s.refreshEnvoy()
	ok(c, nil)
}

func (s *Server) instanceReplicate(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name      string `json:"name" binding:"required"`
		ReplicaOf string `json:"replica_of" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	inst, srv, unlock, err := s.resolveAndLock(c, req.Name, "replicate")
	if err != nil {
		return
	}
	defer unlock()

	client := newAgentClient(srv)
	if _, err := client.post("/instance/replicate", map[string]string{
		"name":       req.Name,
		"replica_of": req.ReplicaOf,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent replicate failed: "+err.Error())
		return
	}

	// 更新状态
	instances, _ := s.state.ReadInstances()
	if i, ok2 := instances.Instances[req.Name]; ok2 {
		i.Role = "replica"
		i.ReplicaOf = req.ReplicaOf
		s.state.WriteInstances(instances)
	}

	s.audit.Log(audit.Record{
		Action: "topology.replicate", Level: audit.LevelImportant, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
		Params:   map[string]interface{}{"replica_of": req.ReplicaOf},
	})

	s.refreshEnvoy()
	ok(c, nil)
}

// --- 备份管理 ---

func (s *Server) backupExec(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	inst, srv, unlock, err := s.resolveAndLock(c, req.Name, "backup")
	if err != nil {
		return
	}
	defer unlock()

	client := newAgentClient(srv)
	resp, err := client.post("/instance/backup", map[string]string{
		"name":   req.Name,
		"engine": inst.Engine,
	})
	if err != nil {
		fail(c, http.StatusInternalServerError, "agent backup failed: "+err.Error())
		return
	}

	// 更新 last_backup
	instances, _ := s.state.ReadInstances()
	if i, ok2 := instances.Instances[req.Name]; ok2 {
		if i.Backup == nil {
			i.Backup = &apitypes.BackupConfig{}
		}
		i.Backup.LastBackup = time.Now().Format(time.RFC3339)
		s.state.WriteInstances(instances)
	}

	s.audit.Log(audit.Record{
		Action: "backup.create", Level: audit.LevelNormal, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	var result interface{}
	json.Unmarshal(resp, &result)
	ok(c, result)
}

func (s *Server) backupRestore(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name     string `json:"name" binding:"required"`
		BackupTs string `json:"backup_ts" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	inst, srv, unlock, err := s.resolveAndLock(c, req.Name, "restore")
	if err != nil {
		return
	}
	defer unlock()

	client := newAgentClient(srv)
	if _, err := client.post("/instance/restore", map[string]string{
		"name":      req.Name,
		"engine":    inst.Engine,
		"backup_ts": req.BackupTs,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent restore failed: "+err.Error())
		return
	}

	s.audit.Log(audit.Record{
		Action: "backup.restore", Level: audit.LevelCritical, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
		Params:   map[string]interface{}{"backup_ts": req.BackupTs},
	})

	ok(c, nil)
}

func (s *Server) backupList(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}

	inst, srv, err := s.resolveInstance(c, name)
	if err != nil {
		return
	}
	_ = inst

	client := newAgentClient(srv)
	resp, err := client.get("/instance/backups?name=" + name)
	if err != nil {
		fail(c, http.StatusInternalServerError, "agent backups failed: "+err.Error())
		return
	}

	var result interface{}
	json.Unmarshal(resp, &result)
	ok(c, result)
}

// --- 辅助方法 ---

// instanceSimpleOp 处理 start/stop 这类只需要转发给 Agent 的操作
func (s *Server) instanceSimpleOp(c *gin.Context, newStatus, agentPath, auditAction string, level audit.Level) {
	start := time.Now()
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	inst, srv, unlock, err := s.resolveAndLock(c, req.Name, newStatus)
	if err != nil {
		return
	}
	defer unlock()

	client := newAgentClient(srv)
	if _, err := client.post(agentPath, map[string]string{
		"name":   req.Name,
		"engine": inst.Engine,
	}); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	statusMap := map[string]string{"start": "running", "stop": "stopped"}
	instances, _ := s.state.ReadInstances()
	if i, ok2 := instances.Instances[req.Name]; ok2 {
		if status, ok3 := statusMap[newStatus]; ok3 {
			i.Status = status
		}
		s.state.WriteInstances(instances)
	}

	s.audit.Log(audit.Record{
		Action: auditAction, Level: level, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	ok(c, nil)
}

// resolveInstance 查找实例及其所在服务器（只读操作用）
func (s *Server) resolveInstance(c *gin.Context, name string) (*apitypes.Instance, *apitypes.PoolServer, error) {
	instances, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, err
	}
	inst, exists := instances.Instances[name]
	if !exists {
		fail(c, http.StatusNotFound, "instance not found: "+name)
		return nil, nil, err
	}

	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, err
	}
	srv := pool.Servers[inst.Server]
	if srv == nil {
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return nil, nil, err
	}
	return inst, srv, nil
}

// resolveAndLock 查找实例 + 对整个实例组加操作锁（写操作用）。返回 unlock 函数供 defer 调用。
func (s *Server) resolveAndLock(c *gin.Context, name, operation string) (*apitypes.Instance, *apitypes.PoolServer, func(), error) {
	instances, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}
	inst, exists := instances.Instances[name]
	if !exists {
		fail(c, http.StatusNotFound, "instance not found: "+name)
		return nil, nil, nil, fmt.Errorf("not found")
	}

	// 锁整个实例组（主库+所有从库）
	sessionID := c.GetHeader("X-Session-ID")
	group := state.InstanceGroup(instances, name)
	for _, n := range group {
		if i, ok := instances.Instances[n]; ok {
			if err := state.TryAcquireLock(i, sessionID, operation, 300); err != nil {
				fail(c, http.StatusConflict, fmt.Sprintf("instance %s: %s", n, err.Error()))
				return nil, nil, nil, err
			}
		}
	}
	s.state.WriteInstances(instances)

	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}
	srv := pool.Servers[inst.Server]
	if srv == nil {
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return nil, nil, nil, fmt.Errorf("server not found")
	}

	unlock := func() {
		if is, err := s.state.ReadInstances(); err == nil {
			for _, n := range group {
				if i, ok := is.Instances[n]; ok {
					state.ReleaseLock(i, sessionID)
				}
			}
			s.state.WriteInstances(is)
		}
	}
	return inst, srv, unlock, nil
}

// updatePoolAllocated 更新 pool-state 的资源分配
func (s *Server) updatePoolAllocated(pool *apitypes.PoolState, serverName, memory string, cpus int, instanceName string, add bool) {
	srv := pool.Servers[serverName]
	if srv == nil {
		return
	}
	if add {
		srv.Allocated.CPUCores += cpus
		srv.Allocated.Memory = addMemory(srv.Allocated.Memory, memory)
		srv.Instances = append(srv.Instances, instanceName)
	} else {
		srv.Allocated.CPUCores -= cpus
		if srv.Allocated.CPUCores < 0 {
			srv.Allocated.CPUCores = 0
		}
		srv.Allocated.Memory = subMemory(srv.Allocated.Memory, memory)
		// 从 instances 列表中移除
		filtered := srv.Instances[:0]
		for _, n := range srv.Instances {
			if n != instanceName {
				filtered = append(filtered, n)
			}
		}
		srv.Instances = filtered
	}
	s.state.WritePool(pool)
}

func toMap(m map[string]string) map[string]interface{} {
	r := make(map[string]interface{}, len(m))
	for k, v := range m {
		r[k] = v
	}
	return r
}

// parseMemoryGi 解析内存字符串为 Gi 数值，如 "4Gi" → 4, "512Mi" → 0
func parseMemoryGi(s string) int {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "Gi") {
		n, _ := strconv.Atoi(strings.TrimSuffix(s, "Gi"))
		return n
	}
	return 0
}

func addMemory(current, delta string) string {
	return strconv.Itoa(parseMemoryGi(current)+parseMemoryGi(delta)) + "Gi"
}

func subMemory(current, delta string) string {
	v := parseMemoryGi(current) - parseMemoryGi(delta)
	if v < 0 {
		v = 0
	}
	return strconv.Itoa(v) + "Gi"
}
