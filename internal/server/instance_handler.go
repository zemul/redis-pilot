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

// resolveReplicaOf 将 replica_of 参数解析为 ip:port 和主库实例名。
// 支持两种格式：实例名（如 "order-master"）或 ip:port（如 "10.0.1.10:6379"）。
func resolveReplicaOf(pool *apitypes.PoolState, instances *apitypes.InstancesState, replicaOf string) (addr string, masterName string, err error) {
	if replicaOf == "" {
		return "", "", nil
	}
	if strings.Contains(replicaOf, ":") {
		// ip:port 格式，反查实例名
		for name, inst := range instances.Instances {
			if inst.Role == "master" || inst.Role == "standalone" {
				a := fmt.Sprintf("%s:%d", poolEndpoint(pool, inst.Server), inst.Port)
				if a == replicaOf {
					return replicaOf, name, nil
				}
			}
		}
		return replicaOf, "", nil
	}
	// 实例名格式，解析为 ip:port
	inst, exists := instances.Instances[replicaOf]
	if !exists {
		return "", "", fmt.Errorf("replica_of instance not found: %s", replicaOf)
	}
	if inst.Role != "master" && inst.Role != "standalone" {
		return "", "", fmt.Errorf("replica_of target must be master or standalone, got %s", inst.Role)
	}
	endpoint := poolEndpoint(pool, inst.Server)
	if endpoint == "" {
		return "", "", fmt.Errorf("cannot resolve endpoint for server: %s", inst.Server)
	}
	return fmt.Sprintf("%s:%d", endpoint, inst.Port), replicaOf, nil
}

// defaultPersistence 根据 engine/category/overrides 生成默认持久化配置记录。
// Kvrocks 返回 nil（RocksDB 原生持久化，无需 RDB/AOF）。
func defaultPersistence(engine, category string, overrides map[string]string) *apitypes.Persistence {
	if engine == "kvrocks" {
		return nil
	}
	p := &apitypes.Persistence{
		RDB:          true,
		RDBFrequency: "3600 1 300 100 60 10000",
		AOF:          category != "cache",
		AOFPolicy:    "everysec",
	}
	// 用 config_overrides 中的显式值修正记录
	if v, ok := overrides["appendfsync"]; ok {
		p.AOFPolicy = v
	}
	if v, ok := overrides["appendonly"]; ok {
		p.AOF = v == "yes"
	}
	if v, ok := overrides["save"]; ok {
		if v == "" {
			p.RDB = false
			p.RDBFrequency = ""
		} else {
			p.RDBFrequency = v
		}
	}
	return p
}

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

	// 查找目标服务器（需要在原子操作外读 pool，因为调度需要 instances 快照）
	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	dataDir := "/data/redis/" + req.Name
	role := "standalone"
	if req.Type == "replication" {
		if req.ReplicaOf != "" {
			role = "replica"
		} else {
			role = "master"
		}
	}
	req.Group = strings.TrimSpace(req.Group)

	var inst *apitypes.Instance
	var srv *apitypes.PoolServer
	var masterName string // 用于维护 Replicas 列表
	groupName := req.Group

	// 原子操作：检查冲突 → 调度 → 分配端口 → 写入 creating 状态
	var resolvedAddr string // replica_of 解析后的 ip:port，用于 Agent 调用
	err = s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		if _, exists := instances.Instances[req.Name]; exists {
			return fmt.Errorf("conflict: instance already exists: %s", req.Name)
		}

		// 解析 replica_of：支持实例名或 ip:port
		if req.ReplicaOf != "" {
			addr, mName, err := resolveReplicaOf(pool, instances, req.ReplicaOf)
			if err != nil {
				return err
			}
			resolvedAddr = addr
			if mName != "" {
				masterName = mName
			}
		}
		if role == "replica" {
			if masterName == "" {
				return fmt.Errorf("replica_of must reference an existing managed master instance")
			}
			master := instances.Instances[masterName]
			if master == nil {
				return fmt.Errorf("replica_of instance not found: %s", req.ReplicaOf)
			}
			groupName = master.Group
			if groupName == "" {
				groupName = masterName
			}
		} else {
			if groupName == "" {
				return fmt.Errorf("group is required for master or standalone instance")
			}
			for name, existing := range instances.Instances {
				if existing == nil || existing.Group != groupName || existing.Status == "failed" {
					continue
				}
				if existing.Role == "master" || existing.Role == "standalone" {
					return fmt.Errorf("conflict: group already has primary instance %s", name)
				}
			}
		}

		if req.Server == "" {
			selected, err := selectServer(pool, instances, req.Memory, req.CPUs, resolvedAddr)
			if err != nil {
				return fmt.Errorf("schedule: %s", err.Error())
			}
			req.Server = selected
		}
		var exists bool
		srv, exists = pool.Servers[req.Server]
		if !exists {
			return fmt.Errorf("server not found: %s", req.Server)
		}

		port, err := allocRedisPort(s.cfg.Ports, instances, req.Server)
		if err != nil {
			return err
		}
		req.Port = port

		var envoyConf *apitypes.EnvoyConfig
		if role == "master" || role == "standalone" {
			ec, err := allocEnvoyPorts(s.cfg.Ports, instances)
			if err != nil {
				return err
			}
			envoyConf = ec
		}

		inst = &apitypes.Instance{
			Category:        req.Category,
			Group:           groupName,
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
			Persistence:     defaultPersistence(req.Engine, req.Category, req.ConfigOverrides),
			KvrocksConfig:   req.KvrocksConfig,
			ConfigOverrides: req.ConfigOverrides,
			ReplicaOf:       masterName, // 存实例名而非 ip:port
			Envoy:           envoyConf,
			Status:          "creating",
			CreatedAt:       time.Now().Format(time.RFC3339),
		}
		instances.Instances[req.Name] = inst

		// 维护主库的 Replicas 列表
		if role == "replica" && masterName != "" {
			if master := instances.Instances[masterName]; master != nil {
				master.Replicas = append(master.Replicas, req.Name)
			}
		}

		return nil
	})
	if err != nil {
		if strings.Contains(err.Error(), "conflict:") {
			fail(c, http.StatusConflict, err.Error())
		} else {
			fail(c, http.StatusBadRequest, err.Error())
		}
		return
	}

	// 调用 Agent 创建（传解析后的 ip:port）
	req.ReplicaOf = resolvedAddr
	client := newAgentClient(srv)
	if _, err := client.post("/instance/create", req); err != nil {
		// 调用 Agent 清理已创建的资源（best-effort，忽略错误）
		client.post("/instance/delete", map[string]interface{}{
			"name":       req.Name,
			"engine":     req.Engine,
			"clean_data": true,
		})
		// 标记实例为 failed，保留记录供排查
		s.state.WithInstances(func(instances *apitypes.InstancesState) error {
			if i := instances.Instances[req.Name]; i != nil {
				i.Status = "failed"
			}
			return nil
		})
		s.audit.Log(audit.Record{Operator: operatorFrom(c), Action: "instance.create", Level: audit.LevelImportant, Result: "failed", Detail: err.Error(),
			Target: map[string]interface{}{"instance": req.Name, "server": req.Server}})
		fail(c, http.StatusInternalServerError, "agent create failed: "+err.Error())
		return
	}

	// 更新状态为 running
	s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		if i := instances.Instances[req.Name]; i != nil {
			i.Status = "running"
		}
		return nil
	})

	// 更新 pool-state: allocated + instances 列表
	s.updatePoolAllocated(pool, req.Server, req.Memory, req.CPUs, req.Name, true)

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   "instance.create",
		Level:    audit.LevelImportant,
		Result:   "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "group": groupName, "engine": req.Engine, "server": req.Server},
		Params:   map[string]interface{}{"memory": req.Memory, "cpus": req.CPUs, "category": req.Category},
	})

	s.log.Infof("instance created: %s on %s", req.Name, req.Server)
	s.refreshEnvoy()
	s.syncSentinel()
	ok(c, inst)
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

	// 原子加锁并获取实例信息
	sessionID := c.GetHeader("X-Session-ID")
	if sessionID == "" {
		sessionID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
	}
	var inst apitypes.Instance // 拷贝一份，避免后续引用过期
	var group []string
	err := s.state.WithInstances(func(is *apitypes.InstancesState) error {
		i, exists := is.Instances[req.Name]
		if !exists {
			return fmt.Errorf("not found: %s", req.Name)
		}
		inst = *i
		group = state.InstanceGroup(is, req.Name)
		for _, n := range group {
			if gi, ok2 := is.Instances[n]; ok2 {
				if err := state.TryAcquireLock(gi, sessionID, "delete", 300); err != nil {
					return fmt.Errorf("instance %s: %s", n, err.Error())
				}
			}
		}
		return nil
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found:") {
			fail(c, http.StatusNotFound, "instance not found: "+req.Name)
		} else {
			fail(c, http.StatusConflict, err.Error())
		}
		return
	}
	defer s.releaseLockGroup(group, sessionID)

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

	// 原子删除实例记录
	if err := s.state.WithInstances(func(is *apitypes.InstancesState) error {
		delete(is.Instances, req.Name)
		return nil
	}); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action: "instance.delete", Level: audit.LevelCritical, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	s.log.Infof("instance deleted: %s", req.Name)
	if inst.Type == "replication" && inst.Role == "master" {
		group := inst.Group
		if group == "" {
			group = req.Name
		}
		s.removeSentinelMaster(group)
	}
	s.refreshEnvoy()
	s.syncSentinel()
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
		"type":             inst.Type,
		"config_overrides": req.ConfigOverrides,
		"restart":          req.Restart,
		"password":         inst.Password,
		"memory":           inst.Memory,
		"category":         inst.Category,
		"replica_of":       inst.ReplicaOf,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent config failed: "+err.Error())
		return
	}

	// 更新 instances-state 中的 config_overrides
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok2 := is.Instances[req.Name]; ok2 {
			if i.ConfigOverrides == nil {
				i.ConfigOverrides = make(map[string]string)
			}
			for k, v := range req.ConfigOverrides {
				i.ConfigOverrides[k] = v
			}
		}
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
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

	if inst.Role != "replica" {
		fail(c, http.StatusBadRequest, "only replica can be promoted, current role: "+inst.Role)
		return
	}

	client := newAgentClient(srv)
	if _, err := client.post("/instance/promote", map[string]string{"name": req.Name}); err != nil {
		fail(c, http.StatusInternalServerError, "agent promote failed: "+err.Error())
		return
	}

	// 更新状态：角色变更 + 从旧主库 Replicas 移除 + 分配 Envoy 端口
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		i := is.Instances[req.Name]
		if i == nil {
			return nil
		}
		// 从旧主库的 Replicas 列表移除
		for _, other := range is.Instances {
			filtered := other.Replicas[:0]
			for _, r := range other.Replicas {
				if r != req.Name {
					filtered = append(filtered, r)
				}
			}
			other.Replicas = filtered
		}
		i.Role = "master"
		i.ReplicaOf = ""
		// 分配 Envoy 端口
		if i.Envoy == nil {
			ec, err := allocEnvoyPorts(s.cfg.Ports, is)
			if err == nil {
				i.Envoy = ec
			}
		}
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action: "topology.failover", Level: audit.LevelCritical, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	s.refreshEnvoy()
	s.syncSentinel()
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

	// 解析 replica_of：支持实例名或 ip:port
	pool, _ := s.state.ReadPool()
	instances, _ := s.state.ReadInstances()
	resolvedAddr, masterName, err := resolveReplicaOf(pool, instances, req.ReplicaOf)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if masterName == "" {
		fail(c, http.StatusBadRequest, "replica_of must reference an existing managed master instance")
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
		"replica_of": resolvedAddr,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent replicate failed: "+err.Error())
		return
	}

	// 更新状态：角色变更 + 维护新旧主库 Replicas + 清理 Envoy 端口
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		i := is.Instances[req.Name]
		if i == nil {
			return nil
		}
		// 从旧主库的 Replicas 移除
		for _, other := range is.Instances {
			filtered := other.Replicas[:0]
			for _, r := range other.Replicas {
				if r != req.Name {
					filtered = append(filtered, r)
				}
			}
			other.Replicas = filtered
		}
		// 加入新主库的 Replicas
		groupName := ""
		if masterName != "" {
			if master := is.Instances[masterName]; master != nil {
				master.Replicas = append(master.Replicas, req.Name)
				groupName = master.Group
				if groupName == "" {
					groupName = masterName
				}
			}
		}
		// 如果原来是 master/standalone 有 Envoy 端口，变 replica 后释放
		if i.Role == "master" || i.Role == "standalone" {
			i.Envoy = nil
		}
		i.Role = "replica"
		i.ReplicaOf = masterName // 存实例名
		i.Group = groupName
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action: "topology.replicate", Level: audit.LevelImportant, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
		Params:   map[string]interface{}{"replica_of": req.ReplicaOf},
	})

	s.refreshEnvoy()
	s.syncSentinel()
	ok(c, nil)
}

// --- 备份管理 ---

func (s *Server) backupExec(c *gin.Context) {
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.execBackup(req.Name, operatorFrom(c)); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

// execBackup 执行一次备份，供 HTTP handler 和定时调度器共用。
func (s *Server) execBackup(name, operator string) error {
	start := time.Now()

	inst, srv, unlock, err := s.resolveAndLockInternal(name, "backup")
	if err != nil {
		return err
	}
	defer unlock()

	// 构建请求，传入 retention
	body := map[string]interface{}{
		"name":   name,
		"engine": inst.Engine,
	}
	if inst.Backup != nil && inst.Backup.Retention > 0 {
		body["retention"] = inst.Backup.Retention
	}

	client := newAgentClient(srv)
	if _, err := client.post("/instance/backup", body); err != nil {
		return fmt.Errorf("agent backup failed: %w", err)
	}

	// 更新 last_backup
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok2 := is.Instances[name]; ok2 {
			if i.Backup == nil {
				i.Backup = &apitypes.BackupConfig{}
			}
			i.Backup.LastBackup = time.Now().Format(time.RFC3339)
		}
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operator,
		Action: "backup.create", Level: audit.LevelNormal, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": name, "server": inst.Server},
	})
	return nil
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
		Operator: operatorFrom(c),
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

func (s *Server) backupGetSchedule(c *gin.Context) {
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
	inst, ok2 := instances.Instances[name]
	if !ok2 {
		fail(c, http.StatusNotFound, "instance not found")
		return
	}
	if inst.Backup == nil {
		ok(c, map[string]interface{}{"name": name, "schedule": "", "retention": 0})
		return
	}
	ok(c, map[string]interface{}{
		"name":      name,
		"schedule":  inst.Backup.Schedule,
		"retention": inst.Backup.Retention,
	})
}

func (s *Server) backupSetSchedule(c *gin.Context) {
	var req struct {
		Name      string `json:"name" binding:"required"`
		Schedule  string `json:"schedule"`
		Retention int    `json:"retention"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	err := s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		inst, ok2 := instances.Instances[req.Name]
		if !ok2 {
			return fmt.Errorf("instance not found")
		}
		if inst.Backup == nil {
			inst.Backup = &apitypes.BackupConfig{}
		}
		inst.Backup.Schedule = req.Schedule
		if req.Retention > 0 {
			inst.Backup.Retention = req.Retention
		}
		return nil
	})
	if err != nil {
		if err.Error() == "instance not found" {
			fail(c, http.StatusNotFound, err.Error())
		} else {
			fail(c, http.StatusInternalServerError, err.Error())
		}
		return
	}
	ok(c, nil)
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
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok2 := is.Instances[req.Name]; ok2 {
			if status, ok3 := statusMap[newStatus]; ok3 {
				i.Status = status
			}
		}
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
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
		return nil, nil, fmt.Errorf("instance not found: %s", name)
	}

	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, err
	}
	srv := pool.Servers[inst.Server]
	if srv == nil {
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return nil, nil, fmt.Errorf("server not found: %s", inst.Server)
	}
	return inst, srv, nil
}

// resolveAndLock 查找实例 + 对整个实例组加操作锁（写操作用）。返回 unlock 函数供 defer 调用。
func (s *Server) resolveAndLock(c *gin.Context, name, operation string) (*apitypes.Instance, *apitypes.PoolServer, func(), error) {
	sessionID := c.GetHeader("X-Session-ID")
	if sessionID == "" {
		sessionID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
	}

	var inst *apitypes.Instance
	var group []string

	// 原子操作：read → 加锁 → write
	err := s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		var exists bool
		inst, exists = instances.Instances[name]
		if !exists {
			return fmt.Errorf("not found")
		}
		group = state.InstanceGroup(instances, name)
		for _, n := range group {
			if i, ok := instances.Instances[n]; ok {
				if err := state.TryAcquireLock(i, sessionID, operation, 300); err != nil {
					return fmt.Errorf("instance %s: %s", n, err.Error())
				}
			}
		}
		return nil
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			fail(c, http.StatusNotFound, "instance not found: "+name)
		} else {
			fail(c, http.StatusConflict, err.Error())
		}
		return nil, nil, nil, err
	}

	// 加锁成功后查找服务器，失败时释放锁
	pool, err := s.state.ReadPool()
	if err != nil {
		s.releaseLockGroup(group, sessionID)
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}
	srv := pool.Servers[inst.Server]
	if srv == nil {
		s.releaseLockGroup(group, sessionID)
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return nil, nil, nil, fmt.Errorf("server not found")
	}

	unlock := func() { s.releaseLockGroup(group, sessionID) }
	return inst, srv, unlock, nil
}

// resolveAndLockInternal 与 resolveAndLock 相同，但不依赖 gin.Context，供内部定时任务使用。
func (s *Server) resolveAndLockInternal(name, operation string) (*apitypes.Instance, *apitypes.PoolServer, func(), error) {
	sessionID := fmt.Sprintf("scheduler-%d", time.Now().UnixNano())

	var inst *apitypes.Instance
	var group []string

	err := s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		var exists bool
		inst, exists = instances.Instances[name]
		if !exists {
			return fmt.Errorf("instance not found: %s", name)
		}
		group = state.InstanceGroup(instances, name)
		for _, n := range group {
			if i, ok := instances.Instances[n]; ok {
				if err := state.TryAcquireLock(i, sessionID, operation, 300); err != nil {
					return fmt.Errorf("instance %s: %s", n, err.Error())
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	pool, err := s.state.ReadPool()
	if err != nil {
		s.releaseLockGroup(group, sessionID)
		return nil, nil, nil, err
	}
	srv := pool.Servers[inst.Server]
	if srv == nil {
		s.releaseLockGroup(group, sessionID)
		return nil, nil, nil, fmt.Errorf("server not found: %s", inst.Server)
	}

	unlock := func() { s.releaseLockGroup(group, sessionID) }
	return inst, srv, unlock, nil
}

// releaseLockGroup 原子释放实例组的操作锁。
func (s *Server) releaseLockGroup(group []string, sessionID string) {
	s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		for _, n := range group {
			if i, ok := instances.Instances[n]; ok {
				state.ReleaseLock(i, sessionID)
			}
		}
		return nil
	})
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
