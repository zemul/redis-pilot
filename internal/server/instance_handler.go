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
func resolveReplicaOf(node *apitypes.NodeState, instances *apitypes.InstancesState, replicaOf string) (addr string, masterName string, err error) {
	if replicaOf == "" {
		return "", "", nil
	}
	if strings.Contains(replicaOf, ":") {
		// ip:port 格式，反查实例名
		for name, inst := range instances.Instances {
			if inst.Role == "master" {
				a := fmt.Sprintf("%s:%d", nodeEndpoint(node, inst.Server), inst.Port)
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
	if inst.Role != "master" {
		return "", "", fmt.Errorf("replica_of target must be master, got %s", inst.Role)
	}
	endpoint := nodeEndpoint(node, inst.Server)
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

func (s *Server) resolveEngineImage(engine, version string) (resolvedVersion, image string, err error) {
	engineCfg, ok := s.cfg.Images[engine]
	if !ok {
		return "", "", fmt.Errorf("unsupported engine: %s", engine)
	}
	if version == "" {
		version = engineCfg.Default
	}
	image, ok = engineCfg.Versions[version]
	if !ok || image == "" {
		return "", "", fmt.Errorf("unsupported %s version: %s", engine, version)
	}
	return version, image, nil
}

func (s *Server) instanceList(c *gin.Context) {
	state, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	groupFilter := c.Query("group")
	serverFilter := c.Query("server")
	if serverFilter == "" {
		serverFilter = c.Query("node")
	}
	engineFilter := c.Query("engine")
	engineVersionFilter := c.Query("engine_version")
	roleFilter := c.Query("role")
	statusFilter := c.Query("status")
	categoryFilter := c.Query("category")

	if groupFilter != "" || serverFilter != "" || engineFilter != "" || engineVersionFilter != "" ||
		roleFilter != "" || statusFilter != "" || categoryFilter != "" {
		filteredGroups := make(map[string]*apitypes.InstanceGroupState)
		filteredInstances := make(map[string]*apitypes.Instance)
		for name, inst := range state.Instances {
			group := state.Groups[inst.Group]
			if !instanceMatchesListFilters(inst, group, groupFilter, serverFilter, engineFilter, engineVersionFilter, roleFilter, statusFilter, categoryFilter) {
				continue
			}
			filteredInstances[name] = inst
			if group != nil {
				filteredGroups[inst.Group] = group
			}
		}
		if groupFilter != "" && len(filteredInstances) == 0 && serverFilter == "" && engineFilter == "" &&
			engineVersionFilter == "" && roleFilter == "" && statusFilter == "" && categoryFilter == "" {
			if g, exists := state.Groups[groupFilter]; exists {
				filteredGroups[groupFilter] = g
			}
		}
		ok(c, &apitypes.InstancesState{
			Groups:    filteredGroups,
			Instances: filteredInstances,
		})
		return
	}
	ok(c, state)
}

func instanceMatchesListFilters(inst *apitypes.Instance, group *apitypes.InstanceGroupState, groupFilter, serverFilter, engineFilter, engineVersionFilter, roleFilter, statusFilter, categoryFilter string) bool {
	if groupFilter != "" && inst.Group != groupFilter {
		return false
	}
	if serverFilter != "" && inst.Server != serverFilter {
		return false
	}
	if roleFilter != "" && inst.Role != roleFilter {
		return false
	}
	if statusFilter != "" && inst.Status != statusFilter {
		return false
	}
	if engineFilter != "" {
		if group == nil || group.Engine != engineFilter {
			return false
		}
	}
	if engineVersionFilter != "" {
		if group == nil || group.EngineVersion != engineVersionFilter {
			return false
		}
	}
	if categoryFilter != "" {
		if group == nil || group.Category != categoryFilter {
			return false
		}
	}
	return true
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
	req.Engine = strings.TrimSpace(req.Engine)
	req.EngineVersion = strings.TrimSpace(req.EngineVersion)
	if req.Engine != "redis" && req.Engine != "kvrocks" {
		fail(c, http.StatusBadRequest, "engine must be redis or kvrocks")
		return
	}
	resolvedVersion, engineImage, err := s.resolveEngineImage(req.Engine, req.EngineVersion)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	req.EngineVersion = resolvedVersion
	req.EngineImage = engineImage

	// 查找目标服务器（需要在原子操作外读 node，因为调度需要 instances 快照）
	node, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	dataDir := "/data/redis/" + req.Name
	role := "master"
	if req.ReplicaOf != "" {
		role = "replica"
	}
	req.Group = strings.TrimSpace(req.Group)

	var inst *apitypes.Instance
	var srv *apitypes.NodeServer
	var masterName string
	groupName := req.Group

	// 原子操作：检查冲突 → 调度 → 分配端口 → 写入 creating 状态
	var resolvedAddr string // replica_of 解析后的 ip:port，用于 Agent 调用
	err = s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		if _, exists := instances.Instances[req.Name]; exists {
			return fmt.Errorf("conflict: instance already exists: %s", req.Name)
		}

		// 解析 replica_of：支持实例名或 ip:port
		if req.ReplicaOf != "" {
			addr, mName, err := resolveReplicaOf(node, instances, req.ReplicaOf)
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
			group := instances.Groups[groupName]
			if groupName == "" || group == nil {
				return fmt.Errorf("replica_of target has no group: %s", masterName)
			}
			if group.Engine != req.Engine {
				return fmt.Errorf("replica engine must match master group engine: %s", group.Engine)
			}
			req.EngineVersion = group.EngineVersion
			_, engineImage, err = s.resolveEngineImage(req.Engine, req.EngineVersion)
			if err != nil {
				return err
			}
			req.EngineImage = engineImage
			if group.Type == "standalone" {
				group.Type = "replication"
			}
			if group.Envoy != nil && group.Envoy.AutoPort == 0 {
				if ec, err := allocEnvoyPorts(s.cfg.Ports, instances, true); err == nil {
					group.Envoy.AutoPort = ec.AutoPort
				}
			}
		} else {
			if groupName == "" {
				return fmt.Errorf("group is required for master or standalone instance")
			}
			if _, exists := instances.Groups[groupName]; exists {
				return fmt.Errorf("conflict: group already exists: %s", groupName)
			}
		}

		if req.Server == "" {
			selected, err := selectServer(node, instances, req.Memory, req.CPUs, req.Disk, resolvedAddr)
			if err != nil {
				return fmt.Errorf("schedule: %s", err.Error())
			}
			req.Server = selected
		}
		var exists bool
		srv, exists = node.Servers[req.Server]
		if !exists {
			return fmt.Errorf("server not found: %s", req.Server)
		}

		port, err := allocRedisPort(s.cfg.Ports, instances, req.Server)
		if err != nil {
			return err
		}
		req.Port = port

		if role == "master" {
			ec, err := allocEnvoyPorts(s.cfg.Ports, instances, req.Type == "replication")
			if err != nil {
				return err
			}
			now := time.Now().Format(time.RFC3339)
			instances.Groups[groupName] = &apitypes.InstanceGroupState{
				Type:           req.Type,
				Engine:         req.Engine,
				EngineVersion:  req.EngineVersion,
				Category:       req.Category,
				CurrentMaster:  req.Name,
				TopologyStatus: "degraded",
				Envoy:          ec,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
		}

		inst = &apitypes.Instance{
			Group:           groupName,
			Role:            role,
			Server:          req.Server,
			Container:       req.Engine + "-" + req.Name,
			Port:            req.Port,
			Memory:          req.Memory,
			CPUs:            req.CPUs,
			Disk:            req.Disk,
			Password:        req.Password,
			ConfigPath:      dataDir + "/conf",
			DataPath:        dataDir + "/data",
			BackupPath:      dataDir + "/backup",
			Persistence:     defaultPersistence(req.Engine, req.Category, req.ConfigOverrides),
			ConfigOverrides: req.ConfigOverrides,
			ReplicaOf:       masterName, // 存实例名而非 ip:port
			Status:          "creating",
			CreatedAt:       time.Now().Format(time.RFC3339),
		}
		instances.Instances[req.Name] = inst
		state.RecalculateGroupTopology(instances, groupName)

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
				state.RecalculateGroupTopology(instances, i.Group)
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
			state.RecalculateGroupTopology(instances, i.Group)
		}
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   "instance.create",
		Level:    audit.LevelImportant,
		Result:   "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "group": groupName, "engine": req.Engine, "server": req.Server},
		Params:   map[string]interface{}{"memory": req.Memory, "cpus": req.CPUs, "category": req.Category, "engine_version": req.EngineVersion},
	})

	s.log.Infof("instance created: %s on %s", req.Name, req.Server)
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
	var groupState apitypes.InstanceGroupState
	var group []string
	err := s.state.WithInstances(func(is *apitypes.InstancesState) error {
		i, exists := is.Instances[req.Name]
		if !exists {
			return fmt.Errorf("not found: %s", req.Name)
		}
		inst = *i
		if g := is.Groups[i.Group]; g != nil {
			groupState = *g
		} else {
			return fmt.Errorf("group not found: %s", i.Group)
		}
		group = state.InstanceGroup(is, req.Name)
		if i.Role == "master" && len(group) > 1 {
			return fmt.Errorf("cannot delete current master %s while group %s has replicas; failover first or delete the group", req.Name, i.Group)
		}
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

	node, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	srv := node.Servers[inst.Server]
	if srv == nil {
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return
	}

	client := newAgentClient(srv)
	if _, err := client.post("/instance/delete", map[string]interface{}{
		"name":       req.Name,
		"engine":     groupState.Engine,
		"clean_data": req.CleanData,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent delete failed: "+err.Error())
		return
	}

	// 原子删除实例记录
	if err := s.state.WithInstances(func(is *apitypes.InstancesState) error {
		groupName := inst.Group
		delete(is.Instances, req.Name)
		if len(state.InstanceGroup(is, req.Name)) == 1 && is.Instances[req.Name] == nil {
			hasAny := false
			for _, other := range is.Instances {
				if other != nil && other.Group == groupName {
					hasAny = true
					break
				}
			}
			if !hasAny {
				delete(is.Groups, groupName)
				return nil
			}
		}
		state.RecalculateGroupTopology(is, groupName)
		return nil
	}); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   "instance.delete", Level: audit.LevelCritical, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	s.log.Infof("instance deleted: %s", req.Name)
	if groupState.Type == "replication" && inst.Role == "master" {
		group := inst.Group
		s.removeSentinelMaster(group)
	}
	s.syncSentinel()
	ok(c, nil)
}

func (s *Server) instanceStart(c *gin.Context) {
	s.instanceSimpleOp(c, "start", "/instance/start", "instance.start", audit.LevelNormal)
}

func (s *Server) instanceStop(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name  string `json:"name" binding:"required"`
		Force bool   `json:"force"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	// 如果是主从组的 master，且未传 force，拒绝并给出提示
	if !req.Force {
		instances, err := s.state.ReadInstances()
		if err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		if inst, ok := instances.Instances[req.Name]; ok && inst.Role == "master" {
			if group := instances.Groups[inst.Group]; group != nil && group.Type == "replication" {
				fail(c, http.StatusBadRequest,
					fmt.Sprintf("stopping master %q will remove Sentinel monitoring for group %q; promote a replica first, or pass force=true to override", req.Name, inst.Group))
				return
			}
		}
	}

	inst, srv, unlock, err := s.resolveAndLock(c, req.Name, "stop")
	if err != nil {
		return
	}
	defer unlock()

	groupState, err := s.groupForInstance(req.Name)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	client := newAgentClient(srv)
	if _, err := client.post("/instance/stop", map[string]string{
		"name":   req.Name,
		"engine": groupState.Engine,
	}); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok := is.Instances[req.Name]; ok {
			i.Status = "stopped"
			state.RecalculateGroupTopology(is, i.Group)
		}
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   "instance.stop", Level: audit.LevelImportant, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	s.syncSentinel()
	ok(c, nil)
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

	groupState, err := s.groupForInstance(req.Name)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	client := newAgentClient(srv)
	if _, err := client.post("/instance/config", map[string]interface{}{
		"name":             req.Name,
		"engine":           groupState.Engine,
		"type":             groupState.Type,
		"config_overrides": req.ConfigOverrides,
		"restart":          req.Restart,
		"password":         inst.Password,
		"memory":           inst.Memory,
		"category":         groupState.Category,
		"replica_of":       inst.ReplicaOf,
		"port":             inst.Port,
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
		Action:   "config.update", Level: audit.LevelImportant, Result: "success",
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

	// 更新状态：角色变更 + group current_master 切换。
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		i := is.Instances[req.Name]
		if i == nil {
			return nil
		}
		group := is.Groups[i.Group]
		if group != nil {
			if oldMaster := is.Instances[group.CurrentMaster]; oldMaster != nil && oldMaster != i {
				oldMaster.Role = "replica"
				oldMaster.ReplicaOf = req.Name
			}
			group.CurrentMaster = req.Name
			group.Type = "replication"
		}
		i.Role = "master"
		i.ReplicaOf = ""
		state.RecalculateGroupTopology(is, i.Group)
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   "topology.failover", Level: audit.LevelCritical, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

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
	node, _ := s.state.ReadNode()
	instances, _ := s.state.ReadInstances()
	resolvedAddr, masterName, err := resolveReplicaOf(node, instances, req.ReplicaOf)
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

	// 更新状态：角色变更 + 归入目标 group。
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		i := is.Instances[req.Name]
		if i == nil {
			return nil
		}
		groupName := ""
		if masterName != "" {
			if master := is.Instances[masterName]; master != nil {
				groupName = master.Group
				group := is.Groups[groupName]
				if group != nil {
					group.Type = "replication"
					if group.Envoy != nil && group.Envoy.AutoPort == 0 {
						if ec, err := allocEnvoyPorts(s.cfg.Ports, is, true); err == nil {
							group.Envoy.AutoPort = ec.AutoPort
						}
					}
				}
			}
		}
		oldGroup := i.Group
		i.Role = "replica"
		i.ReplicaOf = masterName // 存实例名
		i.Group = groupName
		if oldGroup != "" && oldGroup != groupName {
			state.RecalculateGroupTopology(is, oldGroup)
		}
		state.RecalculateGroupTopology(is, groupName)
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   "topology.replicate", Level: audit.LevelImportant, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
		Params:   map[string]interface{}{"replica_of": req.ReplicaOf},
	})

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

	groupState, err := s.groupForInstance(name)
	if err != nil {
		return err
	}
	// 构建请求，传入 retention
	body := map[string]interface{}{
		"name":   name,
		"engine": groupState.Engine,
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
		Action:   "backup.create", Level: audit.LevelNormal, Result: "success",
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

	groupState, err := s.groupForInstance(req.Name)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	client := newAgentClient(srv)
	if _, err := client.post("/instance/restore", map[string]string{
		"name":      req.Name,
		"engine":    groupState.Engine,
		"backup_ts": req.BackupTs,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent restore failed: "+err.Error())
		return
	}

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   "backup.restore", Level: audit.LevelCritical, Result: "success",
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
			inst.Backup = &apitypes.BackupConfig{Retention: 7}
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

	groupState, err := s.groupForInstance(req.Name)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	client := newAgentClient(srv)
	if _, err := client.post(agentPath, map[string]string{
		"name":   req.Name,
		"engine": groupState.Engine,
	}); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	statusMap := map[string]string{"start": "running", "stop": "stopped"}
	s.state.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok2 := is.Instances[req.Name]; ok2 {
			if status, ok3 := statusMap[newStatus]; ok3 {
				i.Status = status
				state.RecalculateGroupTopology(is, i.Group)
			}
		}
		return nil
	})

	s.audit.Log(audit.Record{
		Operator: operatorFrom(c),
		Action:   auditAction, Level: level, Result: "success",
		Duration: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	ok(c, nil)
}

// resolveInstance 查找实例及其所在服务器（只读操作用）
func (s *Server) resolveInstance(c *gin.Context, name string) (*apitypes.Instance, *apitypes.NodeServer, error) {
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

	node, err := s.state.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, err
	}
	srv := node.Servers[inst.Server]
	if srv == nil {
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return nil, nil, fmt.Errorf("server not found: %s", inst.Server)
	}
	return inst, srv, nil
}

// resolveAndLock 查找实例 + 对整个实例组加操作锁（写操作用）。返回 unlock 函数供 defer 调用。
func (s *Server) resolveAndLock(c *gin.Context, name, operation string) (*apitypes.Instance, *apitypes.NodeServer, func(), error) {
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
	node, err := s.state.ReadNode()
	if err != nil {
		s.releaseLockGroup(group, sessionID)
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}
	srv := node.Servers[inst.Server]
	if srv == nil {
		s.releaseLockGroup(group, sessionID)
		fail(c, http.StatusBadRequest, "server not found: "+inst.Server)
		return nil, nil, nil, fmt.Errorf("server not found")
	}

	unlock := func() { s.releaseLockGroup(group, sessionID) }
	return inst, srv, unlock, nil
}

func (s *Server) groupForInstance(name string) (*apitypes.InstanceGroupState, error) {
	instances, err := s.state.ReadInstances()
	if err != nil {
		return nil, err
	}
	inst := instances.Instances[name]
	if inst == nil {
		return nil, fmt.Errorf("instance not found: %s", name)
	}
	group := instances.Groups[inst.Group]
	if group == nil {
		return nil, fmt.Errorf("group not found: %s", inst.Group)
	}
	return group, nil
}

// resolveAndLockInternal 与 resolveAndLock 相同，但不依赖 gin.Context，供内部定时任务使用。
func (s *Server) resolveAndLockInternal(name, operation string) (*apitypes.Instance, *apitypes.NodeServer, func(), error) {
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

	node, err := s.state.ReadNode()
	if err != nil {
		s.releaseLockGroup(group, sessionID)
		return nil, nil, nil, err
	}
	srv := node.Servers[inst.Server]
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
	if strings.HasSuffix(s, "Mi") {
		n, _ := strconv.Atoi(strings.TrimSuffix(s, "Mi"))
		return (n + 1023) / 1024
	}
	return 0
}
