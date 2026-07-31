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
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/redisconf"
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
	state, err := s.db.ReadInstances()
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
		ok(c, (&apitypes.InstancesState{
			Groups:    filteredGroups,
			Instances: filteredInstances,
		}).ToView())
		return
	}
	ok(c, state.ToView())
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
	// 按派生状态过滤，保持 CLI 侧的词汇不变（running/stopped/failed/...）
	if statusFilter != "" && inst.Phase() != statusFilter {
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
	instances, err := s.db.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	inst, exists := instances.Instances[name]
	if !exists {
		fail(c, http.StatusNotFound, "instance not found: "+name)
		return
	}
	ok(c, inst.ToView())
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

	// 与 instanceConfig 同理：在调度和写库之前拦下非法的配置覆盖项。创建路径尤其
	// 需要提前拦，因为一旦走到后面，Agent 拒绝时 Server 已经写了 creating 记录、
	// 分配了端口，要走一遍回滚。
	if err := redisconf.ValidateOverrides(req.ConfigOverrides); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	// 查找目标服务器（需要在原子操作外读 node，因为调度需要 instances 快照）
	node, err := s.db.ReadNode()
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
	err = s.db.WithInstances(func(instances *apitypes.InstancesState) error {
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
			// 只写意图。观测字段留空，Phase() 会推导为 "creating"——
			// 语义是"要求它跑起来，但还没观测到"，比存一个 creating 字面值更准确
			DesiredState: apitypes.DesiredRunning,
			CreatedAt:    time.Now().Format(time.RFC3339),
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
	if _, err := client.postLong("/instance/create", req); err != nil {
		// 调用 Agent 清理已创建的资源（best-effort，忽略错误）
		client.post("/instance/delete", map[string]interface{}{
			"name":       req.Name,
			"engine":     req.Engine,
			"clean_data": true,
		})
		// 标记实例为 failed，保留记录供排查
		if markErr := s.db.WithInstances(func(instances *apitypes.InstancesState) error {
			if i := instances.Instances[req.Name]; i != nil {
				// 意图不变（用户仍然要这个实例），但已确认容器不存在。
				// 这是观测而非意图：Phase() 会推导为 "failed"
				i.ObservedStatus = apitypes.ObservedMissing
				state.RecalculateGroupTopology(instances, i.Group)
			}
			return nil
		}); markErr != nil {
			s.log.Errorf("instance create: failed to mark %s as failed: %v", req.Name, markErr)
		}
		s.audit.Log(store.AuditRecord{Operator: operatorFrom(c), Action: "instance.create", Level: audit.LevelImportant, Result: "failed", Detail: err.Error(),
			Target: map[string]interface{}{"instance": req.Name, "server": req.Server}})
		fail(c, http.StatusInternalServerError, "agent create failed: "+err.Error())
		return
	}

	// 更新状态为 running
	if err := s.db.WithInstances(func(instances *apitypes.InstancesState) error {
		if i := instances.Instances[req.Name]; i != nil {
			i.ObservedStatus = apitypes.ObservedRunning
			state.RecalculateGroupTopology(instances, i.Group)
		}
		return nil
	}); err != nil {
		s.log.Errorf("instance create: failed to mark %s as running: %v", req.Name, err)
	} else {
		inst.ObservedStatus = apitypes.ObservedRunning
	}

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   "instance.create",
		Level:    audit.LevelImportant,
		Result:   "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "group": groupName, "engine": req.Engine, "server": req.Server},
		Params:   map[string]interface{}{"memory": req.Memory, "cpus": req.CPUs, "category": req.Category, "engine_version": req.EngineVersion},
	})

	s.log.Infof("instance created: %s on %s", req.Name, req.Server)
	s.syncSentinel()
	// 用 ToView() 剥离密码字段。此前直接返回 inst，创建接口的响应体里带着 Redis 明文密码，
	// 会被 HTTP 访问日志、反向代理、CLI 调试输出等环节捕获。
	ok(c, inst.ToView())
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
	err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
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

	node, err := s.db.ReadNode()
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
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
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

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   "instance.delete", Level: audit.LevelCritical, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
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
		instances, err := s.db.ReadInstances()
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

	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok := is.Instances[req.Name]; ok {
			// 意图与观测一起更新：Agent 的 stop 调用成功返回本身就是一次观测。
			// 只写意图会让 Phase() 在 reconcile 跑到之前一直显示 "stopping"。
			i.DesiredState = apitypes.DesiredStopped
			i.ObservedStatus = apitypes.ObservedStopped
			state.RecalculateGroupTopology(is, i.Group)
		}
		return nil
	}); err != nil {
		s.log.Errorf("instance stop: failed to mark %s as stopped: %v", req.Name, err)
	}

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   "instance.stop", Level: audit.LevelImportant, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
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
	// 在动任何状态之前先校验。Agent 侧也会校验（它才是真正写配置文件的那一端，
	// 且 Agent API 可被直接访问，不能只依赖 Server 把门），这里提前拦一次是为了
	// 让用户拿到 400 和明确原因，而不是一个来自 Agent 的 500。
	// 两端共用 pkg/redisconf 的同一份规则，不存在放行标准不一致的可能。
	if err := redisconf.ValidateOverrides(req.ConfigOverrides); err != nil {
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
	// inst.ReplicaOf 存的是实例名（见 :343 与 :1002 的注释），而配置模板期待 ip:port
	// （config_template.go:157 把冒号替换成空格拼进 replicaof 指令）。此处此前直接传
	// inst.ReplicaOf，导致对 replica 执行 config --restart 时配置文件被写成
	// "replicaof order-master"——缺少端口，Redis 启动时报 wrong number of arguments，
	// 而容器已经被 Stop，实例起不来。必须先解析成地址。
	replicaOfAddr, err := s.resolveReplicaOfAddrFor(req.Name)
	if err != nil {
		fail(c, http.StatusInternalServerError, "resolve replica_of: "+err.Error())
		return
	}
	if _, err := client.post("/instance/config", map[string]interface{}{
		"name":             req.Name,
		"engine":           groupState.Engine,
		"type":             groupState.Type,
		"config_overrides": req.ConfigOverrides,
		"restart":          req.Restart,
		"password":         inst.Password,
		"memory":           inst.Memory,
		"category":         groupState.Category,
		"replica_of":       replicaOfAddr,
		"port":             inst.Port,
	}); err != nil {
		fail(c, http.StatusInternalServerError, "agent config failed: "+err.Error())
		return
	}

	// 更新 instances-state 中的 config_overrides
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok2 := is.Instances[req.Name]; ok2 {
			if i.ConfigOverrides == nil {
				i.ConfigOverrides = make(map[string]string)
			}
			for k, v := range req.ConfigOverrides {
				i.ConfigOverrides[k] = v
			}
		}
		return nil
	}); err != nil {
		s.log.Errorf("instance config: failed to persist config_overrides for %s: %v", req.Name, err)
	}

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   "config.update", Level: audit.LevelImportant, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
		Params:   toMap(req.ConfigOverrides),
	})

	ok(c, nil)
}

// instanceFailover 触发一次故障转移。
//
// 语义说明：若该实例所在的复制组由 Sentinel 托管（sentinel.enabled=true 且组类型为
// replication），本操作只是"请求对这个组做一次故障转移"，不保证 req.Name 对应的实例
// 一定会成为新 master —— 具体提升哪个 replica，由 Sentinel 自己的选举算法决定（复制
// 进度、replica-priority 等），这是 SENTINEL FAILOVER 命令本身的语义，不是本实现的限制。
// req.Name 在这种场景下只是用来定位"操作哪个复制组"的入口。真正的角色变更（新 master
// 提升、旧 master 降级）完全交给 Sentinel 执行，Server 只是等待并被动同步结果，不再向
// 任意节点直接下发 REPLICAOF——避免 Server 和 Sentinel 两个独立的决策者同时操作同一份
// 拓扑，产生冲突或状态不一致（这正是脑裂问题的根源之一）。
//
// 若该组未被 Sentinel 托管（Sentinel 未启用，或该组不是 replication 类型），退回原有的
// 直接操作方式：调用新 master 所在 Agent 执行 REPLICAOF NO ONE，并尝试让旧 master 追随
// 新 master——这是没有 Sentinel 可用时唯一的手段。
func (s *Server) instanceFailover(c *gin.Context) {
	start := time.Now()
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	instances, err := s.db.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	inst := instances.Instances[req.Name]
	if inst == nil {
		fail(c, http.StatusNotFound, "instance not found: "+req.Name)
		return
	}
	group := instances.Groups[inst.Group]
	if group == nil || group.Type != "replication" {
		fail(c, http.StatusBadRequest, "instance is not part of a replication group: "+req.Name)
		return
	}

	if s.cfg.Sentinel.Enabled {
		s.sentinelFailover(c, start, inst.Group, req.Name)
		return
	}
	s.directFailover(c, start, req.Name)
}

// sentinelFailover 通过 SENTINEL FAILOVER 触发故障转移，并等待 Sentinel 完成后
// 把结果同步到 Server 自己的状态记录（复用 handleSentinelFailover，不重复写状态更新逻辑）。
func (s *Server) sentinelFailover(c *gin.Context, start time.Time, group, requestedName string) {
	node, err := s.db.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	nodes := s.selectSentinelNodes(node)
	if !validSentinelNodeCount(len(nodes)) {
		fail(c, http.StatusInternalServerError, fmt.Sprintf("sentinel misconfigured: need exactly 3 or 5 sentinel.nodes, got %d", len(nodes)))
		return
	}

	beforeAddr, err := s.querySentinelMaster(node, nodes, group)
	if err != nil {
		s.log.Errorf("instance failover: query current sentinel master for group %s failed: %v", group, err)
	}

	var lastErr error
	triggered := false
	for _, name := range nodes {
		srv := node.Servers[name]
		if srv == nil || srv.Endpoint == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
		if _, err := sentinelCommand(addr, "SENTINEL", "FAILOVER", group); err != nil {
			lastErr = err
			continue
		}
		triggered = true
		break
	}
	if !triggered {
		fail(c, http.StatusInternalServerError, "failed to trigger sentinel failover: "+errString(lastErr))
		return
	}

	// SENTINEL FAILOVER 是异步的：轮询 GET-MASTER-ADDR-BY-NAME 等待 Sentinel 完成切换。
	// 超时时间参考 sentinel.failover_timeout 配置（默认场景下留出足够余量）。
	timeout := time.Duration(s.cfg.Sentinel.FailoverTimeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	var newAddr string
	for time.Now().Before(deadline) {
		time.Sleep(1 * time.Second)
		addr, err := s.querySentinelMaster(node, nodes, group)
		if err != nil {
			continue
		}
		if addr != "" && addr != beforeAddr {
			newAddr = addr
			break
		}
	}
	if newAddr == "" {
		fail(c, http.StatusGatewayTimeout, fmt.Sprintf("sentinel failover for group %s did not complete within %s", group, timeout))
		return
	}

	// GET-MASTER-ADDR-BY-NAME 在 failover 状态机走到 reconf-slaves 阶段就已经会返回新地址，
	// 但 Sentinel 内部还需要几百毫秒完成 failover-end/switch-master 事件、把旧 master
	// "虚拟加入"新 master 的 slave 列表（这是 Sentinel 后续自动纠正旧 master 角色的依据）。
	// 如果这个时候立刻调用 syncSentinel（REMOVE+MONITOR 该组以确保配置一致），会打断 Sentinel
	// 还没走完的收尾流程，抹掉它刚要建立的旧 master 追踪记录，导致旧 master 永远不会被自动
	// 降级——这是实测踩到的真实竞态（Kvrocks 复现明显，Redis 复现窗口更短但原理相同）。
	// 用一个保守的安全等待期，让 Sentinel 自己完成收尾后再触碰它的配置。
	time.Sleep(3 * time.Second)

	if err := s.handleSentinelFailover(group, newAddr, "manual", operatorFrom(c)); err != nil {
		fail(c, http.StatusInternalServerError, "failover triggered but failed to sync state: "+err.Error())
		return
	}

	instances, err := s.db.ReadInstances()
	newMasterName := requestedName
	if err == nil {
		if name := findInstanceByAddress(node, instances, newAddr); name != "" {
			newMasterName = name
		}
	}

	ok(c, map[string]interface{}{
		"group":       group,
		"new_master":  newMasterName,
		"requested":   requestedName,
		"matched":     newMasterName == requestedName,
		"duration_ms": time.Since(start).Milliseconds(),
	})
}

// directFailover 是 Sentinel 未启用时的 fallback：直接对新旧 master 下发 REPLICAOF，
// 语义上等价于原来的 instancePromote 实现，保证指定的 req.Name 一定会成为新 master。
func (s *Server) directFailover(c *gin.Context, start time.Time, name string) {
	inst, srv, unlock, err := s.resolveAndLock(c, name, "failover")
	if err != nil {
		return
	}
	defer unlock()

	if inst.Role != "replica" {
		fail(c, http.StatusBadRequest, "only replica can be promoted, current role: "+inst.Role)
		return
	}

	client := newAgentClient(srv)
	if _, err := client.post("/instance/promote", map[string]string{"name": name}); err != nil {
		fail(c, http.StatusInternalServerError, "agent promote failed: "+err.Error())
		return
	}

	// 找到当前 group 里被替换掉的旧 master，实际下发 REPLICAOF 让它追随新 master，
	// 避免旧 master 停留在独立 master 状态造成脑裂（双写导致数据不一致）。
	node, err := s.db.ReadNode()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	newMasterAddr := fmt.Sprintf("%s:%d", nodeEndpoint(node, inst.Server), inst.Port)
	oldMasterName := ""
	var oldMasterInst apitypes.Instance
	var oldMasterSrv apitypes.NodeServer
	hasOldMaster := false
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
		i := is.Instances[name]
		if i == nil {
			return nil
		}
		group := is.Groups[i.Group]
		if group != nil {
			if om := is.Instances[group.CurrentMaster]; om != nil && om != i {
				oldMasterName = group.CurrentMaster
				oldMasterInst = *om
				hasOldMaster = true
			}
		}
		return nil
	}); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if hasOldMaster {
		if s := node.Servers[oldMasterInst.Server]; s != nil {
			oldMasterSrv = *s
		} else {
			fail(c, http.StatusInternalServerError, "server not found: "+oldMasterInst.Server)
			return
		}
		oldClient := newAgentClient(&oldMasterSrv)
		if _, err := oldClient.post("/instance/replicate", map[string]string{
			"name":       oldMasterName,
			"replica_of": newMasterAddr,
		}); err != nil {
			// 旧 master 可能因为故障（网络不可达/进程宕机）才触发了本次故障转移，
			// 这种情况下无法阻塞 promote：新 master 已经完成提升，必须继续完成状态切换，
			// 否则会卡在"新 master 已提升但状态未落地"的中间态。
			// 仅记录警告，提示运维旧 master 需要人工确认/隔离，避免恢复后产生脑裂。
			s.log.Errorf("instance failover: failed to demote old master %s (may be unreachable, needs manual check to avoid split-brain): %v", oldMasterName, err)
		}
	}

	// 更新状态：角色变更 + group current_master 切换。
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
		i := is.Instances[name]
		if i == nil {
			return nil
		}
		group := is.Groups[i.Group]
		if group != nil {
			if oldMaster := is.Instances[group.CurrentMaster]; oldMaster != nil && oldMaster != i {
				oldMaster.Role = "replica"
				oldMaster.ReplicaOf = name
			}
			group.CurrentMaster = name
			group.Type = "replication"
		}
		i.Role = "master"
		i.ReplicaOf = ""
		state.RecalculateGroupTopology(is, i.Group)
		return nil
	}); err != nil {
		s.log.Errorf("instance failover: failed to persist role change for %s: %v", name, err)
		fail(c, http.StatusInternalServerError, "promote state update failed: "+err.Error())
		return
	}

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   "topology.failover", Level: audit.LevelCritical, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": name, "server": inst.Server},
	})

	// 把新角色写进各实例的磁盘配置文件，避免容器重启后角色回退（与 Sentinel 路径一致）。
	s.persistGroupRoles(inst.Group)
	s.syncSentinel()
	ok(c, map[string]interface{}{
		"new_master": name,
		"requested":  name,
		"matched":    true,
	})
}

func errString(err error) string {
	if err == nil {
		return "unknown error"
	}
	return err.Error()
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
	node, _ := s.db.ReadNode()
	instances, _ := s.db.ReadInstances()
	resolvedAddr, masterName, err := resolveReplicaOf(node, instances, req.ReplicaOf)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if masterName == "" {
		fail(c, http.StatusBadRequest, "replica_of must reference an existing managed master instance")
		return
	}

	// 手动指定复制目标与 Sentinel 的自动化管理直接冲突：Sentinel 会持续监控该组的
	// master/replica 拓扑，绕过它直接下发 REPLICAOF 可能在下一次 Sentinel 探测/failover
	// 时被覆盖或产生冲突判断。该组一旦被 Sentinel 托管，只允许通过 instance failover
	// 触发变更。
	if s.cfg.Sentinel.Enabled {
		if master := instances.Instances[masterName]; master != nil {
			if group := instances.Groups[master.Group]; group != nil && group.Type == "replication" {
				fail(c, http.StatusConflict, fmt.Sprintf("group %q is managed by Sentinel; use 'instance failover' to change replication topology instead of setting it directly", master.Group))
				return
			}
		}
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
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
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
	}); err != nil {
		s.log.Errorf("instance replicate: failed to persist role change for %s: %v", req.Name, err)
		fail(c, http.StatusInternalServerError, "replicate state update failed: "+err.Error())
		return
	}

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   "topology.replicate", Level: audit.LevelImportant, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
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
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok2 := is.Instances[name]; ok2 {
			if i.Backup == nil {
				i.Backup = &apitypes.BackupConfig{}
			}
			i.Backup.LastBackup = time.Now().Format(time.RFC3339)
		}
		return nil
	}); err != nil {
		s.log.Errorf("backup exec: failed to persist last_backup for %s: %v", name, err)
	}

	s.audit.Log(store.AuditRecord{
		Operator: operator,
		Action:   "backup.create", Level: audit.LevelNormal, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
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

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   "backup.restore", Level: audit.LevelCritical, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
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
	instances, err := s.db.ReadInstances()
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
	err := s.db.WithInstances(func(instances *apitypes.InstancesState) error {
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
func (s *Server) instanceSimpleOp(c *gin.Context, newStatus, agentPath, auditAction string, level string) {
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

	// start/stop 同时更新意图与观测：Agent 调用成功返回即为一次观测。
	// 其他操作（如 restart）不落状态，交给 reconcile 观测。
	type stateChange struct{ desired, observed string }
	stateMap := map[string]stateChange{
		"start": {apitypes.DesiredRunning, apitypes.ObservedRunning},
		"stop":  {apitypes.DesiredStopped, apitypes.ObservedStopped},
	}
	if err := s.db.WithInstances(func(is *apitypes.InstancesState) error {
		if i, ok2 := is.Instances[req.Name]; ok2 {
			if sc, ok3 := stateMap[newStatus]; ok3 {
				i.DesiredState = sc.desired
				i.ObservedStatus = sc.observed
				state.RecalculateGroupTopology(is, i.Group)
			}
		}
		return nil
	}); err != nil {
		s.log.Errorf("instance %s: failed to persist status for %s: %v", newStatus, req.Name, err)
	}

	s.audit.Log(store.AuditRecord{
		Operator: operatorFrom(c),
		Action:   auditAction, Level: level, Result: "success",
		DurationMs: time.Since(start).Milliseconds(),
		Target:   map[string]interface{}{"instance": req.Name, "server": inst.Server},
	})

	ok(c, nil)
}

// resolveInstance 查找实例及其所在服务器（只读操作用）
func (s *Server) resolveInstance(c *gin.Context, name string) (*apitypes.Instance, *apitypes.NodeServer, error) {
	instances, err := s.db.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return nil, nil, err
	}
	inst, exists := instances.Instances[name]
	if !exists {
		fail(c, http.StatusNotFound, "instance not found: "+name)
		return nil, nil, fmt.Errorf("instance not found: %s", name)
	}

	node, err := s.db.ReadNode()
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
	err := s.db.WithInstances(func(instances *apitypes.InstancesState) error {
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
	node, err := s.db.ReadNode()
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
	instances, err := s.db.ReadInstances()
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

	err := s.db.WithInstances(func(instances *apitypes.InstancesState) error {
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

	node, err := s.db.ReadNode()
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
	if err := s.db.WithInstances(func(instances *apitypes.InstancesState) error {
		for _, n := range group {
			if i, ok := instances.Instances[n]; ok {
				state.ReleaseLock(i, sessionID)
			}
		}
		return nil
	}); err != nil {
		s.log.Errorf("release lock: failed to persist unlock for session %s (group %v): %v", sessionID, group, err)
	}
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
	if strings.HasSuffix(s, "G") {
		n, _ := strconv.Atoi(strings.TrimSuffix(s, "G"))
		return n
	}
	if strings.HasSuffix(s, "Mi") {
		n, _ := strconv.Atoi(strings.TrimSuffix(s, "Mi"))
		return (n + 1023) / 1024
	}
	if strings.HasSuffix(s, "M") {
		n, _ := strconv.Atoi(strings.TrimSuffix(s, "M"))
		return (n + 1023) / 1024
	}
	return 0
}
