package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func normalizeClusterResource(r *apitypes.ClusterResourceSpec) error {
	r.Memory = strings.TrimSpace(r.Memory)
	if strings.HasSuffix(r.Memory, "M") || strings.HasSuffix(r.Memory, "G") {
		r.Memory += "i"
	}
	if err := validateCapacitySpec("memory", r.Memory); err != nil {
		return err
	}
	r.Disk = strings.TrimSpace(r.Disk)
	if r.Disk != "" {
		if err := validateCapacitySpec("disk", r.Disk); err != nil {
			return err
		}
	}
	if r.CPUs == 0 {
		r.CPUs = 1
	}
	if r.CPUs < 0 {
		return fmt.Errorf("cpus must be positive")
	}
	r.EngineVersion = strings.TrimSpace(r.EngineVersion)
	return nil
}

func (s *Server) clusterCreateFromSpec(c *gin.Context) {
	var req apitypes.ClusterCreateRequest
	if !coldBody(c, &req) {
		return
	}
	if req.Engine == "" {
		req.Engine = "kvrocks"
	}
	if (req.Engine != "kvrocks" && req.Engine != "redis") || (req.Engine == "redis" && req.Cache != nil) || !validProvisionName.MatchString(req.ID) || len(req.ID) > 32 || !validProvisionName.MatchString(req.GatewayID) || req.NodeID == "" || req.Cold.Count < 1 || req.Cold.Count > 128 || (len(req.Cold.Servers) != 0 && len(req.Cold.Servers) != req.Cold.Count) {
		fail(c, 400, "requires Redis (without cache) or Kvrocks, valid cluster ID (up to 32 chars), Gateway/node, 1..128 shards and either zero or count server entries")
		return
	}
	if err := normalizeClusterResource(&req.Cold.ClusterResourceSpec); err != nil {
		fail(c, 400, err.Error())
		return
	}
	if parseMemoryBytes(req.Cold.Memory) < 64<<20 {
		fail(c, 400, "storage shard memory must be at least 64Mi")
		return
	}
	if req.Cache != nil {
		if err := normalizeClusterResource(&req.Cache.ClusterResourceSpec); err != nil {
			fail(c, 400, err.Error())
			return
		}
		if req.Cache.WritePolicy == "" {
			req.Cache.WritePolicy = "invalidate"
		}
		if req.Cache.WritePolicy != "invalidate" && req.Cache.WritePolicy != "update_existing" {
			fail(c, 400, "unsupported cache write_policy")
			return
		}
	}
	raw, err := json.Marshal(req)
	if err != nil {
		fail(c, 400, err.Error())
		return
	}
	hash := sha256.Sum256(raw)
	digest := hex.EncodeToString(hash[:])
	reply := func(id string) {
		t, err := s.db.GetTask(id)
		if err != nil {
			fail(c, 500, "cannot read cluster task")
			return
		}
		c.JSON(202, apitypes.APIResponse{Success: true, Data: gin.H{"task_id": id, "cluster_id": req.ID, "status": t.Status, "phase": t.Phase}})
	}
	if id, err := s.db.ClusterPlanReplay(req.ID, digest); err != nil {
		s.coldError(c, err)
		return
	} else if id != "" {
		reply(id)
		return
	}
	if len(s.cfg.GatewayTokens[req.GatewayID]) < 16 {
		fail(c, 400, "configure Gateway machine credential first")
		return
	}
	nodes, err := s.db.ReadNode()
	if err != nil {
		fail(c, 500, "cannot load deployment nodes")
		return
	}
	if nodes.Servers[req.NodeID] == nil {
		fail(c, 400, "Gateway node not found")
		return
	}
	cache := req.Cache != nil
	plan := &store.ClusterPlan{Request: req, Digest: digest, Spec: apitypes.ColdClusterSpec{ID: req.ID, Engine: req.Engine, GatewayID: req.GatewayID, NodeID: req.NodeID, CacheEnabled: &cache}, Parent: store.Task{ID: newProvisionID(), Operator: operatorFrom(c), Source: "api", Trigger: "manual"}}
	if cache {
		plan.Spec.WritePolicy = req.Cache.WritePolicy
	}
	plan.Nodes = nodes
	var resources []apitypes.CreateInstanceRequest
	makeResource := func(name, engine, profile, category, host string, r apitypes.ClusterResourceSpec) error {
		version, image, err := s.resolveEngineImage(engine, r.EngineVersion)
		if err != nil {
			return err
		}
		resources = append(resources, apitypes.CreateInstanceRequest{Name: name, Group: name, Engine: engine, Profile: profile, Category: category, Type: "standalone", Server: host, Memory: r.Memory, CPUs: r.CPUs, Disk: r.Disk, NodeSelector: r.NodeSelector, EngineVersion: version, EngineImage: image, DisableProxy: true})
		return nil
	}
	for i := 0; i < req.Cold.Count; i++ {
		name := fmt.Sprintf("%s-cold-%d", req.ID, i+1)
		host := ""
		if len(req.Cold.Servers) > 0 {
			host = req.Cold.Servers[i]
		}
		if err := makeResource(name, req.Engine, req.Engine+"-storage", "persistent", host, req.Cold.ClusterResourceSpec); err != nil {
			fail(c, 400, err.Error())
			return
		}
		plan.Spec.Shards = append(plan.Spec.Shards, apitypes.ColdShard{ID: fmt.Sprintf("s%d", i+1), Instance: name})
	}
	if cache {
		plan.Spec.HotInstance = req.ID + "-hot"
		if err := makeResource(plan.Spec.HotInstance, "redis", "redis-cache", "cache", req.Cache.Server, req.Cache.ClusterResourceSpec); err != nil {
			fail(c, 400, err.Error())
			return
		}
	}
	err = s.db.ReserveClusterPlan(plan, func(st *apitypes.InstancesState) error {
		for _, r := range resources {
			if st.Instances[r.Name] != nil || st.Groups[r.Group] != nil {
				return fmt.Errorf("%w: resource name %s already exists", store.ErrColdConflict, r.Name)
			}
			eligible := &apitypes.NodeState{Servers: map[string]*apitypes.NodeServer{}}
			for name, node := range nodes.Servers {
				if r.Server != "" && name != r.Server {
					continue
				}
				if name != req.NodeID {
					ip, err := netip.ParseAddr(node.Endpoint)
					if err != nil || !ip.Unmap().IsPrivate() || ip.Zone() != "" {
						continue
					}
				}
				eligible.Servers[name] = node
			}
			host, err := selectServer(eligible, st, r.Memory, r.CPUs, r.Disk, "", r.NodeSelector)
			if err != nil {
				return fmt.Errorf("%w: %s: %v", store.ErrColdInvalid, r.Name, err)
			}
			r.Server = host
			node := nodes.Servers[host]
			r.Port, err = allocRedisPort(s.cfg.Ports, st, host)
			if err != nil {
				return fmt.Errorf("%w: %v", store.ErrColdInvalid, err)
			}
			id := newProvisionID()
			now := time.Now().Format(time.RFC3339)
			dir := "/data/redis/" + r.Name
			st.Groups[r.Group] = &apitypes.InstanceGroupState{Type: r.Type, Engine: r.Engine, EngineVersion: r.EngineVersion, Category: r.Category, AccessMode: "standard", CurrentMaster: r.Name, TopologyStatus: "degraded", Backup: defaultGroupBackup(r.Category, r.Group), CreatedAt: now, UpdatedAt: now}
			st.Instances[r.Name] = &apitypes.Instance{Group: r.Group, Role: "master", Profile: r.Profile, Server: host, Container: r.Engine + "-" + r.Name, Port: r.Port, Memory: r.Memory, CPUs: r.CPUs, Disk: r.Disk, ConfigPath: dir + "/conf", DataPath: dir + "/data", BackupPath: dir + "/backup", DesiredState: apitypes.DesiredRunning, CreatedAt: now, Lock: &apitypes.Lock{HeldBy: id, Operation: "profile.create", AcquiredAt: now, Timeout: 600}}
			if r.Engine == "redis" {
				storage := r.Profile == "redis-storage"
				st.Instances[r.Name].Persistence = &apitypes.Persistence{RDB: storage, AOF: storage}
			}
			payload, err := json.Marshal(profileTaskPayload{EnsureProfileRequest: apitypes.EnsureProfileRequest{OperationID: id, CreateInstanceRequest: r}, AgentEndpoint: node.Endpoint, AgentPort: node.AgentPort})
			if err != nil {
				return err
			}
			plan.Children = append(plan.Children, &store.Task{ID: id, Kind: "profile.create", Status: "pending", Phase: "queued", Operator: operatorFrom(c), Source: "api", Trigger: "manual", InstanceName: r.Name, GroupName: r.Group, ServerName: host, Payload: string(payload)})
		}
		return nil
	})
	if err != nil {
		// Concurrent identical submissions may race before either receipt exists.
		if id, replayErr := s.db.ClusterPlanReplay(req.ID, digest); replayErr == nil && id != "" {
			reply(id)
			return
		}
		s.coldError(c, err)
		return
	}
	s.coldAudit(c, "create-from-spec", req.ID, nil)
	reply(plan.Parent.ID)
}
