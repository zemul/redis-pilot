package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) sentinelEvent(c *gin.Context) {
	var req struct {
		Event     string `json:"event"`
		Group     string `json:"group"`
		OldMaster string `json:"old_master"`
		NewMaster string `json:"new_master"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, 400, err.Error())
		return
	}
	s.log.Infof("sentinel event received: event=%s group=%s old=%s new=%s", req.Event, req.Group, req.OldMaster, req.NewMaster)
	if req.Group != "" && req.NewMaster != "" {
		if err := s.handleSentinelFailover(req.Group, req.NewMaster, "event", operatorFrom(c)); err != nil {
			fail(c, 500, err.Error())
			return
		}
	}
	ok(c, nil)
}

type sentinelNodeStatus struct {
	Name   string                   `json:"name"`
	Status *apitypes.SentinelStatus `json:"status,omitempty"`
	Error  string                   `json:"error,omitempty"`
}

type sentinelClusterStatus struct {
	Enabled bool                 `json:"enabled"`
	Port    int                  `json:"port"`
	Quorum  int                  `json:"quorum"`
	Nodes   []sentinelNodeStatus `json:"nodes"`
}

func (s *Server) sentinelStatus(c *gin.Context) {
	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	result := sentinelClusterStatus{
		Enabled: s.cfg.Sentinel.Enabled,
		Port:    s.sentinelPort(),
		Quorum:  s.sentinelQuorum(),
	}
	for _, name := range s.selectSentinelNodes(pool) {
		node := sentinelNodeStatus{Name: name}
		srv := pool.Servers[name]
		if srv == nil {
			node.Error = "server not found in pool-state"
			result.Nodes = append(result.Nodes, node)
			continue
		}
		status, err := s.queryAgentSentinelStatus(srv)
		if err != nil {
			node.Error = err.Error()
		} else {
			node.Status = status
		}
		result.Nodes = append(result.Nodes, node)
	}
	ok(c, result)
}

func (s *Server) sentinelSync(c *gin.Context) {
	if err := s.syncSentinelMasters(); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (s *Server) StartSentinelReconcileLoop() {
	if !s.cfg.Sentinel.Enabled {
		return
	}
	go func() {
		s.reconcileSentinel()
		for range time.Tick(10 * time.Second) {
			s.reconcileSentinel()
		}
	}()
}

func (s *Server) reconcileSentinel() {
	if err := s.reconcileSentinelOnce(); err != nil {
		s.log.Errorf("sentinel reconcile failed: %v", err)
	}
}

func (s *Server) reconcileSentinelOnce() error {
	if !s.cfg.Sentinel.Enabled {
		return nil
	}
	pool, err := s.state.ReadPool()
	if err != nil {
		return err
	}
	instances, err := s.state.ReadInstances()
	if err != nil {
		return err
	}
	nodes := s.selectSentinelNodes(pool)
	if len(nodes) == 0 {
		s.log.Infof("sentinel reconcile skipped: no sentinel.nodes configured")
		return nil
	}
	if !validSentinelNodeCount(len(nodes)) {
		return fmt.Errorf("invalid configured sentinel node count: got=%d want=3 or 5", len(nodes))
	}
	if len(nodes) < s.sentinelQuorum() {
		return fmt.Errorf("not enough configured sentinel nodes: have=%d quorum=%d", len(nodes), s.sentinelQuorum())
	}
	for _, master := range s.buildSentinelMasters(pool, instances) {
		current, err := s.querySentinelMaster(pool, nodes, master.Group)
		if err != nil {
			s.log.Errorf("query sentinel master %s failed: %v", master.Group, err)
			continue
		}
		expected := fmt.Sprintf("%s:%d", master.Host, master.Port)
		if current != "" && current != expected {
			if err := s.handleSentinelFailover(master.Group, current, "reconcile", "system"); err != nil {
				s.log.Errorf("handle sentinel failover group=%s new=%s failed: %v", master.Group, current, err)
			}
		}
	}
	return nil
}

func (s *Server) querySentinelMaster(pool *apitypes.PoolState, nodes []string, group string) (string, error) {
	var firstErr error
	for _, node := range nodes {
		srv := pool.Servers[node]
		if srv == nil || srv.Endpoint == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
		master, err := sentinelGetMasterAddr(addr, group)
		if err == nil {
			return master, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("no sentinel nodes available")
	}
	return "", firstErr
}

func (s *Server) queryAgentSentinelStatus(srv *apitypes.PoolServer) (*apitypes.SentinelStatus, error) {
	data, err := newAgentClient(srv).get("/sentinel/status")
	if err != nil {
		return nil, err
	}
	var resp apitypes.APIResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}
	encoded, err := json.Marshal(resp.Data)
	if err != nil {
		return nil, err
	}
	var status apitypes.SentinelStatus
	if err := json.Unmarshal(encoded, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func sentinelGetMasterAddr(addr, group string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	cmd := "get-master-addr-by-name"
	if _, err := fmt.Fprintf(conn, "*3\r\n$8\r\nSENTINEL\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cmd), cmd, len(group), group); err != nil {
		return "", err
	}
	reader := bufio.NewReader(conn)
	values, err := readRESPArrayStrings(reader)
	if err != nil {
		return "", err
	}
	if len(values) < 2 {
		return "", fmt.Errorf("sentinel returned %d values", len(values))
	}
	return values[0] + ":" + values[1], nil
}

func readRESPArrayStrings(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "-") {
		return nil, fmt.Errorf("sentinel error: %s", strings.TrimPrefix(line, "-"))
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	count, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}
	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		header, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimSpace(header)
		if !strings.HasPrefix(header, "$") {
			return nil, fmt.Errorf("expected bulk string, got %q", header)
		}
		n, err := strconv.Atoi(strings.TrimPrefix(header, "$"))
		if err != nil {
			return nil, err
		}
		if n < 0 {
			values = append(values, "")
			continue
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		values = append(values, string(buf[:n]))
	}
	return values, nil
}

func (s *Server) handleSentinelFailover(group, newMasterAddr, source, operator string) error {
	sessionID := fmt.Sprintf("sentinel-%s-%d", group, time.Now().UnixNano())
	start := time.Now()
	var oldMasterName, oldMasterServer, newMasterName, newMasterServer string
	var envoy *apitypes.EnvoyConfig
	var oldMasterFound bool
	var replicaCount int

	err := s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		groupNames := instanceNamesByGroup(instances, group)
		if len(groupNames) == 0 {
			groupNames = state.InstanceGroup(instances, group)
		}
		var oldMaster *apitypes.Instance
		for _, name := range groupNames {
			inst := instances.Instances[name]
			if inst != nil && inst.Role == "master" && inst.Type == "replication" {
				oldMasterName = name
				oldMaster = inst
				break
			}
		}
		if oldMaster == nil {
			for name, inst := range instances.Instances {
				if inst != nil && inst.Group == "" && name == group && inst.Role == "master" && inst.Type == "replication" {
					oldMasterName = name
					oldMaster = inst
					break
				}
			}
		}
		if oldMaster == nil {
			return fmt.Errorf("old master not found for group %s", group)
		}
		oldMasterFound = true
		for _, name := range groupNames {
			if inst := instances.Instances[name]; inst != nil {
				if err := state.TryAcquireLock(inst, sessionID, "sentinel-failover", 60); err != nil {
					return fmt.Errorf("instance %s: %w", name, err)
				}
			}
		}
		defer func() {
			for _, name := range groupNames {
				if inst := instances.Instances[name]; inst != nil {
					state.ReleaseLock(inst, sessionID)
				}
			}
		}()

		pool, err := s.state.ReadPool()
		if err != nil {
			return err
		}
		newMasterName = findInstanceByAddress(pool, instances, newMasterAddr)
		if newMasterName == "" {
			return fmt.Errorf("new master %s not found in instances-state", newMasterAddr)
		}
		newMaster := instances.Instances[newMasterName]
		if newMaster == nil {
			return fmt.Errorf("new master instance missing: %s", newMasterName)
		}
		if newMaster.Group != "" && newMaster.Group != group {
			return fmt.Errorf("new master %s belongs to group %s, not %s", newMasterName, newMaster.Group, group)
		}
		newMaster.Group = group
		oldMaster.Group = group

		if oldMaster.Role == "master" && oldMaster.Envoy != nil {
			envoy = oldMaster.Envoy
			oldMaster.Envoy = nil
		}
		if newMaster.Envoy == nil && envoy != nil {
			newMaster.Envoy = envoy
		}
		oldMasterServer = oldMaster.Server
		newMasterServer = newMaster.Server

		for _, inst := range instances.Instances {
			if inst == nil {
				continue
			}
			filtered := inst.Replicas[:0]
			for _, replica := range inst.Replicas {
				if replica != newMasterName {
					filtered = append(filtered, replica)
				}
			}
			inst.Replicas = filtered
		}

		oldMaster.Role = "replica"
		oldMaster.Status = "failed"
		oldMaster.ReplicaOf = newMasterName
		oldMaster.Replicas = nil

		newMaster.Role = "master"
		newMaster.Status = "running"
		newMaster.ReplicaOf = ""
		newMaster.Replicas = nil

		for name, inst := range instances.Instances {
			if inst == nil || name == newMasterName {
				continue
			}
			if name == oldMasterName || containsString(groupNames, name) {
				inst.Group = group
				if inst.Status == "running" {
					inst.Role = "replica"
					inst.ReplicaOf = newMasterName
					newMaster.Replicas = append(newMaster.Replicas, name)
					replicaCount++
				}
			}
		}
		sort.Strings(newMaster.Replicas)
		return nil
	})
	if err != nil {
		s.audit.Log(audit.Record{
			Operator: operator,
			Action:   "topology.failover", Level: audit.LevelCritical, Result: "failed",
			Duration: time.Since(start).Milliseconds(),
			Target:   map[string]interface{}{"instance_group": group, "new_master": newMasterAddr},
			Params:   map[string]interface{}{"source": source},
			Detail:   err.Error(),
		})
		return err
	}
	if !oldMasterFound {
		return fmt.Errorf("old master not found for group %s", group)
	}

	result := "success"
	detail := "sentinel failover synchronized"
	if replicaCount == 0 {
		result = "degraded"
		detail = "sentinel failover synchronized; replica replenishment is still required"
	}
	s.audit.Log(audit.Record{
		Operator: operator,
		Action:   "topology.failover", Level: audit.LevelCritical, Result: result,
		Duration: time.Since(start).Milliseconds(),
		Target: map[string]interface{}{
			"instance_group":     group,
			"old_master_server":  oldMasterServer,
			"new_master":         newMasterName,
			"new_master_addr":    newMasterAddr,
			"new_master_server":  newMasterServer,
			"remaining_replicas": replicaCount,
		},
		Params: map[string]interface{}{"source": source},
		Detail: detail,
	})
	s.refreshEnvoy()
	s.syncSentinel()
	return nil
}

func findInstanceByAddress(pool *apitypes.PoolState, instances *apitypes.InstancesState, addr string) string {
	for name, inst := range instances.Instances {
		if inst == nil {
			continue
		}
		if fmt.Sprintf("%s:%d", poolEndpoint(pool, inst.Server), inst.Port) == addr {
			return name
		}
	}
	return ""
}

func instanceNamesByGroup(instances *apitypes.InstancesState, group string) []string {
	names := make([]string, 0)
	if group == "" {
		return names
	}
	for name, inst := range instances.Instances {
		if inst != nil && inst.Group == group {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (s *Server) syncSentinel() {
	if err := s.syncSentinelMasters(); err != nil {
		s.log.Errorf("sentinel sync failed: %v", err)
	}
}

func (s *Server) syncSentinelMasters() error {
	if !s.cfg.Sentinel.Enabled {
		return nil
	}
	pool, err := s.state.ReadPool()
	if err != nil {
		return err
	}
	instances, err := s.state.ReadInstances()
	if err != nil {
		return err
	}

	nodes := s.selectSentinelNodes(pool)
	if !validSentinelNodeCount(len(nodes)) {
		return fmt.Errorf("sentinel disabled: configure exactly 3 or 5 sentinel.nodes, got %d", len(nodes))
	}

	masters := s.buildSentinelMasters(pool, instances)
	req := apitypes.SentinelSyncRequest{
		Port:    s.sentinelPort(),
		Quorum:  s.sentinelQuorum(),
		Masters: masters,
	}

	var firstErr error
	success := 0
	for _, name := range nodes {
		srv := pool.Servers[name]
		if srv == nil {
			continue
		}
		if _, err := newAgentClient(srv).post("/sentinel/sync", req); err != nil {
			s.log.Errorf("sentinel sync failed on %s: %v", name, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		success++
	}
	if success < req.Quorum {
		return fmt.Errorf("sentinel quorum not reached: success=%d quorum=%d: %w", success, req.Quorum, firstErr)
	}
	return nil
}

func (s *Server) removeSentinelMaster(group string) {
	if !s.cfg.Sentinel.Enabled || group == "" {
		return
	}
	pool, err := s.state.ReadPool()
	if err != nil {
		s.log.Errorf("sentinel remove read pool: %v", err)
		return
	}
	req := apitypes.SentinelRemoveMasterRequest{Group: group, Port: s.sentinelPort()}
	for _, name := range s.selectSentinelNodes(pool) {
		srv := pool.Servers[name]
		if srv == nil {
			continue
		}
		if _, err := newAgentClient(srv).post("/sentinel/remove-master", req); err != nil {
			s.log.Errorf("sentinel remove %s on %s failed: %v", group, name, err)
		}
	}
}

func (s *Server) buildSentinelMasters(pool *apitypes.PoolState, instances *apitypes.InstancesState) []apitypes.SentinelMaster {
	var masters []apitypes.SentinelMaster
	for name, inst := range instances.Instances {
		if inst == nil || inst.Type != "replication" || inst.Role != "master" || inst.Status != "running" {
			continue
		}
		group := inst.Group
		if group == "" {
			group = name
		}
		endpoint := poolEndpoint(pool, inst.Server)
		if endpoint == "" {
			continue
		}
		masters = append(masters, apitypes.SentinelMaster{
			Group:                 group,
			Host:                  endpoint,
			Port:                  inst.Port,
			Password:              inst.Password,
			DownAfterMilliseconds: s.sentinelDownAfter(),
			FailoverTimeout:       s.sentinelFailoverTimeout(),
			ParallelSyncs:         s.sentinelParallelSyncs(),
		})
	}
	sort.Slice(masters, func(i, j int) bool { return masters[i].Group < masters[j].Group })
	return masters
}

func (s *Server) selectSentinelNodes(pool *apitypes.PoolState) []string {
	var selected []string
	seen := map[string]bool{}
	for _, name := range s.cfg.Sentinel.Nodes {
		if name == "" || seen[name] {
			continue
		}
		if pool.Servers[name] == nil {
			s.log.Errorf("configured sentinel node %s not found in pool-state", name)
			continue
		}
		selected = append(selected, name)
		seen[name] = true
	}
	return selected
}

func (s *Server) sentinelReplicas() int {
	if len(s.cfg.Sentinel.Nodes) > 0 {
		return len(s.cfg.Sentinel.Nodes)
	}
	if s.cfg.Sentinel.Replicas == 5 {
		return 5
	}
	return 3
}

func validSentinelNodeCount(count int) bool {
	return count == 3 || count == 5
}

func (s *Server) sentinelQuorum() int {
	if s.cfg.Sentinel.Quorum > 0 {
		return s.cfg.Sentinel.Quorum
	}
	if s.sentinelReplicas() == 5 {
		return 3
	}
	return 2
}

func (s *Server) sentinelPort() int {
	if s.cfg.Sentinel.Port > 0 {
		return s.cfg.Sentinel.Port
	}
	return 26379
}

func (s *Server) sentinelDownAfter() int {
	if s.cfg.Sentinel.DownAfterMilliseconds > 0 {
		return s.cfg.Sentinel.DownAfterMilliseconds
	}
	return 5000
}

func (s *Server) sentinelFailoverTimeout() int {
	if s.cfg.Sentinel.FailoverTimeout > 0 {
		return s.cfg.Sentinel.FailoverTimeout
	}
	return 30000
}

func (s *Server) sentinelParallelSyncs() int {
	if s.cfg.Sentinel.ParallelSyncs > 0 {
		return s.cfg.Sentinel.ParallelSyncs
	}
	return 1
}

func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
