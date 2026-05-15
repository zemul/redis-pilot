package server

import (
	"bufio"
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
		newMaster := req.NewMaster
		if confirmed, err := s.confirmSentinelMaster(req.Group); err == nil && confirmed != "" {
			newMaster = confirmed
		} else if err != nil {
			s.log.Errorf("sentinel event confirm failed group=%s fallback=%s: %v", req.Group, req.NewMaster, err)
		}
		if err := s.handleSentinelFailover(req.Group, newMaster, "event", operatorFrom(c)); err != nil {
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
		if srv == nil || srv.Endpoint == "" {
			node.Error = "server not found in pool-state"
			result.Nodes = append(result.Nodes, node)
			continue
		}
		status, err := s.querySentinelStatus(srv)
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

func (s *Server) StartSentinelWatchLoop() {
	if !s.cfg.Sentinel.Enabled {
		return
	}
	nodes := uniqueStrings(s.cfg.Sentinel.Nodes)
	if len(nodes) == 0 {
		return
	}
	for _, node := range nodes {
		go s.watchSentinelNode(node)
	}
}

func (s *Server) watchSentinelNode(node string) {
	backoff := time.Second
	for {
		addr, err := s.sentinelNodeAddress(node)
		if err != nil {
			s.log.Errorf("sentinel watcher %s waiting for node address: %v", node, err)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		s.log.Infof("sentinel watcher %s connecting to %s", node, addr)
		backoff = time.Second
		err = s.subscribeSentinelSwitchMaster(addr, func(group string) {
			current, err := s.confirmSentinelMaster(group)
			if err != nil {
				s.log.Errorf("sentinel watcher %s confirm master group=%s failed: %v", node, group, err)
				return
			}
			if current == "" {
				s.log.Errorf("sentinel watcher %s confirm master group=%s returned empty address", node, group)
				return
			}
			if err := s.handleSentinelFailover(group, current, "watch", "system"); err != nil {
				s.log.Errorf("sentinel watcher %s handle failover group=%s new=%s failed: %v", node, group, current, err)
			}
		})
		s.log.Errorf("sentinel watcher %s disconnected: %v", node, err)
		time.Sleep(backoff)
		backoff = nextBackoff(backoff)
	}
}

func (s *Server) sentinelNodeAddress(node string) (string, error) {
	pool, err := s.state.ReadPool()
	if err != nil {
		return "", err
	}
	srv := pool.Servers[node]
	if srv == nil || srv.Endpoint == "" {
		return "", fmt.Errorf("sentinel node %s not found in pool-state", node)
	}
	return fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort()), nil
}

func (s *Server) subscribeSentinelSwitchMaster(addr string, onSwitch func(group string)) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := writeRESPCommand(conn, "SUBSCRIBE", "+switch-master"); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	for {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
		reply, err := readRESP(reader)
		if err != nil {
			return err
		}
		group, ok := parseSwitchMasterMessage(reply)
		if ok {
			onSwitch(group)
		}
	}
}

func parseSwitchMasterMessage(reply interface{}) (string, bool) {
	values, ok := reply.([]interface{})
	if !ok || len(values) < 3 {
		return "", false
	}
	kind, _ := values[0].(string)
	channel, _ := values[1].(string)
	payload, _ := values[2].(string)
	if kind != "message" || channel != "+switch-master" {
		return "", false
	}
	fields := strings.Fields(payload)
	if len(fields) < 5 {
		return "", false
	}
	return fields[0], true
}

func (s *Server) confirmSentinelMaster(group string) (string, error) {
	pool, err := s.state.ReadPool()
	if err != nil {
		return "", err
	}
	nodes := s.selectSentinelNodes(pool)
	if len(nodes) == 0 {
		return "", errors.New("no sentinel nodes configured")
	}
	return s.querySentinelMaster(pool, nodes, group)
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
	// 遍历所有 replication group，不依赖 master 是否 running
	for groupName, group := range instances.Groups {
		if group == nil || group.Type != "replication" || group.CurrentMaster == "" {
			continue
		}
		inst := instances.Instances[group.CurrentMaster]
		if inst == nil {
			continue
		}
		endpoint := poolEndpoint(pool, inst.Server)
		if endpoint == "" {
			continue
		}
		expected := fmt.Sprintf("%s:%d", endpoint, inst.Port)
		current, err := s.querySentinelMaster(pool, nodes, groupName)
		if err != nil {
			s.log.Errorf("query sentinel master %s failed: %v", groupName, err)
			continue
		}
		if current != "" && current != expected {
			if err := s.handleSentinelFailover(groupName, current, "reconcile", "system"); err != nil {
				s.log.Errorf("handle sentinel failover group=%s new=%s failed: %v", groupName, current, err)
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

func (s *Server) querySentinelStatus(srv *apitypes.PoolServer) (*apitypes.SentinelStatus, error) {
	addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
	reply, err := sentinelCommand(addr, "SENTINEL", "MASTERS")
	if err != nil {
		return nil, err
	}
	return &apitypes.SentinelStatus{
		Running: true,
		Port:    s.sentinelPort(),
		Masters: sentinelMasterNames(reply),
	}, nil
}

func sentinelGetMasterAddr(addr, group string) (string, error) {
	reply, err := sentinelCommand(addr, "SENTINEL", "get-master-addr-by-name", group)
	if err != nil {
		return "", err
	}
	values, ok := reply.([]interface{})
	if !ok {
		return "", fmt.Errorf("expected array, got %T", reply)
	}
	if len(values) < 2 {
		return "", fmt.Errorf("sentinel returned %d values", len(values))
	}
	host, ok := values[0].(string)
	if !ok {
		return "", fmt.Errorf("sentinel returned non-string host: %T", values[0])
	}
	port, ok := values[1].(string)
	if !ok {
		return "", fmt.Errorf("sentinel returned non-string port: %T", values[1])
	}
	return host + ":" + port, nil
}

func sentinelCommand(addr string, args ...string) (interface{}, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeRESPCommand(conn, args...); err != nil {
		return nil, err
	}
	return readRESP(bufio.NewReader(conn))
}

func writeRESPCommand(w io.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

func readRESP(r *bufio.Reader) (interface{}, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, errors.New("empty RESP line")
	}
	switch line[0] {
	case '+':
		return strings.TrimPrefix(line, "+"), nil
	case '-':
		return nil, fmt.Errorf("sentinel error: %s", strings.TrimPrefix(line, "-"))
	case ':':
		return strconv.Atoi(strings.TrimPrefix(line, ":"))
	case '$':
		n, err := strconv.Atoi(strings.TrimPrefix(line, "$"))
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return "", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		count, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return []interface{}(nil), nil
		}
		values := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			value, err := readRESP(r)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unknown RESP type %q", line)
	}
}

func sentinelMasterNames(reply interface{}) []string {
	masters, ok := reply.([]interface{})
	if !ok {
		return nil
	}
	names := make([]string, 0, len(masters))
	for _, rawMaster := range masters {
		fields, ok := rawMaster.([]interface{})
		if !ok {
			continue
		}
		for i := 0; i+1 < len(fields); i += 2 {
			key, _ := fields[i].(string)
			if key != "name" {
				continue
			}
			name, ok := fields[i+1].(string)
			if ok {
				names = append(names, name)
			}
			break
		}
	}
	sort.Strings(names)
	return names
}

func (s *Server) handleSentinelFailover(group, newMasterAddr, source, operator string) error {
	sessionID := fmt.Sprintf("sentinel-%s-%d", group, time.Now().UnixNano())
	start := time.Now()
	var oldMasterName, oldMasterServer, newMasterName, newMasterServer string
	var oldMasterFound bool
	var noOp bool

	err := s.state.WithInstances(func(instances *apitypes.InstancesState) error {
		groupState := instances.Groups[group]
		if groupState == nil || groupState.Type != "replication" {
			return fmt.Errorf("replication group not found: %s", group)
		}
		groupNames := instanceNamesByGroup(instances, group)
		oldMasterName = groupState.CurrentMaster
		oldMaster := instances.Instances[oldMasterName]
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
		if newMaster.Group != group {
			return fmt.Errorf("new master %s belongs to group %s, not %s", newMasterName, newMaster.Group, group)
		}
		if groupState.CurrentMaster == newMasterName && newMaster.Role == "master" && newMaster.Status == "running" {
			noOp = true
			return nil
		}
		oldMasterServer = oldMaster.Server
		newMasterServer = newMaster.Server

		oldMaster.Role = "replica"
		oldMaster.Status = "failed"
		oldMaster.ReplicaOf = newMasterName

		newMaster.Role = "master"
		newMaster.Status = "running"
		newMaster.ReplicaOf = ""
		groupState.CurrentMaster = newMasterName

		for name, inst := range instances.Instances {
			if inst == nil || name == newMasterName {
				continue
			}
			if name == oldMasterName || containsString(groupNames, name) {
				inst.Group = group
				if inst.Status == "running" {
					inst.Role = "replica"
					inst.ReplicaOf = newMasterName
				}
			}
		}
		state.RecalculateGroupTopology(instances, group)
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
	if noOp {
		s.log.Infof("sentinel failover already synchronized: group=%s master=%s source=%s", group, newMasterName, source)
		return nil
	}

	result := "success"
	detail := "sentinel failover synchronized"
	s.audit.Log(audit.Record{
		Operator: operator,
		Action:   "topology.failover", Level: audit.LevelCritical, Result: result,
		Duration: time.Since(start).Milliseconds(),
		Target: map[string]interface{}{
			"instance_group":    group,
			"old_master_server": oldMasterServer,
			"new_master":        newMasterName,
			"new_master_addr":   newMasterAddr,
			"new_master_server": newMasterServer,
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

	var firstErr error
	success := 0
	for _, name := range nodes {
		srv := pool.Servers[name]
		if srv == nil || srv.Endpoint == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
		if err := syncSentinelNode(addr, masters, s.sentinelQuorum()); err != nil {
			s.log.Errorf("sentinel sync failed on %s: %v", name, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		success++
	}
	if success < s.sentinelQuorum() {
		return fmt.Errorf("sentinel quorum not reached: success=%d quorum=%d: %w", success, s.sentinelQuorum(), firstErr)
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
	for _, name := range s.selectSentinelNodes(pool) {
		srv := pool.Servers[name]
		if srv == nil || srv.Endpoint == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", srv.Endpoint, s.sentinelPort())
		if _, err := sentinelCommand(addr, "SENTINEL", "REMOVE", group); err != nil {
			s.log.Errorf("sentinel remove %s on %s failed: %v", group, name, err)
		}
	}
}

func syncSentinelNode(addr string, desired []apitypes.SentinelMaster, quorum int) error {
	reply, err := sentinelCommand(addr, "SENTINEL", "MASTERS")
	if err != nil {
		return err
	}
	desiredByGroup := map[string]apitypes.SentinelMaster{}
	for _, master := range desired {
		desiredByGroup[master.Group] = master
	}
	for _, group := range sentinelMasterNames(reply) {
		if _, ok := desiredByGroup[group]; !ok {
			if _, err := sentinelCommand(addr, "SENTINEL", "REMOVE", group); err != nil {
				return err
			}
		}
	}
	for _, master := range desired {
		_, _ = sentinelCommand(addr, "SENTINEL", "REMOVE", master.Group)
		if _, err := sentinelCommand(addr, "SENTINEL", "MONITOR", master.Group, master.Host, fmt.Sprintf("%d", master.Port), fmt.Sprintf("%d", quorum)); err != nil {
			return err
		}
		if master.Password != "" {
			if _, err := sentinelCommand(addr, "SENTINEL", "SET", master.Group, "auth-pass", master.Password); err != nil {
				return err
			}
		}
		if _, err := sentinelCommand(addr, "SENTINEL", "SET", master.Group, "down-after-milliseconds", fmt.Sprintf("%d", master.DownAfterMilliseconds)); err != nil {
			return err
		}
		if _, err := sentinelCommand(addr, "SENTINEL", "SET", master.Group, "failover-timeout", fmt.Sprintf("%d", master.FailoverTimeout)); err != nil {
			return err
		}
		if _, err := sentinelCommand(addr, "SENTINEL", "SET", master.Group, "parallel-syncs", fmt.Sprintf("%d", master.ParallelSyncs)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) buildSentinelMasters(pool *apitypes.PoolState, instances *apitypes.InstancesState) []apitypes.SentinelMaster {
	var masters []apitypes.SentinelMaster
	for groupName, group := range instances.Groups {
		if group == nil || group.Type != "replication" {
			continue
		}
		inst := instances.Instances[group.CurrentMaster]
		if inst == nil || inst.Role != "master" || inst.Status != "running" {
			continue
		}
		endpoint := poolEndpoint(pool, inst.Server)
		if endpoint == "" {
			continue
		}
		masters = append(masters, apitypes.SentinelMaster{
			Group:                 groupName,
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

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}
