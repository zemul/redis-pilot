package server

import "gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"

type proxyEndpoint struct {
	Address string
	Port    int
}

type instanceGroup struct {
	autoPort         int
	masterPort       int
	password         string
	accessMode       string // rw-split | standard（空视为 rw-split）
	masterEndpoints  []proxyEndpoint
	replicaEndpoints []proxyEndpoint
}

// buildInstanceGroups按稳定实例组聚合，提取代理端口和后端地址。
func (s *Server) buildInstanceGroups(instances *apitypes.InstancesState, node *apitypes.NodeState) map[string]*instanceGroup {
	groups := make(map[string]*instanceGroup)

	for groupName, group := range instances.Groups {
		if group == nil || group.Proxy == nil {
			continue
		}
		g := &instanceGroup{
			autoPort:   group.Proxy.AutoPort,
			masterPort: group.Proxy.MasterPort,
			password:   "",
			accessMode: group.AccessMode,
		}
		for name, inst := range instances.Instances {
			// 只有观测到在跑的实例才能接业务流量。用意图判断会把流量
			// 发给一个"应该在跑但实际已经挂了"的实例。
			if inst == nil || inst.Group != groupName || !inst.IsServing() {
				continue
			}
			srv := node.Servers[inst.Server]
			if srv == nil {
				continue
			}
			ep := proxyEndpoint{Address: srv.Endpoint, Port: inst.Port}
			if inst.Role == "master" && group.CurrentMaster == name {
				g.masterEndpoints = append(g.masterEndpoints, ep)
				g.password = inst.Password
			} else if inst.Role == "replica" {
				g.replicaEndpoints = append(g.replicaEndpoints, ep)
			}
		}
		groups[groupName] = g
	}

	return groups
}
