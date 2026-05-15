package server

import "gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"

type envoyEndpoint struct {
	Address string
	Port    int
}

type instanceGroup struct {
	autoPort         int
	masterPort       int
	password         string
	masterEndpoints  []envoyEndpoint
	replicaEndpoints []envoyEndpoint
}

// buildInstanceGroups按稳定实例组聚合，提取代理端口和后端地址。
func (s *Server) buildInstanceGroups(instances *apitypes.InstancesState, node *apitypes.NodeState) map[string]*instanceGroup {
	groups := make(map[string]*instanceGroup)

	for groupName, group := range instances.Groups {
		if group == nil || group.Envoy == nil {
			continue
		}
		g := &instanceGroup{
			autoPort:   group.Envoy.AutoPort,
			masterPort: group.Envoy.MasterPort,
			password:   "",
		}
		for name, inst := range instances.Instances {
			if inst == nil || inst.Group != groupName || inst.Status != "running" {
				continue
			}
			srv := node.Servers[inst.Server]
			if srv == nil {
				continue
			}
			ep := envoyEndpoint{Address: srv.Endpoint, Port: inst.Port}
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
