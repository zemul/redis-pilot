package server

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) inventory(c *gin.Context) {
	view := c.DefaultQuery("view", "summary")
	portFilter := c.Query("port")
	serverFilter := c.Query("server")
	engineFilter := c.Query("engine")

	instances, err := s.state.ReadInstances()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	pool, err := s.state.ReadPool()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	switch view {
	case "port":
		ok(c, s.buildPortView(instances, portFilter, engineFilter))
	case "server":
		ok(c, s.buildServerView(instances, pool, serverFilter, engineFilter))
	default:
		ok(c, s.buildSummaryView(instances, pool, engineFilter))
	}
}

func (s *Server) buildPortView(instances *apitypes.InstancesState, portFilter, engineFilter string) []apitypes.PortInventoryItem {
	var items []apitypes.PortInventoryItem
	var filterPort int
	if portFilter != "" {
		filterPort, _ = strconv.Atoi(portFilter)
	}

	for groupName, group := range instances.Groups {
		if group == nil || group.Envoy == nil {
			continue
		}
		if engineFilter != "" && group.Engine != engineFilter {
			continue
		}
		backends := backendServersForGroup(instances, groupName)
		if group.Envoy.MasterPort > 0 {
			if filterPort > 0 && group.Envoy.MasterPort != filterPort {
				goto checkAuto
			}
			items = append(items, apitypes.PortInventoryItem{
				EnvoyPort:      group.Envoy.MasterPort,
				Mode:           "master",
				InstanceName:   groupName,
				Engine:         group.Engine,
				Category:       group.Category,
				Role:           "group",
				BackendServers: backends,
			})
			if filterPort > 0 {
				continue
			}
		}
	checkAuto:
		if group.Envoy.AutoPort > 0 {
			if filterPort > 0 && group.Envoy.AutoPort != filterPort {
				continue
			}
			items = append(items, apitypes.PortInventoryItem{
				EnvoyPort:      group.Envoy.AutoPort,
				Mode:           "auto",
				InstanceName:   groupName,
				Engine:         group.Engine,
				Category:       group.Category,
				Role:           "group",
				BackendServers: backends,
			})
		}
	}
	return items
}

func (s *Server) buildServerView(instances *apitypes.InstancesState, pool *apitypes.PoolState, serverFilter, engineFilter string) map[string]*apitypes.ServerInventoryItem {
	result := make(map[string]*apitypes.ServerInventoryItem)

	for name, inst := range instances.Instances {
		if serverFilter != "" && inst.Server != serverFilter {
			continue
		}
		group := instances.Groups[inst.Group]
		if group == nil {
			continue
		}
		if engineFilter != "" && group.Engine != engineFilter {
			continue
		}
		item, exists := result[inst.Server]
		if !exists {
			ip := ""
			totalMem := ""
			totalCPU := 0
			if ps, ok := pool.Servers[inst.Server]; ok {
				ip = ps.Endpoint
				totalMem = ps.Capacity.Memory
				totalCPU = ps.Capacity.CPUCores
			}
			item = &apitypes.ServerInventoryItem{
				IP:          ip,
				TotalMemory: totalMem,
				TotalCPU:    totalCPU,
			}
			result[inst.Server] = item
		}
		item.Instances = append(item.Instances, apitypes.ServerInstanceSummary{
			Name:          name,
			Engine:        group.Engine,
			ContainerPort: inst.Port,
			Memory:        inst.Memory,
			CPUs:          inst.CPUs,
			Status:        inst.Status,
		})
		item.AllocatedCPU += inst.CPUs
	}

	// 填充 allocated memory（简单拼接，实际可做单位换算）
	for _, item := range result {
		mem := ""
		for _, si := range item.Instances {
			if mem == "" {
				mem = si.Memory
			} else {
				mem += "+" + si.Memory
			}
		}
		item.AllocatedMemory = mem
	}
	return result
}

func (s *Server) buildSummaryView(instances *apitypes.InstancesState, pool *apitypes.PoolState, engineFilter string) *apitypes.InventorySummary {
	summary := &apitypes.InventorySummary{}
	servers := make(map[string]bool)
	totalCPU := 0

	for name, inst := range instances.Instances {
		group := instances.Groups[inst.Group]
		if group == nil {
			continue
		}
		if engineFilter != "" && group.Engine != engineFilter {
			continue
		}
		envoyPorts := ""
		if group.Envoy != nil {
			if group.Envoy.MasterPort > 0 {
				envoyPorts = "master:" + strconv.Itoa(group.Envoy.MasterPort)
			}
			if group.Envoy.AutoPort > 0 {
				if envoyPorts != "" {
					envoyPorts += "/"
				}
				envoyPorts += "auto:" + strconv.Itoa(group.Envoy.AutoPort)
			}
		}
		summary.Instances = append(summary.Instances, apitypes.InstanceSummaryItem{
			Name:       name,
			Engine:     group.Engine,
			Category:   group.Category,
			EnvoyPorts: envoyPorts,
			Server:     inst.Server,
			Status:     inst.Status,
		})
		servers[inst.Server] = true
		totalCPU += inst.CPUs
	}

	summary.TotalInstances = len(summary.Instances)
	summary.TotalServers = len(servers)
	summary.AllocatedCPU = totalCPU
	return summary
}

func backendServersForGroup(instances *apitypes.InstancesState, groupName string) []string {
	var backends []string
	for _, inst := range instances.Instances {
		if inst == nil || inst.Group != groupName {
			continue
		}
		backends = append(backends, fmt.Sprintf("%s:%d(%s)", inst.Server, inst.Port, inst.Role))
	}
	return backends
}
