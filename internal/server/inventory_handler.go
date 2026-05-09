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

	for name, inst := range instances.Instances {
		if engineFilter != "" && inst.Engine != engineFilter {
			continue
		}
		if inst.Envoy == nil {
			continue
		}
		if inst.Envoy.ReadWritePort > 0 {
			if filterPort > 0 && inst.Envoy.ReadWritePort != filterPort {
				goto checkWriteOnly
			}
			items = append(items, apitypes.PortInventoryItem{
				EnvoyPort:      inst.Envoy.ReadWritePort,
				Mode:           "readwrite",
				InstanceName:   name,
				Engine:         inst.Engine,
				Category:       inst.Category,
				Role:           inst.Role,
				BackendServers: []string{fmt.Sprintf("%s:%d(%s)", inst.Server, inst.Port, inst.Role)},
			})
			if filterPort > 0 {
				continue
			}
		}
	checkWriteOnly:
		if inst.Envoy.WriteOnlyPort > 0 {
			if filterPort > 0 && inst.Envoy.WriteOnlyPort != filterPort {
				continue
			}
			items = append(items, apitypes.PortInventoryItem{
				EnvoyPort:      inst.Envoy.WriteOnlyPort,
				Mode:           "writeonly",
				InstanceName:   name,
				Engine:         inst.Engine,
				Category:       inst.Category,
				Role:           inst.Role,
				BackendServers: []string{fmt.Sprintf("%s:%d(%s)", inst.Server, inst.Port, inst.Role)},
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
		if engineFilter != "" && inst.Engine != engineFilter {
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
				IP:       ip,
				TotalMemory: totalMem,
				TotalCPU:    totalCPU,
			}
			result[inst.Server] = item
		}
		item.Instances = append(item.Instances, apitypes.ServerInstanceSummary{
			Name:          name,
			Engine:        inst.Engine,
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
		if engineFilter != "" && inst.Engine != engineFilter {
			continue
		}
		envoyPorts := ""
		if inst.Envoy != nil {
			if inst.Envoy.ReadWritePort > 0 {
				envoyPorts = strconv.Itoa(inst.Envoy.ReadWritePort)
			}
			if inst.Envoy.WriteOnlyPort > 0 {
				if envoyPorts != "" {
					envoyPorts += "/"
				}
				envoyPorts += strconv.Itoa(inst.Envoy.WriteOnlyPort)
			}
		}
		summary.Instances = append(summary.Instances, apitypes.InstanceSummaryItem{
			Name:       name,
			Engine:     inst.Engine,
			Category:   inst.Category,
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
