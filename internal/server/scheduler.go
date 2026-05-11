package server

import (
	"fmt"
	"sort"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// selectServer 根据调度策略自动选择服务器
// 策略：
//  1. 过滤：排除 status != healthy
//  2. 过滤：排除剩余资源不足的
//  3. 排序：主从不同服务器 > 不同可用区 > 剩余资源最多
func selectServer(pool *apitypes.PoolState, instances *apitypes.InstancesState, reqMemory string, reqCPUs int, replicaOf string) (string, error) {
	if len(pool.Servers) == 0 {
		return "", fmt.Errorf("no servers in pool")
	}

	reqMem := parseMemoryGi(reqMemory)

	// 找到主库所在的服务器和可用区（用于从库调度）
	var masterServer, masterZone string
	if replicaOf != "" {
		for _, inst := range instances.Instances {
			if inst.Role == "master" {
				for _, r := range inst.Replicas {
					_ = r // 遍历找主库
				}
				// 通过 replicaOf 地址匹配主库
				masterServer = inst.Server
				if srv := pool.Servers[inst.Server]; srv != nil {
					masterZone = srv.Labels["zone"]
				}
			}
		}
		// 更精确：通过端口和地址匹配
		for name, inst := range instances.Instances {
			_ = name
			if inst.Role == "master" || inst.Role == "standalone" {
				addr := fmt.Sprintf("%s:%d", poolEndpoint(pool, inst.Server), inst.Port)
				if addr == replicaOf {
					masterServer = inst.Server
					if srv := pool.Servers[inst.Server]; srv != nil {
						masterZone = srv.Labels["zone"]
					}
					break
				}
			}
		}
	}

	type candidate struct {
		name          string
		remainingMem  int
		remainingCPU  int
		sameMaster    bool // 和主库在同一台服务器
		sameZone      bool // 和主库在同一个可用区
	}

	var candidates []candidate
	for name, srv := range pool.Servers {
		// 过滤不健康
		if srv.Status != "healthy" && srv.Status != "" {
			continue
		}
		// 从 instances-state 计算已分配资源
		allocMem, allocCPU := computeAllocated(instances, name)
		remainMem := parseMemoryGi(srv.Capacity.Memory) - allocMem
		remainCPU := srv.Capacity.CPUCores - allocCPU
		if reqMem > 0 && remainMem < reqMem {
			continue
		}
		if reqCPUs > 0 && remainCPU < reqCPUs {
			continue
		}

		c := candidate{
			name:         name,
			remainingMem: remainMem,
			remainingCPU: remainCPU,
		}
		if replicaOf != "" {
			c.sameMaster = (name == masterServer)
			zone := srv.Labels["zone"]
			c.sameZone = (zone != "" && zone == masterZone)
		}
		candidates = append(candidates, c)
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no server has enough resources (need %s memory, %d cpus)", reqMemory, reqCPUs)
	}

	// 排序：不同服务器 > 不同可用区 > 剩余资源多
	sort.Slice(candidates, func(i, j int) bool {
		ci, cj := candidates[i], candidates[j]
		// 从库优先不和主库同服务器
		if ci.sameMaster != cj.sameMaster {
			return !ci.sameMaster
		}
		// 优先不同可用区
		if ci.sameZone != cj.sameZone {
			return !ci.sameZone
		}
		// 剩余内存多的优先
		return ci.remainingMem > cj.remainingMem
	})

	return candidates[0].name, nil
}

func poolEndpoint(pool *apitypes.PoolState, serverName string) string {
	if srv := pool.Servers[serverName]; srv != nil {
		return srv.Endpoint
	}
	return ""
}

func computeAllocated(instances *apitypes.InstancesState, serverName string) (memGi int, cpus int) {
	for _, inst := range instances.Instances {
		if inst == nil || inst.Server != serverName || inst.Status == "failed" {
			continue
		}
		memGi += parseMemoryGi(inst.Memory)
		cpus += inst.CPUs
	}
	return
}
