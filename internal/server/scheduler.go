package server

import (
	"fmt"
	"sort"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// selectServer 根据调度策略自动选择服务器
// 策略：
//  1. 过滤：排除 status != healthy
//  2. 过滤：排除 labels 不满足 nodeSelector 的
//  3. 过滤：排除剩余资源不足的
//  4. 排序：主从不同服务器 > 不同可用区 > 剩余资源最多
func selectServer(node *apitypes.NodeState, instances *apitypes.InstancesState, reqMemory string, reqCPUs float64, reqDisk string, replicaOf string, nodeSelector map[string]string) (string, error) {
	if len(node.Servers) == 0 {
		return "", fmt.Errorf("no servers in node")
	}

	reqMem := parseMemoryGi(reqMemory)
	reqDiskGi := parseMemoryGi(reqDisk)

	// 找到主库所在的服务器和可用区（用于从库调度）
	var masterServer, masterZone string
	if replicaOf != "" {
		for _, inst := range instances.Instances {
			if inst.Role == "master" {
				addr := fmt.Sprintf("%s:%d", nodeEndpoint(node, inst.Server), inst.Port)
				if addr == replicaOf {
					masterServer = inst.Server
					if srv := node.Servers[inst.Server]; srv != nil {
						masterZone = srv.Labels["zone"]
					}
					break
				}
			}
		}
	}

	type candidate struct {
		name         string
		remainingMem int
		remainingCPU float64
		sameMaster   bool // 和主库在同一台服务器
		sameZone     bool // 和主库在同一个可用区
	}

	var candidates []candidate
	for name, srv := range node.Servers {
		// 过滤不健康
		if srv.Status != "healthy" && srv.Status != "" {
			continue
		}
		// 过滤标签不匹配
		if !matchLabels(srv.Labels, nodeSelector) {
			continue
		}
		allocMem, allocCPU, allocDisk := computeAllocated(instances, name)
		remainMem := parseMemoryGi(srv.Capacity.Memory) - allocMem
		remainCPU := srv.Capacity.CPUCores - allocCPU
		remainDisk := parseMemoryGi(srv.Capacity.Disk) - allocDisk
		if reqMem > 0 && remainMem < reqMem {
			continue
		}
		if reqCPUs > 0 && remainCPU < reqCPUs {
			continue
		}
		if reqDiskGi > 0 && remainDisk < reqDiskGi {
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
		return "", fmt.Errorf("no server has enough resources (need %s memory, %g cpus)", reqMemory, reqCPUs)
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

func nodeEndpoint(node *apitypes.NodeState, serverName string) string {
	if srv := node.Servers[serverName]; srv != nil {
		return srv.Endpoint
	}
	return ""
}

func computeAllocated(instances *apitypes.InstancesState, serverName string) (memGi int, cpus float64, diskGi int) {
	return computeAllocatedExcluding(instances, serverName, "")
}

// computeAllocatedExcluding 统计某服务器上已分配的资源，但排除名为 excludeName 的实例。
// 变配（resize）时目标实例已按旧规格计入配额，校验新规格能否放下必须先把它自己排除，
// 否则会把它的旧占用重复计算，导致升配被误判为容量不足。excludeName 为空时等价于全量统计。
func computeAllocatedExcluding(instances *apitypes.InstancesState, serverName, excludeName string) (memGi int, cpus float64, diskGi int) {
	for name, inst := range instances.Instances {
		// 容器确实不存在的实例不占用资源。注意这里读观测而非意图：
		// 一个"意图 running 但容器已消失"的实例不该继续占配额。
		if inst == nil || inst.Server != serverName || inst.ObservedStatus == apitypes.ObservedMissing {
			continue
		}
		if name == excludeName {
			continue
		}
		memGi += parseMemoryGi(inst.Memory)
		cpus += inst.CPUs
		diskGi += parseMemoryGi(inst.Disk)
	}
	return
}

// matchLabels 检查节点标签是否包含 selector 中的所有 key=value。
// selector 为空时匹配所有节点。
func matchLabels(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}
