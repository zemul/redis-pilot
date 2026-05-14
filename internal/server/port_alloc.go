package server

import (
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// allocRedisPort 在指定服务器上分配一个可用的 Redis 实例端口。
// 扫描该服务器上所有已有实例的端口，从配置范围中取第一个未占用的。
func allocRedisPort(cfg PortConfig, instances *apitypes.InstancesState, serverName string) (int, error) {
	used := make(map[int]bool)
	for _, inst := range instances.Instances {
		if inst.Server == serverName {
			used[inst.Port] = true
		}
	}
	for p := cfg.Redis.Start; p <= cfg.Redis.End; p++ {
		if !used[p] {
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available redis port on server %s (range %d-%d)", serverName, cfg.Redis.Start, cfg.Redis.End)
}

// allocEnvoyPorts 为一个实例组分配 Envoy 端口。
// withAuto=true 时额外分配自动读写分离端口（用于主从拓扑）。
func allocEnvoyPorts(cfg PortConfig, instances *apitypes.InstancesState, withAuto bool) (*apitypes.EnvoyConfig, error) {
	usedAuto := make(map[int]bool)
	usedMaster := make(map[int]bool)
	for _, group := range instances.Groups {
		if group.Envoy == nil {
			continue
		}
		usedAuto[group.Envoy.AutoPort] = true
		usedMaster[group.Envoy.MasterPort] = true
	}

	master, err := nextFree(cfg.EnvoyMaster, usedMaster)
	if err != nil {
		return nil, fmt.Errorf("envoy master port: %w", err)
	}

	ec := &apitypes.EnvoyConfig{MasterPort: master}
	if withAuto {
		auto, err := nextFree(cfg.EnvoyAuto, usedAuto)
		if err != nil {
			return nil, fmt.Errorf("envoy auto port: %w", err)
		}
		ec.AutoPort = auto
	}
	return ec, nil
}

func nextFree(r PortRange, used map[int]bool) (int, error) {
	for p := r.Start; p <= r.End; p++ {
		if !used[p] {
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available port in range %d-%d", r.Start, r.End)
}
