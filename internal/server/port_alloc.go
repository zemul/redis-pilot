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

// allocEnvoyPorts 为一个实例组（master/standalone）分配 Envoy 端口。
// 返回 readwrite_port, readonly_port。
func allocEnvoyPorts(cfg PortConfig, instances *apitypes.InstancesState) (*apitypes.EnvoyConfig, error) {
	usedRW := make(map[int]bool)
	usedRO := make(map[int]bool)
	for _, inst := range instances.Instances {
		if inst.Envoy == nil {
			continue
		}
		usedRW[inst.Envoy.ReadWritePort] = true
		usedRO[inst.Envoy.ReadOnlyPort] = true
	}

	rw, err := nextFree(cfg.EnvoyRW, usedRW)
	if err != nil {
		return nil, fmt.Errorf("envoy readwrite port: %w", err)
	}
	ro, err := nextFree(cfg.EnvoyWO, usedRO)
	if err != nil {
		return nil, fmt.Errorf("envoy readonly port: %w", err)
	}

	return &apitypes.EnvoyConfig{
		ReadWritePort: rw,
		ReadOnlyPort:  ro,
	}, nil
}

func nextFree(r PortRange, used map[int]bool) (int, error) {
	for p := r.Start; p <= r.End; p++ {
		if !used[p] {
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available port in range %d-%d", r.Start, r.End)
}
