package xds

import (
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestBuildSnapshot(t *testing.T) {
	snapshot := &apitypes.ProxySnapshot{
		Version: "test-version",
		Listeners: []apitypes.ProxyListener{{
			Name:        "redis-cache-auto",
			Bind:        "0.0.0.0",
			Port:        16379,
			StatPrefix:  "redis_cache_auto",
			Cluster:     "redis-cache-master-cluster",
			ReadCluster: "redis-cache-replica-cluster",
			ReadPolicy:  "MASTER",
		}},
		Clusters: []apitypes.ProxyCluster{
			{
				Name: "redis-cache-master-cluster",
				Endpoints: []apitypes.ProxyEndpoint{{
					Address: "10.0.0.1",
					Port:    6379,
				}},
			},
			{
				Name: "redis-cache-replica-cluster",
				Endpoints: []apitypes.ProxyEndpoint{{
					Address: "10.0.0.2",
					Port:    6379,
				}},
			},
		},
	}

	xdsSnapshot, err := buildSnapshot(snapshot)
	if err != nil {
		t.Fatalf("buildSnapshot() error = %v", err)
	}
	if err := xdsSnapshot.Consistent(); err != nil {
		t.Fatalf("snapshot inconsistent: %v", err)
	}
}
