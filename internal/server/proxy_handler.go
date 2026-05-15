package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) proxySnapshot(c *gin.Context) {
	snapshot, err := s.buildProxySnapshot()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, snapshot)
}

func (s *Server) buildProxySnapshot() (*apitypes.ProxySnapshot, error) {
	instances, err := s.state.ReadInstances()
	if err != nil {
		return nil, err
	}
	pool, err := s.state.ReadPool()
	if err != nil {
		return nil, err
	}

	groups := s.buildInstanceGroups(instances, pool)
	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)

	snapshot := &apitypes.ProxySnapshot{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Listeners:   make([]apitypes.ProxyListener, 0),
		Clusters:    make([]apitypes.ProxyCluster, 0),
	}

	for _, groupName := range groupNames {
		g := groups[groupName]
		sortEnvoyEndpoints(g.masterEndpoints)
		sortEnvoyEndpoints(g.replicaEndpoints)

		masterClusterName := "redis-" + groupName + "-master-cluster"
		replicaClusterName := "redis-" + groupName + "-replica-cluster"
		statPrefix := "redis_" + strings.ReplaceAll(groupName, "-", "_")

		if g.masterPort > 0 && len(g.masterEndpoints) > 0 {
			snapshot.Listeners = append(snapshot.Listeners, apitypes.ProxyListener{
				Name:       "redis-" + groupName + "-master",
				Group:      groupName,
				Mode:       "master",
				Bind:       "0.0.0.0",
				Port:       g.masterPort,
				StatPrefix: statPrefix + "_master",
				Cluster:    masterClusterName,
				Password:   g.password,
				ReadPolicy: "MASTER",
			})
		}

		if g.autoPort > 0 && len(g.masterEndpoints) > 0 && len(g.replicaEndpoints) > 0 {
			snapshot.Listeners = append(snapshot.Listeners, apitypes.ProxyListener{
				Name:        "redis-" + groupName + "-auto",
				Group:       groupName,
				Mode:        "auto",
				Bind:        "0.0.0.0",
				Port:        g.autoPort,
				StatPrefix:  statPrefix + "_auto",
				Cluster:     masterClusterName,
				ReadCluster: replicaClusterName,
				Password:    g.password,
				ReadPolicy:  "MASTER",
			})
		}

		if len(g.masterEndpoints) > 0 {
			snapshot.Clusters = append(snapshot.Clusters, apitypes.ProxyCluster{
				Name:      masterClusterName,
				Password:  g.password,
				Endpoints: proxyEndpoints(g.masterEndpoints),
			})
		}
		if g.autoPort > 0 && len(g.replicaEndpoints) > 0 {
			snapshot.Clusters = append(snapshot.Clusters, apitypes.ProxyCluster{
				Name:      replicaClusterName,
				Password:  g.password,
				Endpoints: proxyEndpoints(g.replicaEndpoints),
			})
		}
	}

	snapshot.Version = proxySnapshotVersion(snapshot)
	return snapshot, nil
}

func sortEnvoyEndpoints(endpoints []envoyEndpoint) {
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Address == endpoints[j].Address {
			return endpoints[i].Port < endpoints[j].Port
		}
		return endpoints[i].Address < endpoints[j].Address
	})
}

func proxyEndpoints(endpoints []envoyEndpoint) []apitypes.ProxyEndpoint {
	out := make([]apitypes.ProxyEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		out = append(out, apitypes.ProxyEndpoint{Address: ep.Address, Port: ep.Port})
	}
	return out
}

func proxySnapshotVersion(snapshot *apitypes.ProxySnapshot) string {
	stable := struct {
		Listeners []apitypes.ProxyListener `json:"listeners"`
		Clusters  []apitypes.ProxyCluster  `json:"clusters"`
	}{
		Listeners: snapshot.Listeners,
		Clusters:  snapshot.Clusters,
	}
	data, _ := json.Marshal(stable)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
