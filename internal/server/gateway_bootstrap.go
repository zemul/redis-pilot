package server

import (
	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"net"
	"strconv"
)

// Read-only configuration generation. It neither deploys a service nor confirms ingress.
func (s *Server) gatewayBootstrap(c *gin.Context) {
	var req apitypes.GatewayBootstrapRequest
	if !coldBody(c, &req) {
		return
	}
	cluster, err := s.db.GetColdCluster(c.Param("cluster"))
	if err != nil {
		s.coldError(c, err)
		return
	}
	if cluster.Revision != 1 || cluster.Task != nil {
		fail(c, 409, "initial configuration cannot replace existing migration state")
		return
	}
	if len(s.cfg.GatewayTokens[cluster.GatewayID]) < 16 {
		fail(c, 400, "configure Gateway machine credential first")
		return
	}
	instances, err := s.db.ReadInstances()
	if err != nil {
		fail(c, 500, "cannot read shard replicas")
		return
	}
	nodes, err := s.db.ReadNode()
	if err != nil {
		fail(c, 500, "cannot read replica servers")
		return
	}
	groups := map[string]bool{}
	anchors := map[string]bool{}
	for _, shard := range cluster.Shards {
		anchors[shard.Instance] = true
		if anchor := instances.Instances[shard.Instance]; anchor != nil {
			groups[anchor.Group] = true
		}
	}
	var addresses []string
	for name, i := range instances.Instances {
		if groups[i.Group] && !anchors[name] {
			if node := nodes.Servers[i.Server]; node != nil {
				host := node.Endpoint
				if i.Server == cluster.NodeID {
					host = "127.0.0.1"
				}
				addresses = append(addresses, net.JoinHostPort(host, strconv.Itoa(i.Port)))
			}
		}
	}
	config, err := gateway.InitialConfig(*cluster, req, addresses...)
	if err != nil {
		fail(c, 400, err.Error())
		return
	}
	c.Header("Cache-Control", "no-store")
	ok(c, apitypes.GatewayBootstrap{ClusterID: cluster.ID, NodeID: cluster.NodeID, Revision: cluster.Revision, YAML: config})
}
