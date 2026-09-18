package server

import (
	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
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
	config, err := gateway.InitialConfig(*cluster, req)
	if err != nil {
		fail(c, 400, err.Error())
		return
	}
	c.Header("Cache-Control", "no-store")
	ok(c, apitypes.GatewayBootstrap{ClusterID: cluster.ID, NodeID: cluster.NodeID, Revision: cluster.Revision, YAML: config})
}
