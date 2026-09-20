package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func coldBody(c *gin.Context, dst any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	var extra any
	if d.Decode(dst) != nil || d.Decode(&extra) != io.EOF {
		fail(c, 400, "invalid cold cluster JSON (maximum 64KiB)")
		return false
	}
	return true
}
func (s *Server) coldError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrColdInvalid):
		fail(c, 400, err.Error())
	case errors.Is(err, store.ErrColdNotFound):
		fail(c, 404, "cold cluster not found")
	case errors.Is(err, store.ErrColdConflict):
		fail(c, 409, "cold cluster revision, task or ownership conflict")
	default:
		s.log.Errorf("cold cluster: %v", err)
		fail(c, 500, "cold cluster state unavailable")
	}
}
func (s *Server) coldList(c *gin.Context) {
	v, err := s.db.ListColdClusters()
	if err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) coldGet(c *gin.Context) {
	v, err := s.db.GetColdCluster(c.Param("cluster"))
	if err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) coldCreate(c *gin.Context) {
	var req apitypes.ColdClusterSpec
	if !coldBody(c, &req) {
		return
	}
	if len(s.cfg.GatewayTokens[req.GatewayID]) < 16 {
		fail(c, 400, "configure the Gateway machine credential before registration")
		return
	}
	v, err := s.db.CreateColdCluster(req)
	s.coldAudit(c, "create", req.ID, err)
	if err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) coldTaskCreate(c *gin.Context) {
	var req apitypes.ColdTaskRequest
	if !coldBody(c, &req) {
		return
	}
	v, err := s.db.CreateColdTask(c.Param("cluster"), req)
	s.coldAudit(c, "migrate", c.Param("cluster"), err)
	if err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, v)
}

func (s *Server) coldIngressCreate(c *gin.Context) {
	var req apitypes.ColdIngressRequest
	if !coldBody(c, &req) {
		return
	}
	v, err := s.db.CreateColdIngress(c.Param("cluster"), req, s.cfg.Ports.ProxyMaster.Start, s.cfg.Ports.ProxyMaster.End)
	s.coldAudit(c, "ingress", c.Param("cluster"), err)
	if err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) coldIngressGet(c *gin.Context) {
	cluster, err := s.db.GetColdCluster(c.Param("cluster"))
	if err != nil {
		s.coldError(c, err)
		return
	}
	routes, err := s.db.ColdIngressRoutes(cluster.NodeID)
	if err != nil {
		s.coldError(c, err)
		return
	}
	for _, r := range routes {
		if r.ClusterID == cluster.ID {
			ok(c, r.ColdIngress)
			return
		}
	}
	fail(c, 404, "cold ingress not registered")
}

func (s *Server) coldIngressReady(c *gin.Context) {
	var req struct {
		GatewayAddress string `json:"gateway_address"`
	}
	if !coldBody(c, &req) {
		return
	}
	if err := s.db.ConfirmColdIngress(c.Param("name"), c.Param("cluster"), req.GatewayAddress); err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, nil)
}
func (s *Server) coldAssignment(c *gin.Context) {
	v, err := s.db.GetColdCluster(c.Param("cluster"))
	if err != nil {
		s.coldError(c, err)
		return
	}
	if v.GatewayID != c.Param("name") {
		fail(c, 403, "Gateway does not own this cluster")
		return
	}
	v, err = s.db.ColdAssignment(c.Param("name"), c.Param("cluster"))
	if err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) coldReport(c *gin.Context) {
	var req apitypes.ColdTaskReport
	if !coldBody(c, &req) {
		return
	}
	if err := s.db.ReportColdTask(c.Param("name"), c.Param("cluster"), req); err != nil {
		s.coldError(c, err)
		return
	}
	ok(c, nil)
}

func (s *Server) coldAudit(c *gin.Context, action, id string, err error) {
	result := "success"
	if err != nil {
		result = "failed"
	}
	if len(id) > 64 {
		id = "invalid"
	}
	s.audit.Log(store.AuditRecord{Operator: operatorFrom(c), Action: "cold-cluster." + action, Level: audit.LevelImportant, Result: result, ObjectType: "cold_cluster", ObjectID: id})
}
