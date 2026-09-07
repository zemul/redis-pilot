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

func gatewayDraftBody(c *gin.Context, dst any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 8192)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		fail(c, http.StatusBadRequest, "invalid gateway draft JSON (maximum 8KiB; unknown fields are rejected)")
		return false
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		fail(c, http.StatusBadRequest, "expected one JSON object")
		return false
	}
	return true
}

func (s *Server) gatewayDraftError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrGatewayDraftInvalid):
		fail(c, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrGatewayDraftNotFound):
		fail(c, http.StatusNotFound, "gateway draft not found")
	case errors.Is(err, store.ErrGatewayDraftConflict):
		fail(c, http.StatusConflict, "gateway draft exists or revision changed; reload before editing")
	default:
		s.log.Errorf("gateway draft store error: %v", err)
		fail(c, http.StatusInternalServerError, "gateway draft storage unavailable")
	}
}

func (s *Server) gatewayDraftList(c *gin.Context) {
	v, err := s.db.ListGatewayDrafts()
	if err != nil {
		s.gatewayDraftError(c, err)
		return
	}
	ok(c, gin.H{"drafts": v})
}
func (s *Server) gatewayDraftGet(c *gin.Context) {
	v, err := s.db.GetGatewayDraft(c.Param("name"))
	if err != nil {
		s.gatewayDraftError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) gatewayDraftCreate(c *gin.Context) {
	var req struct {
		Name string                    `json:"name"`
		Spec apitypes.GatewayDraftSpec `json:"spec"`
	}
	if !gatewayDraftBody(c, &req) {
		return
	}
	v, err := s.db.SaveGatewayDraft(req.Name, req.Spec, "")
	s.gatewayDraftAudit(c, "create", req.Name, err)
	if err != nil {
		s.gatewayDraftError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) gatewayDraftUpdate(c *gin.Context) {
	var req struct {
		Revision string                    `json:"revision"`
		Spec     apitypes.GatewayDraftSpec `json:"spec"`
	}
	if !gatewayDraftBody(c, &req) {
		return
	}
	if req.Revision == "" || len(req.Revision) > 128 {
		fail(c, http.StatusBadRequest, "revision is required (maximum 128 bytes)")
		return
	}
	v, err := s.db.SaveGatewayDraft(c.Param("name"), req.Spec, req.Revision)
	s.gatewayDraftAudit(c, "update", c.Param("name"), err)
	if err != nil {
		s.gatewayDraftError(c, err)
		return
	}
	ok(c, v)
}
func (s *Server) gatewayDraftDelete(c *gin.Context) {
	var req struct {
		Revision string `json:"revision"`
	}
	if !gatewayDraftBody(c, &req) {
		return
	}
	if req.Revision == "" || len(req.Revision) > 128 {
		fail(c, http.StatusBadRequest, "revision is required (maximum 128 bytes)")
		return
	}
	err := s.db.DeleteGatewayDraft(c.Param("name"), req.Revision)
	s.gatewayDraftAudit(c, "delete", c.Param("name"), err)
	if err != nil {
		s.gatewayDraftError(c, err)
		return
	}
	ok(c, nil)
}
func (s *Server) gatewayDraftAudit(c *gin.Context, action, name string, err error) {
	result := "success"
	if err != nil {
		result = "failed"
	}
	// Do not copy request bodies, credentials or arbitrary storage errors to audit.
	if len(name) > 64 {
		name = "invalid"
	}
	s.audit.Log(store.AuditRecord{Operator: operatorFrom(c), Action: "gateway.draft." + action, Level: audit.LevelNormal, Result: result, ObjectType: "gateway_draft", ObjectID: name, Target: map[string]interface{}{"gateway_draft": name}})
}
