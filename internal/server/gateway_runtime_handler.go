package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func (s *Server) gatewayHeartbeatAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		want := s.cfg.GatewayTokens[c.Param("name")]
		header := c.GetHeader("Authorization")
		got := strings.TrimPrefix(header, "Bearer ")
		if len(want) < 16 || got == header || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid gateway credential"})
			return
		}
		c.Next()
	}
}

func (s *Server) gatewayHeartbeat(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16384)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	var v apitypes.GatewayHeartbeat
	var extra any
	if err := d.Decode(&v); err != nil || d.Decode(&extra) != io.EOF || v.GatewayID != c.Param("name") || v.Validate() != nil {
		fail(c, 400, "invalid gateway heartbeat (maximum 16KiB)")
		return
	}
	if err := s.db.SaveGatewayHeartbeat(v); err != nil {
		if errors.Is(err, store.ErrGatewayHeartbeatConflict) {
			fail(c, 409, "gateway heartbeat sequence or identity conflict")
			return
		}
		fail(c, 500, "gateway observation storage unavailable")
		return
	}
	ok(c, gin.H{"accepted_sequence": v.Sequence})
}

func (s *Server) gatewayRuntimeList(c *gin.Context) {
	v, err := s.db.ListGatewayObservations()
	if err != nil {
		fail(c, 500, "gateway observation storage unavailable")
		return
	}
	ok(c, gin.H{"observations": v, "stale_after_seconds": int(apitypes.GatewayStaleAfter.Seconds())})
}
