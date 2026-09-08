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

func (s *Server) syncerHeartbeatAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		want := s.cfg.SyncerTokens[c.Param("name")]
		header := c.GetHeader("Authorization")
		got := strings.TrimPrefix(header, "Bearer ")
		if len(want) < 16 || got == header || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid syncer credential"})
			return
		}
		c.Next()
	}
}

func (s *Server) syncerHeartbeat(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16384)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	var v apitypes.SyncerHeartbeat
	var extra any
	if err := d.Decode(&v); err != nil || d.Decode(&extra) != io.EOF || v.SyncerID != c.Param("name") || v.Validate() != nil {
		fail(c, 400, "invalid syncer heartbeat (maximum 16KiB)")
		return
	}
	if err := s.db.SaveSyncerHeartbeat(v); err != nil {
		if errors.Is(err, store.ErrSyncerHeartbeatConflict) {
			fail(c, 409, "syncer heartbeat identity, sequence or binding conflict")
			return
		}
		fail(c, 500, "syncer observation storage unavailable")
		return
	}
	ok(c, gin.H{"accepted_sequence": v.Sequence})
}

func (s *Server) syncerRuntimeList(c *gin.Context) {
	v, err := s.db.ListSyncerRuntime()
	if err != nil {
		fail(c, 500, "syncer observation storage unavailable")
		return
	}
	c.Header("Cache-Control", "no-store")
	ok(c, v)
}
