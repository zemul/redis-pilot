package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
)

func (s *Server) auditQuery(c *gin.Context) {
	f := audit.QueryFilter{
		From:     c.Query("from"),
		To:       c.Query("to"),
		Group:    c.Query("group"),
		Instance: c.Query("instance"),
		Level:    c.Query("level"),
		Action:   c.Query("action"),
	}
	records, err := s.audit.Query(f)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	ok(c, gin.H{"records": records, "count": len(records)})
}
