package server

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

func (s *Server) auditQuery(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "30"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 30
	}

	f := store.AuditFilter{
		From:      c.Query("from"),
		To:        c.Query("to"),
		Group:     c.Query("group"),
		Instance:  c.Query("instance"),
		Operator:  c.Query("operator"),
		ActorType: c.Query("actor_type"),
		Source:    c.Query("source"),
		Level:     c.Query("level"),
		Action:    c.Query("action"),
		Result:    c.Query("result"),
		Query:     c.Query("q"),
		Limit:     pageSize,
		Offset:    (page - 1) * pageSize,
	}
	records, total, err := s.audit.Query(f)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	ok(c, gin.H{"records": records, "total": total, "page": page, "page_size": pageSize})
}
