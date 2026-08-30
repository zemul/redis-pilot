package server

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

func (s *Server) taskQuery(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "30"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 30
	}
	tasks, total, err := s.db.QueryTasks(store.TaskFilter{
		Status: c.Query("status"), Kind: c.Query("kind"), Operator: c.Query("operator"), Query: c.Query("q"),
		Limit: pageSize, Offset: (page - 1) * pageSize,
	})
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	ok(c, gin.H{"records": tasks, "total": total, "page": page, "page_size": pageSize})
}

func (s *Server) taskDetail(c *gin.Context) {
	task, err := s.db.GetTask(c.Param("id"))
	if errors.Is(err, sql.ErrNoRows) {
		fail(c, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, task)
}

func (s *Server) taskCancel(c *gin.Context) {
	task, err := s.db.GetTask(c.Param("id"))
	if errors.Is(err, sql.ErrNoRows) {
		fail(c, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if task.Kind != "diagnostic.bigkeys" || !task.Cancelable || task.Status != "running" {
		fail(c, http.StatusConflict, "task is not cancelable")
		return
	}
	node, err := s.db.ReadNode()
	if err != nil || node.Servers[task.ServerName] == nil {
		fail(c, http.StatusServiceUnavailable, "task agent is unavailable")
		return
	}
	if _, err := newAgentClient(node.Servers[task.ServerName]).post("/instance/bigkeys/cancel", map[string]string{"task_id": task.ID}); err != nil {
		fail(c, http.StatusBadGateway, err.Error())
		return
	}
	_, _ = s.db.MarkTaskCancelRequested(task.ID)
	ok(c, gin.H{"task_id": task.ID, "status": "canceling"})
}
