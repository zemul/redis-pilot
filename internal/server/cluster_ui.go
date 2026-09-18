package server

import (
	"encoding/json"
	"sort"
	"strconv"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Expose only safe plan fields, not arbitrary task payloads or credentials.
func (s *Server) clusterPending(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	if page < 1 {
		page = 1
	}
	tasks, total, err := s.db.QueryTasks(store.TaskFilter{Kind: "cluster.provision", Status: "active", Limit: 100, Offset: (page - 1) * 100})
	if err != nil {
		s.coldError(c, err)
		return
	}
	type entry struct {
		Spec     apitypes.ColdClusterSpec `json:"spec"`
		Task     store.Task               `json:"task"`
		Children []store.Task             `json:"children"`
	}
	items := []entry{}
	for _, task := range tasks {
		var spec apitypes.ColdClusterSpec
		if err := json.Unmarshal([]byte(task.Payload), &spec); err != nil {
			s.coldError(c, err)
			return
		}
		children, err := s.db.ClusterBuildChildren(task.ID)
		if err != nil {
			s.coldError(c, err)
			return
		}
		items = append(items, entry{Spec: spec, Task: task, Children: children})
	}
	ok(c, gin.H{"records": items, "total": total, "page": page, "page_size": 100})
}

func (s *Server) clusterCreateOptions(c *gin.Context) {
	if c.GetString("auth_role") != "admin" {
		fail(c, 403, "admin role required")
		return
	}
	ids := []string{}
	for id, token := range s.cfg.GatewayTokens {
		if len(token) >= 16 {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	ok(c, gin.H{"gateway_ids": ids})
}
