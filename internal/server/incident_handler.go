package server

import (
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

// incidentQuery 返回分页后的异常列表（只读，所有登录用户可查）。
func (s *Server) incidentQuery(c *gin.Context) {
	page, _ := strconv.Atoi(c.Query("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(c.Query("page_size"))
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 30
	}
	f := store.IncidentFilter{
		Status:     c.Query("status"),
		Severity:   c.Query("severity"),
		Type:       c.Query("type"),
		Source:     c.Query("source"),
		ObjectType: c.Query("object_type"),
		ObjectID:   c.Query("object_id"),
		Group:      c.Query("group"),
		Instance:   c.Query("instance"),
		Node:       c.Query("node"),
		From:       c.Query("from"),
		To:         c.Query("to"),
		Limit:      pageSize,
		Offset:     (page - 1) * pageSize,
	}
	records, total, err := s.db.QueryIncidents(f)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if records == nil {
		records = []store.Incident{}
	}
	ok(c, gin.H{"records": records, "total": total})
}

// incidentDetail 返回单条异常记录。
func (s *Server) incidentDetail(c *gin.Context) {
	inc, err := s.db.GetIncident(c.Param("id"))
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if inc == nil {
		fail(c, http.StatusNotFound, "incident not found")
		return
	}
	ok(c, inc)
}

// incidentSummary 供 Dashboard 总览"当前异常"卡片使用：
// open 总数、按级别统计、持续时间最长的前 5 条。Dashboard 不从全量列表自行聚合。
func (s *Server) incidentSummary(c *gin.Context) {
	open, err := s.db.ListOpenIncidents()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	var critical, warning int
	for i := range open {
		fillDurationInc(&open[i])
		switch open[i].Severity {
		case "critical":
			critical++
		default:
			warning++
		}
	}
	// critical 优先，其次持续时间倒序，取前 5。
	sortIncidentsForSummary(open)
	top := open
	if len(top) > 5 {
		top = top[:5]
	}
	ok(c, gin.H{
		"total":    len(open),
		"critical": critical,
		"warning":  warning,
		"top":      top,
	})
}

// fillDurationInc 计算 open 记录的持续时间(now - first_observed_at)，毫秒。
func fillDurationInc(i *store.Incident) {
	start, err := time.Parse(time.RFC3339, i.FirstObservedAt)
	if err != nil {
		return
	}
	if ms := time.Since(start).Milliseconds(); ms > 0 {
		i.DurationMs = ms
	}
}

// sortIncidentsForSummary 排序：critical 优先，其次持续时间倒序。
func sortIncidentsForSummary(list []store.Incident) {
	sort.SliceStable(list, func(a, b int) bool {
		sa, sb := list[a].Severity == "critical", list[b].Severity == "critical"
		if sa != sb {
			return sa // critical 在前
		}
		return list[a].DurationMs > list[b].DurationMs
	})
}
