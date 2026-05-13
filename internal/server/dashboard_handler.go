package server

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/gin-gonic/gin"
)

//go:embed dashboard_dist
var dashboardDist embed.FS

func (s *Server) dashboardIndex(c *gin.Context) {
	serveDashboardFile(c, "index.html")
}

func (s *Server) dashboardAsset(c *gin.Context) {
	filePath := strings.TrimPrefix(c.Param("filepath"), "/")
	if filePath == "" || strings.Contains(filePath, "..") {
		s.dashboardIndex(c)
		return
	}
	serveDashboardFile(c, filePath)
}

func serveDashboardFile(c *gin.Context, name string) {
	dist, err := fs.Sub(dashboardDist, "dashboard_dist")
	if err != nil {
		c.String(http.StatusInternalServerError, "dashboard assets unavailable")
		return
	}
	name = path.Clean(name)
	if name == "." || strings.HasPrefix(name, "../") {
		name = "index.html"
	}
	if _, err := fs.Stat(dist, name); err != nil {
		name = "index.html"
	}
	http.ServeFileFS(c.Writer, c.Request, dist, name)
}
