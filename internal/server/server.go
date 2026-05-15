package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/audit"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type Server struct {
	cfg       *Config
	state     *state.Manager
	log       *logger.Logger
	audit     *audit.Logger
	scheduler *backupScheduler
}

func New(cfg *Config) *Server {
	normalizeImageConfig(cfg)
	if cfg.Ports.Redis.Start == 0 && cfg.Ports.Redis.End == 0 {
		cfg.Ports.Redis = PortRange{Start: 6379, End: 6499}
	}
	if cfg.Ports.EnvoyAuto.Start == 0 && cfg.Ports.EnvoyAuto.End == 0 {
		cfg.Ports.EnvoyAuto = PortRange{Start: 16379, End: 16499}
	}
	if cfg.Ports.EnvoyMaster.Start == 0 && cfg.Ports.EnvoyMaster.End == 0 {
		cfg.Ports.EnvoyMaster = PortRange{Start: 16500, End: 16619}
	}
	s := &Server{
		cfg:   cfg,
		state: state.NewManager(cfg.DataDir),
		log:   logger.New(cfg.Log.Dir, cfg.Log.Stdout),
		audit: audit.New(cfg.DataDir),
	}
	s.scheduler = newBackupScheduler(s)
	return s
}

// StartReconcileLoop 启动定时状态校验，每 30 秒执行一次
func (s *Server) StartReconcileLoop() {
	go func() {
		for range time.Tick(30 * time.Second) {
			results, err := s.runReconcile()
			if err != nil {
				s.log.Errorf("reconcile failed: %v", err)
				continue
			}
			for _, r := range results {
				if r.Action != "none" {
					s.log.Infof("reconcile: %s on %s: desired=%s actual=%s action=%s", r.Instance, r.Server, r.Desired, r.Actual, r.Action)
				}
			}
		}
	}()
}

// StartBackupScheduler 启动定时备份调度器
func (s *Server) StartBackupScheduler() {
	s.scheduler.Start()
}

func (s *Server) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(s.requestLogger())

	r.GET("/", s.dashboardIndex)
	r.GET("/dashboard", s.dashboardIndex)
	r.GET("/dashboard/*filepath", s.dashboardAsset)

	r.Use(s.authMiddleware())

	pool := r.Group("/pool")
	{
		pool.GET("/query", s.poolQuery)
		pool.POST("/add", s.poolAdd)
		pool.POST("/remove", s.poolRemove)
		pool.POST("/update", s.poolUpdate)
	}

	instance := r.Group("/instance")
	{
		instance.GET("/list", s.instanceList)
		instance.GET("/status", s.instanceStatus)
		instance.POST("/create", s.instanceCreate)
		instance.POST("/delete", s.instanceDelete)
		instance.POST("/start", s.instanceStart)
		instance.POST("/stop", s.instanceStop)
		instance.POST("/config", s.instanceConfig)
		instance.POST("/promote", s.instancePromote)
		instance.POST("/replicate", s.instanceReplicate)
	}

	backup := r.Group("/backup")
	{
		backup.POST("/exec", s.backupExec)
		backup.POST("/restore", s.backupRestore)
		backup.GET("/list", s.backupList)
		backup.GET("/schedule", s.backupGetSchedule)
		backup.POST("/schedule", s.backupSetSchedule)
	}

	r.GET("/audit/query", s.auditQuery)
	r.GET("/inventory", s.inventory)
	r.POST("/reconcile", s.reconcile)
	r.GET("/metrics", s.metricsCollect)

	r.GET("/proxy/snapshot", s.proxySnapshot)
	r.GET("/api/v1/proxy/snapshot", s.proxySnapshot)

	sentinel := r.Group("/sentinel")
	{
		sentinel.POST("/event", s.sentinelEvent)
		sentinel.GET("/status", s.sentinelStatus)
		sentinel.POST("/sync", s.sentinelSync)
	}

	return r
}

func (s *Server) requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		s.log.Infof("%s %s %d %s", c.Request.Method, c.Request.URL.Path, c.Writer.Status(), fmt.Sprintf("%dms", time.Since(start).Milliseconds()))
	}
}

func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.cfg.Token == "" {
			c.Next()
			return
		}
		auth := c.GetHeader("Authorization")
		token := strings.TrimPrefix(auth, "Bearer ")
		if token != s.cfg.Token {
			c.AbortWithStatusJSON(http.StatusUnauthorized, apitypes.APIResponse{Error: "unauthorized"})
			return
		}
		c.Next()
	}
}

func operatorFrom(c *gin.Context) string {
	if op := c.GetHeader("X-Operator"); op != "" {
		return op
	}
	return "unknown"
}

func ok(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, apitypes.APIResponse{Success: true, Data: data})
}

func fail(c *gin.Context, code int, err string) {
	c.JSON(code, apitypes.APIResponse{Error: err})
}
