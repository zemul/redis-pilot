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
	s := &Server{
		cfg:   cfg,
		state: state.NewManager(cfg.DataDir),
		log:   logger.New(cfg.Log.Dir, cfg.Log.Stdout),
		audit: audit.New(cfg.DataDir),
	}
	s.scheduler = newBackupScheduler(s)
	return s
}

// StartReconcileLoop 启动定时状态校验，每 5 分钟执行一次
func (s *Server) StartReconcileLoop() {
	go func() {
		for range time.Tick(5 * time.Minute) {
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
	r.Use(s.requestLogger(), s.authMiddleware())

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

	r.GET("/inventory", s.inventory)
	r.POST("/reconcile", s.reconcile)

	envoy := r.Group("/envoy")
	{
		envoy.POST("/route/update", s.envoyRouteUpdate)
		envoy.GET("/config", s.envoyConfig)
	}

	sentinel := r.Group("/sentinel")
	{
		sentinel.POST("/event", s.sentinelEvent)
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

func ok(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, apitypes.APIResponse{Success: true, Data: data})
}

func fail(c *gin.Context, code int, err string) {
	c.JSON(code, apitypes.APIResponse{Error: err})
}
