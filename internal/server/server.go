package server

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"sync"
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

	// topologyMu 串行化"读取拓扑状态 → 把它推给 Sentinel / 推给 Agent 写配置文件"
	// 这个复合序列。
	//
	// state.WithInstances 自身持数据库互斥锁，所以状态变更是原子的；但故障转移在事务
	// 提交之后还要做两件事——persistGroupRoles（把新角色写进各实例的磁盘配置文件）和
	// syncSentinel（把新 master 地址下发给 Sentinel）——这两步在事务外，没有任何互斥。
	//
	// 缺这把锁的具体后果：连续两次故障转移 A→B、B→C 会产生两个同步协程，各自读到不同
	// 的状态快照（一个读到 CurrentMaster=B，一个读到 C）。若后者先把 MONITOR 指向 C、
	// 前者随后把 MONITOR 改回 B，Sentinel 就被指向了一个已经降级的实例。接着
	// reconcileSentinelOnce 会发现 Sentinel 与 Server 认知不一致，而 Sentinel 是设计上
	// 的权威，于是把 Server 的 CurrentMaster 也改回 B —— 一次来回震荡，两边都错。
	// 磁盘配置文件同理：两个 persistGroupRoles 交错，可能把过期的 replicaof 写进去，
	// 容器下次重启就按错的角色启动。
	//
	// 加锁后，"读快照 → 下发"整体互斥，最后一次下发用的一定是它自己在锁内读到的快照。
	topologyMu sync.Mutex
}

func New(cfg *Config) (*Server, error) {
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

	st, err := state.NewManager(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("server: init state: %w", err)
	}

	s := &Server{
		cfg:   cfg,
		state: st,
		log:   logger.New(cfg.Log.Dir, cfg.Log.Stdout),
		audit: audit.New(st.DB()),
	}
	s.scheduler = newBackupScheduler(s)
	return s, nil
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
	r.Use(gin.Recovery())
	r.Use(s.requestLogger())

	r.GET("/", s.dashboardIndex)
	r.GET("/dashboard", s.dashboardIndex)
	r.GET("/dashboard/*filepath", s.dashboardAsset)

	r.Use(s.authMiddleware())

	node := r.Group("/node")
	{
		node.GET("/list", s.nodeQuery)
		node.POST("/add", s.nodeAdd)
		node.POST("/remove", s.nodeRemove)
		node.POST("/update", s.nodeUpdate)
	}

	instance := r.Group("/instance")
	{
		instance.GET("/list", s.instanceList)
		instance.GET("/status", s.instanceStatus)
		instance.GET("/metrics", s.metricsCollect)
		instance.POST("/create", s.instanceCreate)
		instance.POST("/delete", s.instanceDelete)
		instance.POST("/start", s.instanceStart)
		instance.POST("/stop", s.instanceStop)
		instance.POST("/config", s.instanceConfig)
		instance.POST("/failover", s.instanceFailover)
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
		// 用 subtle.ConstantTimeCompare 而非 !=：字符串比较在首个不同字节处短路，
		// 耗时随匹配前缀长度变化。这条路径的可利用性很低（网络抖动远大于纳秒级差异），
		// 改动成本也极低，按纵深防御处理。
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.Token)) != 1 {
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
