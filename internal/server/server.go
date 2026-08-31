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
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/server/ai"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type Server struct {
	cfg       *Config
	db        *store.DB
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

	// alertInstances tracks which instances are currently in alert state,
	// so reconcile only logs state transitions (ok->alert, alert->ok) instead
	// of repeating the same alert every 30 seconds.
	alertInstances map[string]bool

	// memoryUsed 缓存各实例最近一次 reconcile 拿到的 used_memory（字节）。
	// 由 reconcile 写入，列表接口读取。重启后为空，下一轮 reconcile 自动填充。
	memoryUsedMu sync.RWMutex
	memoryUsed   map[string]int64

	// aiAgent 内嵌式 AI 运维助手；仅在 cfg.AI.Enabled 时构造，否则为 nil。
	aiAgent *ai.Agent
}

func New(cfg *Config) (*Server, error) {
	normalizeImageConfig(cfg)
	if cfg.Ports.Redis.Start == 0 && cfg.Ports.Redis.End == 0 {
		cfg.Ports.Redis = PortRange{Start: 6379, End: 6499}
	}
	if cfg.Ports.ProxyAuto.Start == 0 && cfg.Ports.ProxyAuto.End == 0 {
		cfg.Ports.ProxyAuto = PortRange{Start: 16379, End: 16499}
	}
	if cfg.Ports.ProxyMaster.Start == 0 && cfg.Ports.ProxyMaster.End == 0 {
		cfg.Ports.ProxyMaster = PortRange{Start: 16500, End: 16619}
	}

	db, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("server: init store: %w", err)
	}

	s := &Server{
		cfg:            cfg,
		db:             db,
		log:            logger.Setup(logger.Config{Dir: cfg.Log.Dir, Stdout: cfg.Log.Stdout, RetentionDays: cfg.Log.RetentionDays, Level: cfg.Log.Level, Component: "server"}),
		audit:          audit.New(db),
		alertInstances: make(map[string]bool),
		memoryUsed:     make(map[string]int64),
	}
	s.scheduler = newBackupScheduler(s)
	s.initAIAgent()
	return s, nil
}

// initAIAgent 在 AI 助手启用时构造 Agent；失败只记日志、不阻断 Server 启动。
func (s *Server) initAIAgent() {
	if !s.cfg.AI.Enabled {
		return
	}
	timeout, _ := time.ParseDuration(s.cfg.AI.Timeout)
	agent, err := ai.NewAgent(aiPlatform{s}, ai.Options{
		BaseURL:   s.cfg.AI.BaseURL,
		APIKey:    s.cfg.AI.APIKey,
		Model:     s.cfg.AI.Model,
		MaxTurns:  s.cfg.AI.MaxTurns,
		MaxTokens: s.cfg.AI.MaxTokens,
		Timeout:   timeout,
		SkillsDir: s.cfg.AI.SkillsDir,
	})
	if err != nil {
		s.log.Errorf("ai assistant init failed: %v", err)
		return
	}
	s.aiAgent = agent
	s.log.Infof("ai assistant enabled: model=%s", s.cfg.AI.Model)
}

// StartWALCheckpointLoop 每 5 分钟执行一次 WAL checkpoint（TRUNCATE），
// 防止持续写入导致 WAL 文件无限增长。
//
// SQLite 的自动 checkpoint 仅在默认阈值（1000 页 ≈ 4MB）满足且没有活跃读者时触发；
// 长轮询等长连接可能持续持有读事务（snapshot），导致自动 checkpoint 被无限推迟。
// 显式定期 checkpoint 在 busy_timeout 窗口内重试，能可靠回收 WAL 空间。
func (s *Server) StartWALCheckpointLoop() {
	go func() {
		for range time.Tick(5 * time.Minute) {
			if err := s.db.Checkpoint(); err != nil {
				s.log.Errorf("wal checkpoint failed: %v", err)
			}
		}
	}()
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

	// ── 公开路由（无需认证）───────────────────────────────────────────────
	r.POST("/auth/login", s.authLogin)
	r.GET("/", s.dashboardRoot)
	r.GET("/dashboard", s.dashboardIndex)
	r.GET("/dashboard/*filepath", s.dashboardAsset)

	// ── 内部组件路由（静态 internal_token 校验）──────────────────────────
	// Proxy 拉快照：机器凭证走配置文件（server.yaml 的 internal_token 与
	// proxy.yaml 的 server_token 一致），与用户体系（JWT / 页面签发 rpt_ token）分离。
	internal := r.Group("")
	internal.Use(s.internalTokenMiddleware())
	internal.GET("/proxy/snapshot", s.proxySnapshot)
	internal.GET("/api/v1/proxy/snapshot", s.proxySnapshot)

	// ── 需要认证的路由（JWT 或 API Token）────────────────────────────────
	authed := r.Group("")
	authed.Use(s.unifiedAuthMiddleware())

	// 用户与 Token 管理（自己的密码/token 任何角色都能操作，用户增删仅 admin）
	authed.GET("/auth/me", s.authMe)
	authed.POST("/auth/change-password", s.authChangePassword)
	authed.POST("/auth/token/create", s.authTokenCreate)
	authed.GET("/auth/token/list", s.authTokenList)
	authed.POST("/auth/token/delete", s.authTokenDelete)

	// 用户管理（仅 admin）
	adminAuthed := authed.Group("")
	adminAuthed.Use(s.adminOnly())
	adminAuthed.POST("/auth/user/create", s.authUserCreate)
	adminAuthed.GET("/auth/user/list", s.authUserList)
	adminAuthed.POST("/auth/user/delete", s.authUserDelete)

	// 节点管理（需要 admin 才能写）
	node := authed.Group("/node")
	node.Use(s.adminOnly())
	{
		node.GET("/list", s.nodeQuery)
		node.POST("/add", s.nodeAdd)
		node.POST("/remove", s.nodeRemove)
		node.POST("/update", s.nodeUpdate)
	}

	// 实例管理（需要 admin 才能写）
	instance := authed.Group("/instance")
	instance.Use(s.adminOnly())
	{
		instance.GET("/list", s.instanceList)
		instance.GET("/status", s.instanceStatus)
		instance.GET("/metrics", s.metricsCollect)
		instance.GET("/metrics/history", s.instanceMetrics)
		instance.GET("/diagnose", s.instanceDiagnose)
		instance.GET("/slowlog", s.instanceSlowlog)
		instance.GET("/bigkeys", s.instanceBigkeys)
		instance.POST("/bigkeys", s.instanceBigkeysStart)
		instance.GET("/config", s.instanceConfigGet)
		instance.POST("/create", s.instanceCreate)
		instance.POST("/delete", s.instanceDelete)
		instance.POST("/start", s.instanceStart)
		instance.POST("/stop", s.instanceStop)
		instance.POST("/config", s.instanceConfig)
		instance.POST("/resize", s.instanceResize)
		instance.POST("/access-mode", s.instanceAccessMode)
		instance.POST("/labels", s.instanceGroupLabels)
		instance.POST("/failover", s.instanceFailover)
		instance.POST("/replicate", s.instanceReplicate)
	}

	// 备份（需要 admin 才能写）
	backup := authed.Group("/backup")
	backup.Use(s.adminOnly())
	{
		backup.POST("/exec", s.backupExec)
		backup.GET("/status", s.backupStatus)
		backup.POST("/restore", s.backupRestore)
		backup.GET("/list", s.backupList)
		backup.GET("/schedule", s.backupGetSchedule)
		backup.POST("/schedule", s.backupSetSchedule)
	}

	authed.GET("/audit/query", s.auditQuery)
	authed.GET("/tasks", s.taskQuery)
	authed.GET("/tasks/:id", s.taskDetail)
	authed.POST("/tasks/:id/cancel", s.adminOnly(), s.taskCancel)
	authed.GET("/server/info", s.serverInfo)
	authed.GET("/metrics", s.metricsCollect)

	// AI 运维助手（SSE 流式对话）。挂在 authed：所有登录用户可用只读诊断。
	// 写操作默认关闭（cfg.AI.AllowWrite）；开启后写工具也只产出 confirm，
	// 真正执行经下方 /ai/execute 端点，且额外校验 admin（见 §7.2）。
	authed.POST("/api/v1/ai/chat", s.aiChat)
	// AI 写操作执行端点：仅在用户二次确认后由前端调用，handler 内硬校验 admin。
	authed.POST("/api/v1/ai/execute", s.aiExecute)
	// AI 能力探测：前端据此决定是否展示写操作预置引导（allow_write = 总闸 && admin）。
	authed.GET("/api/v1/ai/capabilities", s.aiCapabilities)
	// AI 会话历史（§13）：严格私有，handler 内按 operatorFrom(c) 归属校验，非本人 → 404。
	authed.GET("/api/v1/ai/conversations", s.aiListConversations)
	authed.GET("/api/v1/ai/conversations/:id", s.aiGetConversation)
	authed.DELETE("/api/v1/ai/conversations/:id", s.aiDeleteConversation)

	// 以下写操作仅 admin
	adminAuthed.POST("/reconcile", s.reconcile)

	// Sentinel 管理（需要 admin 才能写）
	sentinel := authed.Group("/sentinel")
	sentinel.Use(s.adminOnly())
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

// internalTokenMiddleware 校验内部组件（Proxy）携带的静态 internal_token。
// 机器凭证与用户体系分离：不查库、不依赖登录态，server.yaml 的 internal_token
// 与 proxy.yaml 的 server_token 两侧保持一致即可；为空则跳过校验（本地开发便捷，
// 快照含实例地址与密码，生产环境务必配置）。
func (s *Server) internalTokenMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.cfg.InternalToken == "" {
			c.Next()
			return
		}
		auth := c.GetHeader("Authorization")
		token := strings.TrimPrefix(auth, "Bearer ")
		// 用 subtle.ConstantTimeCompare 而非 !=：字符串比较在首个不同字节处短路，
		// 耗时随匹配前缀长度变化。这条路径的可利用性很低（网络抖动远大于纳秒级差异），
		// 改动成本也极低，按纵深防御处理。
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.InternalToken)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, apitypes.APIResponse{Error: "unauthorized"})
			return
		}
		c.Next()
	}
}

func operatorFrom(c *gin.Context) string {
	if user, ok := c.Get("auth_user"); ok {
		if s, _ := user.(string); s != "" {
			return s
		}
	}
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
