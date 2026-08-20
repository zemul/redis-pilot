package agent

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/podman"
	s3client "gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/s3"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/redisconf"
)

// validInstanceName 限制实例名只能包含字母、数字、下划线、中横线，长度 1-64，且不能以 "-" 开头。
// 用于防止 req.Name 被拼入文件路径/容器名时发生路径穿越（如 "../../etc"）或命令注入；
// 禁止以 "-" 开头是为了防止参数注入（如名字为 "--privileged"）——该名字会被拼入
// podman run 的命令行参数（见 internal/podman/podman.go），若允许以 "-" 开头，
// podman 的参数解析器可能把实例名当作一个真正的 flag 处理，从而绕过预期的容器隔离。
var validInstanceName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func validateInstanceName(name string) error {
	if !validInstanceName.MatchString(name) {
		return fmt.Errorf("invalid instance name: must match ^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")
	}
	return nil
}

// validBackupTs 限制备份时间戳格式为 "2006-01-02T15:04:05"，防止 backup_ts 被用于路径穿越。
var validBackupTs = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$`)

func validateBackupTs(ts string) error {
	if !validBackupTs.MatchString(ts) {
		return fmt.Errorf("invalid backup_ts: must match ^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$")
	}
	return nil
}

type Agent struct {
	cfg           *Config
	log           *logger.Logger
	mon           *monitor
	runtime       podman.ContainerRuntime
	s3            *s3client.Client // nil if S3 not configured
	passwords     sync.Map         // instanceName -> password
	instanceLocks sync.Map         // instanceName -> *sync.Mutex
	backupTasks   sync.Map         // taskID -> *backupTask
}

// backupTask tracks the state of an async backup operation.
type backupTask struct {
	ID        string `json:"id"`
	Instance  string `json:"instance"`
	Status    string `json:"status"` // "running", "success", "failed"
	StartedAt string `json:"started_at"`
	DoneAt    string `json:"done_at,omitempty"`
	Error     string `json:"error,omitempty"`
	Backup    string `json:"backup,omitempty"` // timestamp of the backup on success
}

func New(cfg *Config) *Agent {
	a := &Agent{
		cfg:     cfg,
		log:     logger.Setup(logger.Config{Dir: cfg.Log.Dir, Stdout: cfg.Log.Stdout, RetentionDays: cfg.Log.RetentionDays, Level: cfg.Log.Level, Component: "agent"}),
		mon:     newMonitor(),
		runtime: podman.NewRuntime(),
	}
	// Initialize S3 client; nil is fine (backup/list/restore will return error).
	if cfg.Backup.S3.Endpoint != "" {
		client, err := s3client.New(s3client.Config{
			Endpoint:  cfg.Backup.S3.Endpoint,
			Bucket:    cfg.Backup.S3.Bucket,
			AccessKey: cfg.Backup.S3.AccessKey,
			SecretKey: cfg.Backup.S3.SecretKey,
			Region:    cfg.Backup.S3.Region,
			UseSSL:    cfg.Backup.S3.UseSSL,
		})
		if err != nil {
			a.log.Errorf("s3 client init failed: %v", err)
		} else {
			a.s3 = client
		}
	}
	a.loadPasswords()
	agentDataDir = cfg.DataDir
	return a
}

// lockInstance 获取指定实例的互斥锁，返回解锁函数。
// 用于防止对同一实例的并发操作（如备份期间被 stop/delete）导致数据损坏。
func (a *Agent) lockInstance(name string) func() {
	v, _ := a.instanceLocks.LoadOrStore(name, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// loadPasswords scans existing instance config files to recover passwords after restart.
func (a *Agent) loadPasswords() {
	entries, err := os.ReadDir(a.cfg.DataDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		for _, conf := range []string{"conf/redis.conf", "conf/kvrocks.conf"} {
			data, err := os.ReadFile(filepath.Join(a.cfg.DataDir, name, conf))
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "requirepass ") {
					pw := strings.TrimPrefix(line, "requirepass ")
					if pw != "" {
						a.passwords.Store(name, pw)
					}
				}
			}
			break
		}
	}
}

// getPassword returns the stored password for an instance, or empty string.
func (a *Agent) getPassword(instanceName string) string {
	if v, ok := a.passwords.Load(instanceName); ok {
		return v.(string)
	}
	return ""
}

func (a *Agent) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(a.requestLogger())
	r.Use(a.authMiddleware())

	r.GET("/host/resources", a.hostResources)
	r.GET("/host/health", a.hostHealth)

	inst := r.Group("/instance")
	{
		inst.GET("/list", a.instanceList)
		inst.GET("/status", a.instanceStatus)
		inst.POST("/create", a.instanceCreate)
		inst.POST("/start", a.instanceStart)
		inst.POST("/stop", a.instanceStop)
		inst.POST("/delete", a.instanceDelete)
		inst.POST("/config", a.instanceConfig)
		inst.POST("/promote", a.instancePromote)
		inst.POST("/replicate", a.instanceReplicate)
		inst.POST("/backup", a.instanceBackup)
		inst.GET("/backup/status", a.instanceBackupStatus)
		inst.POST("/restore", a.instanceRestore)
		inst.GET("/backups", a.instanceBackups)
		inst.GET("/diagnose", a.instanceDiagnose)
		inst.GET("/slowlog", a.instanceSlowlog)
		inst.GET("/bigkeys", a.instanceBigkeys)
	}

	return r
}

func (a *Agent) Start() {
	go a.runHealthCheck()
	go a.runMetricsCollect()
	addr := fmt.Sprintf(":%d", a.cfg.Port)
	a.log.Infof("redis-pilot agent listening on %s", addr)
	a.Router().Run(addr)
}

func (a *Agent) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if a.cfg.Token == "" {
			c.Next()
			return
		}
		token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
		// 用 subtle.ConstantTimeCompare 而非 !=：字符串比较在首个不同字节处短路，
		// 耗时随匹配前缀长度变化。这条路径的可利用性很低（网络抖动远大于纳秒级差异），
		// 改动成本也极低，按纵深防御处理。
		if subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.Token)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, apitypes.APIResponse{Error: "unauthorized"})
			return
		}
		c.Next()
	}
}

func (a *Agent) requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		a.log.Infof("%s %s %d %dms", c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start).Milliseconds())
	}
}

// instanceCreate 创建实例
func (a *Agent) instanceCreate(c *gin.Context) {
	var req apitypes.CreateInstanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if req.EngineImage == "" {
		fail(c, http.StatusBadRequest, "engine_image is required")
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	dataDir := filepath.Join(a.cfg.DataDir, req.Name)
	for _, sub := range []string{"conf", "data", "backup"} {
		if err := os.MkdirAll(filepath.Join(dataDir, sub), 0755); err != nil {
			fail(c, http.StatusInternalServerError, "mkdir: "+err.Error())
			return
		}
	}

	// 渲染配置文件
	overrides, err := configOverridesString(req.ConfigOverrides)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	minReplicas := minReplicasToWrite(req.Type, req.ReplicaOf)
	var createErr error
	if req.Engine == "kvrocks" {
		createErr = writeKvrocksConfig(dataDir, KvrocksConfigParams{
			Port:               req.Port,
			Password:           req.Password,
			Memory:             req.Memory,
			ReplicaOf:          req.ReplicaOf,
			ConfigOverrides:    overrides,
			MinReplicasToWrite: minReplicas,
			AnnounceIP:         a.cfg.AnnounceIP,
			AnnouncePort:       req.Port,
		})
	} else {
		policy := "allkeys-lru"
		if req.Category == "persistent" {
			policy = "noeviction"
		}
		aof := "yes"
		if req.Category == "cache" {
			aof = "no"
		}
		createErr = writeRedisConfig(dataDir, RedisConfigParams{
			Version:            req.EngineVersion,
			Port:               req.Port,
			Password:           req.Password,
			Memory:             req.Memory,
			MaxmemoryPolicy:    policy,
			Appendonly:         aof,
			ReplicaOf:          req.ReplicaOf,
			ConfigOverrides:    overrides,
			MinReplicasToWrite: minReplicas,
			AnnounceIP:         a.cfg.AnnounceIP,
			AnnouncePort:       req.Port,
		})
	}
	if createErr != nil {
		fail(c, http.StatusInternalServerError, "write config: "+createErr.Error())
		return
	}

	// 启动容器
	containerName := req.Engine + "-" + req.Name
	containerID, createErr := a.runtime.Create(req.Engine, containerName, req.EngineImage, req.Port, req.Memory, req.CPUs, dataDir)
	if createErr != nil {
		fail(c, http.StatusInternalServerError, "podman create: "+createErr.Error())
		return
	}

	a.passwords.Store(req.Name, req.Password)
	a.log.Infof("instance created: %s (container: %s)", req.Name, containerName)
	ok(c, gin.H{
		"container_id":   containerID,
		"container_name": containerName,
		"data_dir":       dataDir,
		"engine_version": req.EngineVersion,
		"image":          req.EngineImage,
	})
}

func (a *Agent) instanceStart(c *gin.Context) {
	var req struct {
		Name   string `json:"name" binding:"required"`
		Engine string `json:"engine" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	if err := a.runtime.Start(req.Engine + "-" + req.Name); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	a.log.Infof("instance started: %s", req.Name)
	ok(c, nil)
}

func (a *Agent) instanceStop(c *gin.Context) {
	var req struct {
		Name   string `json:"name" binding:"required"`
		Engine string `json:"engine" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	if err := a.runtime.Stop(req.Engine + "-" + req.Name); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	a.log.Infof("instance stopped: %s", req.Name)
	ok(c, nil)
}

func (a *Agent) instanceDelete(c *gin.Context) {
	var req struct {
		Name      string `json:"name" binding:"required"`
		Engine    string `json:"engine" binding:"required"`
		CleanData bool   `json:"clean_data"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	containerName := req.Engine + "-" + req.Name
	// 先优雅停止，让 Redis 有机会 flush 数据；忽略错误（容器可能已停止）
	_ = a.runtime.Stop(containerName)
	if err := a.runtime.Remove(containerName); err != nil {
		// 容器不存在时不应报错（幂等删除）
		if !isContainerNotFound(err) {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if req.CleanData {
		os.RemoveAll(filepath.Join(a.cfg.DataDir, req.Name))
		// Best-effort S3 cleanup: delete all backup objects for this instance.
		if a.s3 != nil {
			if err := a.s3.DeletePrefix(c.Request.Context(), req.Name+"/"); err != nil {
				a.log.Errorf("instance delete: s3 cleanup for %s failed: %v", req.Name, err)
			}
		}
	}
	a.log.Infof("instance deleted: %s (clean_data=%v)", req.Name, req.CleanData)
	ok(c, nil)
}

// instanceConfigRequest 是 Server 下发的配置变更载荷。
//
// 三种互斥的作用模式：
//   - RewriteConfigOnly=true：只重写磁盘配置文件，不碰运行中的容器
//   - Restart=true：重写磁盘配置文件并重启容器（配置项需要重启才生效时用）
//   - 两者都为 false：热更新，逐项 CONFIG SET，不动磁盘配置文件
type instanceConfigRequest struct {
	Name   string `json:"name" binding:"required"`
	Engine string `json:"engine" binding:"required"`
	Type   string `json:"type"`
	// 注意：这里不加 binding:"required"。RewriteConfigOnly 模式下（角色落盘）
	// 实例可能本来就没有任何 config_overrides，空 map 是合法输入。
	// 面向用户的 config 变更由 Server 侧校验非空。
	ConfigOverrides map[string]string `json:"config_overrides"`
	Restart         bool              `json:"restart"`
	// RewriteConfigOnly 只把磁盘配置文件重写成与请求一致，不重启容器、也不做 CONFIG SET。
	//
	// 用途是角色落盘。Sentinel 在运行时用 REPLICAOF 改变实例角色，这个变更只存在于进程
	// 内存中；磁盘配置文件里还写着旧的 replicaof。容器一旦重启（podman 的
	// --restart on-failure:5 会在进程异常退出时自动重启），实例就按旧配置回退到旧角色，
	// 与 Sentinel 的认知产生分歧。Server 在角色变化后调用本模式把新角色写进配置文件，
	// 使"重启后读到的角色"与"Sentinel 决定的角色"一致。
	//
	// 刻意不重启容器：重启本身就是要避免的动作，而且此时运行时角色已经是对的，
	// 需要修正的只有磁盘上那份。
	RewriteConfigOnly bool   `json:"rewrite_config_only"`
	EngineVersion     string `json:"engine_version"`
	Password          string `json:"password"`
	Memory            string `json:"memory"`
	Category          string `json:"category"`
	ReplicaOf         string `json:"replica_of"`
	Port              int    `json:"port"`
}

// writeInstanceConfig 按请求参数渲染并落盘实例配置文件。
func (a *Agent) writeInstanceConfig(req *instanceConfigRequest) error {
	dataDir := filepath.Join(a.cfg.DataDir, req.Name)
	overrides, err := configOverridesString(req.ConfigOverrides)
	if err != nil {
		return err
	}
	minReplicas := minReplicasToWrite(req.Type, req.ReplicaOf)
	if req.Engine == "kvrocks" {
		return writeKvrocksConfig(dataDir, KvrocksConfigParams{
			Port:               req.Port,
			Password:           req.Password,
			Memory:             req.Memory,
			ReplicaOf:          req.ReplicaOf,
			ConfigOverrides:    overrides,
			MinReplicasToWrite: minReplicas,
			AnnounceIP:         a.cfg.AnnounceIP,
			AnnouncePort:       req.Port,
		})
	}
	policy := "allkeys-lru"
	if req.Category == "persistent" {
		policy = "noeviction"
	}
	aof := "yes"
	if req.Category == "cache" {
		aof = "no"
	}
	return writeRedisConfig(dataDir, RedisConfigParams{
		Version:            req.EngineVersion,
		Port:               req.Port,
		Password:           req.Password,
		Memory:             req.Memory,
		MaxmemoryPolicy:    policy,
		Appendonly:         aof,
		ReplicaOf:          req.ReplicaOf,
		ConfigOverrides:    overrides,
		MinReplicasToWrite: minReplicas,
		AnnounceIP:         a.cfg.AnnounceIP,
		AnnouncePort:       req.Port,
	})
}

func (a *Agent) instanceConfig(c *gin.Context) {
	var req instanceConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	// 在入口统一校验，让三种模式（rewrite_config_only / restart / 热更新）都对非法输入
	// 返回 400。若只在各分支内部依赖 writeInstanceConfig 返回的 error，客户端输入问题
	// 会被报成 500，看起来像是 Agent 自己出了故障。
	if err := redisconf.ValidateOverrides(req.ConfigOverrides); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	containerName := req.Engine + "-" + req.Name
	if req.Password != "" {
		a.passwords.Store(req.Name, req.Password)
	}

	switch {
	case req.RewriteConfigOnly:
		// 角色落盘：只同步磁盘配置文件，运行中的实例完全不受影响。
		if err := a.writeInstanceConfig(&req); err != nil {
			fail(c, http.StatusInternalServerError, "write config: "+err.Error())
			return
		}

	case req.Restart:
		// 重写配置文件并重启容器
		if err := a.writeInstanceConfig(&req); err != nil {
			fail(c, http.StatusInternalServerError, "write config: "+err.Error())
			return
		}
		if err := a.runtime.Stop(containerName); err != nil {
			fail(c, http.StatusInternalServerError, "stop container: "+err.Error())
			return
		}
		if err := a.runtime.Start(containerName); err != nil {
			fail(c, http.StatusInternalServerError, "start container after config update failed, instance may be down: "+err.Error())
			return
		}

	default:
		// 热更新：通过 CONFIG SET 逐个设置。
		//
		// 这条路径不经过配置文件渲染，没有换行注入问题（redisCmd 用 argv 数组调用
		// podman，值作为独立参数传递），但保留字校验同样必要——运行时把 replicaof
		// 或 requirepass 改掉，破坏力与写进配置文件一样，只是不会在重启后留存。
		// 校验已在本函数入口统一做过。
		//
		// 按 key 排序执行，让失败位置可复现：map 随机序下同一请求可能停在不同的项上，
		// 留下不同的中间状态。
		for _, k := range redisconf.SortedKeys(req.ConfigOverrides) {
			if _, err := redisCmdChecked(req.Name, a.getPassword(req.Name), "CONFIG", "SET", k, req.ConfigOverrides[k]); err != nil {
				fail(c, http.StatusInternalServerError, "CONFIG SET "+k+": "+err.Error())
				return
			}
		}
	}

	ok(c, nil)
}

func (a *Agent) instancePromote(c *gin.Context) {
	var req struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	if _, err := redisCmd(req.Name, a.getPassword(req.Name), "REPLICAOF", "NO", "ONE"); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (a *Agent) instanceReplicate(c *gin.Context) {
	var req struct {
		Name      string `json:"name" binding:"required"`
		ReplicaOf string `json:"replica_of" binding:"required"` // "ip port"
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	parts := strings.SplitN(req.ReplicaOf, ":", 2)
	if len(parts) != 2 {
		fail(c, http.StatusBadRequest, "replica_of must be ip:port")
		return
	}
	password := a.getPassword(req.Name)
	// 目标 master 大多率下与本实例使用同一份密码（Server 侧创建同组实例时会强制保持一致），
	// 运行时切换复制目标必须显式 CONFIG SET masterauth，否则 REPLICAOF 握手会因为
	// 目标 master 要求 AUTH 而失败（-NOAUTH Authentication required）。
	if password != "" {
		if _, err := redisCmd(req.Name, password, "CONFIG", "SET", "masterauth", password); err != nil {
			fail(c, http.StatusInternalServerError, "failed to set masterauth before replicaof: "+err.Error())
			return
		}
	}
	// Kvrocks 在处理 SLAVEOF/REPLICAOF 时会拒绝把"当前仍连接着的自己的 slave"设为新 master
	// （防止环形复制），错误为 "can't replicate your own replicas"。这种场景常见于故障转移场景：
	// 旧 master 刚失去一个 slave（该 slave 被提升为新 master），旧 master 内部清理这条陈旧
	// slave 记录是异步的（依赖心跳超时探测），存在短暂的时间窗口。这里做几次重试，
	// 给旧 master 时间清理陈旧状态，避免故障转移后旧 master 永久停留在独立 master（脑裂）。
	// 注意：redis-cli 即使收到 RESP 错误回复（-ERR ...），进程本身也以退出码 0 正常结束，
	// 所以必须检查输出内容而不是 err，否则永远走不到重试分支。
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		out, err := redisCmd(req.Name, password, "REPLICAOF", parts[0], parts[1])
		if err != nil {
			lastErr = err
			break
		}
		if !isRedisErrReply(out) {
			lastErr = nil
			break
		}
		lastErr = fmt.Errorf("%s", out)
		if !strings.Contains(out, "own replicas") {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		fail(c, http.StatusInternalServerError, lastErr.Error())
		return
	}
	ok(c, nil)
}

// ---------------------------------------------------------------------------
// 磁盘配置文件中的角色
//
// 角色回退问题的根源：Sentinel 在运行时用 REPLICAOF 改变实例角色，这个变更只在进程
// 内存里；磁盘配置文件仍写着旧的 replicaof。容器重启（podman --restart on-failure:5
// 会在进程异常退出时自动重启）后实例按旧配置起来，角色就回退了。
//
// Server 需要能看到"磁盘上写的角色"才能发现这种漂移并补推重写。这里刻意读配置文件
// 而不是查运行时角色：要修的就是磁盘那份，直接比对它最贴合问题本身，而且只需一次
// 文件读取，不用对每个实例 exec redis-cli。
// ---------------------------------------------------------------------------

// readConfiguredReplicaOf 从实例的磁盘配置文件中读出 replicaof/slaveof 的目标，
// 归一化为 "ip:port"。配置文件里没有该指令（即配置为 master）时返回空串。
//
// 配置文件不存在时返回空串且不报错——实例可能刚创建完还没写配置，或已被删除，
// 这不是需要 Server 介入的漂移。
func readConfiguredReplicaOf(dataDir, instanceName string) (string, error) {
	confDir := filepath.Join(dataDir, instanceName, "conf")
	for _, filename := range []string{"redis.conf", "kvrocks.conf"} {
		data, err := os.ReadFile(filepath.Join(confDir, filename))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		return parseReplicaOfDirective(string(data)), nil
	}
	return "", nil
}

// parseReplicaOfDirective 从配置文件内容里提取 replicaof/slaveof 的目标地址。
// 配置文件中的形式是 "replicaof <ip> <port>"（模板把 ip:port 中的冒号换成了空格），
// 这里归一化回 "ip:port" 以便与 Server 侧解析出的地址直接比较。
func parseReplicaOfDirective(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		directive := strings.ToLower(fields[0])
		if directive != "replicaof" && directive != "slaveof" {
			continue
		}
		// "replicaof no one" 表示显式声明为 master
		if len(fields) == 3 && strings.EqualFold(fields[1], "no") && strings.EqualFold(fields[2], "one") {
			return ""
		}
		if len(fields) >= 3 {
			return fields[1] + ":" + fields[2]
		}
		// 只有一个参数的畸形配置（例如误把实例名当地址写入），原样返回让 Server 判定为漂移
		return fields[1]
	}
	return ""
}

func (a *Agent) instanceList(c *gin.Context) {
	containers, err := a.runtime.ListAll()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	// 附带每个实例磁盘配置文件里的角色，让 Server 的 reconcile 能发现角色漂移。
	// 与 containers 一起返回是为了让 reconcile 保持"每台服务器一次调用"。
	configured := make(map[string]string)
	for _, ct := range containers {
		instName := trimPrefix(ct.Name)
		if instName == "" {
			continue
		}
		replicaOf, err := readConfiguredReplicaOf(a.cfg.DataDir, instName)
		if err != nil {
			a.log.Errorf("read configured replicaof for %s: %v", instName, err)
			continue
		}
		configured[instName] = replicaOf
	}
	// 以及每个实例进程此刻实际在复制谁，让 reconcile 能发现运行时复制漂移。
	//
	// 取的是健康探测（每 30s）缓存下来的结果，不在这里现场探测：现场探测意味着本接口耗时
	// 随实例数线性增长，而任何一个 hang 住的实例都会把整个响应拖过 Server 侧
	// agentHTTPClient 的超时，让 reconcile 把这台机器上所有实例误判为 unreachable——
	// 用一个纯观测功能换掉整台机器的可观测性是净损失。
	// 观测过期或探测失败的实例不出现在 map 里，Server 侧据此跳过。
	ok(c, gin.H{
		"containers":            containers,
		"configured_replica_of": configured,
		"runtime_replica_of":    a.mon.replicaOfMap(),
		"used_memory":           a.mon.usedMemoryMap(),
	})
}

func (a *Agent) instanceStatus(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	if err := validateInstanceName(name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	info, err := redisCmd(name, a.getPassword(name), "INFO")
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp := gin.H{"info": info, "timestamp": time.Now().Format(time.RFC3339)}
	// 附带最近一次健康探测的分类结果。容器 reconcile 只能看到「容器在不在跑」，
	// 看不到「容器活着但实例无响应 / 正在 loading / 认证不一致」，这里把该信息暴露出来
	// 供 Server 和运维判断（Agent 自己不再据此采取任何动作，见 runHealthCheck 注释）。
	if h := a.mon.getHealth(name); h != nil {
		resp["health"] = gin.H{
			"state":      string(h.Health),
			"detail":     h.Detail,
			"checked_at": h.UpdatedAt.Format(time.RFC3339),
		}
	}
	ok(c, resp)
}

func (a *Agent) instanceDiagnose(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	if err := validateInstanceName(name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	pw := a.getPassword(name)

	info, err := redisCmd(name, pw, "INFO")
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	slowlog, _ := redisCmd(name, pw, "SLOWLOG", "GET", "20")
	memDoctor, _ := redisCmd(name, pw, "MEMORY", "DOCTOR")

	ok(c, gin.H{
		"info":          info,
		"slowlog":       slowlog,
		"memory_doctor": memDoctor,
		"timestamp":     time.Now().Format(time.RFC3339),
	})
}

func (a *Agent) instanceSlowlog(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	if err := validateInstanceName(name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	count := c.DefaultQuery("count", "50")
	pw := a.getPassword(name)

	raw, err := redisCmd(name, pw, "SLOWLOG", "GET", count)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	entries := parseSlowlog(raw)
	ok(c, gin.H{"entries": entries, "raw": raw})
}

func (a *Agent) instanceBigkeys(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	if err := validateInstanceName(name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	pw := a.getPassword(name)

	// 阈值: string > 100KB, 集合类 > 10000 元素
	const stringThreshold = 102400
	const collectionThreshold = 10000
	const maxDuration = 60 * time.Second
	const scanCount = "200"
	const sleepInterval = 10 * time.Millisecond

	var bigKeys []map[string]interface{}
	cursor := "0"
	start := time.Now()
	scanned := 0

	for {
		if time.Since(start) > maxDuration {
			break
		}
		out, err := redisCmd(name, pw, "SCAN", cursor, "COUNT", scanCount)
		if err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		lines := strings.SplitN(out, "\n", 2)
		if len(lines) < 2 {
			break
		}
		cursor = strings.TrimSpace(lines[0])
		keys := strings.Split(strings.TrimSpace(lines[1]), "\n")

		for _, key := range keys {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			scanned++
			typeOut, err := redisCmd(name, pw, "TYPE", key)
			if err != nil {
				continue
			}
			keyType := strings.TrimSpace(typeOut)

			var size int64
			var unit string
			switch keyType {
			case "string":
				lenOut, err := redisCmd(name, pw, "STRLEN", key)
				if err != nil {
					continue
				}
				size = parseIntFromReply(lenOut)
				if size <= int64(stringThreshold) {
					continue
				}
				unit = "bytes"
			case "list":
				lenOut, err := redisCmd(name, pw, "LLEN", key)
				if err != nil {
					continue
				}
				size = parseIntFromReply(lenOut)
				if size <= int64(collectionThreshold) {
					continue
				}
				unit = "elements"
			case "hash":
				lenOut, err := redisCmd(name, pw, "HLEN", key)
				if err != nil {
					continue
				}
				size = parseIntFromReply(lenOut)
				if size <= int64(collectionThreshold) {
					continue
				}
				unit = "fields"
			case "set":
				lenOut, err := redisCmd(name, pw, "SCARD", key)
				if err != nil {
					continue
				}
				size = parseIntFromReply(lenOut)
				if size <= int64(collectionThreshold) {
					continue
				}
				unit = "members"
			case "zset":
				lenOut, err := redisCmd(name, pw, "ZCARD", key)
				if err != nil {
					continue
				}
				size = parseIntFromReply(lenOut)
				if size <= int64(collectionThreshold) {
					continue
				}
				unit = "members"
			case "stream":
				lenOut, err := redisCmd(name, pw, "XLEN", key)
				if err != nil {
					continue
				}
				size = parseIntFromReply(lenOut)
				if size <= int64(collectionThreshold) {
					continue
				}
				unit = "entries"
			default:
				continue
			}
			bigKeys = append(bigKeys, map[string]interface{}{
				"key":       key,
				"type":      keyType,
				"size":      size,
				"size_desc": fmt.Sprintf("%d %s", size, unit),
			})
		}

		if cursor == "0" {
			break
		}
		time.Sleep(sleepInterval)
	}

	ok(c, gin.H{
		"keys":      bigKeys,
		"scanned":   scanned,
		"timeout":   time.Since(start) > maxDuration,
		"elapsed_ms": time.Since(start).Milliseconds(),
		"threshold": map[string]interface{}{
			"string_bytes":        stringThreshold,
			"collection_elements": collectionThreshold,
		},
	})
}

func (a *Agent) instanceBackup(c *gin.Context) {
	var req struct {
		Name      string `json:"name" binding:"required"`
		Engine    string `json:"engine" binding:"required"`
		Retention int    `json:"retention"` // 保留份数，0 表示不清理
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if a.s3 == nil {
		fail(c, http.StatusServiceUnavailable, "s3 not configured")
		return
	}

	// Create async task and return immediately.
	taskID := fmt.Sprintf("%s-%d", req.Name, time.Now().UnixNano())
	task := &backupTask{
		ID:        taskID,
		Instance:  req.Name,
		Status:    "running",
		StartedAt: time.Now().Format("2006-01-02T15:04:05"),
	}
	a.backupTasks.Store(taskID, task)

	go a.runBackup(task, req.Name, req.Engine, req.Retention)

	ok(c, gin.H{"task_id": taskID})
}

// instanceBackupStatus returns the current state of a backup task.
func (a *Agent) instanceBackupStatus(c *gin.Context) {
	taskID := c.Query("task_id")
	if taskID == "" {
		fail(c, http.StatusBadRequest, "task_id required")
		return
	}
	v, exists := a.backupTasks.Load(taskID)
	if !exists {
		fail(c, http.StatusNotFound, "task not found")
		return
	}
	ok(c, v)
}

// runBackup executes the full backup pipeline in the background.
func (a *Agent) runBackup(task *backupTask, name, engine string, retention int) {
	unlock := a.lockInstance(name)
	defer unlock()

	backupDir := filepath.Join(a.cfg.DataDir, name, "backup")
	os.MkdirAll(backupDir, 0o755)
	ts := time.Now().Format("2006-01-02T15:04:05")
	startedAt := time.Now()
	const persistenceTimeout = 60 * time.Second

	var localPath, s3Key, s3Ext string

	if engine == "kvrocks" {
		pw := a.getPassword(name)
		if _, err := redisCmdChecked(name, pw, "BGSAVE"); err != nil {
			a.failTask(task, "BGSAVE failed: "+err.Error())
			return
		}
		if err := waitForPersistenceDone(name, pw, "bgsave_in_progress:0", persistenceTimeout); err != nil {
			a.failTask(task, "BGSAVE did not complete: "+err.Error())
			return
		}
		src := filepath.Join(a.cfg.DataDir, name, "data", "backup")
		if err := verifyBackupSource(src, startedAt); err != nil {
			a.failTask(task, "BGSAVE produced no usable snapshot: "+err.Error())
			return
		}
		s3Ext = ".checkpoint.tar.gz"
		localPath = filepath.Join(backupDir, ts+s3Ext)
		s3Key = name + "/" + ts + s3Ext
		if _, err := runShell("tar", "-czf", localPath, "-C", src, "."); err != nil {
			os.Remove(localPath)
			a.failTask(task, err.Error())
			return
		}
	} else {
		pw := a.getPassword(name)
		info, err := redisCmd(name, pw, "INFO", "persistence")
		if err != nil {
			a.failTask(task, "read persistence info failed: "+err.Error())
			return
		}
		hasAOF := strings.Contains(info, "aof_enabled:1")

		if _, err := redisCmdChecked(name, pw, "BGSAVE"); err != nil {
			a.failTask(task, "BGSAVE failed: "+err.Error())
			return
		}
		if err := waitForPersistenceDone(name, pw, "rdb_bgsave_in_progress:0", persistenceTimeout); err != nil {
			a.failTask(task, "BGSAVE did not complete: "+err.Error())
			return
		}
		rdbPath := filepath.Join(a.cfg.DataDir, name, "data", "dump.rdb")
		if err := verifyBackupFile(rdbPath, startedAt); err != nil {
			a.failTask(task, "BGSAVE produced no usable dump.rdb: "+err.Error())
			return
		}

		if hasAOF {
			if _, err := redisCmdChecked(name, pw, "BGREWRITEAOF"); err != nil {
				a.failTask(task, "BGREWRITEAOF failed: "+err.Error())
				return
			}
			if err := waitForPersistenceDone(name, pw, "aof_rewrite_in_progress:0", persistenceTimeout); err != nil {
				a.failTask(task, "AOF rewrite did not complete: "+err.Error())
				return
			}
			stageDir := filepath.Join(backupDir, ts+".incomplete")
			os.RemoveAll(stageDir)
			if err := os.MkdirAll(stageDir, 0o755); err != nil {
				a.failTask(task, "create backup staging dir failed: "+err.Error())
				return
			}
			if err := copyFile(rdbPath, filepath.Join(stageDir, "dump.rdb")); err != nil {
				os.RemoveAll(stageDir)
				a.failTask(task, "backup dump.rdb failed: "+err.Error())
				return
			}
			aofDir := filepath.Join(a.cfg.DataDir, name, "data", "appendonlydir")
			if _, err := os.Stat(aofDir); err == nil {
				if _, err := runShell("cp", "-r", aofDir, filepath.Join(stageDir, "appendonlydir")); err != nil {
					os.RemoveAll(stageDir)
					a.failTask(task, "backup AOF dir failed: "+err.Error())
					return
				}
			} else {
				aofFile := filepath.Join(a.cfg.DataDir, name, "data", "appendonly.aof")
				if err := copyFile(aofFile, filepath.Join(stageDir, "appendonly.aof")); err != nil {
					os.RemoveAll(stageDir)
					a.failTask(task, "backup AOF file failed: "+err.Error())
					return
				}
			}
			s3Ext = ".rdbaof.tar.gz"
			localPath = filepath.Join(backupDir, ts+s3Ext)
			s3Key = name + "/" + ts + s3Ext
			if _, err := runShell("tar", "-czf", localPath, "-C", stageDir, "."); err != nil {
				os.Remove(localPath)
				os.RemoveAll(stageDir)
				a.failTask(task, "tar rdb+aof backup failed: "+err.Error())
				return
			}
			os.RemoveAll(stageDir)
		} else {
			s3Ext = ".rdb"
			localPath = filepath.Join(backupDir, ts+s3Ext)
			s3Key = name + "/" + ts + s3Ext
			if err := copyFile(rdbPath, localPath); err != nil {
				os.Remove(localPath)
				a.failTask(task, err.Error())
				return
			}
		}
	}

	// Upload to S3, then remove the local staging file.
	if err := a.s3.Upload(context.Background(), s3Key, localPath); err != nil {
		os.Remove(localPath)
		a.failTask(task, "s3 upload failed: "+err.Error())
		return
	}
	os.Remove(localPath)

	// S3 rotation cleanup (best-effort).
	if retention > 0 {
		a.cleanupS3Backups(context.Background(), name, retention)
	}

	task.Status = "success"
	task.DoneAt = time.Now().Format("2006-01-02T15:04:05")
	task.Backup = ts
}

func (a *Agent) failTask(task *backupTask, errMsg string) {
	task.Status = "failed"
	task.DoneAt = time.Now().Format("2006-01-02T15:04:05")
	task.Error = errMsg
	a.log.Errorf("backup task %s failed: %s", task.ID, errMsg)
}

func (a *Agent) instanceRestore(c *gin.Context) {
	var req struct {
		Name     string `json:"name" binding:"required"`
		Engine   string `json:"engine" binding:"required"`
		BackupTs string `json:"backup_ts" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateBackupTs(req.BackupTs); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if a.s3 == nil {
		fail(c, http.StatusServiceUnavailable, "s3 not configured")
		return
	}

	unlock := a.lockInstance(req.Name)
	defer unlock()

	ctx := c.Request.Context()
	dataDir := filepath.Join(a.cfg.DataDir, req.Name, "data")
	containerName := req.Engine + "-" + req.Name

	// 下载、解压和校验都在实例仍运行时完成，只有确认恢复数据完整可用后才进入停机窗口。
	// 暂存目录必须是 dataDir 的同级目录：最后整体 rename dataDir 才能把 RDB+AOF 作为
	// 一个数据集原子替换，避免两次独立 swap 产生“新 RDB + 旧 AOF”的混合状态。
	stageRoot := filepath.Join(a.cfg.DataDir, req.Name, ".restore-staging")
	os.RemoveAll(stageRoot)
	stagedData := filepath.Join(stageRoot, "data")
	if err := os.MkdirAll(stagedData, 0o755); err != nil {
		fail(c, http.StatusInternalServerError, "create restore staging dir failed: "+err.Error())
		return
	}
	// 失败时清掉暂存目录；成功路径下 data 已经被 rename 移走，这里只清理下载包。
	defer os.RemoveAll(stageRoot)

	if req.Engine == "kvrocks" {
		// Download checkpoint archive from S3 and stage it as a complete data/db tree.
		s3Key := req.Name + "/" + req.BackupTs + ".checkpoint.tar.gz"
		archivePath := filepath.Join(stageRoot, "backup.tar.gz")
		if err := a.s3.Download(ctx, s3Key, archivePath); err != nil {
			fail(c, http.StatusBadRequest, "download backup from s3 failed (original data untouched): "+err.Error())
			return
		}
		if err := verifyBackupArchiveExists(archivePath); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		stagedDB := filepath.Join(stagedData, "db")
		if err := os.MkdirAll(stagedDB, 0o755); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		if _, err := runShell("tar", "-xzf", archivePath, "-C", stagedDB); err != nil {
			fail(c, http.StatusInternalServerError, "extract backup failed (original data untouched): "+err.Error())
			return
		}
		if nonEmpty, err := dirHasEntries(stagedDB); err != nil || !nonEmpty {
			fail(c, http.StatusInternalServerError,
				"backup archive extracted to an empty dataset, refusing to replace existing data")
			return
		}
	} else {
		// Redis: determine backup format by checking S3 for rdbaof vs rdb.
		rdbaofKey := req.Name + "/" + req.BackupTs + ".rdbaof.tar.gz"
		rdbKey := req.Name + "/" + req.BackupTs + ".rdb"

		if a.s3.Exists(ctx, rdbaofKey) {
			// RDB + AOF joint backup: extract both into one staged data directory.
			archivePath := filepath.Join(stageRoot, "backup.tar.gz")
			if err := a.s3.Download(ctx, rdbaofKey, archivePath); err != nil {
				fail(c, http.StatusInternalServerError, "download rdbaof backup failed (original data untouched): "+err.Error())
				return
			}
			if _, err := runShell("tar", "-xzf", archivePath, "-C", stagedData); err != nil {
				fail(c, http.StatusInternalServerError, "extract rdbaof backup failed (original data untouched): "+err.Error())
				return
			}
			if err := verifyBackupArchiveExists(filepath.Join(stagedData, "dump.rdb")); err != nil {
				fail(c, http.StatusInternalServerError, "invalid dump.rdb in backup (original data untouched): "+err.Error())
				return
			}
			aofDir := filepath.Join(stagedData, "appendonlydir")
			if _, err := os.Stat(aofDir); err == nil {
				if nonEmpty, err := dirHasEntries(aofDir); err != nil || !nonEmpty {
					fail(c, http.StatusInternalServerError, "AOF directory in backup is empty or unreadable (original data untouched)")
					return
				}
			} else {
				if err := verifyBackupArchiveExists(filepath.Join(stagedData, "appendonly.aof")); err != nil {
					fail(c, http.StatusInternalServerError, "invalid AOF in backup (original data untouched): "+err.Error())
					return
				}
			}
		} else {
			// RDB-only backup: download single file, stage, swap.
			archivePath := filepath.Join(stagedData, "dump.rdb")
			if err := a.s3.Download(ctx, rdbKey, archivePath); err != nil {
				fail(c, http.StatusBadRequest, "backup not found in s3 (original data untouched): "+err.Error())
				return
			}
			if err := verifyBackupArchiveExists(archivePath); err != nil {
				fail(c, http.StatusInternalServerError, err.Error())
				return
			}
			// stagedData 中没有 AOF；整体替换 dataDir 后不会残留旧 AOF。
		}
	}

	if err := a.runtime.Stop(containerName); err != nil {
		fail(c, http.StatusInternalServerError, "stop container failed: "+err.Error())
		return
	}
	preserve := restorePreservePath(dataDir)
	if err := swapInPath(stagedData, dataDir, preserve); err != nil {
		// swap 自身会回滚；仍要把刚停止的原实例重新拉起。
		startErr := a.runtime.Start(containerName)
		if startErr != nil {
			fail(c, http.StatusInternalServerError, err.Error()+"; restart original container also failed: "+startErr.Error())
			return
		}
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if err := a.runtime.Start(containerName); err != nil {
		// 新数据无法启动时恢复整个旧数据集，再尽力恢复服务。
		rollbackErr := rollbackSwappedPath(dataDir, preserve)
		restartErr := a.runtime.Start(containerName)
		fail(c, http.StatusInternalServerError, fmt.Sprintf(
			"start restored container failed: %v; rollback=%v; restart_original=%v", err, rollbackErr, restartErr))
		return
	}
	ok(c, nil)
}

func (a *Agent) instanceBackups(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	if err := validateInstanceName(name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if a.s3 == nil {
		fail(c, http.StatusServiceUnavailable, "s3 not configured")
		return
	}

	objects, err := a.s3.List(c.Request.Context(), name+"/")
	if err != nil {
		fail(c, http.StatusInternalServerError, "s3 list failed: "+err.Error())
		return
	}

	type backupEntry struct {
		Name      string `json:"name"`
		Timestamp string `json:"timestamp"`
		Size      int64  `json:"size"`
		Format    string `json:"format"`
	}
	var backups []backupEntry
	for _, obj := range objects {
		// Key format: {instance}/{ts}.{ext}; strip the instance prefix.
		key := strings.TrimPrefix(obj.Key, name+"/")
		var ts, format string
		switch {
		case strings.HasSuffix(key, ".checkpoint.tar.gz"):
			ts = strings.TrimSuffix(key, ".checkpoint.tar.gz")
			format = "kvrocks_checkpoint"
		case strings.HasSuffix(key, ".rdbaof.tar.gz"):
			ts = strings.TrimSuffix(key, ".rdbaof.tar.gz")
			format = "rdb_aof"
		case strings.HasSuffix(key, ".rdb"):
			ts = strings.TrimSuffix(key, ".rdb")
			format = "rdb"
		default:
			continue
		}
		backups = append(backups, backupEntry{
			Name:      key,
			Timestamp: ts,
			Size:      obj.Size,
			Format:    format,
		})
	}
	ok(c, gin.H{"backups": backups})
}

// cleanupS3Backups retains only the most recent `retention` backups in S3 for
// the given instance, deleting older ones. Best-effort: errors are logged but
// do not fail the backup operation.
func (a *Agent) cleanupS3Backups(ctx context.Context, instanceName string, retention int) {
	if retention <= 0 || a.s3 == nil {
		return
	}
	objects, err := a.s3.List(ctx, instanceName+"/")
	if err != nil {
		a.log.Errorf("s3 cleanup: list failed for %s: %v", instanceName, err)
		return
	}
	if len(objects) <= retention {
		return
	}
	// Objects are already sorted by key ascending (s3.Client.List sorts).
	// Delete the oldest ones beyond retention.
	var toDelete []string
	for _, obj := range objects[:len(objects)-retention] {
		toDelete = append(toDelete, obj.Key)
	}
	if err := a.s3.Delete(ctx, toDelete); err != nil {
		a.log.Errorf("s3 cleanup: delete failed for %s: %v", instanceName, err)
	}
}

func (a *Agent) hostResources(c *gin.Context) {
	ok(c, a.mon.hostResources())
}

func (a *Agent) hostHealth(c *gin.Context) {
	ok(c, gin.H{"status": "ok", "timestamp": time.Now().Format(time.RFC3339)})
}

func ok(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, apitypes.APIResponse{Success: true, Data: data})
}

func fail(c *gin.Context, code int, err string) {
	c.JSON(code, apitypes.APIResponse{Error: err})
}

// minReplicasToWrite returns 1 for replication masters, 0 otherwise.
func minReplicasToWrite(instType, replicaOf string) int {
	// 依赖 Sentinel 秒级故障转移兜底，不用 min-replicas-to-write 牺牲可用性。
	// 与 AWS ElastiCache / GCP Memorystore 等托管服务保持一致。
	return 0
}

// configOverridesString 把配置覆盖项渲染成配置文件片段。
//
// 返回 error 而不是只返回字符串，是刻意的：渲染结果会被追加到配置模板的末尾，
// 而 Redis 配置是后者覆盖前者，所以这里是用户输入唯一能改变实例语义的入口。
// 把校验放在渲染函数内部，调用方就没有"忘记校验"的可能——想拿到字符串就必须
// 先通过校验。校验规则见 validateConfigOverrides。
//
// 输出按 key 排序，让同样的输入总是渲染出同样的文件。map 迭代顺序随机会导致
// 每次重写配置文件内容都不同，既让 diff 失去意义，也让"配置文件是否需要更新"
// 这类判断无法靠比较内容做出。
func configOverridesString(overrides map[string]string) (string, error) {
	if err := redisconf.ValidateOverrides(overrides); err != nil {
		return "", err
	}
	var sb strings.Builder
	for _, k := range redisconf.SortedKeys(overrides) {
		sb.WriteString(k + " " + overrides[k] + "\n")
	}
	return sb.String(), nil
}
