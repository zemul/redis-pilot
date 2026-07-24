package agent

import (
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
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
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
	cfg       *Config
	log       *logger.Logger
	mon       *monitor
	runtime   podman.ContainerRuntime
	passwords sync.Map // instanceName → password
}

func New(cfg *Config) *Agent {
	a := &Agent{
		cfg:     cfg,
		log:     logger.New(cfg.Log.Dir, cfg.Log.Stdout),
		mon:     newMonitor(),
		runtime: podman.NewRuntime(),
	}
	a.loadPasswords()
	return a
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
		inst.POST("/restore", a.instanceRestore)
		inst.GET("/backups", a.instanceBackups)
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
		if token != a.cfg.Token {
			c.AbortWithStatusJSON(http.StatusUnauthorized, apitypes.APIResponse{Error: "unauthorized"})
			return
		}
		c.Next()
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

	dataDir := filepath.Join(a.cfg.DataDir, req.Name)
	for _, sub := range []string{"conf", "data", "backup"} {
		if err := os.MkdirAll(filepath.Join(dataDir, sub), 0755); err != nil {
			fail(c, http.StatusInternalServerError, "mkdir: "+err.Error())
			return
		}
	}

	// 渲染配置文件
	overrides := configOverridesString(req.ConfigOverrides)
	minReplicas := minReplicasToWrite(req.Type, req.ReplicaOf)
	var createErr error
	if req.Engine == "kvrocks" {
		createErr = writeKvrocksConfig(dataDir, KvrocksConfigParams{
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
	if err := a.runtime.Start(req.Engine + "-" + req.Name); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
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
	if err := a.runtime.Stop(req.Engine + "-" + req.Name); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
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
	if err := a.runtime.Remove(req.Engine + "-" + req.Name); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if req.CleanData {
		os.RemoveAll(filepath.Join(a.cfg.DataDir, req.Name))
	}
	ok(c, nil)
}

func (a *Agent) instanceConfig(c *gin.Context) {
	var req struct {
		Name            string            `json:"name" binding:"required"`
		Engine          string            `json:"engine" binding:"required"`
		Type            string            `json:"type"`
		ConfigOverrides map[string]string `json:"config_overrides" binding:"required"`
		Restart         bool              `json:"restart"`
		Password        string            `json:"password"`
		Memory          string            `json:"memory"`
		Category        string            `json:"category"`
		ReplicaOf       string            `json:"replica_of"`
		Port            int               `json:"port"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	containerName := req.Engine + "-" + req.Name
	if req.Password != "" {
		a.passwords.Store(req.Name, req.Password)
	}

	if req.Restart {
		// 重写配置文件并重启容器
		dataDir := filepath.Join(a.cfg.DataDir, req.Name)
		overrides := configOverridesString(req.ConfigOverrides)
		minReplicas := minReplicasToWrite(req.Type, req.ReplicaOf)
		var writeErr error
		if req.Engine == "kvrocks" {
			writeErr = writeKvrocksConfig(dataDir, KvrocksConfigParams{
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
			writeErr = writeRedisConfig(dataDir, RedisConfigParams{
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
		if writeErr != nil {
			fail(c, http.StatusInternalServerError, "write config: "+writeErr.Error())
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
	} else {
		// 热更新：通过 CONFIG SET 逐个设置
		for k, v := range req.ConfigOverrides {
			if _, err := redisCmd(req.Name, a.getPassword(req.Name), "CONFIG", "SET", k, v); err != nil {
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

func (a *Agent) instanceList(c *gin.Context) {
	containers, err := a.runtime.ListAll()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, gin.H{"containers": containers})
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
	ok(c, gin.H{"info": info, "timestamp": time.Now().Format(time.RFC3339)})
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

	backupDir := filepath.Join(a.cfg.DataDir, req.Name, "backup")
	ts := time.Now().Format("2006-01-02T15:04:05")

	if req.Engine == "kvrocks" {
		// Kvrocks: 官方推荐通过 BGSAVE 生成备份（不存在 ROCKSDB.CHECKPOINT 命令，
		// 实测 kvrocks 2.15.0 返回 "ERR unknown command"）。BGSAVE 会将 RocksDB
		// 数据文件写入容器内 /data/backup（即宿主机 dataDir/data/backup），
		// 而不是 -v 挂载的 /backup（dataDir/backup）。
		pw := a.getPassword(req.Name)
		if _, err := redisCmd(req.Name, pw, "BGSAVE"); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		for i := 0; i < 60; i++ {
			info, _ := redisCmd(req.Name, pw, "INFO", "persistence")
			if strings.Contains(info, "bgsave_in_progress:0") {
				break
			}
			time.Sleep(time.Second)
		}
		src := filepath.Join(a.cfg.DataDir, req.Name, "data", "backup")
		dst := filepath.Join(backupDir, ts+".checkpoint.tar.gz")
		if _, err := runShell("tar", "-czf", dst, "-C", src, "."); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		// Redis: 检查是否开启 AOF
		pw := a.getPassword(req.Name)
		info, _ := redisCmd(req.Name, pw, "INFO", "persistence")
		hasAOF := strings.Contains(info, "aof_enabled:1")

		// 执行 BGSAVE
		if _, err := redisCmd(req.Name, pw, "BGSAVE"); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		for i := 0; i < 60; i++ {
			info, _ := redisCmd(req.Name, pw, "INFO", "persistence")
			if strings.Contains(info, "rdb_bgsave_in_progress:0") {
				break
			}
			time.Sleep(time.Second)
		}

		if hasAOF {
			// AOF 联合备份：BGREWRITEAOF + 同时备份 RDB+AOF
			if _, err := redisCmd(req.Name, pw, "BGREWRITEAOF"); err != nil {
				fail(c, http.StatusInternalServerError, "BGREWRITEAOF failed: "+err.Error())
				return
			}
			for i := 0; i < 60; i++ {
				info, _ := redisCmd(req.Name, pw, "INFO", "persistence")
				if strings.Contains(info, "aof_rewrite_in_progress:0") {
					break
				}
				time.Sleep(time.Second)
			}
			// 创建目录备份集
			dstDir := filepath.Join(backupDir, ts)
			os.MkdirAll(dstDir, 0755)
			if err := copyFile(filepath.Join(a.cfg.DataDir, req.Name, "data", "dump.rdb"), filepath.Join(dstDir, "dump.rdb")); err != nil {
				fail(c, http.StatusInternalServerError, "backup dump.rdb failed: "+err.Error())
				return
			}
			// AOF 文件可能在 appendonlydir/ 或直接是 appendonly.aof
			aofDir := filepath.Join(a.cfg.DataDir, req.Name, "data", "appendonlydir")
			if _, err := os.Stat(aofDir); err == nil {
				if _, err := runShell("cp", "-r", aofDir, filepath.Join(dstDir, "appendonlydir")); err != nil {
					fail(c, http.StatusInternalServerError, "backup AOF dir failed: "+err.Error())
					return
				}
			} else {
				aofFile := filepath.Join(a.cfg.DataDir, req.Name, "data", "appendonly.aof")
				if err := copyFile(aofFile, filepath.Join(dstDir, "appendonly.aof")); err != nil {
					fail(c, http.StatusInternalServerError, "backup AOF file failed: "+err.Error())
					return
				}
			}
		} else {
			// 仅 RDB 备份
			src := filepath.Join(a.cfg.DataDir, req.Name, "data", "dump.rdb")
			dst := filepath.Join(backupDir, ts+".rdb")
			if err := copyFile(src, dst); err != nil {
				fail(c, http.StatusInternalServerError, err.Error())
				return
			}
		}
	}

	// 轮转清理
	if req.Retention > 0 {
		cleanupBackups(backupDir, req.Retention)
	}

	ok(c, gin.H{"backup": ts})
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

	backupDir := filepath.Join(a.cfg.DataDir, req.Name, "backup")
	dataDir := filepath.Join(a.cfg.DataDir, req.Name, "data")
	containerName := req.Engine + "-" + req.Name

	if err := a.runtime.Stop(containerName); err != nil {
		fail(c, http.StatusInternalServerError, "stop container failed: "+err.Error())
		return
	}

	if req.Engine == "kvrocks" {
		// 备份包内容是 BGSAVE 生成的 RocksDB 快照文件（SST/MANIFEST 等），
		// 恢复时整体替换 data/db 目录，让 kvrocks 重启后从中加载。
		src := filepath.Join(backupDir, req.BackupTs+".checkpoint.tar.gz")
		dbDir := filepath.Join(dataDir, "db")
		os.RemoveAll(dbDir)
		os.MkdirAll(dbDir, 0755)
		if _, err := runShell("tar", "-xzf", src, "-C", dbDir); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		// 优先 AOF 恢复
		jointDir := filepath.Join(backupDir, req.BackupTs)
		if info, err := os.Stat(jointDir); err == nil && info.IsDir() {
			if err := copyFile(filepath.Join(jointDir, "dump.rdb"), filepath.Join(dataDir, "dump.rdb")); err != nil {
				fail(c, http.StatusInternalServerError, "restore dump.rdb failed: "+err.Error())
				return
			}
			aofSrcDir := filepath.Join(jointDir, "appendonlydir")
			aofDstDir := filepath.Join(dataDir, "appendonlydir")
			if _, err := os.Stat(aofSrcDir); err == nil {
				os.RemoveAll(aofDstDir)
				if _, err := runShell("cp", "-r", aofSrcDir, aofDstDir); err != nil {
					fail(c, http.StatusInternalServerError, "restore AOF dir failed: "+err.Error())
					return
				}
			} else {
				if err := copyFile(filepath.Join(jointDir, "appendonly.aof"), filepath.Join(dataDir, "appendonly.aof")); err != nil {
					fail(c, http.StatusInternalServerError, "restore AOF file failed: "+err.Error())
					return
				}
			}
		} else {
			src := filepath.Join(backupDir, req.BackupTs+".rdb")
			if err := copyFile(src, filepath.Join(dataDir, "dump.rdb")); err != nil {
				fail(c, http.StatusInternalServerError, err.Error())
				return
			}
			os.RemoveAll(filepath.Join(dataDir, "appendonlydir"))
			os.Remove(filepath.Join(dataDir, "appendonly.aof"))
		}
	}

	if err := a.runtime.Start(containerName); err != nil {
		fail(c, http.StatusInternalServerError, "start container failed: "+err.Error())
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
	backupDir := filepath.Join(a.cfg.DataDir, name, "backup")
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		ok(c, gin.H{"backups": []string{}})
		return
	}
	var backups []string
	for _, e := range entries {
		backups = append(backups, e.Name())
	}
	ok(c, gin.H{"backups": backups})
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
	if instType == "replication" && replicaOf == "" {
		return 1
	}
	return 0
}

func configOverridesString(overrides map[string]string) string {
	var sb strings.Builder
	for k, v := range overrides {
		sb.WriteString(k + " " + v + "\n")
	}
	return sb.String()
}
