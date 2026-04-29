package agent

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/podman"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type Agent struct {
	cfg     *Config
	log     *logger.Logger
	mon     *monitor
	runtime podman.ContainerRuntime
}

func New(cfg *Config) *Agent {
	a := &Agent{
		cfg:     cfg,
		log:     logger.New(cfg.Log.Dir, cfg.Log.Stdout),
		mon:     newMonitor(),
		runtime: podman.NewRuntime(),
	}
	return a
}

func (a *Agent) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
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

	dataDir := filepath.Join(a.cfg.DataDir, req.Name)
	for _, sub := range []string{"conf", "data", "backup"} {
		if err := os.MkdirAll(filepath.Join(dataDir, sub), 0755); err != nil {
			fail(c, http.StatusInternalServerError, "mkdir: "+err.Error())
			return
		}
	}

	// 渲染配置文件
	overrides := configOverridesString(req.ConfigOverrides)
	var createErr error
	if req.Engine == "kvrocks" {
		createErr = writeKvrocksConfig(dataDir, KvrocksConfigParams{
			Password:        req.Password,
			ReplicaOf:       req.ReplicaOf,
			ConfigOverrides: overrides,
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
			Password:        req.Password,
			Memory:          req.Memory,
			MaxmemoryPolicy: policy,
			Appendonly:      aof,
			ReplicaOf:       req.ReplicaOf,
			ConfigOverrides: overrides,
		})
	}
	if createErr != nil {
		fail(c, http.StatusInternalServerError, "write config: "+createErr.Error())
		return
	}

	// 启动容器
	containerName := req.Engine + "-" + req.Name
	containerID, createErr := a.runtime.Create(req.Engine, containerName, req.Port, req.Memory, req.CPUs, dataDir)
	if createErr != nil {
		fail(c, http.StatusInternalServerError, "podman create: "+createErr.Error())
		return
	}

	a.log.Infof("instance created: %s (container: %s)", req.Name, containerName)
	ok(c, gin.H{
		"container_id":   containerID,
		"container_name": containerName,
		"data_dir":       dataDir,
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
		ConfigOverrides map[string]string `json:"config_overrides" binding:"required"`
		Restart         bool              `json:"restart"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}

	containerName := req.Engine + "-" + req.Name

	if req.Restart {
		// 重写配置文件并重启容器
		dataDir := filepath.Join(a.cfg.DataDir, req.Name)
		overrides := configOverridesString(req.ConfigOverrides)
		if req.Engine == "kvrocks" {
			writeKvrocksConfig(dataDir, KvrocksConfigParams{ConfigOverrides: overrides})
		} else {
			writeRedisConfig(dataDir, RedisConfigParams{ConfigOverrides: overrides})
		}
		a.runtime.Stop(containerName)
		a.runtime.Start(containerName)
	} else {
		// 热更新：通过 CONFIG SET 逐个设置
		for k, v := range req.ConfigOverrides {
			if _, err := redisCmd(req.Name, "CONFIG", "SET", k, v); err != nil {
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
	if _, err := redisCmd(req.Name, "REPLICAOF", "NO", "ONE"); err != nil {
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
	parts := strings.SplitN(req.ReplicaOf, ":", 2)
	if len(parts) != 2 {
		fail(c, http.StatusBadRequest, "replica_of must be ip:port")
		return
	}
	if _, err := redisCmd(req.Name, "REPLICAOF", parts[0], parts[1]); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
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
	info, err := redisCmd(name, "INFO")
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, gin.H{"info": info, "cached_metrics": a.mon.get(name), "timestamp": time.Now().Format(time.RFC3339)})
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

	backupDir := filepath.Join(a.cfg.DataDir, req.Name, "backup")
	ts := time.Now().Format("2006-01-02T15:04:05")

	if req.Engine == "kvrocks" {
		// Kvrocks: RocksDB Checkpoint
		if _, err := redisCmd(req.Name, "ROCKSDB.CHECKPOINT"); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		src := filepath.Join(backupDir, "checkpoint")
		dst := filepath.Join(backupDir, ts+".checkpoint.tar.gz")
		if _, err := runShell("tar", "-czf", dst, "-C", src, "."); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		os.RemoveAll(src)
	} else {
		// Redis: 检查是否开启 AOF
		info, _ := redisCmd(req.Name, "INFO", "persistence")
		hasAOF := strings.Contains(info, "aof_enabled:1")

		// 执行 BGSAVE
		if _, err := redisCmd(req.Name, "BGSAVE"); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		for i := 0; i < 60; i++ {
			info, _ := redisCmd(req.Name, "INFO", "persistence")
			if strings.Contains(info, "rdb_bgsave_in_progress:0") {
				break
			}
			time.Sleep(time.Second)
		}

		if hasAOF {
			// AOF 联合备份：BGREWRITEAOF + 同时备份 RDB+AOF
			redisCmd(req.Name, "BGREWRITEAOF")
			for i := 0; i < 60; i++ {
				info, _ := redisCmd(req.Name, "INFO", "persistence")
				if strings.Contains(info, "aof_rewrite_in_progress:0") {
					break
				}
				time.Sleep(time.Second)
			}
			// 创建目录备份集
			dstDir := filepath.Join(backupDir, ts)
			os.MkdirAll(dstDir, 0755)
			copyFile(filepath.Join(a.cfg.DataDir, req.Name, "data", "dump.rdb"), filepath.Join(dstDir, "dump.rdb"))
			// AOF 文件可能在 appendonlydir/ 或直接是 appendonly.aof
			aofDir := filepath.Join(a.cfg.DataDir, req.Name, "data", "appendonlydir")
			if _, err := os.Stat(aofDir); err == nil {
				runShell("cp", "-r", aofDir, filepath.Join(dstDir, "appendonlydir"))
			} else {
				aofFile := filepath.Join(a.cfg.DataDir, req.Name, "data", "appendonly.aof")
				copyFile(aofFile, filepath.Join(dstDir, "appendonly.aof"))
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

	backupDir := filepath.Join(a.cfg.DataDir, req.Name, "backup")
	dataDir := filepath.Join(a.cfg.DataDir, req.Name, "data")
	containerName := req.Engine + "-" + req.Name

	a.runtime.Stop(containerName)

	if req.Engine == "kvrocks" {
		src := filepath.Join(backupDir, req.BackupTs+".checkpoint.tar.gz")
		os.RemoveAll(dataDir)
		os.MkdirAll(dataDir, 0755)
		if _, err := runShell("tar", "-xzf", src, "-C", dataDir); err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		// 优先 AOF 恢复
		jointDir := filepath.Join(backupDir, req.BackupTs)
		if info, err := os.Stat(jointDir); err == nil && info.IsDir() {
			// RDB+AOF 联合备份恢复
			copyFile(filepath.Join(jointDir, "dump.rdb"), filepath.Join(dataDir, "dump.rdb"))
			// 恢复 AOF
			aofSrcDir := filepath.Join(jointDir, "appendonlydir")
			aofDstDir := filepath.Join(dataDir, "appendonlydir")
			if _, err := os.Stat(aofSrcDir); err == nil {
				os.RemoveAll(aofDstDir)
				runShell("cp", "-r", aofSrcDir, aofDstDir)
			} else {
				copyFile(filepath.Join(jointDir, "appendonly.aof"), filepath.Join(dataDir, "appendonly.aof"))
			}
		} else {
			// 仅 RDB 恢复
			src := filepath.Join(backupDir, req.BackupTs+".rdb")
			if err := copyFile(src, filepath.Join(dataDir, "dump.rdb")); err != nil {
				fail(c, http.StatusInternalServerError, err.Error())
				return
			}
			// 清空旧 AOF 避免不一致
			os.RemoveAll(filepath.Join(dataDir, "appendonlydir"))
			os.Remove(filepath.Join(dataDir, "appendonly.aof"))
		}
	}

	a.runtime.Start(containerName)
	ok(c, nil)
}

func (a *Agent) instanceBackups(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
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
		if !e.IsDir() {
			backups = append(backups, e.Name())
		}
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

func configOverridesString(overrides map[string]string) string {
	var sb strings.Builder
	for k, v := range overrides {
		sb.WriteString(k + " " + v + "\n")
	}
	return sb.String()
}
