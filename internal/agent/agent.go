package agent

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/logger"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/podman"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

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
	minReplicas := minReplicasToWrite(req.Type, req.ReplicaOf)
	var createErr error
	if req.Engine == "kvrocks" {
		createErr = writeKvrocksConfig(dataDir, KvrocksConfigParams{
			Password:           req.Password,
			Memory:             req.Memory,
			ReplicaOf:          req.ReplicaOf,
			ConfigOverrides:    overrides,
			MinReplicasToWrite: minReplicas,
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

	a.passwords.Store(req.Name, req.Password)
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
		Type            string            `json:"type"`
		ConfigOverrides map[string]string `json:"config_overrides" binding:"required"`
		Restart         bool              `json:"restart"`
		Password        string            `json:"password"`
		Memory          string            `json:"memory"`
		Category        string            `json:"category"`
		ReplicaOf       string            `json:"replica_of"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
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
		if req.Engine == "kvrocks" {
			writeKvrocksConfig(dataDir, KvrocksConfigParams{
				Password:           req.Password,
				Memory:             req.Memory,
				ReplicaOf:          req.ReplicaOf,
				ConfigOverrides:    overrides,
				MinReplicasToWrite: minReplicas,
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
			writeRedisConfig(dataDir, RedisConfigParams{
				Password:           req.Password,
				Memory:             req.Memory,
				MaxmemoryPolicy:    policy,
				Appendonly:         aof,
				ReplicaOf:          req.ReplicaOf,
				ConfigOverrides:    overrides,
				MinReplicasToWrite: minReplicas,
			})
		}
		a.runtime.Stop(containerName)
		a.runtime.Start(containerName)
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
	parts := strings.SplitN(req.ReplicaOf, ":", 2)
	if len(parts) != 2 {
		fail(c, http.StatusBadRequest, "replica_of must be ip:port")
		return
	}
	if _, err := redisCmd(req.Name, a.getPassword(req.Name), "REPLICAOF", parts[0], parts[1]); err != nil {
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
	info, err := redisCmd(name, a.getPassword(name), "INFO")
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
		if _, err := redisCmd(req.Name, a.getPassword(req.Name), "ROCKSDB.CHECKPOINT"); err != nil {
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

	backupDir := filepath.Join(a.cfg.DataDir, req.Name, "backup")
	dataDir := filepath.Join(a.cfg.DataDir, req.Name, "data")
	containerName := req.Engine + "-" + req.Name

	if err := a.runtime.Stop(containerName); err != nil {
		fail(c, http.StatusInternalServerError, "stop container failed: "+err.Error())
		return
	}

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
