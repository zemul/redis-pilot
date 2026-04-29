package apitypes

// PoolServer 服务器资源池中的一台服务器
type PoolServer struct {
	Endpoint   string            `yaml:"endpoint"`
	AgentPort  int               `yaml:"agent_port"`
	AgentToken string            `yaml:"agent_token"`
	Labels     map[string]string `yaml:"labels"`
	Capacity   ResourceSpec      `yaml:"capacity"`
	Allocated  ResourceSpec      `yaml:"allocated"`
	Instances  []string          `yaml:"instances"`
	Status     string            `yaml:"status"` // healthy | unhealthy | drain
	LastHeartbeat string         `yaml:"last_heartbeat"`
}

// ResourceSpec 资源规格
type ResourceSpec struct {
	CPUCores int    `yaml:"cpu_cores"`
	Memory   string `yaml:"memory"`
	Disk     string `yaml:"disk"`
}

// PoolState pool-state.yaml 根结构
type PoolState struct {
	Servers map[string]*PoolServer `yaml:"servers"`
}

// EnvoyConfig 实例的 Envoy 端口配置
type EnvoyConfig struct {
	ReadWritePort int `yaml:"readwrite_port,omitempty"`
	WriteOnlyPort int `yaml:"writeonly_port,omitempty"`
	MgmtPort      int `yaml:"mgmt_port,omitempty"`
}

// BackupConfig 备份配置
type BackupConfig struct {
	Schedule   string `yaml:"schedule"`
	Retention  int    `yaml:"retention"`
	LastBackup string `yaml:"last_backup,omitempty"`
}

// Lock 实例操作锁
type Lock struct {
	HeldBy     string `yaml:"held_by"`
	Operation  string `yaml:"operation"`
	AcquiredAt string `yaml:"acquired_at"`
	Timeout    int    `yaml:"timeout"`
}

// Persistence Redis 持久化配置（Kvrocks 实例为 nil）
type Persistence struct {
	RDB          bool   `yaml:"rdb" json:"rdb"`
	RDBFrequency string `yaml:"rdb_frequency,omitempty" json:"rdb_frequency,omitempty"` // save 配置
	AOF          bool   `yaml:"aof" json:"aof"`
	AOFPolicy    string `yaml:"aof_policy,omitempty" json:"aof_policy,omitempty"` // everysec | always | no
}

// KvrocksConfig Kvrocks RocksDB 调优参数
type KvrocksConfig struct {
	Compression          string `yaml:"rocksdb.compression,omitempty" json:"compression,omitempty"`
	WriteBufferSize      string `yaml:"rocksdb.write_buffer_size,omitempty" json:"write_buffer_size,omitempty"`
	MaxWriteBufferNumber int    `yaml:"rocksdb.max_write_buffer_number,omitempty" json:"max_write_buffer_number,omitempty"`
}

// Instance 实例完整状态
type Instance struct {
	Category        string            `yaml:"category"` // cache | persistent
	Engine          string            `yaml:"engine"`   // redis | kvrocks
	Type            string            `yaml:"type"`     // standalone | replication
	Role            string            `yaml:"role"`     // master | replica | standalone
	Server          string            `yaml:"server"`
	Container       string            `yaml:"container"`
	Port            int               `yaml:"port"`
	Memory          string            `yaml:"memory"`
	CPUs            int               `yaml:"cpus"`
	Password        string            `yaml:"password"`
	ConfigPath      string            `yaml:"config_path"`
	DataPath        string            `yaml:"data_path"`
	BackupPath      string            `yaml:"backup_path"`
	Persistence     *Persistence      `yaml:"persistence,omitempty"`
	KvrocksConfig   *KvrocksConfig    `yaml:"kvrocks_config,omitempty"`
	ConfigOverrides map[string]string `yaml:"config_overrides,omitempty"`
	ReplicaOf       string            `yaml:"replica_of,omitempty"`
	Replicas        []string          `yaml:"replicas,omitempty"`
	Envoy           *EnvoyConfig      `yaml:"envoy,omitempty"`
	Backup          *BackupConfig     `yaml:"backup,omitempty"`
	Status          string            `yaml:"status"` // creating | running | stopped | failed
	Lock            *Lock             `yaml:"lock,omitempty"`
	CreatedAt       string            `yaml:"created_at"`
}

// InstancesState instances-state.yaml 根结构
type InstancesState struct {
	Instances map[string]*Instance `yaml:"instances"`
}

// APIResponse 通用 API 响应
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// CreateInstanceRequest 创建实例请求
type CreateInstanceRequest struct {
	Name            string            `json:"name" binding:"required"`
	Category        string            `json:"category" binding:"required"` // cache | persistent
	Engine          string            `json:"engine" binding:"required"`   // redis | kvrocks
	Type            string            `json:"type" binding:"required"`     // standalone | replication
	Server          string            `json:"server"`                    // 可选，为空时自动调度
	Port            int               `json:"port"`
	Memory          string            `json:"memory" binding:"required"`
	CPUs            int               `json:"cpus"`
	Password        string            `json:"password"`
	ReplicaOf       string            `json:"replica_of,omitempty"`
	ConfigOverrides map[string]string `json:"config_overrides,omitempty"`
}
