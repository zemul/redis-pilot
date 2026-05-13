package apitypes

// PoolServer 服务器资源池中的一台服务器
type PoolServer struct {
	Endpoint      string            `yaml:"endpoint" json:"endpoint"`
	AgentPort     int               `yaml:"agent_port" json:"agent_port"`
	AgentToken    string            `yaml:"agent_token" json:"agent_token"`
	Labels        map[string]string `yaml:"labels" json:"labels"`
	Capacity      ResourceSpec      `yaml:"capacity" json:"capacity"`
	Allocated     ResourceSpec      `yaml:"allocated" json:"allocated"`
	Instances     []string          `yaml:"instances" json:"instances"`
	Status        string            `yaml:"status" json:"status"` // healthy | unhealthy | drain
	LastHeartbeat string            `yaml:"last_heartbeat" json:"last_heartbeat"`
}

// ResourceSpec 资源规格
type ResourceSpec struct {
	CPUCores int    `yaml:"cpu_cores" json:"cpu_cores"`
	Memory   string `yaml:"memory" json:"memory"`
	Disk     string `yaml:"disk" json:"disk"`
}

// PoolState pool-state.yaml 根结构
type PoolState struct {
	Servers map[string]*PoolServer `yaml:"servers"`
}

// EnvoyConfig 实例的 Envoy 端口配置
type EnvoyConfig struct {
	ReadWritePort int `yaml:"readwrite_port,omitempty"`
	ReadOnlyPort  int `yaml:"readonly_port,omitempty"`
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

// Instance 实例完整状态
type Instance struct {
	Category        string            `yaml:"category"`           // cache | persistent
	Group           string            `yaml:"group" json:"group"` // stable logical instance group
	Engine          string            `yaml:"engine"`             // redis | kvrocks
	Type            string            `yaml:"type"`               // standalone | replication
	Role            string            `yaml:"role"`               // master | replica | standalone
	Server          string            `yaml:"server"`
	Container       string            `yaml:"container"`
	Port            int               `yaml:"port"`
	Memory          string            `yaml:"memory"`
	CPUs            int               `yaml:"cpus"`
	Disk            string            `yaml:"disk,omitempty"`
	Password        string            `yaml:"password"`
	ConfigPath      string            `yaml:"config_path"`
	DataPath        string            `yaml:"data_path"`
	BackupPath      string            `yaml:"backup_path"`
	Persistence     *Persistence      `yaml:"persistence,omitempty"`
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

// PortInventoryItem 端口-实例映射（视图 A）
type PortInventoryItem struct {
	EnvoyPort      int      `json:"envoy_port"`
	Mode           string   `json:"mode"` // readwrite | readonly
	InstanceName   string   `json:"instance_name"`
	Engine         string   `json:"engine"`
	Category       string   `json:"category"`
	Role           string   `json:"role"`
	BackendServers []string `json:"backend_servers"`
}

// ServerInventoryItem 服务器-实例分布（视图 B）
type ServerInventoryItem struct {
	IP              string                  `json:"ip"`
	Instances       []ServerInstanceSummary `json:"instances"`
	AllocatedMemory string                  `json:"allocated_memory"`
	TotalMemory     string                  `json:"total_memory"`
	AllocatedCPU    int                     `json:"allocated_cpu"`
	TotalCPU        int                     `json:"total_cpu"`
}

// ServerInstanceSummary 服务器上的实例摘要
type ServerInstanceSummary struct {
	Name          string `json:"name"`
	Engine        string `json:"engine"`
	ContainerPort int    `json:"container_port"`
	Memory        string `json:"memory"`
	CPUs          int    `json:"cpus"`
	Status        string `json:"status"`
}

// InventorySummary 全局摘要（视图 C）
type InventorySummary struct {
	Instances       []InstanceSummaryItem `json:"instances"`
	TotalInstances  int                   `json:"total_instances"`
	TotalServers    int                   `json:"total_servers"`
	AllocatedMemory string                `json:"allocated_memory"`
	AllocatedCPU    int                   `json:"allocated_cpu"`
}

// InstanceSummaryItem 实例摘要条目
type InstanceSummaryItem struct {
	Name       string `json:"name"`
	Engine     string `json:"engine"`
	Category   string `json:"category"`
	EnvoyPorts string `json:"envoy_ports"`
	Server     string `json:"server"`
	Status     string `json:"status"`
}

// CreateInstanceRequest 创建实例请求
type CreateInstanceRequest struct {
	Name            string            `json:"name" binding:"required"`
	Category        string            `json:"category" binding:"required"` // cache | persistent
	Group           string            `json:"group,omitempty"`             // stable logical group; required for master/standalone
	Engine          string            `json:"engine" binding:"required"`   // redis | kvrocks
	Type            string            `json:"type" binding:"required"`     // standalone | replication
	Server          string            `json:"server"`                      // 可选，为空时自动调度
	Port            int               `json:"port"`
	Memory          string            `json:"memory" binding:"required"`
	CPUs            int               `json:"cpus"`
	Disk            string            `json:"disk,omitempty"`
	Password        string            `json:"password"`
	ReplicaOf       string            `json:"replica_of,omitempty"`
	ConfigOverrides map[string]string `json:"config_overrides,omitempty"`
}

// SentinelMaster Sentinel 监控的一个实例组主库
type SentinelMaster struct {
	Group                 string `json:"group" yaml:"group"`
	Host                  string `json:"host" yaml:"host"`
	Port                  int    `json:"port" yaml:"port"`
	Password              string `json:"password,omitempty" yaml:"password,omitempty"`
	DownAfterMilliseconds int    `json:"down_after_milliseconds,omitempty" yaml:"down_after_milliseconds,omitempty"`
	FailoverTimeout       int    `json:"failover_timeout,omitempty" yaml:"failover_timeout,omitempty"`
	ParallelSyncs         int    `json:"parallel_syncs,omitempty" yaml:"parallel_syncs,omitempty"`
}

// SentinelSyncRequest 下发给 Agent 的 Sentinel 监控配置
type SentinelSyncRequest struct {
	Port     int              `json:"port"`
	Quorum   int              `json:"quorum"`
	Masters  []SentinelMaster `json:"masters"`
	Announce string           `json:"announce_ip,omitempty"`
}

// SentinelRemoveMasterRequest 从 Sentinel 中移除一个监控对象
type SentinelRemoveMasterRequest struct {
	Group string `json:"group" binding:"required"`
	Port  int    `json:"port,omitempty"`
}

// SentinelStatus Agent 返回的 Sentinel 状态
type SentinelStatus struct {
	Running   bool     `json:"running"`
	Port      int      `json:"port"`
	Masters   []string `json:"masters"`
	Config    string   `json:"config"`
	UpdatedAt string   `json:"updated_at,omitempty"`
}
