package apitypes

// NodeServer 服务器节点中的一台服务器
type NodeServer struct {
	Endpoint      string            `yaml:"endpoint" json:"endpoint"`
	AgentPort     int               `yaml:"agent_port" json:"agent_port"`
	AgentToken    string            `yaml:"agent_token" json:"agent_token"`
	Labels        map[string]string `yaml:"labels" json:"labels"`
	Capacity      ResourceSpec      `yaml:"capacity" json:"capacity"`
	Allocated     ResourceSpec      `yaml:"-" json:"allocated"`
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

// NodeState pool-state.yaml 根结构
type NodeState struct {
	Servers map[string]*NodeServer `yaml:"servers"`
}

// EnvoyConfig 实例组的 Envoy 端口配置
type EnvoyConfig struct {
	AutoPort   int `yaml:"auto_port,omitempty" json:"auto_port,omitempty"`
	MasterPort int `yaml:"master_port,omitempty" json:"master_port,omitempty"`
}

// ProxySnapshot is the stable, read-only view consumed by redis-pilot-xds.
type ProxySnapshot struct {
	Version     string          `json:"version"`
	GeneratedAt string          `json:"generated_at"`
	Listeners   []ProxyListener `json:"listeners"`
	Clusters    []ProxyCluster  `json:"clusters"`
}

// ProxyListener describes one Envoy Redis proxy listener.
type ProxyListener struct {
	Name        string `json:"name"`
	Group       string `json:"group"`
	Mode        string `json:"mode"` // master | auto
	Bind        string `json:"bind"`
	Port        int    `json:"port"`
	StatPrefix  string `json:"stat_prefix"`
	Cluster     string `json:"cluster"`
	ReadCluster string `json:"read_cluster,omitempty"`
	Password    string `json:"password,omitempty"`
	ReadPolicy  string `json:"read_policy"`
}

// ProxyCluster describes one logical upstream cluster.
type ProxyCluster struct {
	Name      string          `json:"name"`
	Password  string          `json:"password,omitempty"`
	Endpoints []ProxyEndpoint `json:"endpoints"`
}

// ProxyEndpoint is a concrete Redis/Kvrocks backend endpoint.
type ProxyEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
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

// InstanceGroupState 实例组权威状态。
type InstanceGroupState struct {
	Type             string       `yaml:"type" json:"type"` // standalone | replication
	Engine           string       `yaml:"engine" json:"engine"`
	EngineVersion    string       `yaml:"engine_version,omitempty" json:"engine_version,omitempty"`
	Category         string       `yaml:"category" json:"category"`
	CurrentMaster    string       `yaml:"current_master" json:"current_master"`
	TopologyStatus   string       `yaml:"topology_status" json:"topology_status"` // healthy | degraded
	FailoverConflict bool         `yaml:"failover_conflict" json:"failover_conflict"`
	Envoy            *EnvoyConfig `yaml:"envoy,omitempty" json:"envoy,omitempty"`
	CreatedAt        string       `yaml:"created_at" json:"created_at"`
	UpdatedAt        string       `yaml:"updated_at" json:"updated_at"`
}

// Instance 单容器运行状态
type Instance struct {
	Group           string            `yaml:"group" json:"group"`
	Role            string            `yaml:"role" json:"role"`
	Server          string            `yaml:"server" json:"server"`
	Container       string            `yaml:"container" json:"container"`
	Port            int               `yaml:"port" json:"port"`
	Memory          string            `yaml:"memory" json:"memory"`
	CPUs            int               `yaml:"cpus" json:"cpus"`
	Disk            string            `yaml:"disk,omitempty" json:"disk,omitempty"`
	Password        string            `yaml:"password" json:"password"`
	ConfigPath      string            `yaml:"config_path" json:"config_path"`
	DataPath        string            `yaml:"data_path" json:"data_path"`
	BackupPath      string            `yaml:"backup_path" json:"backup_path"`
	Persistence     *Persistence      `yaml:"persistence,omitempty" json:"persistence,omitempty"`
	ConfigOverrides map[string]string `yaml:"config_overrides,omitempty" json:"config_overrides,omitempty"`
	ReplicaOf       string            `yaml:"replica_of,omitempty" json:"replica_of,omitempty"`
	Backup          *BackupConfig     `yaml:"backup,omitempty" json:"backup,omitempty"`
	Status          string            `yaml:"status" json:"status"`
	Lock            *Lock             `yaml:"lock,omitempty" json:"lock,omitempty"`
	CreatedAt       string            `yaml:"created_at" json:"created_at"`
}

// InstancesState instances-state.yaml 根结构
type InstancesState struct {
	Groups    map[string]*InstanceGroupState `yaml:"groups" json:"groups"`
	Instances map[string]*Instance           `yaml:"instances" json:"instances"`
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
	Mode           string   `json:"mode"` // auto | master
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
	EngineVersion   string            `json:"engine_version,omitempty"`    // e.g. redis 5 | 6.2 | 7
	EngineImage     string            `json:"engine_image,omitempty"`      // resolved by Server and consumed by Agent
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

// SentinelStatus Server 直连 Sentinel 返回的状态
type SentinelStatus struct {
	Running bool     `json:"running"`
	Port    int      `json:"port"`
	Masters []string `json:"masters"`
}
