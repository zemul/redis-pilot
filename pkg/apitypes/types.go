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

// NodeServerView 是 NodeServer 面向对外查询接口的安全投影，不包含 AgentToken 等凭证字段，
// 用于防止 Agent Token 通过只读查询接口（如 /node/list）泄露给调用方。
type NodeServerView struct {
	Endpoint      string            `json:"endpoint"`
	AgentPort     int               `json:"agent_port"`
	Labels        map[string]string `json:"labels"`
	Capacity      ResourceSpec      `json:"capacity"`
	Allocated     ResourceSpec      `json:"allocated"`
	Instances     []string          `json:"instances"`
	Status        string            `json:"status"`
	LastHeartbeat string            `json:"last_heartbeat"`
}

// ToView 将 NodeServer 转换为不含敏感凭证的对外展示视图。
func (n *NodeServer) ToView() *NodeServerView {
	if n == nil {
		return nil
	}
	return &NodeServerView{
		Endpoint:      n.Endpoint,
		AgentPort:     n.AgentPort,
		Labels:        n.Labels,
		Capacity:      n.Capacity,
		Allocated:     n.Allocated,
		Instances:     n.Instances,
		Status:        n.Status,
		LastHeartbeat: n.LastHeartbeat,
	}
}

// ResourceSpec 资源规格
type ResourceSpec struct {
	CPUCores float64 `yaml:"cpu_cores" json:"cpu_cores"`
	Memory   string  `yaml:"memory" json:"memory"`
	Disk     string  `yaml:"disk" json:"disk"`
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
	CPUs            float64           `yaml:"cpus" json:"cpus"`
	Disk            string            `yaml:"disk,omitempty" json:"disk,omitempty"`
	Password        string            `yaml:"password" json:"password"`
	ConfigPath      string            `yaml:"config_path" json:"config_path"`
	DataPath        string            `yaml:"data_path" json:"data_path"`
	BackupPath      string            `yaml:"backup_path" json:"backup_path"`
	Persistence     *Persistence      `yaml:"persistence,omitempty" json:"persistence,omitempty"`
	ConfigOverrides map[string]string `yaml:"config_overrides,omitempty" json:"config_overrides,omitempty"`
	ReplicaOf       string            `yaml:"replica_of,omitempty" json:"replica_of,omitempty"`
	Backup          *BackupConfig     `yaml:"backup,omitempty" json:"backup,omitempty"`
	// DesiredState 是人的意图：这个实例应该处于什么状态。
	//
	// 只有用户发起的操作可以写：create/start → running，stop → stopped。
	// reconcile 与 Sentinel 事件处理**不得**写这个字段——它们观测到的是事实，
	// 不是意图。取值见 DesiredState* 常量。
	DesiredState string `yaml:"desired_state" json:"desired_state"`
	// ObservedStatus 是最近一次观测到的实际状态。
	//
	// 只有观测通道可以写：runReconcile（问 Agent 容器在不在跑），以及
	// handleSentinelFailover 对新 master 的那一次写入（Sentinel 能提升它，
	// 说明 Sentinel 观测到它是健康的）。用户操作**不得**写这个字段——
	// 用户表达的是意图，实例是否真的进入该状态要靠观测确认。
	//
	// 空串表示尚未观测过（实例刚创建，reconcile 还没跑过）。
	// 取值见 Observed* 常量。
	ObservedStatus string `yaml:"observed_status" json:"observed_status"`
	Lock           *Lock  `yaml:"lock,omitempty" json:"lock,omitempty"`
	CreatedAt      string `yaml:"created_at" json:"created_at"`
}

// DesiredState 的取值。意图只有两种：要么该跑，要么该停。
const (
	DesiredRunning = "running"
	DesiredStopped = "stopped"
)

// ObservedStatus 的取值。
const (
	ObservedUnknown     = ""            // 尚未观测过
	ObservedRunning     = "running"     // 容器存在且在跑
	ObservedStopped     = "stopped"     // 容器存在但已停止
	ObservedMissing     = "missing"     // 容器不存在
	ObservedUnreachable = "unreachable" // Agent 不可达或响应异常，无法判断
)

// Phase 由 DesiredState 与 ObservedStatus 推导出供人阅读的综合状态。
//
// 刻意不存储这个值。此前 Status 单字段同时承担意图与观测，导致
// "unexpected_stopped"（意图跑着但观测到停了）这类本质上是**两个字段的差值**的信息
// 被塞进同一个字段，于是写入者和读取者对同一个值的理解出现分歧——
// 这正是"故障转移时 down 掉的 replica 拓扑记录永久错误"那个 bug 的根源。
//
// 派生值只读不存，就不会出现"派生值与来源不一致"的第三种坏状态。
func (i *Instance) Phase() string {
	if i == nil {
		return "unknown"
	}
	switch {
	case i.DesiredState == DesiredStopped:
		if i.ObservedStatus == ObservedStopped || i.ObservedStatus == ObservedMissing {
			return "stopped"
		}
		// 已要求停止但还观测到在跑，或者还没观测过
		return "stopping"

	case i.ObservedStatus == ObservedUnknown:
		// 意图是跑，但一次都还没观测到——刚创建
		return "creating"

	case i.ObservedStatus == ObservedRunning:
		return "running"

	case i.ObservedStatus == ObservedStopped:
		// 意图是跑，实际停了：没人要求它停，它自己停了
		return "unexpected_stopped"

	case i.ObservedStatus == ObservedMissing:
		return "failed"

	case i.ObservedStatus == ObservedUnreachable:
		return "unreachable"
	}
	return "unknown"
}

// IsServing 报告实例当前是否可以承接业务流量。
// 判据是观测结果而非意图——Envoy 端点、备份调度这类决策必须基于事实。
func (i *Instance) IsServing() bool {
	return i != nil && i.ObservedStatus == ObservedRunning
}

// InstancesState instances-state.yaml 根结构
type InstancesState struct {
	Groups    map[string]*InstanceGroupState `yaml:"groups" json:"groups"`
	Instances map[string]*Instance           `yaml:"instances" json:"instances"`
}

// InstanceView 是 Instance 面向对外查询接口的安全投影，不包含 Password 字段，
// 用于防止 Redis/Kvrocks 明文密码通过只读查询接口（如 /instance/list、/instance/status）泄露。
type InstanceView struct {
	Group           string            `json:"group"`
	Role            string            `json:"role"`
	Server          string            `json:"server"`
	Container       string            `json:"container"`
	Port            int               `json:"port"`
	Memory          string            `json:"memory"`
	CPUs            float64           `json:"cpus"`
	Disk            string            `json:"disk,omitempty"`
	ConfigPath      string            `json:"config_path"`
	DataPath        string            `json:"data_path"`
	BackupPath      string            `json:"backup_path"`
	Persistence     *Persistence      `json:"persistence,omitempty"`
	ConfigOverrides map[string]string `json:"config_overrides,omitempty"`
	ReplicaOf       string            `json:"replica_of,omitempty"`
	Backup          *BackupConfig     `json:"backup,omitempty"`
	// Status 是派生的综合状态（Phase()），保持对外词汇稳定：
	// creating / running / stopped / stopping / unexpected_stopped / failed / unreachable
	Status string `json:"status"`
	// 同时暴露两个来源字段，让"为什么是这个状态"可以被直接看出来
	DesiredState   string `json:"desired_state"`
	ObservedStatus string `json:"observed_status"`
	Lock           *Lock  `json:"lock,omitempty"`
	CreatedAt      string `json:"created_at"`
}

// ToView 将 Instance 转换为不含明文密码的对外展示视图。
func (i *Instance) ToView() *InstanceView {
	if i == nil {
		return nil
	}
	return &InstanceView{
		Group:           i.Group,
		Role:            i.Role,
		Server:          i.Server,
		Container:       i.Container,
		Port:            i.Port,
		Memory:          i.Memory,
		CPUs:            i.CPUs,
		Disk:            i.Disk,
		ConfigPath:      i.ConfigPath,
		DataPath:        i.DataPath,
		BackupPath:      i.BackupPath,
		Persistence:     i.Persistence,
		ConfigOverrides: i.ConfigOverrides,
		ReplicaOf:       i.ReplicaOf,
		Backup:          i.Backup,
		Status:          i.Phase(),
		DesiredState:    i.DesiredState,
		ObservedStatus:  i.ObservedStatus,
		Lock:            i.Lock,
		CreatedAt:       i.CreatedAt,
	}
}

// InstancesStateView 是 InstancesState 的对外安全投影，Instances 中不含 Password。
type InstancesStateView struct {
	Groups    map[string]*InstanceGroupState `json:"groups"`
	Instances map[string]*InstanceView       `json:"instances"`
}

// ToView 将 InstancesState 转换为不含明文密码的对外展示视图。
func (s *InstancesState) ToView() *InstancesStateView {
	if s == nil {
		return nil
	}
	views := make(map[string]*InstanceView, len(s.Instances))
	for name, inst := range s.Instances {
		views[name] = inst.ToView()
	}
	return &InstancesStateView{
		Groups:    s.Groups,
		Instances: views,
	}
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
	AllocatedCPU    float64                 `json:"allocated_cpu"`
	TotalCPU        float64                 `json:"total_cpu"`
}

// ServerInstanceSummary 服务器上的实例摘要
type ServerInstanceSummary struct {
	Name          string  `json:"name"`
	Engine        string  `json:"engine"`
	ContainerPort int     `json:"container_port"`
	Memory        string  `json:"memory"`
	CPUs          float64 `json:"cpus"`
	Status        string  `json:"status"`
}

// InventorySummary 全局摘要（视图 C）
type InventorySummary struct {
	Instances       []InstanceSummaryItem `json:"instances"`
	TotalInstances  int                   `json:"total_instances"`
	TotalServers    int                   `json:"total_servers"`
	AllocatedMemory string                `json:"allocated_memory"`
	AllocatedCPU    float64               `json:"allocated_cpu"`
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
	CPUs            float64           `json:"cpus"`
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
