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

// ProxyConfig 实例组的 Proxy 端口配置
type ProxyConfig struct {
	AutoPort   int `yaml:"auto_port,omitempty" json:"auto_port,omitempty"`
	MasterPort int `yaml:"master_port,omitempty" json:"master_port,omitempty"`
}

// ProxySnapshot is the stable, read-only view consumed by redis-proxy.
type ProxySnapshot struct {
	Version     string `json:"version"`
	GeneratedAt string `json:"generated_at"`
	// AnnounceIP 是 Proxy 对外暴露的域名/IP（不含端口），由 Server 的
	// server.yaml 的 proxy_public_endpoint 统一配置分发。伪 Sentinel
	// 协议应答 SENTINEL get-master-addr-by-name 时使用该值作为返回的
	// host；为空时退化为连接命中的本地 IP（跨 NAT 环境下不可用）。
	AnnounceIP string          `json:"announce_ip,omitempty"`
	Listeners  []ProxyListener `json:"listeners"`
	Clusters   []ProxyCluster  `json:"clusters"`
}

// ProxyListener describes one Proxy Redis proxy listener.
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
	AccessMode  string `json:"access_mode,omitempty"` // rw-split | standard（空视为 rw-split）
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
	Schedule  string `yaml:"schedule"`
	Retention int    `yaml:"retention"`
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
	Type          string   `yaml:"type" json:"type"` // standalone | replication
	Engine        string   `yaml:"engine" json:"engine"`
	EngineVersion string   `yaml:"engine_version,omitempty" json:"engine_version,omitempty"`
	Category      string   `yaml:"category" json:"category"`
	AccessMode    string   `yaml:"access_mode,omitempty" json:"access_mode,omitempty"` // rw-split | standard（空视为 rw-split）
	Labels        []string `yaml:"labels,omitempty" json:"labels,omitempty"`
	// Password 是 Proxy 前端密码（客户端 → Proxy 鉴权）。
	// 实例本身不设 requirepass，鉴权只在 Proxy 前端终结；空串表示不鉴权。
	Password         string       `yaml:"password,omitempty" json:"password,omitempty"`
	CurrentMaster    string       `yaml:"current_master" json:"current_master"`
	TopologyStatus   string       `yaml:"topology_status" json:"topology_status"` // healthy | degraded
	FailoverConflict bool         `yaml:"failover_conflict" json:"failover_conflict"`
	Proxy            *ProxyConfig `yaml:"proxy,omitempty" json:"proxy,omitempty"`
	// Backup 是组级备份策略。nil 或 Schedule=="" 表示未配置自动备份。
	// 执行节点由平台在触发时选定（优先健康从节点，回退 master），不落在配置里；
	// 备份对象在 S3 上按组名归档（Server 下发 s3_prefix=组名），failover 后仍归属同一组。
	Backup    *BackupConfig `yaml:"backup,omitempty" json:"backup,omitempty"`
	CreatedAt string        `yaml:"created_at" json:"created_at"`
	UpdatedAt string        `yaml:"updated_at" json:"updated_at"`
}

// InstanceGroupView 是实例组面向用户 API 的安全投影。
// Password 只供 Server 生成 Proxy 内部快照，不通过实例查询接口返回。
type InstanceGroupView struct {
	Type             string       `json:"type"`
	Engine           string       `json:"engine"`
	EngineVersion    string       `json:"engine_version,omitempty"`
	Category         string       `json:"category"`
	AccessMode       string       `json:"access_mode,omitempty"`
	Labels           []string     `json:"labels,omitempty"`
	CurrentMaster    string       `json:"current_master"`
	TopologyStatus   string       `json:"topology_status"`
	FailoverConflict bool         `json:"failover_conflict"`
	Proxy            *ProxyConfig `json:"proxy,omitempty"`
	Backup           *BackupConfig `json:"backup,omitempty"`
	CreatedAt        string       `json:"created_at"`
	UpdatedAt        string       `json:"updated_at"`
	// SentinelMaster 是 Sentinel 共识认为的当前 master（已解析成实例名，解析不到时
	// 退回原始 host:port）。仅 replication 组有意义；由 Server 缓存注入，ToView 不填。
	// 空表示 Sentinel 未启用或本轮尚未取得共识。
	SentinelMaster string `json:"sentinel_master,omitempty"`
	// RuntimeMaster 是组内此刻进程自认 master 的实例名（运行时实际），由 Server 从
	// reconcile 观测缓存推导注入。空表示尚未观测到。
	RuntimeMaster string `json:"runtime_master,omitempty"`
}

func (g *InstanceGroupState) ToView() *InstanceGroupView {
	if g == nil {
		return nil
	}
	return &InstanceGroupView{
		Type: g.Type, Engine: g.Engine, EngineVersion: g.EngineVersion,
		Category: g.Category, AccessMode: g.AccessMode, Labels: g.Labels, CurrentMaster: g.CurrentMaster,
		TopologyStatus: g.TopologyStatus, FailoverConflict: g.FailoverConflict,
		Proxy: g.Proxy, Backup: g.Backup, CreatedAt: g.CreatedAt, UpdatedAt: g.UpdatedAt,
	}
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
	ConfigPath      string            `yaml:"config_path" json:"config_path"`
	DataPath        string            `yaml:"data_path" json:"data_path"`
	BackupPath      string            `yaml:"backup_path" json:"backup_path"`
	Persistence     *Persistence      `yaml:"persistence,omitempty" json:"persistence,omitempty"`
	ConfigOverrides map[string]string `yaml:"config_overrides,omitempty" json:"config_overrides,omitempty"`
	ReplicaOf       string            `yaml:"replica_of,omitempty" json:"replica_of,omitempty"`
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
// 判据是观测结果而非意图——Proxy 端点、备份调度这类决策必须基于事实。
func (i *Instance) IsServing() bool {
	return i != nil && i.ObservedStatus == ObservedRunning
}

// InstancesState instances-state.yaml 根结构
type InstancesState struct {
	Groups    map[string]*InstanceGroupState `yaml:"groups" json:"groups"`
	Instances map[string]*Instance           `yaml:"instances" json:"instances"`
}

// InstanceView 是 Instance 面向对外查询接口的安全投影，
// 用于提供派生状态（Phase()）等计算字段的序列化形式。
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
	// Status 是派生的综合状态（Phase()），保持对外词汇稳定：
	// creating / running / stopped / stopping / unexpected_stopped / failed / unreachable
	Status string `json:"status"`
	// 同时暴露两个来源字段，让"为什么是这个状态"可以被直接看出来
	DesiredState   string `json:"desired_state"`
	ObservedStatus string `json:"observed_status"`
	Lock           *Lock  `json:"lock,omitempty"`
	CreatedAt      string `json:"created_at"`
	MemoryUsed     int64  `json:"memory_used,omitempty"` // 实时 used_memory（字节），由 Server 缓存注入
}

// ToView 将 Instance 转换为对外展示视图（含派生状态字段）。
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
		Status:          i.Phase(),
		DesiredState:    i.DesiredState,
		ObservedStatus:  i.ObservedStatus,
		Lock:            i.Lock,
		CreatedAt:       i.CreatedAt,
	}
}

// InstancesStateView 是 InstancesState 的对外投影，Instances 使用含派生状态的 InstanceView。
type InstancesStateView struct {
	Groups    map[string]*InstanceGroupView `json:"groups"`
	Instances map[string]*InstanceView      `json:"instances"`
}

// ToView 将 InstancesState 转换为对外展示视图。
func (s *InstancesState) ToView() *InstancesStateView {
	if s == nil {
		return nil
	}
	views := make(map[string]*InstanceView, len(s.Instances))
	for name, inst := range s.Instances {
		views[name] = inst.ToView()
	}
	groups := make(map[string]*InstanceGroupView, len(s.Groups))
	for name, group := range s.Groups {
		groups[name] = group.ToView()
	}
	return &InstancesStateView{
		Groups:    groups,
		Instances: views,
	}
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
	Group           string            `json:"group,omitempty"`             // stable logical group; required for master/standalone
	Engine          string            `json:"engine" binding:"required"`   // redis | kvrocks
	EngineVersion   string            `json:"engine_version,omitempty"`    // e.g. redis 5 | 6.2 | 7
	EngineImage     string            `json:"engine_image,omitempty"`      // resolved by Server and consumed by Agent
	Type            string            `json:"type" binding:"required"`     // standalone | replication
	AccessMode      string            `json:"access_mode,omitempty"`       // rw-split | standard（空=按 type 推断默认值）
	Server          string            `json:"server"`                      // 可选，为空时自动调度
	Port            int               `json:"port"`
	Memory          string            `json:"memory" binding:"required"`
	CPUs            float64           `json:"cpus"`
	Disk            string            `json:"disk,omitempty"`
	Password        string            `json:"password"`
	ReplicaOf       string            `json:"replica_of,omitempty"`
	ConfigOverrides map[string]string `json:"config_overrides,omitempty"`
	NodeSelector    map[string]string `json:"node_selector,omitempty"` // 标签亲和：只调度到 labels 包含所有 kv 的节点
	Labels          []string          `json:"labels,omitempty"`        // 实例组分类标签；仅创建新组时可设置
}

// ResizeInstanceRequest 变配实例请求。
//
// 三个规格字段均为指针，nil 表示"本次不改该维度"，用于区分"未指定"与"指定为零值"。
// 至少要指定一个非 nil 字段，否则 Server 返回 400。
//
// 变配语义：
//   - Memory：同时调整容器 cgroup（podman update --memory/--memory-swap）与
//     Redis maxmemory / Kvrocks max-db-size。不停机生效；cgroup 变更在容器重建前有效，
//     maxmemory 通过重写配置文件持久化。
//   - CPUs：仅调整容器 cgroup（podman update --cpus）。不停机生效；重建前有效。
//   - Disk：纯容量规划元数据，只更新控制面记录，不下发容器（与创建时语义一致）。
type ResizeInstanceRequest struct {
	Name   string   `json:"name" binding:"required"`
	Memory *string  `json:"memory,omitempty"`
	CPUs   *float64 `json:"cpus,omitempty"`
	Disk   *string  `json:"disk,omitempty"`
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

// ─── 受限 K/V 浏览器（Server ↔ Agent 共享）─────────────────────────────────
// 仅供 admin 使用的受限 key 查看/读写。读走 /instance/key/get，写走
// /instance/key/write（op 区分），整 key 删除走 /instance/key/delete。
// 设计见 KEY_BROWSER_PLAN.md（feature 分支临时文档）。

// KVItem 是 K/V 浏览器读结果里的一个元素，字段按 key 类型取用：
//
//	string → Value
//	hash   → Field + Value
//	list   → Index + Value（Index 为绝对下标，用指针以保留 0）
//	set    → Member
//	zset   → Member + Score
type KVItem struct {
	Field  string `json:"field,omitempty"`
	Member string `json:"member,omitempty"`
	Value  string `json:"value,omitempty"`
	Score  string `json:"score,omitempty"`
	Index  *int64 `json:"index,omitempty"`
}

// KeyGetResponse 是 /instance/key/get 的响应：一次返回元信息 + 一页值。
type KeyGetResponse struct {
	Type       string   `json:"type"`                  // string|hash|list|set|zset|none
	Size       int64    `json:"size"`                  // STRLEN/HLEN/LLEN/SCARD/ZCARD
	TTL        int64    `json:"ttl"`                   // -1 无过期，-2 不存在
	Items      []KVItem `json:"items"`                 // meta=true 时为空
	NextCursor string   `json:"next_cursor,omitempty"` // 仅 scan 类型；"0"=到底
	Truncated  bool     `json:"truncated"`             // 命中字节兜底或分片未读完
}

// KeyWriteArgs 是写操作的参数，按 op 取用不同字段。
type KeyWriteArgs struct {
	Value      string `json:"value,omitempty"`       // set / hset / push / lset
	Field      string `json:"field,omitempty"`       // hset / hdel
	Member     string `json:"member,omitempty"`      // sadd / zadd / srem / zrem
	Score      string `json:"score,omitempty"`       // zadd
	Side       string `json:"side,omitempty"`        // push: left|right
	Index      *int64 `json:"index,omitempty"`       // lset: 目标下标
	TTLSeconds int64  `json:"ttl_seconds,omitempty"` // expire: <0 表示 PERSIST
}

// KeyWriteRequest 是 /instance/key/write 的请求体。
// Op ∈ set|hset|push|sadd|zadd|expire|hdel|srem|zrem|lset（白名单在 Server service 与 Agent 双重校验）。
type KeyWriteRequest struct {
	Name string       `json:"name" binding:"required"`
	Key  string       `json:"key" binding:"required"`
	Op   string       `json:"op" binding:"required"`
	Args KeyWriteArgs `json:"args"`
}

// KeyWriteResponse 是写/删操作的响应，Result 为 redis 回复原文（如 OK / 整数）。
type KeyWriteResponse struct {
	OK     bool   `json:"ok"`
	Result string `json:"result"`
}

// KeyDeleteRequest 是 /instance/key/delete 的请求体（整 key 删除）。
type KeyDeleteRequest struct {
	Name string `json:"name" binding:"required"`
	Key  string `json:"key" binding:"required"`
}
