# Redis 多实例管理系统 - 功能梳理

## 项目概述
这是一个基于 Podman 的 Redis/Kvrocks 多实例管理系统，采用 Server-Agent 架构：
- **Server**: 中央管理服务，提供 HTTP API 和定时调度
- **Agent**: 部署在每台物理服务器上，负责容器生命周期管理
- **CLI**: 命令行工具，与 Server 交互
- **状态管理**: 基于 YAML 文件的分布式状态存储

---

## 1. Server 端 API 功能

### 1.1 节点管理 (`/node` 路由)

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/node/list` | GET | 查询所有服务器及资源分配情况 | `nodeQuery()` |
| `/node/add` | POST | 注册新服务器到节点 | `nodeAdd()` |
| `/node/remove` | POST | 从节点移除服务器 | `nodeRemove()` |
| `/node/update` | POST | 更新服务器信息（容量、标签等） | `nodeUpdate()` |

**关键数据结构**:
- `NodeServer`: 服务器信息（IP、Agent 端口、容量、已分配资源、实例列表、状态、心跳时间）
- `ResourceSpec`: 资源规格（CPU 核数、内存、磁盘）

---

### 1.2 实例管理 (`/instance` 路由)

#### 1.2.1 基础操作

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/list` | GET | 列出所有实例 | `instanceList()` |
| `/instance/status` | GET | 查询单个实例状态（需 `name` 参数） | `instanceStatus()` |
| `/instance/create` | POST | 创建新实例 | `instanceCreate()` |
| `/instance/delete` | POST | 删除实例 | `instanceDelete()` |
| `/instance/start` | POST | 启动实例 | `instanceStart()` |
| `/instance/stop` | POST | 停止实例 | `instanceStop()` |

#### 1.2.2 配置管理

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/config` | POST | 更新实例配置（支持热更新或重启生效） | `instanceConfig()` |

**配置更新支持两种模式**:
- **热更新** (`restart: false`): 通过 `CONFIG SET` 逐个设置参数
- **重启生效** (`restart: true`): 重写配置文件并重启容器

#### 1.2.3 拓扑管理

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/promote` | POST | 从库提升为主库（REPLICAOF NO ONE） | `instancePromote()` |
| `/instance/replicate` | POST | 设置复制关系（从库指向新主库） | `instanceReplicate()` |

**拓扑变更特性**:
- 自动维护主库的 `Replicas` 列表
- 提升时自动分配 Envoy 端口
- 变为从库时释放 Envoy 端口
- 自动刷新 Envoy 配置

#### 1.2.4 创建实例的完整流程

```
1. 原子操作（写锁保护）:
   - 检查实例名冲突
   - 自动调度（若未指定服务器）
   - 分配 Redis 端口
   - 分配 Envoy 端口（仅主库/standalone）
   - 写入 "creating" 状态

2. 调用 Agent 创建容器
   - 失败时自动清理（best-effort）
   - 清理实例记录和主库 Replicas

3. 更新状态为 "running"

4. 更新 pool-state 资源分配

5. 刷新 Envoy 配置
```

**支持的参数**:
- `name`: 实例名称（必需）
- `category`: cache | persistent
- `engine`: redis | kvrocks
- `type`: standalone | replication
- `server`: 目标服务器（可选，为空时自动调度）
- `port`: Redis 端口（可选，自动分配）
- `memory`: 内存规格（如 "4Gi"）
- `cpus`: CPU 核数
- `password`: 访问密码
- `replica_of`: 主库地址（仅从库，格式 "ip:port"）
- `config_overrides`: 配置覆盖（键值对）

---

### 1.3 备份管理 (`/backup` 路由)

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/backup/exec` | POST | 执行一次备份 | `backupExec()` |
| `/backup/restore` | POST | 从备份恢复数据 | `backupRestore()` |
| `/backup/list` | GET | 列出可用备份（需 `name` 参数） | `backupList()` |

**备份特性**:
- **Redis 备份**:
  - 仅 RDB: `BGSAVE` → 等待完成 → 复制 `dump.rdb`
  - RDB+AOF: `BGSAVE` + `BGREWRITEAOF` → 同时备份 RDB 和 AOF 文件
  - 支持 AOF 目录模式（`appendonlydir/`）和单文件模式（`appendonly.aof`）

- **Kvrocks 备份**:
  - `ROCKSDB.CHECKPOINT` → 压缩为 `.checkpoint.tar.gz`

- **轮转清理**: 支持 `retention` 参数限制备份份数

- **恢复优先级**: AOF > RDB（若存在 AOF 则优先恢复）

---

### 1.4 Envoy xDS 代理控制面

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/api/v1/proxy/snapshot` | GET | 返回代理层结构化快照 | `proxySnapshot()` |
| `/proxy/snapshot` | GET | 返回代理层结构化快照 | `proxySnapshot()` |

**xDS 配置生成逻辑**:
1. 按主库名聚合实例组（主库 + 所有从库）
2. `redis-pilot-xds` 为每个实例组创建业务 Listener:
   - **MASTER 端口** (`MasterPort`): 后端仅包含主库，所有命令都走主库
   - **AUTO 端口** (`AutoPort`): 默认走主库，读命令通过 `read_command_policy` 走从库
3. 创建 master/replica Cluster，master 指向当前主库，replica 指向所有从库
4. Envoy 通过 LDS/CDS/EDS 动态接收 Listener、Cluster、Endpoint

**自动刷新机制**:
- `redis-pilot-xds` 定时轮询 Server proxy snapshot
- snapshot 版本变化后推送新的 xDS Snapshot
- Server 不写 Envoy 配置文件，也不执行 Envoy reload

---

### 1.5 状态校验 (`/reconcile` 路由)

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/reconcile` | POST | 执行一次状态校验 | `reconcile()` |

**校验流程**:
1. 按服务器分组实例
2. 调用每台服务器的 Agent 获取实际容器列表
3. 对比期望状态 vs 实际状态
4. 自动修复或告警:
   - `running` → `running`: 无操作
   - `creating/failed` → `running`: 更新为 `running`
   - `running` → `stopped`: 告警 + 标记为 `unexpected_stopped`
   - `running` → `missing`: 告警 + 标记为 `failed`

**返回结果**:
```json
[
  {
    "instance": "redis-1",
    "server": "server-1",
    "desired": "running",
    "actual": "running",
    "action": "none"
  }
]
```

---

## 2. Agent 端功能

### 2.1 主机资源接口

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/host/resources` | GET | 获取主机资源使用情况 | `hostResources()` |
| `/host/health` | GET | 健康检查 | `hostHealth()` |

**返回的主机指标**:
- CPU 核数、内存总量/已用、磁盘总量/已用
- 实例列表、容器总数、运行中的容器数
- 更新时间戳

---

### 2.2 实例生命周期管理

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/list` | GET | 列出所有容器及运行状态 | `instanceList()` |
| `/instance/status` | GET | 获取实例详细信息（INFO + 缓存指标） | `instanceStatus()` |
| `/instance/create` | POST | 创建容器并启动 | `instanceCreate()` |
| `/instance/start` | POST | 启动容器 | `instanceStart()` |
| `/instance/stop` | POST | 停止容器 | `instanceStop()` |
| `/instance/delete` | POST | 删除容器（可选清理数据） | `instanceDelete()` |

**创建实例的详细步骤**:
1. 创建数据目录结构（`conf/`, `data/`, `backup/`）
2. 根据引擎类型渲染配置文件:
   - **Redis**: 支持 RDB/AOF、内存策略、复制配置
   - **Kvrocks**: RocksDB 调优参数、复制配置
3. 通过 Podman 启动容器:
   - 挂载配置、数据、备份目录
   - 设置内存限制和 CPU 限制
   - 配置自动重启策略（失败 5 次后重启）
   - 端口映射（Redis: 6379, Kvrocks: 6666）
4. 缓存密码用于后续操作

---

### 2.3 配置管理

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/config` | POST | 更新实例配置 | `instanceConfig()` |

**支持两种更新模式**:
- **热更新** (`restart: false`): 通过 `CONFIG SET` 逐个设置参数
- **重启生效** (`restart: true`): 重写配置文件 → 停止容器 → 启动容器

---

### 2.4 拓扑管理

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/promote` | POST | 从库提升为主库 | `instancePromote()` |
| `/instance/replicate` | POST | 设置复制关系 | `instanceReplicate()` |

**实现方式**:
- 提升: `REPLICAOF NO ONE`
- 复制: `REPLICAOF <ip> <port>`

---

### 2.5 备份与恢复

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/backup` | POST | 执行备份 | `instanceBackup()` |
| `/instance/restore` | POST | 从备份恢复 | `instanceRestore()` |
| `/instance/backups` | GET | 列出可用备份 | `instanceBackups()` |

**Redis 备份流程**:
1. 检查是否启用 AOF（通过 `INFO persistence`）
2. 执行 `BGSAVE`，轮询等待完成（最多 60s）
3. 若启用 AOF:
   - 执行 `BGREWRITEAOF`，轮询等待完成
   - 创建目录备份集，同时复制 RDB 和 AOF 文件
4. 若仅 RDB:
   - 直接复制 `dump.rdb` 为 `<timestamp>.rdb`
5. 轮转清理（保留指定份数）

**Kvrocks 备份流程**:
1. 执行 `ROCKSDB.CHECKPOINT`
2. 压缩 checkpoint 目录为 `.checkpoint.tar.gz`
3. 删除原 checkpoint 目录

**恢复流程**:
1. 停止容器
2. 清空数据目录
3. 恢复备份文件（优先 AOF）
4. 启动容器

---

### 2.6 定时任务

#### 2.6.1 健康检查 (`runHealthCheck`)
- **周期**: 30 秒
- **功能**: 
  - 列出所有运行中的容器
  - 对每个容器执行 `PING` 命令
  - 若失败，自动重启容器
  - 记录错误日志

#### 2.6.2 指标采集 (`runMetricsCollect`)
- **周期**: 60 秒
- **功能**:
  - 采集每个实例的 `INFO` 命令输出，缓存在内存中
  - 采集主机指标（CPU、内存、磁盘、容器数）
  - 更新时间戳

**缓存结构**:
```go
type metrics struct {
    Info      string    // INFO 命令输出
    UpdatedAt time.Time
}
```

---

## 3. CLI 命令

### 3.1 节点命令

```bash
redis-pilot-cli node list
  # 查询所有服务器及资源分配

redis-pilot-cli node add server-1 \
  --endpoint 192.168.1.10 \
  --agent-port 8400 \
  --agent-token xxx \
  --cpu 32 \
  --memory 256Gi \
  --disk 2Ti \
  --zone zone-a \
  --role production
  # 注册服务器

redis-pilot-cli node remove server-1
  # 移除服务器

redis-pilot-cli node update server-1 --json server.json
  # 更新服务器信息
```

### 3.2 实例命令

```bash
redis-pilot-cli instance list
  # 列出所有实例

redis-pilot-cli instance status redis-1
  # 查看实例状态

redis-pilot-cli instance create redis-1 \
  --group redis-1 \
  --category cache \
  --engine redis \
  --type standalone \
  --node server-1 \
  --memory 4Gi \
  --cpus 2 \
  --password mypass \
  --config "maxmemory-policy=allkeys-lru"
  # 创建实例

redis-pilot-cli instance delete redis-1 --clean-data
  # 删除实例

redis-pilot-cli instance start redis-1
redis-pilot-cli instance stop redis-1
  # 启动/停止实例

redis-pilot-cli instance config redis-1 \
  --set "timeout=300,tcp-keepalive=60" \
  --restart
  # 更新配置

redis-pilot-cli instance promote replica-1
  # 从库提升为主库

redis-pilot-cli instance replicate replica-1 --replica-of redis-1
  # 设置复制关系
```

### 3.3 备份命令

```bash
redis-pilot-cli backup exec redis-1
  # 执行备份

redis-pilot-cli backup list redis-1
  # 列出可用备份

redis-pilot-cli backup restore redis-1 \
  --backup-ts 2024-01-15T10:30:00
  # 从备份恢复

redis-pilot-cli backup set-schedule redis-1 --cron "0 2 * * *" --retention 7
  # 设置定时备份
```

### 3.4 Sentinel 命令

```bash
redis-pilot-cli sentinel status
  # 查看已声明 Sentinel 节点状态

redis-pilot-cli sentinel sync
  # 将当前主从组 master 同步到已部署的 Sentinel
```

---

## 4. 状态管理

### 4.1 状态文件结构

#### `pool-state.yaml`
```yaml
servers:
  server-1:
    endpoint: 192.168.1.10
    agent_port: 8400
    agent_token: xxx
    labels:
      zone: zone-a
      role: production
    capacity:
      cpu_cores: 32
      memory: 256Gi
      disk: 2Ti
    allocated:
      cpu_cores: 8
      memory: 16Gi
      disk: 500Gi
    instances:
      - redis-1
      - redis-2
    status: healthy
    last_heartbeat: 2024-01-15T10:30:00Z
```

#### `instances-state.yaml`
```yaml
instances:
  redis-1:
    category: cache
    engine: redis
    type: standalone
    role: standalone
    server: server-1
    container: redis-redis-1
    port: 6379
    memory: 4Gi
    cpus: 2
    password: xxx
    config_path: /data/redis/redis-1/conf
    data_path: /data/redis/redis-1/data
    backup_path: /data/redis/redis-1/backup
    config_overrides:
      timeout: "300"
    status: running
    envoy:
      master_port: 8000
      auto_port: 8001
    backup:
      schedule: "0 2 * * *"
      retention: 7
      last_backup: 2024-01-15T02:00:00Z
    created_at: 2024-01-10T10:00:00Z
    lock:
      held_by: auto-1234567890
      operation: config
      acquired_at: 2024-01-15T10:30:00Z
      timeout: 300
```

### 4.2 状态管理 API

**原子操作**:
```go
// 读写锁保护的原子操作
WithInstances(func(instances *InstancesState) error {
    // 修改 instances
    return nil
})
```

**操作锁**:
- 每个实例可持有一个操作锁
- 锁包含: 持有者、操作类型、获取时间、超时时间
- 同会话可重入，不同会话需等待超时释放
- 用于防止并发操作冲突

**实例组概念**:
- 主库 + 所有从库 = 一个实例组
- 对实例组中任何实例的写操作都会锁定整个组
- 防止主从拓扑变更时的并发冲突

---

## 5. Podman 容器运行时封装

### 5.1 容器创建

**Redis 容器**:
```bash
podman run -d \
  --name redis-<instance-name> \
  --memory <memory> \
  --memory-swap <memory> \
  --cpus <cpus> \
  --restart on-failure:5 \
  -p <port>:6379 \
  -v <datadir>/conf/redis.conf:/etc/redis/redis.conf:Z \
  -v <datadir>/data:/data:Z \
  -v <datadir>/backup:/backup:Z \
  <server.yaml 中 images.redis.versions 解析出的镜像> \
  redis-server /etc/redis/redis.conf
```

**Kvrocks 容器**:
```bash
podman run -d \
  --name kvrocks-<instance-name> \
  --memory <memory> \
  --memory-swap <memory> \
  --cpus <cpus> \
  --restart on-failure:5 \
  -p <port>:6379 \
  -v <datadir>/conf/kvrocks.conf:/etc/kvrocks/kvrocks.conf:Z \
  -v <datadir>/data:/data:Z \
  -v <datadir>/backup:/backup:Z \
  --entrypoint kvrocks \
  <server.yaml 中 images.kvrocks.versions 解析出的镜像> \
  -c /etc/kvrocks/kvrocks.conf
```

### 5.2 容器操作

| 操作 | 命令 |
|------|------|
| 启动 | `podman start <name>` |
| 停止 | `podman stop <name>` |
| 删除 | `podman rm -f <name>` |
| 列表 | `podman ps -a --format "{{.Names}}\t{{.State}}"` |

---

## 6. 定时调度

### 6.1 备份调度器

**启动方式**:
```go
server.StartBackupScheduler()
```

**工作流程**:
1. 启动时立即同步一次
2. 每 60 秒同步一次
3. 读取 instances-state，构建期望状态（running 且有 schedule 的实例）
4. 对比当前 cron 任务:
   - 删除不再需要的任务
   - 新增或更新任务
5. 任务执行时调用 `execBackup()`

**Cron 表达式支持**:
- 标准 5 字段格式: `minute hour day month weekday`
- 示例: `0 2 * * *` (每天 2:00 执行)

### 6.2 状态校验循环

**启动方式**:
```go
server.StartReconcileLoop()
```

**工作流程**:
1. 每 5 分钟执行一次 `runReconcile()`
2. 对比期望状态 vs 实际状态
3. 自动修复或告警
4. 记录审计日志

---

## 7. 配置文件生成

### 7.1 Redis 配置模板

**关键参数**:
- 端口: 6379
- 内存限制: `maxmemory <memory>`
- 内存策略: `allkeys-lru` (cache) | `noeviction` (persistent)
- 持久化: 
  - RDB: `save 3600 1 300 100 60 10000`
  - AOF: `appendonly yes/no` (cache: no, persistent: yes)
- 复制: `replicaof <ip> <port>` (仅从库)
- 最小从库数: `min-replicas-to-write 1` (仅主库)
- 禁用危险命令: `FLUSHDB`, `FLUSHALL`, `DEBUG`

### 7.2 Kvrocks 配置模板

**关键参数**:
- 端口: 6666
- 内存限制: `maxmemory <memory>`
- RocksDB 调优:
  - 压缩: `rocksdb.compression snappy`
  - 块大小: `rocksdb.block_size 16384`
  - 写缓冲: `rocksdb.write_buffer_size 64MB`
  - 最大打开文件: `rocksdb.max_open_files 4096`
- 复制: `replicaof <ip> <port>` (仅从库)
- 最小从库数: `min-replicas-to-write 1` (仅主库)
- Checkpoint 目录: `/backup`

---

## 8. 端口分配

### 8.1 Redis 端口分配

**分配策略**:
1. 扫描指定服务器上所有已有实例的端口
2. 从配置范围 (`redis.start` - `redis.end`) 中取第一个未占用的端口
3. 若无可用端口，返回错误

### 8.2 Envoy 端口分配

**分配策略**:
1. 为每个实例组分配 Envoy 端口:
   - MASTER 端口 (MasterPort): 从 `envoy_master` 范围分配
   - AUTO 端口 (AutoPort): 从 `envoy_auto` 范围分配，仅主从拓扑需要
2. 扫描所有已有实例的 Envoy 端口
3. 从配置范围中取第一个未占用的端口

---

## 9. 审计日志

**记录的操作**:
- `instance.create`: 实例创建
- `instance.delete`: 实例删除
- `instance.start`: 实例启动
- `instance.stop`: 实例停止
- `config.update`: 配置更新
- `topology.failover`: 从库提升
- `topology.replicate`: 设置复制
- `backup.create`: 备份执行
- `backup.restore`: 备份恢复
- `reconcile`: 状态校验

**审计记录包含**:
- 操作类型、级别（Normal/Important/Critical）
- 结果（success/failed）
- 执行时间（毫秒）
- 目标（实例名、服务器名）
- 参数（内存、CPU、配置等）

---

## 10. 其他辅助功能

### 10.1 认证

**Server 端**:
- 支持 Bearer Token 认证
- 通过 `Authorization: Bearer <token>` 头传递
- 若未配置 Token，则允许所有请求

**Agent 端**:
- 同样支持 Bearer Token 认证
- Server 调用 Agent 时自动传递 Token

### 10.2 请求日志

**Server 端**:
- 记录每个请求的方法、路径、状态码、耗时

### 10.3 密码管理

**Agent 端**:
- 启动时扫描现有实例配置文件，恢复密码
- 在内存中缓存密码（`sync.Map`）
- 用于后续 Redis 命令执行

### 10.4 自动调度

**调度策略** (`selectServer`):
- 选择满足资源需求的服务器
- 优先选择资源充足的服务器
- 若为从库，优先选择主库所在服务器（减少网络延迟）

---

## 11. 功能总结表

| 功能模块 | 已实现 | 关键特性 |
|---------|-------|---------|
| **节点管理** | ✅ | 服务器注册、容量管理、资源分配追踪 |
| **实例生命周期** | ✅ | 创建、删除、启动、停止、自动调度 |
| **配置管理** | ✅ | 热更新、重启生效、配置覆盖 |
| **拓扑管理** | ✅ | 主从复制、从库提升、自动 Replicas 维护 |
| **Envoy 代理** | ✅ | 自动配置生成、读写分离、自动重载 |
| **备份恢复** | ✅ | RDB/AOF 联合备份、轮转清理、多引擎支持 |
| **定时调度** | ✅ | 备份调度、状态校验、自动修复 |
| **健康检查** | ✅ | 30s 周期检查、自动重启 |
| **指标采集** | ✅ | 60s 周期采集、缓存存储 |
| **状态校验** | ✅ | 期望 vs 实际对比、自动修复、告警 |
| **操作锁** | ✅ | 防止并发冲突、实例组级别锁定 |
| **审计日志** | ✅ | 完整操作记录、分级记录 |
| **CLI 工具** | ✅ | 完整命令集、JSON 输出 |
