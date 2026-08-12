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
| `/instance/failover` | POST | 触发故障转移。Sentinel 托管组：转发 `SENTINEL FAILOVER` 并等待/同步结果（不保证提升指定的实例）；未托管组：直接 `REPLICAOF NO ONE`（等价于旧版 promote，保证提升指定实例） | `instanceFailover()` |
| `/instance/replicate` | POST | 设置复制关系（从库指向新主库）。Sentinel 托管组上拒绝（409），避免与自动拓扑管理冲突 | `instanceReplicate()` |

**拓扑变更特性**:
- 自动维护主库的 `Replicas` 列表
- 提升时自动分配 Proxy 端口
- 变为从库时释放 Proxy 端口
- 自动刷新 Proxy 配置

#### 1.2.4 创建实例的完整流程

```
1. 原子操作（写锁保护）:
   - 检查实例名冲突
   - 自动调度（若未指定服务器）
   - 分配 Redis 端口
   - 分配 Proxy 端口（仅主库/standalone）
   - 写入 "creating" 状态

2. 调用 Agent 创建容器
   - 失败时自动清理（best-effort）
   - 清理实例记录和主库 Replicas

3. 更新状态为 "running"

4. 更新 `servers` 表资源分配

5. Proxy 自动通过长轮询发现拓扑变更
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
- `config_overrides`: 配置覆盖（键值对）。平台管理的配置项（`replicaof` / `requirepass` /
  `dir` / `min-replicas-to-write` / `rename-command` 等）不允许覆盖，值中不得含换行或 `#`；
  规则见 `pkg/redisconf/validate.go`，Server 与 Agent 共用同一份清单，违规返回 400

---

### 1.3 备份管理 (`/backup` 路由)

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/backup/exec` | POST | 触发一次备份（异步，返回 `task_id`） | `backupExec()` |
| `/backup/status` | GET | 查询备份任务状态（需 `name` 和 `task_id` 参数） | `backupStatus()` |
| `/backup/restore` | POST | 从备份恢复数据 | `backupRestore()` |
| `/backup/list` | GET | 列出可用备份（需 `name` 参数） | `backupList()` |

**备份特性**:
- **异步模型**: `POST /backup/exec` 立即返回 `task_id`，后台 goroutine 执行备份。通过 `GET /backup/status?name=xxx&task_id=xxx` 轮询任务状态（`running` / `success` / `failed`）

- **S3 存储**: 备份产物上传至 S3 兼容对象存储，本地磁盘仅作暂存。上传成功后立即删除本地文件。`backup list` 通过 `ListObjects` 查询；恢复时从 S3 下载

- **Redis 备份**:
  - 仅 RDB: `BGSAVE` -> 等待完成 -> 校验产物 -> 上传 `{instance}/{ts}.rdb`
  - RDB+AOF: `BGSAVE` + `BGREWRITEAOF` -> 暂存目录打包为 tar.gz -> 上传 `{instance}/{ts}.rdbaof.tar.gz`
  - 支持 AOF 目录模式（`appendonlydir/`）和单文件模式（`appendonly.aof`）

- **Kvrocks 备份**:
  - `BGSAVE` -> 校验产物 -> 压缩为 `{instance}/{ts}.checkpoint.tar.gz`
  - 不使用 `ROCKSDB.CHECKPOINT`：kvrocks 2.15.0 不存在该命令，实测返回 `ERR unknown command`
  - `BGSAVE` 把 RocksDB 数据文件写入容器内 `/data/backup`（宿主机 `dataDir/data/backup`），
    而不是 `-v` 挂载的 `/backup`（`dataDir/backup`）

- **完整性保证**（不产出静默损坏的备份包）:
  - `BGSAVE`/`BGREWRITEAOF` 的回复经 `redisCmdChecked` 检查 RESP 错误——redis-cli 收到
    服务端 `-ERR` 时进程仍以退出码 0 结束，必须检查输出内容才能感知失败
  - 等待持久化完成用 `waitForPersistenceDone`：`INFO persistence` 的错误不再被丢弃，
    连续失败 3 次即快速失败；轮询超时返回错误而不是继续打包
  - 打包/复制前校验产物：必须非空，且 mtime 不早于本次 `BGSAVE` 发起时刻。仅校验非空
    不足以区分「本次生成」和「上一次失败留下的残留」——后者会产出一个格式完全有效、
    时间戳崭新、内容却是旧快照的备份包，比没有备份更危险

- **轮转清理**: 支持 `retention` 参数限制 S3 中的备份份数。上传成功后执行 `ListObjects` + `DeleteObjects` 保留最近 N 份。实例删除时（`clean_data=true`）清理该实例在 S3 中的所有对象

- **恢复优先级**: AOF > RDB（若存在 AOF 则优先恢复）

- **恢复原子性**（失败时原数据完好）:
  - 三条恢复路径（kvrocks / Redis AOF 联合 / Redis 仅 RDB）统一为
    **暂存 → 校验 → 原子替换 → 保留原数据**
  - 新数据先准备到 `data/.restore-staging/` 并校验，任一步失败都不触碰现有数据
  - 替换用 `os.Rename`（同文件系统内原子）；原数据不删除，而是改名保留为
    `<path>.before-restore-<ts>`，便于误恢复后回退
  - 恢复前校验备份包存在且非空——空包是上次备份中途失败的残骸，用它覆盖现有数据是纯粹的数据损失

---

### 1.4 Proxy 代理控制面

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/api/v1/proxy/snapshot` | GET | 返回代理层结构化快照 | `proxySnapshot()` |
| `/proxy/snapshot` | GET | 返回代理层结构化快照 | `proxySnapshot()` |

**快照生成逻辑**:
1. 按主库名聚合实例组（主库 + 所有从库）
2. 为每个实例组生成业务监听端口:
   - **MASTER 端口** (`MasterPort`): 后端仅包含主库，所有命令都走主库
   - **AUTO 端口** (`AutoPort`): 默认走主库，读命令走从库
3. 创建 master/replica Cluster，master 指向当前主库，replica 指向所有从库
4. Proxy 通过 HTTP 长轮询获取 Listener、Cluster、Endpoint 快照

**自动刷新机制**:
- Proxy 定时通过 HTTP 长轮询拉取 Server proxy snapshot（`/api/v1/proxy/snapshot`）
- snapshot 版本变化后 Proxy 自动应用新拓扑
- Server 不写 Proxy 配置文件，也不执行 reload

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
   - 意图 `running` + 观测 `stopped`: 告警（`Phase()` 推导为 `unexpected_stopped`）
   - `running` → `missing`: 告警 + 标记为 `failed`
5. 检测并修正**角色漂移**（`detectRoleDrift` / `reconcileRoleDrift`）:
   - 比对 Agent 上报的 `configured_replica_of`（磁盘配置文件里的角色）与 Server 记录
   - 不一致时用 `rewrite_config_only` 模式补推一次配置重写，**不重启容器**
   - 目的是让"容器重启后读到的角色"与 Sentinel 的决定一致，消除角色回退
   - 记审计 `reconcile.role_drift`
   - 保守跳过三种情况：非 `replication` 组、Agent 未上报该实例、期望地址解析失败
   - 状态在持 `topologyMu` 后重新读取；期望值与检测时不一致则跳过，记审计 `skipped`
   - 详见 [architecture/flows.md §4.3.6](architecture/flows.md)
6. 检测并修正**运行时复制漂移**（`detectReplicaDrift` / `reconcileReplicaDrift`）:
   - 比对 Agent 上报的 `runtime_replica_of`（进程实际在复制谁）与 Server 记录
   - 不一致时对 replica 重新下发 `REPLICAOF`，目标取自 Sentinel 确认过的当前 master
   - **绝不提升实例**：期望为 master 却在复制别人时只记审计 `alert`，提升是 Sentinel 的决定
   - Sentinel 与 Server 记录对该组 master 认定不一致时什么都不做（交给 sentinel reconcile）
   - 记审计 `reconcile.replica_drift`（Critical 级别：重发 REPLICAOF 会丢弃分叉数据）
   - 详见 [architecture/flows.md §4.3.7](architecture/flows.md)

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
| `/instance/list` | GET | 列出所有容器及运行状态，附带 `configured_replica_of`（磁盘配置文件里的角色）与 `runtime_replica_of`（进程实际在复制谁） | `instanceList()` |
| `/instance/status` | GET | 获取实例详细信息（实时 INFO） | `instanceStatus()` |
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
| `/instance/config` | POST | 更新实例配置。`rewrite_config_only` 只重写磁盘配置文件不碰容器（角色落盘）；`restart` 重写并重启；都不传则 `CONFIG SET` 热更新 | `instanceConfig()` |

**支持两种更新模式**:
- **热更新** (`restart: false`): 通过 `CONFIG SET` 逐个设置参数
- **重启生效** (`restart: true`): 重写配置文件 → 停止容器 → 启动容器

---

### 2.4 拓扑管理

| API 路径 | 方法 | 功能 | 实现函数 |
|---------|------|------|---------|
| `/instance/failover` | POST | 触发故障转移（Sentinel 托管组走 `SENTINEL FAILOVER`，未托管组直接操作 Agent） | `instanceFailover()` |
| `/instance/replicate` | POST | 设置复制关系（Sentinel 托管组上拒绝） | `instanceReplicate()` |

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
  - 对每个容器通过 RESP 直连执行 `AUTH` + `PING`（`tcpPing`）
  - 把结果分类记录到 monitor：`healthy` / `loading` / `auth_failed` / `repl_link_error` / `unreachable` / `unexpected`
  - 分类结果通过 `/instance/status` 的 `health` 字段暴露给 Server
  - 记录错误日志
- **明确不做**: 不自主重启实例。`-LOADING`（正在加载数据集）和 `auth_failed`（密码不一致）
  都不是实例故障，重启会分别导致「永远起不来」和「健康实例被反复重启」。容器级自动重启
  由 `podman --restart on-failure:5` 承担，详见 [architecture/flows.md §4.3.5](architecture/flows.md)

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

redis-pilot-cli instance failover replica-1
  # 触发故障转移（Sentinel 托管组：不保证提升的就是 replica-1）

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

### 4.1 状态存储结构

状态统一存储在 `pilot.db`（SQLite 单文件），不再使用独立的 YAML 状态文件。以下按表列出核心字段（JSON 编码列已展开为等价结构，便于对照）：

#### `servers` 表

| 列 | 类型 | 说明 |
|----|------|------|
| name | TEXT PK | 服务器名，如 `server-1` |
| endpoint | TEXT | Agent 监听地址 |
| agent_port | INTEGER | Agent 端口，默认 8400 |
| agent_token | TEXT | Server 调用该 Agent 的鉴权 Token，为空则不鉴权 |
| labels | TEXT (JSON) | 如 `{"zone":"zone-a","role":"production"}` |
| capacity | TEXT (JSON) | `ResourceSpec`，如 `{"cpu_cores":32,"memory":"256Gi","disk":"2Ti"}` |
| instances | TEXT (JSON) | 该服务器上的实例名列表 |
| status | TEXT | healthy / unhealthy 等 |
| last_heartbeat | TEXT | 最近心跳时间（RFC3339） |

对应示例（等价 JSON 视图）：
```json
{
  "name": "server-1",
  "endpoint": "192.168.1.10",
  "agent_port": 8400,
  "agent_token": "xxx",
  "labels": { "zone": "zone-a", "role": "production" },
  "capacity": { "cpu_cores": 32, "memory": "256Gi", "disk": "2Ti" },
  "allocated": { "cpu_cores": 8, "memory": "16Gi", "disk": "500Gi" },
  "instances": ["redis-1", "redis-2"],
  "status": "healthy",
  "last_heartbeat": "2024-01-15T10:30:00Z"
}
```
> 注：`allocated` 由 `instances`/`instance_groups` 表实时派生，不单独持久化存储。

#### `instances` 表（单容器运行状态）+ `instance_groups` 表（拓扑权威状态）

| 列（instances） | 类型 | 说明 |
|----|------|------|
| name | TEXT PK | 实例名，如 `redis-1` |
| grp | TEXT | 所属实例组 |
| role | TEXT | master / replica / standalone |
| server | TEXT | 所在服务器名 |
| container | TEXT | 容器名 |
| port | INTEGER | 服务端口 |
| memory / cpus / disk | — | 资源规格 |
| password | TEXT | Redis/Kvrocks 密码（明文存储，见安全说明） |
| config_path / data_path / backup_path | TEXT | 挂载路径 |
| config_overrides | TEXT (JSON) | 配置覆盖项，如 `{"timeout":"300"}` |
| backup | TEXT (JSON) | `{"schedule":"0 2 * * *","retention":7}` |
| desired_state | TEXT | 意图，仅用户操作可写：`running` / `stopped` |
| observed_status | TEXT | 观测，仅观测通道可写：`""`(未观测) / `running` / `stopped` / `missing` / `unreachable` |

> 对外展示的 `status` 是由这两列推导出来的（`Instance.Phase()`），取值
> `creating` / `running` / `stopped` / `stopping` / `unexpected_stopped` / `failed` / `unreachable`。
> 派生值刻意不入库——存储派生值会产生"派生值与来源不一致"的第三种坏状态。
| lock | TEXT (JSON) | 乐观锁，`{"held_by":"...","operation":"config","acquired_at":"...","timeout":300}` |
| created_at | TEXT | 创建时间 |

对应示例（等价 JSON 视图）：
```json
{
  "name": "redis-1",
  "group": "cache",
  "engine": "redis",
  "role": "standalone",
  "server": "server-1",
  "container": "redis-redis-1",
  "port": 6379,
  "memory": "4Gi",
  "cpus": 2,
  "password": "xxx",
  "config_path": "/data/redis/redis-1/conf",
  "data_path": "/data/redis/redis-1/data",
  "backup_path": "/data/redis/redis-1/backup",
  "config_overrides": { "timeout": "300" },
  "status": "running",
  "backup": { "schedule": "0 2 * * *", "retention": 7 },
  "created_at": "2024-01-10T10:00:00Z",
  "lock": {
    "held_by": "auto-1234567890",
    "operation": "config",
    "acquired_at": "2024-01-15T10:30:00Z",
    "timeout": 300
  }
}
```
> `proxy`（`master_port`/`auto_port`）等拓扑/入口相关字段存储在 `instance_groups` 表的 `proxy` 列（JSON），因为它们是实例组级别的属性，不随单个容器变化。

安全说明：`instances.password` 和 `servers.agent_token` 目前明文存储在 `pilot.db` 中；数据目录权限收紧为 `0700`、数据库文件权限收紧为 `0600`（仅属主可访问）以降低本机其他用户读取风险，长期考虑加密存储。

并发控制：`WithInstances`/`WithServers` 通过 `DB.mu`（`sync.Mutex`）将"读快照 → 修改 → 全量重写"过程串行化，避免并发调用基于同一旧快照互相覆盖丢失更新，不依赖文件锁。

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
3. 读取 `instances` 表，构建期望状态（running 且有 schedule 的实例）
4. 对比当前 cron 任务:
   - 删除不再需要的任务
   - 新增或更新任务
5. 任务执行时调用 `execBackup()`，返回 `task_id`（异步）

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

### 8.2 Proxy 端口分配

**分配策略**:
1. 为每个实例组分配 Proxy 端口:
   - MASTER 端口 (MasterPort): 从 `proxy_master` 范围分配
   - AUTO 端口 (AutoPort): 从 `proxy_auto` 范围分配，仅主从拓扑需要
2. 扫描所有已有实例的 Proxy 端口
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
| **Proxy 代理** | ✅ | 自动配置生成、读写分离、自动重载 |
| **备份恢复** | ✅ | RDB/AOF 联合备份、轮转清理、多引擎支持 |
| **定时调度** | ✅ | 备份调度、状态校验、自动修复 |
| **健康检查** | ✅ | 30s 周期检查、自动重启 |
| **指标采集** | ✅ | 60s 周期采集、缓存存储 |
| **状态校验** | ✅ | 期望 vs 实际对比、自动修复、告警 |
| **操作锁** | ✅ | 防止并发冲突、实例组级别锁定 |
| **审计日志** | ✅ | 完整操作记录、分级记录 |
| **CLI 工具** | ✅ | 完整命令集、JSON 输出 |
