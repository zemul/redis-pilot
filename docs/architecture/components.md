# Redis 多实例管理系统 — 组件详细设计

> 上一篇：[架构概述](overview.md) · 下一篇：[关键流程](flows.md)

---

## 3. 组件详细设计

### 3.0 Server（状态管理层）

部署在控制节点，是 CLI 和各服务器 Agent 之间的中间层，负责全局状态管理和并发控制。

#### 职责

| 职责 | 说明 |
|------|------|
| 状态存储 | 持有 pilot.db（SQLite 单文件，servers / instance_groups / instances / audit_log 等表），所有读写经过 Server 串行化 |
| 并发控制 | 内存读写锁（sync.RWMutex）+ 实例级操作锁（业务层），防止多 GAL 会话并发冲突。Server 为单进程部署，`WithInstances`/`WithServers` 通过 `DB.mu`（`sync.Mutex`）将"读快照 → 修改 → 全量重写"过程串行化，避免并发事务基于同一旧快照互相覆盖，无需文件级锁 |
| API 网关 | 暴露与 CLI Tools 一一对应的 HTTP API，转发操作到各服务器 Agent |
| 审计日志 | 记录所有管理操作到 pilot.db 的 audit_log 表 |

#### API 定义

```
节点管理（纯本地，不调用 Agent）
  GET  /node/list              查询节点状态
  POST /node/add                注册新服务器
  POST /node/remove             移除服务器
  POST /node/update             更新服务器信息

实例管理（转发到目标服务器 Agent）
  POST /instance/create         创建实例
  POST /instance/delete         删除实例
  POST /instance/start          启动实例
  POST /instance/stop           停止实例
  POST /instance/config         更新配置
  POST /instance/failover       触发故障转移（Sentinel 托管组：转发 SENTINEL FAILOVER；未托管组：直接操作 Agent，等价于旧版 promote）
  POST /instance/replicate      设置复制目标（Sentinel 托管组上拒绝，避免与自动拓扑管理冲突）
  GET  /instance/list           列出所有实例
  GET  /instance/status         实例详细状态

备份管理（转发到目标服务器 Agent）
  POST /backup/exec             执行备份（异步，返回 task_id）
  GET  /backup/status            查询备份任务状态
  POST /backup/restore          从备份恢复
  GET  /backup/list             列出可用备份

代理管理
  GET  /api/v1/proxy/snapshot   查看代理层结构化快照
  GET  /proxy/snapshot          查看代理层结构化快照
```

#### 部署

```
/opt/redis-server/
  ├── server.yaml               # Server 自身配置
  └── state/
      └── pilot.db               # SQLite：servers / instance_groups / instances / audit_log 等表
```

#### 认证

```yaml
# server.yaml
port: 8080
token: ""           # CLI 调用 Server 的鉴权 Token，为空则不鉴权
data_dir: /opt/redis-server/state
```

```yaml
# 以下字段现存储于 pilot.db 的 servers 表（每行对应一台服务器，含 Agent 连接信息）
# servers 表：
#   name TEXT PRIMARY KEY, endpoint, agent_port, agent_token,
#   labels(JSON), capacity(JSON), instances(JSON), status, last_heartbeat
servers:
  server-a:
    endpoint: 10.0.1.10
    agent_port: 8400
    agent_token: ""  # Server 调用 Agent 的鉴权 Token，为空则不鉴权
    ...
```

CLI 配置：

```yaml
# ~/.redis-pilot-cli/config.yaml
server: 127.0.0.1:8080   # 默认连接本机 Server
token: ""                # 为空则不鉴权
```

CLI Token 优先级（从高到低）：`--token` 参数 > `REDIS_SERVER_TOKEN` 环境变量 > 配置文件。

---

### 3.1 管理 Agent

每台服务器部署一个 Agent，负责本机所有 Redis 实例的生命周期管理。

#### 3.1.1 职责

| 职责 | 说明 |
|------|------|
| Podman 生命周期 | 创建 / 启动 / 停止 / 删除容器 |
| 资源采集 | CPU / 内存 / 磁盘使用率，定时上报 |
| 配置管理 | 生成 redis.conf，挂载到容器 |
| 持久化目录管理 | 创建 / 清理 conf 和 data 目录 |
| 备份执行 | 本地执行 BGSAVE，校验产物非空且确为本次生成后再打包；恢复采用暂存 + 原子替换，原数据保留为 `.before-restore-<ts>` |
| 健康检查 | 定期 ping 实例，把结果**分类上报**（healthy / loading / auth_failed / repl_link_error / unreachable / unexpected），自身不采取任何恢复动作 |
| 指标采集 | INFO 命令解析，内存 / 连接 / 命令统计 |

> **Agent 不做故障自愈。** 这是一条明确的架构约束，理由见 [flows.md §4.3.5](flows.md)。
> 容器级自动重启由 `podman --restart on-failure:5` 承担（响应的是进程退出这个确定事实）；
> 主从组内实例不健康由 Sentinel failover 把流量切走；超过重启上限后容器停住，由 Server
> 的容器 reconcile 检测并告警。

#### 3.1.2 技术选型

**推荐方案：轻量 Python/Go 守护进程**

- 暴露 HTTP API，供 Server 层远程调用
- 内置健康检查循环，探测结果通过 `/instance/status` 的 `health` 字段暴露，**不自主重启实例**
- 使用 Podman API（libpod）管理容器，避免 shell 拼接
- 可选：结合 Podman Quadlet（systemd 单元文件）管理容器生命周期

#### 3.1.3 API 定义

```
实例管理
  POST /instance/create        创建实例
  POST /instance/start         启动实例
  POST /instance/stop          停止实例
  POST /instance/delete        删除实例（需确认数据清理）
  POST /instance/config        更新配置。三种互斥模式：
                               rewrite_config_only 只重写磁盘配置文件（角色落盘，不碰容器）、
                               restart 重写并重启、都不传则 CONFIG SET 热更新
  POST /instance/promote       从库提升为主库（REPLICAOF NO ONE）——底层原语，Server 仅在
                                Sentinel 未启用的组上调用；Sentinel 托管的组由 Sentinel 自己
                                直接对节点下发 REPLICAOF，不经过这个接口
  POST /instance/replicate     设置复制目标（REPLICAOF host port）——同上，仅无 Sentinel 托管
                                的组或 Sentinel 自身触发的降级会用到

  GET  /instance/list          列出本机所有实例，并附带两份角色观测供 Server 检测漂移：
                               configured_replica_of（磁盘配置文件里的 replicaof）
                               runtime_replica_of（进程此刻实际在复制谁，取健康探测缓存，
                               不现场探测；过期或探测失败的实例不出现在其中）
  GET  /instance/status        实例详细状态（含 INFO 解析）

备份管理
  POST /instance/backup        执行备份（异步，返回 task_id）
  GET  /instance/backup/status  查询备份任务状态
  POST /instance/restore       从备份恢复
  GET  /instance/backups       列出可用备份

主机管理
  GET  /host/resources         服务器资源使用情况
  GET  /host/health            健康检查
```

#### 3.1.4 请求/响应示例

**创建实例**

```
POST /instance/create

master/standalone 必须显式传 group；replica 不需要传 group，会从 replica_of 指向的 master 继承。

请求体：
{
  "name": "order-1",
  "group": "order",
  "port": 6379,
  "memory": "4Gi",
  "cpus": 2,
  "password": "******",
  "persistence": {
    "rdb": true,
    "rdb_frequency": "3600 1 300 100",   // save 配置
    "aof": true,
    "aof_policy": "everysec"
  },
  "replica_of": null,                      // 主库不设置
  "config_overrides": {                     // 额外配置覆盖
    "maxmemory-policy": "allkeys-lru",
    "timeout": 300
  }
}

> `config_overrides` 渲染在配置模板最末尾，而 Redis 配置是后者覆盖前者，
> 因此它是用户输入唯一能改变实例语义的入口。平台管理的配置项不允许覆盖，
> 值中不得含换行（否则可注入任意指令）。规则见 §4.7 与 `pkg/redisconf/validate.go`。

响应体：
{
  "success": true,
  "container_id": "a1b2c3d4",
  "container_name": "redis-order-1",
  "port": 6379,
  "conf_dir": "/data/redis/order-1/conf",
  "data_dir": "/data/redis/order-1/data",
  "backup_dir": "/data/redis/order-1/backup"
}
```

**查询主机资源**

```
GET /host/resources

响应体：
{
  "hostname": "server-a",
  "cpu": { "total_cores": 16, "usage_percent": 45.2 },
  "memory": { "total": "64Gi", "used": "28Gi", "available": "36Gi" },
  "disk": {
    "total": "500Gi",
    "used": "120Gi",
    "available": "380Gi",
    "redis_data": "80Gi"
  },
  "instances": ["order-1", "cache-1"],
  "podman": { "containers": 2, "running": 2 }
}
```

---

### 3.2 服务器节点

#### 3.2.1 服务器节点存储

全局维护在 `pilot.db` 的 `servers` 表中，记录每台服务器的容量和分配情况。字段以下用 YAML 风格展示概念性结构，实际存储为 SQLite 表的一行（`labels` / `capacity` / `instances` 等复合字段以 JSON 编码存入对应列）：

```yaml
# 概念性字段说明；实际存储为 pilot.db 的 servers 表
servers:
  server-a:
    endpoint: 10.0.1.10
    agent_port: 8400
    agent_token: ""            # Server 调用 Agent 的鉴权 Token，为空则不鉴权
    labels:
      zone: az-1
      role: production
    capacity:
      cpu_cores: 16
      memory: 64Gi
      disk: 500Gi
    allocated:
      cpu_cores: 4
      memory: 16Gi
      disk: 80Gi
    instances:
      - order-1
      - cache-1
    status: healthy
    last_heartbeat: "2026-04-23T18:00:00Z"

  server-b:
    endpoint: 10.0.1.11
    agent_port: 8400
    labels:
      zone: az-2
      role: production
    capacity:
      cpu_cores: 16
      memory: 64Gi
      disk: 500Gi
    allocated:
      cpu_cores: 8
      memory: 32Gi
      disk: 120Gi
    instances:
      - order-2
      - user-master
      - user-replica
    status: healthy
    last_heartbeat: "2026-04-23T18:00:00Z"

  server-c:
    endpoint: 10.0.1.12
    agent_port: 8400
    labels:
      zone: az-1
      role: standby
    capacity:
      cpu_cores: 8
      memory: 32Gi
      disk: 200Gi
    allocated:
      cpu_cores: 0
      memory: 0Gi
      disk: 0Gi
    instances: []
    status: healthy
    last_heartbeat: "2026-04-23T18:00:00Z"
```

> 注：`labels` / `capacity` / `instances` 在 `servers` 表中以 JSON 文本列存储；`allocated` 不落库，由 Server 在查询时读取 `instances` 表实时汇总计算得出（见 `computeAllocated`），因此不是 `servers` 表的持久化字段，上面的 `allocated` 段仅用于说明查询返回值的语义。

#### 3.2.2 调度策略

创建实例时的服务器选择逻辑：

```
1. 过滤：排除 status != healthy 的服务器
2. 过滤：排除剩余资源 < 申请量的服务器
3. 排序优先级：
   a. 主从必须分布在不同服务器（容灾）
   b. 主从优先分布在不同可用区（zone 标签）
   c. 剩余资源最多的服务器优先（均衡分配）
4. 锁定：选定后写入 `instances` 表（`server` 字段指向选中的服务器），服务器的已分配资源由 Server 查询时从 `instances` 表实时汇总派生，无需单独更新 `servers` 表
```

---

### 3.3 实例状态

#### 3.3.1 实例状态存储

全局维护在 `pilot.db` 的 `instance_groups` 表 + `instances` 表中，其中 `instance_groups` 是拓扑与入口的权威状态，`instances` 只记录单容器运行状态。以下用 YAML 风格展示概念性结构，实际存储为两张 SQLite 表（复合字段如 `proxy` / `persistence` / `config_overrides` / `backup` / `lock` 以 JSON 编码存入对应列）：

```yaml
# 概念性字段说明；实际存储为 pilot.db 的 instance_groups 表 + instances 表
groups:
  order:
    type: replication                 # standalone | replication
    engine: kvrocks                   # redis | kvrocks
    category: persistent              # cache | persistent
    current_master: order-1      # 当前主库实例名
    topology_status: healthy          # standalone: 主库运行即 healthy；replication: 主库和至少 1 个从库运行才 healthy
    failover_conflict: false
    proxy:
      master_port: 16500              # Proxy 强制主库端口，所有命令走主库
      auto_port: 16379                # Proxy 读写分离端口，写走主库，读走主库+从库
    created_at: "2026-04-23T10:00:00Z"
    updated_at: "2026-04-23T10:05:00Z"

instances:
  order-1:
    group: order                      # 稳定实例组名；Sentinel/Proxy/审计均使用该名字
    role: master                      # master | replica
    server: server-a
    container: redis-order-1
    port: 6379
    memory: 4Gi
    cpus: 2
    password: "******"               # 脱敏存储
    config_path: /data/redis/order-1/conf
    data_path: /data/redis/order-1/data
    backup_path: /data/redis/order-1/backup
    persistence: null                 # Kvrocks 实例无需 RDB/AOF，RocksDB 原生持久化
    config_overrides:
      rocksdb.compression: lz4
      rocksdb.write_buffer_size: "256"
      rocksdb.max_write_buffer_number: "4"
      maxmemory-policy: noeviction    # 持久化实例不禁用数据
      timeout: "300"
    replica_of: null
    backup:
      schedule: "0 */6 * * *"        # 每 6 小时
      retention: 7                   # 保留 7 份
    status: running
    created_at: "2026-04-23T10:00:00Z"

  order-2:
    group: order                      # 从库继承主库 group；failover 后不变化
    role: replica
    server: server-b
    container: redis-order-2
    port: 6379
    memory: 4Gi
    cpus: 2
    password: "******"
    config_path: /data/redis/order-2/conf
    data_path: /data/redis/order-2/data
    backup_path: /data/redis/order-2/backup
    persistence: null                 # Kvrocks 实例无需 RDB/AOF，RocksDB 原生持久化
    config_overrides:
      rocksdb.compression: lz4
      rocksdb.write_buffer_size: "256"
      rocksdb.max_write_buffer_number: "4"
      maxmemory-policy: noeviction    # 持久化实例不禁用数据
      timeout: "300"
    replica_of: order-1         # 引用主库实例名
    backup:
      schedule: "0 */6 * * *"
      retention: 7
    status: running
    created_at: "2026-04-23T10:05:00Z"
```

#### 3.3.2 状态一致性规则

- **创建前**：先写入 `instances` 表（`desired_state=running`，`observed_status` 留空，
  `Phase()` 推导为 `creating`），再调用 Agent
- **创建成功**：更新 status → running；服务器已分配资源由查询时从 `instances` 表实时汇总派生，无需单独更新 `servers` 表
- **创建失败**：回滚 — 调用 Agent 清理已创建的资源，更新 status → failed
- **每次操作**：先读状态，操作完成后更新状态
- **定期校验**：Skills 可触发全量状态校验（实际容器 vs `pilot.db` 中的状态）

#### 3.3.3 操作锁机制

多 GAL 会话可能同时操作同一实例（如一个在 scale up，另一个在 migrate），并发读写需要锁保护。Server 通过 `WithInstances`（内部由 `DB.mu` 串行化"读快照 → 修改 → 全量重写"）保证同一时刻只有一个操作在改写 `instances` 表，业务层的实例级操作锁则在此基础上防止跨请求的逻辑冲突（如两个操作都读到了旧快照后各自认为自己拿到了锁）。

**实例级操作锁**：

```yaml
# lock 字段现存储于 instances 表的 lock 列（JSON 编码）
instances:
  order-1:
    name: order-1
    # ... 其他字段 ...
    lock:
      held_by: "gal-session-abc123"    # 持有锁的 GAL 会话 ID
      operation: "migrate"             # 正在执行的操作类型
      acquired_at: "2026-04-23T14:30:00Z"  # 锁获取时间
      timeout: 300                     # 锁超时（秒），默认 300
```

**锁规则**：

1. **获取锁**：任何修改操作前，必须先在 `instances` 表对应行的 `lock` 列（JSON）中写入锁信息
   - 如果 lock 已存在且未超时，拒绝操作并返回冲突提示
   - 如果 lock 已超时（`now - acquired_at > timeout`），视为锁失效，可被新会话抢占
2. **释放锁**：操作完成后（无论成功或失败），必须清除 lock 字段
3. **锁超时**：默认 300 秒，防止会话异常退出后锁永远不释放
4. **锁粒度**：实例级（同一实例组的主从共享一把锁，因为操作主库可能影响从库拓扑）
5. **只读操作**：查询类操作（info / list / status）不需要获取锁

**冲突解决**：

| 场景 | 处理策略 |
|------|----------|
| 锁未超时 + 同一会话 | 允许（同一会话可重入） |
| 锁未超时 + 不同会话 | 拒绝，返回当前锁持有者和操作类型 |
| 锁已超时 | 新会话可抢占，写入新 lock 并执行 |
| 锁字段缺失 | 无锁，可直接操作 |

**定期校验（Reconciliation）**：

- **触发条件**：Skill 手动触发 / 定时任务（每 30 秒）
- **权威源**：以 Agent 实时上报为事实源（actual），YAML 为期望状态（desired）
- **差异处理**：
  - actual=running + desired=running → 无需操作
  - actual=running + desired=creating → 状态漂移，更新 YAML 为 running
  - 观测 stopped + 意图 running → 异常，只写 `observed_status=stopped`，触发告警
    （`Phase()` 推导为 `unexpected_stopped`，不入库）
  - actual=running + desired=failed → 状态漂移，更新 YAML 为 running
- **校验范围**：实例状态 + 端口占用 + 资源分配

---

### 3.4 Proxy 统一代理

#### 3.4.1 端口分配策略

Redis 协议无 Host Header，**只能通过端口区分不同实例**：

| 端口范围 | 用途 | 说明 |
|----------|------|------|
| 16379-16499 | AUTO 端口（读写分离） | 每个实例组分配一个，写命令走主库，读命令在主库+从库间按 weight 分摊 |
| 16500-16619 | MASTER 端口（强制主库） | 主从实例组分配，所有命令都走主库 |

#### 3.4.2 代理模式

**双端口模型（主从实例组）**

```
客户端 -> Proxy:16379 (AUTO)    -> 写走主库，读走主库+从库（读写分离）
客户端 -> Proxy:16500 (MASTER) -> 所有命令走主库 order-1:6379
```

- AUTO 端口：写命令路由到主库，读命令在主库 + 所有健康从库间按 weight 分摊
- MASTER 端口：所有命令（读+写）都走主库，适合需要强一致读的场景
- 从库不可用时 AUTO 端口读回落到主库，同时打告警指标
- 不保证 read-your-writes：AUTO 端口读可能命中未同步的从库，需要强一致读用 MASTER 端口

**单端口模型（单点实例）**

```
客户端 -> Proxy:16379 (AUTO)  -> 主库 cache-1:6379
```

- 单点实例只分配 AUTO 端口，无 MASTER 端口

> **设计说明**：Proxy 是自研 Go 代理（`redis-pilot`），通过 HTTP 长轮询从 Server 获取拓扑快照（`/api/v1/proxy/snapshot`），在单端口上支持全部 Redis 命令（含阻塞/订阅/Stream/事务），读写分离按命令分类自动路由，无需业务侧显式选择端口。MASTER 端口提供强制主库访问入口，用于需要强一致读的场景。

#### 3.4.3 Proxy 配置

Proxy 是 Go 二进制（`redis-pilot`），通过 `proxy.yaml` 配置启动（`redis-pilot -config proxy.yaml`）：

```yaml
# proxy.yaml 配置示例
listen_bind: 0.0.0.0
server:                      # 拉快照的 Server
  endpoint: http://127.0.0.1:8080
  token: ""
poll:
  wait: 20s                  # 长轮询挂起上限（Server 侧上限 30s）
  timeout: 5s
rw_split:
  enabled_groups: []         # 按 group 显式开读写分离
  master_read_weight: 1      # master 在读池的权重
limits:
  deny_commands: [MONITOR, KEYS, FLUSHALL, FLUSHDB, CONFIG, SHUTDOWN, ACL, PSYNC]
  max_connections_per_client: 100
  max_request_bytes: 67108864
```

> **说明**：
> - Proxy 是 Go 二进制（`redis-pilot -config proxy.yaml`），不是外部容器
> - Server 暴露 proxy snapshot（`/api/v1/proxy/snapshot`），Proxy 通过 HTTP 长轮询拉取
> - 拓扑变化通过快照热更新，无需重启 Proxy
> - 代理级策略（deny_commands / master_read_weight）在 `proxy.yaml` 中，变更需重启 Proxy

#### 3.4.4 管理命令

管理命令（`INFO`、`CONFIG GET/SET`、`SLOWLOG GET`、`CLIENT LIST` 等）应通过 MASTER 端口执行，该端口直连主库。AUTO 端口会按命令分类路由，管理类命令自动走主库。

---

### 3.5 持久化与备份

#### 3.5.1 持久化策略

每个实例的持久化配置：

| 参数 | 主库默认值 | 从库默认值 | 说明 |
|------|-----------|-----------|------|
| save | 3600 1 / 300 100 / 60 10000 | 同主库 | RDB 快照频率 |
| appendonly | yes | yes | 开启 AOF |
| appendfsync | everysec | everysec | AOF 刷盘策略 |
| stop-writes-on-bgsave-error | yes | yes | BGSAVE 失败时拒绝写入 |

**Kvrocks 持久化实例（engine: kvrocks）：**

Kvrocks 基于 RocksDB 存储，数据天然持久化到磁盘，无需 RDB/AOF：
- `persistence` 字段设为 `null`（不生成 RDB/AOF 配置）
- 数据写入即落盘（WAL + MemTable → SST），崩溃恢复由 RocksDB 自动完成
- 无需 `save` / `appendonly` / `appendfsync` 等配置
- 持久化相关指标来自 RocksDB（见 §9.1）

#### 3.5.2 备份策略

**备份来源：优先从从库备份**

- **Redis 实例（engine: redis）：**
  - 避免对主库性能影响
  - 从库执行 `BGSAVE`，等待完成并校验产物后，上传 RDB 到 S3
  - 无从库的单点实例，在主库执行（安排在低峰期）
  - **如果开启了 AOF（appendonly: yes），备份时必须同时保存 AOF 文件**：
    1. 先执行 `BGREWRITEAOF` 生成精简 AOF
    2. 等待 `BGREWRITEAOF` 完成（`INFO Persistence: aof_rewrite_in_progress=0`）
    3. 备份 RDB 文件 + 重写后的 AOF 文件，打包为 tar.gz 上传 S3
    4. 两者作为同一备份集，使用相同时间戳

- **Kvrocks 实例（engine: kvrocks）：**
  - 使用 `BGSAVE` 生成 RocksDB 快照（不使用 `ROCKSDB.CHECKPOINT`，kvrocks 2.15.0 不存在该命令）
  - 从库执行 `BGSAVE`，等待完成后打包为 tar.gz 上传 S3
  - 无从库的单点实例，在主库执行（安排在低峰期）

备份调度由 Server 内置 cron 驱动，配置读取自 `instances` 表每个实例的 `backup` 列（JSON）。Server 负责注册定时任务并在触发时调用目标实例所在 Agent 执行备份；Agent 不独立维护调度状态。

```yaml
backup:
  schedule: "0 2 * * *"   # cron 表达式，空字符串表示不启用自动备份
  retention: 7             # S3 保留份数
```

通过 CLI 配置：

```bash
# 设置定时备份
redis-pilot-cli backup set-schedule <instance> --cron "0 2 * * *" --retention 7

# 查看当前配置
redis-pilot-cli backup get-schedule <instance>

# 禁用自动备份
redis-pilot-cli backup set-schedule <instance> --cron ""
```

Server 启动时读取所有实例的 `backup.schedule`，注册内部 cron，并每 60 秒与 `instances` 表同步一次。实例备份配置变更后，Server 调度器在下一轮同步中增删对应任务。

**S3 对象存储：**

备份产物上传至 S3 兼容对象存储（如 MinIO），S3 是备份的唯一持久化位置，本地磁盘仅作暂存。S3 连接配置在 `agent.yaml`：

```yaml
backup:
  s3:
    endpoint: http://oss.internal:9000
    bucket: redis-pilot-backup
    access_key: xxx
    secret_key: xxx
    region: ""
    use_ssl: false
```

Agent 初始化时创建 S3 客户端，若 endpoint 为空则备份/恢复/列表接口返回 503。

**S3 Key 命名约定：**

```
{instance-name}/{backup-ts}.{suffix}

# Redis RDB-only
order-1/2026-08-11T02:00:00.rdb

# Redis RDB+AOF（打包为单个 tar.gz）
order-1/2026-08-11T02:00:00.rdbaof.tar.gz

# Kvrocks
order-2/2026-08-11T02:00:00.checkpoint.tar.gz
```

Key 完全可预测，不需要索引。`backup list` 通过 `ListObjects(prefix="{instance}/")` 获取列表；恢复时由 `{instance}/{backup-ts}.{suffix}` 拼出 key 下载。

**备份轮转：**

备份上传成功后，Agent 立即执行 S3 轮转清理（`ListObjects` + `DeleteObjects`），保留最近 N 份（`retention`）。实例删除时（`clean_data=true`），Agent 清理该实例在 S3 中的所有对象。

**延迟从库（可选高级功能）：**

- 设置 `REPLICA_DELAY`（如 3600 秒），从库延迟 1 小时应用主库写入
- 用途：误删除数据的快速恢复（延迟窗口内提升延迟从库为主库）
- 需要额外从库资源，按需开启

#### 3.5.3 备份恢复流程

**Redis 实例恢复（优先 AOF → 回退 RDB）：**

```
1. 在目标实例仍运行时下载备份到实例目录下的 .restore-staging/data
2. 解压并校验：RDB 必须存在且非空；联合备份还必须包含非空 AOF 文件/目录
3. 校验通过后停止目标实例（停机窗口从这里开始）
4. 以一次目录 rename 原子替换整个 data 目录：
   - RDB+AOF 作为同一个数据集切换，不允许出现“新 RDB + 旧 AOF”
   - 原 data 目录保留为 data.before-restore-<ts>
   - 仅 RDB 备份的 staged data 不包含 AOF，因此不会残留旧 AOF
5. 启动实例；若启动失败，撤销目录替换、恢复原 data 并重新启动原实例
6. 验证步骤：
   - 检查 INFO Server: uptime > 0
   - 检查 INFO Keyspace: keyspace 与预期一致
   - 检查 INFO Persistence: rdb_last_load_status=ok / aof_enabled=1
   - 可选：对关键 key 执行 GET 验证值正确性
```

**Kvrocks 实例恢复：**

```
1. 实例运行时下载并校验 checkpoint.tar.gz，解压到 .restore-staging/data/db
2. 停止目标实例
3. 原子替换整个 data 目录并保留原目录
4. 启动实例；启动失败则回滚原目录并重启
5. 验证数据完整性（DBSIZE / 指定 key 抽查）
```

> **为什么优先使用 AOF 恢复？**
> - AOF 记录了所有写操作，数据完整性高于 RDB（RDB 是定时快照，可能丢失最后一次快照后的写入）
> - 联合备份时已通过 `BGREWRITEAOF` 生成精简 AOF，恢复效率与 RDB 相当
> - 仅在 AOF 文件损坏或备份中无 AOF 时，才回退到 RDB 恢复

---

### 3.7 Sentinel 高可用设计

Sentinel 用于 Redis/Kvrocks 主从实例组的自动故障转移。Sentinel 只负责判定主库故障、选举从库并执行主从切换；平台仍负责状态文件同步、刷新 Proxy 和记录审计。

#### 3.7.1 部署策略

Sentinel 是 Redis 高可用控制面，不要求每台 Agent 都部署，但必须由运维提前规划并在 Server 配置中显式声明。Server 不根据当前 healthy Agent 自动选择 Sentinel 节点，避免控制面随节点健康状态漂移。

| 声明的 Sentinel 节点 | Sentinel 数 | quorum | 处理策略 |
|----------------------|-------------|--------|----------|
| `sentinel.nodes` 未配置 | 0 | - | 不启用自动故障转移，创建主从时返回 warning |
| 3 个显式节点 | 3 | 2 | 标准生产部署 |
| 5 个显式节点 | 5 | 3 | 高等级业务使用，容忍更多 Sentinel 节点故障 |
| 其他数量的显式节点 | - | - | 配置非法，不启用自动故障转移 |

Sentinel 节点规划规则：

1. 节点名必须来自 `servers` 表的 `name` 主键
2. Sentinel 数固定为奇数（3 或 5），避免偶数投票拓扑
3. 节点应跨 `zone` / 机架 / 故障域部署
4. 避免选择临时节点、维护节点、低规格节点或 `role=standby` 节点
5. 节点列表写入 `server.yaml` 的 `sentinel.nodes`，作为 Server 下发和 reconcile 的唯一依据

示例：

```yaml
sentinel:
  enabled: true
  nodes:
    - redis-a
    - redis-b
    - redis-c
  port: 26379
  quorum: 2
```

#### 3.7.2 Server 与 Sentinel 交互

Server 直接通过 Redis/Sentinel 协议连接 `sentinel.nodes` 的 `port` 下发监控配置，不依赖 Agent 操作 Sentinel 容器。Sentinel 由运维独立部署，并负责把动态配置 rewrite 到自身可写、持久化的 `sentinel.conf`。

```
SENTINEL MONITOR <group> <master-ip> <master-port> <quorum>
  添加实例组主库监控

SENTINEL SET <group> down-after-milliseconds <ms>
SENTINEL SET <group> failover-timeout <ms>
SENTINEL SET <group> parallel-syncs <n>
  设置故障检测和切换参数

SENTINEL REMOVE <group>
  移除指定实例组监控

SENTINEL MASTERS
SENTINEL get-master-addr-by-name <group>
  查询 Sentinel 当前监控状态和主库地址
```

Server API：

```
GET /sentinel/status
  Server 直连各 Sentinel 节点，返回已监控 master 列表和连通状态

POST /sentinel/sync
  Server 从 `instances` 表派生完整 master 列表，并直接同步到各 Sentinel 节点

POST /sentinel/event
  外部 Sentinel watcher 上报 +switch-master 事件；Server 会再次查询 Sentinel 确认当前 master 后再同步拓扑
```

Server 下发的逻辑配置示例：

```json
{
  "port": 26379,
  "quorum": 2,
  "masters": [
    {
      "group": "order",
      "host": "10.0.1.10",
      "port": 6379,
      "password": "******",
      "down_after_milliseconds": 5000,
      "failover_timeout": 30000,
      "parallel_syncs": 1
    }
  ]
}
```

#### 3.7.3 sentinel.conf 模板

```conf
port {{ sentinel_port }}
bind 0.0.0.0
dir /data

sentinel resolve-hostnames yes

{% if announce_ip %}
sentinel announce-ip {{ announce_ip }}
sentinel announce-port {{ sentinel_port }}
{% endif %}

{% for master in masters %}
sentinel monitor {{ master.group }} {{ master.host }} {{ master.port }} {{ quorum }}
{% if master.password %}
sentinel auth-pass {{ master.group }} {{ master.password }}
{% endif %}
sentinel down-after-milliseconds {{ master.group }} {{ master.down_after_milliseconds | default(5000) }}
sentinel failover-timeout {{ master.group }} {{ master.failover_timeout | default(30000) }}
sentinel parallel-syncs {{ master.group }} {{ master.parallel_syncs | default(1) }}
{% endfor %}
```

当前采用"仅 Proxy 前端鉴权"模型：后端实例无 `requirepass`，Server 下发的 `master.password` 恒为空，故 `sentinel auth-pass` 分支不会渲染，Sentinel 以无密码方式直连实例执行 INFO、REPLICAOF、故障转移。上面的 `{% if master.password %}` 条件保留是为了兼容"实例带密码"的历史/静态部署——一旦实例启用了 `requirepass`，则必须配置 `sentinel auth-pass`，否则 Sentinel 能检测端口存活但无法执行故障转移命令。

#### 3.7.4 Podman 容器规范

Sentinel 容器推荐使用 host network，避免容器 NAT/端口映射导致 Sentinel 宣告错误地址、无法发现其他 Sentinel 或无法连接从库。

```bash
podman run -d \
  --name redis-sentinel \
  --network host \
  --restart on-failure:5 \
  -v /data/redis-sentinel/conf:/etc/redis:Z \
  -v /data/redis-sentinel/data:/data:Z \
  docker.io/redis:7 \
  redis-sentinel /etc/redis/sentinel.conf
```

**注意：必须挂载目录而非单文件。** Sentinel 保存配置时先写 `sentinel.conf.tmp` 再 `rename` 替换原文件。若将 `sentinel.conf` 以单文件 bind mount 挂入容器，`rename` 会因 inode 被宿主机锁定而报 `Device or resource busy`，导致新增 master 监控失败。挂载目录可让 rename 在同一文件系统内完成。

如果不能使用 host network，必须显式配置 `sentinel announce-ip` / `sentinel announce-port`，并确保 Redis 实例的 replica announce 信息与实际可达地址一致。

#### 3.7.5 创建与删除时机

创建主从实例组成功后，Server 执行：

```
1. 从 server.yaml 的 sentinel.nodes 读取已规划 Sentinel 节点
2. 从 `instances` 表派生所有需要监控的 master 列表
3. 直接连接各 Sentinel 节点 26379，执行 SENTINEL REMOVE / MONITOR / SET
4. 如果 sentinel.nodes 未配置或数量不是 3/5，跳过 Sentinel 并在创建结果中返回 warning
```

删除实例组时，Server 执行：

```
1. 直连所有 Sentinel 节点执行 SENTINEL REMOVE <group>
2. 从 `instance_groups` 表删除实例组
3. 状态变更后 Proxy 自动通过长轮询获取新拓扑
4. 写入审计日志
```

Sentinel 配置应由 Server 统一派生并下发；Agent 不应独立决定监控哪些 master。
Sentinel 的 `group` 必须来自 `instance_groups` 表中显式保存的实例组名，而不是当前 master 实例名；master 发生 failover 后，monitor name 仍保持不变。

#### 3.7.6 故障转移同步机制

实现采用 Server 内置 watcher + 定期 reconcile 的双路径：

```
Server 启动时：
  1. 从 server.yaml 的 sentinel.nodes 读取 3 或 5 个 Sentinel 节点
  2. 对每个 Sentinel 节点启动 1 个 watcher
  3. watcher 执行 SUBSCRIBE +switch-master
  4. watcher 断线只记录日志并退避重连，不修改实例状态
```

`+switch-master` 是 Sentinel 的全局事件频道，一个 watcher 会收到该 Sentinel 监控的所有实例组切换事件；因此 watcher 数量等于 `sentinel.nodes` 数量，不按实例组放大。

事件路径作为加速机制：

```
收到 +switch-master:
  1. 解析事件 payload 中的 group
  2. Server 再次查询 Sentinel：
     SENTINEL get-master-addr-by-name <group>
  3. 使用查询确认后的 master 地址调用 handleFailover(group, confirmedMasterAddr)
```

定期 reconcile 作为权威兜底：

```
每 5-10 秒：
  1. Server 查询任一健康 Sentinel：
     SENTINEL get-master-addr-by-name <group>
  2. 对比 Sentinel 返回的 master 地址和 `instances` 表当前 master
  3. 如果不一致，调用 handleFailover(group, newMasterAddr)
```

`handleFailover` 必须是幂等的：

1. 如果 Sentinel 确认的新 master 已经是 `groups[group].current_master`，直接 no-op
2. 获取实例组编排锁
3. 校验新 master 地址属于当前实例组
4. 更新 `groups[group].current_master/topology_status`，并更新实例 `role` / `replica_of` / `status`
5. 状态变更后 Proxy 自动通过长轮询获取新拓扑
6. 写 `topology.failover` 审计日志
7. 释放编排锁

#### 3.7.7 失败处理

| 场景 | 处理策略 |
|------|----------|
| healthy Sentinel 数 < quorum | 标记自动故障转移能力降级，保留现有实例运行，触发告警 |
| 单个 Sentinel watcher 断线 | 记录日志并后台退避重连，不修改 Redis 实例状态；其他 watcher 和 reconcile 继续工作 |
| 所有 watcher 断线 | 丢失实时事件，但定期 reconcile 仍会查询 Sentinel 当前 master 并最终同步 |
| 直连 Sentinel 同步部分节点失败 | 成功节点数 >= quorum 则继续并告警；否则返回 warning 并等待 reconcile 重试 |
| Sentinel 返回的新 master 不在 `instances` 表 | 标记 `failover_conflict`，不自动改拓扑，触发人工介入 |
| 新主库再次故障 | 不主动抢占 Sentinel，等待下一轮 Sentinel failover；编排锁超时后允许重新同步 |
| Proxy 快照拉取失败 | 不影响状态更新，Proxy 下次长轮询时自动获取新拓扑 |

#### 3.7.8 验证矩阵

| 场景 | 期望 |
|------|------|
| Redis 主库容器停止 | Sentinel 提升从库，Proxy 写入口切到新 master，Server 状态最终同步 |
| Kvrocks 主库容器停止 | 行为同 Redis，需验证 `ROLE` / Sentinel failover / replication 状态兼容 |
| 实例无密码（默认）| Sentinel 无鉴权直连实例，failover 成功；鉴权只在 Proxy 前端 |
| Sentinel 节点少于 quorum | 不发生自动故障转移，系统发出降级告警 |
| Proxy 重启 | 启动时从 Server 拉取快照恢复拓扑，业务端口不变化 |

---
