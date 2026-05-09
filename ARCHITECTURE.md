# Redis 多实例管理系统 — 架构设计文档

> 版本：v0.1  
> 日期：2026-04-23  
> 状态：草案

---

## 1. 概述

### 1.1 目标

构建一套基于 GAL 的 Redis 多实例管理平台，实现：

- **单点 / 主从实例**的创建、配置、删除、查看
- **自动备份与备份管理**（创建、恢复、轮转、清理）
- **服务器资源池化**，实例按资源状态智能分配
- **实例迁移与故障转移**，跨服务器自动/手动切换
- **统一代理**，通过 Envoy 对业务侧暴露标准化接入点
- **问题诊断与可观测**，采集指标 + AI 辅助分析

### 1.2 范围

| 包含 | 不包含 |
|------|--------|
| 单点实例管理 | Redis Cluster 管理 |
| 主从复制管理 | 多活/异地复制 |
| Podman 容器化部署 | 裸金属/VM 部署 |
| Envoy 统一代理 | 自研代理层 |
| RDB + AOF 备份 | 云存储备份集成 |
| 基础监控与诊断 | 完整 Prometheus/Grafana 体系 |

### 1.3 设计原则

1. **分层解耦** — Skills 编排 → CLI 原子操作 → Server 状态管理 → Agent 执行
2. **状态驱动** — 全局状态文件由 Server 统一管理，所有读写经过 Server 串行化
3. **Agent 自治** — 每台服务器 Agent 可独立执行本地操作，GAL 断连仍可自愈
4. **主从分治** — 单点和主从使用独立 Skill 体系，不强行统一
5. **业务流量与管理流量分离** — 业务走 Envoy 代理，运维直连实例

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                       用户 / 运维人员                         │
│                  "创建一个 4G 的订单 Redis"                   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     GAL Agent (对话入口)                      │
│                  自然语言 → Skill 调用                        │
└──────────────────────────┬──────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
   │ redis-create │ │redis-migrate │ │ redis-backup │
   │    Skill     │ │    Skill     │ │    Skill     │
   │ redis-config │ │redis-failover│ │redis-diagnose│
   │ redis-scale  │ │ redis-delete │ │ redis-status │
   │ redis-envoy  │ │              │ │              │
   └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────┐
│                    CLI (原子操作层)                            │
│                                                              │
│  redis-pilot-cli pool-query / pool-add / pool-remove / pool-update│
│  redis-pilot-cli instance-create / delete / start / stop         │
│  redis-pilot-cli instance-config / promote / replicate           │
│  redis-pilot-cli backup-exec / restore / cleanup                 │
│  redis-pilot-cli backup set-schedule / get-schedule              │
│  redis-pilot-cli health-check / metrics-collect                  │
│  redis-pilot-cli envoy-route-update                              │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP API (Token + HTTPS)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Server (状态管理层)                           │
│                                                              │
│  持有 pool-state.yaml + instances-state.yaml                 │
│  所有状态读写串行化，实例级操作锁 + 文件锁                      │
│  暴露与 CLI Tools 一一对应的 HTTP API                         │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP API (Token + HTTPS)
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
   │   Server A   │ │   Server B   │ │   Server C   │
   │ 10.0.1.10    │ │ 10.0.1.11    │ │ 10.0.1.12    │
   │              │ │              │ │              │
   │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
   │ │  Agent   │ │ │ │  Agent   │ │ │ │  Agent   │ │
   │ │ :8400    │ │ │ │ :8400    │ │ │ │ :8400    │ │
   │ └────┬─────┘ │ │ └────┬─────┘ │ │ └────┬─────┘ │
   │      │       │ │      │       │ │      │       │
   │  Podman      │ │  Podman      │ │  Podman      │
   │  ┌───────┐   │ │  ┌───────┐   │ │  ┌───────┐   │
   │  │inst-1 │   │ │  │inst-3 │   │ │  │inst-5 │   │
   │  │inst-2 │   │ │  │inst-4 │   │ │  │       │   │
   │  └───────┘   │ │  └───────┘   │ │  └───────┘   │
   └──────────────┘ └──────────────┘ └──────────────┘

   ┌──────────────┐
   │ Envoy Proxy  │  统一代理层
   │ :16379       │  ← 订单主库（读写分离）
   │ :16380       │  ← 订单主库（仅写）
   │ :16381       │  ← 用户主库
   └──────────────┘
```

---

## 3. 组件详细设计

### 3.0 Server（状态管理层）

部署在控制节点，是 CLI 和各服务器 Agent 之间的中间层，负责全局状态管理和并发控制。

#### 职责

| 职责 | 说明 |
|------|------|
| 状态文件管理 | 持有 pool-state.yaml + instances-state.yaml，所有读写经过 Server 串行化 |
| 并发控制 | 文件锁（底层 IO）+ 实例级操作锁（业务层），防止多 GAL 会话并发冲突 |
| API 网关 | 暴露与 CLI Tools 一一对应的 HTTP API，转发操作到各服务器 Agent |
| 审计日志 | 记录所有管理操作到 audit/audit-YYYYMMDD.jsonl |

#### API 定义

```
资源池管理（纯本地，不调用 Agent）
  GET  /pool/query              查询资源池状态
  POST /pool/add                注册新服务器
  POST /pool/remove             移除服务器
  POST /pool/update             更新服务器信息

实例管理（转发到目标服务器 Agent）
  POST /instance/create         创建实例
  POST /instance/delete         删除实例
  POST /instance/start          启动实例
  POST /instance/stop           停止实例
  POST /instance/config         更新配置
  POST /instance/promote        从库提升为主库
  POST /instance/replicate      设置复制目标
  GET  /instance/list           列出所有实例
  GET  /instance/status         实例详细状态

备份管理（转发到目标服务器 Agent）
  POST /backup/exec             执行备份
  POST /backup/restore          从备份恢复
  GET  /backup/list             列出可用备份

代理管理
  POST /envoy/route/update      更新 Envoy 路由
  GET  /envoy/config            查看当前 Envoy 配置
```

#### 部署

```
/opt/redis-server/
  ├── server.yaml               # Server 自身配置
  ├── state/
  │   ├── pool-state.yaml       # 服务器资源池（含各 Agent 连接信息和 Token）
  │   └── instances-state.yaml
  └── audit/
      └── audit-YYYYMMDD.jsonl
```

#### 认证

```yaml
# server.yaml
port: 8080
token: ""           # CLI 调用 Server 的鉴权 Token，为空则不鉴权
data_dir: /opt/redis-server/state
```

```yaml
# pool-state.yaml（每台服务器记录含 Agent 连接信息）
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
| 备份执行 | 本地执行 BGSAVE / RDB 文件复制 |
| 健康检查 | 定期 ping 实例，检测进程存活 |
| 故障自愈 | 检测到实例挂掉，自动重启 |
| 指标采集 | INFO 命令解析，内存 / 连接 / 命令统计 |

#### 3.1.2 技术选型

**推荐方案：轻量 Python/Go 守护进程**

- 暴露 HTTP API，供 Server 层远程调用
- 内置健康检查循环，异常时自动重启实例
- 使用 Podman API（libpod）管理容器，避免 shell 拼接
- 可选：结合 Podman Quadlet（systemd 单元文件）管理容器生命周期

#### 3.1.3 API 定义

```
实例管理
  POST /instance/create        创建实例
  POST /instance/start         启动实例
  POST /instance/stop          停止实例
  POST /instance/delete        删除实例（需确认数据清理）
  POST /instance/config        更新配置（热更新 or 重启）
  POST /instance/promote       从库提升为主库
  POST /instance/replicate     设置复制目标

  GET  /instance/list          列出本机所有实例
  GET  /instance/status        实例详细状态（含 INFO 解析）

备份管理
  POST /instance/backup        执行备份
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
  "name": "order-master",
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

响应体：
{
  "success": true,
  "container_id": "a1b2c3d4",
  "container_name": "redis-order-master",
  "port": 6379,
  "conf_dir": "/data/redis/order-master/conf",
  "data_dir": "/data/redis/order-master/data",
  "backup_dir": "/data/redis/order-master/backup"
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
  "instances": ["order-master", "cache-1"],
  "podman": { "containers": 2, "running": 2 }
}
```

---

### 3.2 服务器资源池

#### 3.2.1 池状态文件

全局维护一个 `pool-state.yaml`，记录每台服务器的容量和分配情况：

```yaml
# pool-state.yaml
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
      - order-master
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
      - order-replica
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

#### 3.2.2 调度策略

创建实例时的服务器选择逻辑：

```
1. 过滤：排除 status != healthy 的服务器
2. 过滤：排除剩余资源 < 申请量的服务器
3. 排序优先级：
   a. 主从必须分布在不同服务器（容灾）
   b. 主从优先分布在不同可用区（zone 标签）
   c. 剩余资源最多的服务器优先（均衡分配）
4. 锁定：选定后更新 pool-state 的 allocated 字段
```

---

### 3.3 实例状态

#### 3.3.1 实例状态文件

全局维护 `instances-state.yaml`，记录每个实例的完整信息：

```yaml
# instances-state.yaml
instances:
  order-master:
    category: persistent              # cache | persistent
    group: order                      # 稳定实例组名；Sentinel/Envoy/审计均使用该名字
    engine: kvrocks                   # redis | kvrocks
    type: standalone                  # standalone | replication
    role: master                      # master | replica | standalone
    server: server-a
    container: redis-order-master
    port: 6379
    memory: 4Gi
    cpus: 2
    password: "******"               # 脱敏存储
    config_path: /data/redis/order-master/conf
    data_path: /data/redis/order-master/data
    backup_path: /data/redis/order-master/backup
    persistence: null                 # Kvrocks 实例无需 RDB/AOF，RocksDB 原生持久化
    kvrocks_config:
      rocksdb.compression: lz4
      rocksdb.write_buffer_size: 256MB
      rocksdb.max_write_buffer_number: 4
    config_overrides:
      maxmemory-policy: noeviction    # 持久化实例不禁用数据
      timeout: 300
    replica_of: null
    replicas: [order-replica]        # 挂载的从库列表
    envoy:
      readwrite_port: 16379          # Envoy 读写端口
      writeonly_port: 16380          # Envoy 仅写端口
    backup:
      schedule: "0 */6 * * *"        # 每 6 小时
      retention: 7                   # 保留 7 份
      last_backup: "2026-04-23T12:00:00Z"
    status: running
    created_at: "2026-04-23T10:00:00Z"

  order-replica:
    category: persistent              # cache | persistent
    group: order                      # 从库继承主库 group；failover 后不变化
    engine: kvrocks                   # redis | kvrocks
    type: replication
    role: replica
    server: server-b
    container: redis-order-replica
    port: 6379
    memory: 4Gi
    cpus: 2
    password: "******"
    config_path: /data/redis/order-replica/conf
    data_path: /data/redis/order-replica/data
    backup_path: /data/redis/order-replica/backup
    persistence: null                 # Kvrocks 实例无需 RDB/AOF，RocksDB 原生持久化
    kvrocks_config:
      rocksdb.compression: lz4
      rocksdb.write_buffer_size: 256MB
      rocksdb.max_write_buffer_number: 4
    config_overrides:
      maxmemory-policy: noeviction    # 持久化实例不禁用数据
      timeout: 300
    replica_of: order-master         # 引用主库实例名
    replicas: []
    envoy:
      readwrite_port: 16379          # 与主库同端口，Envoy 自动路由到从库读
    backup:
      schedule: "0 */6 * * *"
      retention: 7
      last_backup: "2026-04-23T12:00:00Z"
    status: running
    created_at: "2026-04-23T10:05:00Z"
```

#### 3.3.2 状态一致性规则

- **创建前**：先写 instances-state（status: creating），再调用 Agent
- **创建成功**：更新 status → running，更新 pool-state allocated
- **创建失败**：回滚 — 调用 Agent 清理已创建的资源，更新 status → failed
- **每次操作**：先读状态，操作完成后更新状态
- **定期校验**：Skills 可触发全量状态校验（实际容器 vs 状态文件）

#### 3.3.3 操作锁机制

多 GAL 会话可能同时操作同一实例（如一个在 scale up，另一个在 migrate），YAML 文件的并发读写需要锁保护。

**实例级操作锁**：

```yaml
# instances-state.yaml 中每个实例增加 lock 字段
instances:
  order-master:
    name: order-master
    # ... 其他字段 ...
    lock:
      held_by: "gal-session-abc123"    # 持有锁的 GAL 会话 ID
      operation: "migrate"             # 正在执行的操作类型
      acquired_at: "2026-04-23T14:30:00Z"  # 锁获取时间
      timeout: 300                     # 锁超时（秒），默认 300
```

**锁规则**：

1. **获取锁**：任何修改操作前，必须先在 instances-state 中写入 lock 字段
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

- **触发条件**：Skill 手动触发 / 定时任务（每 5 分钟）
- **权威源**：以 Agent 实时上报为事实源（actual），YAML 为期望状态（desired）
- **差异处理**：
  - actual=running + desired=running → 无需操作
  - actual=running + desired=creating → 状态漂移，更新 YAML 为 running
  - actual=stopped + desired=running → 异常，标记 status: unexpected_stopped，触发告警
  - actual=running + desired=failed → 状态漂移，更新 YAML 为 running
- **校验范围**：实例状态 + 端口占用 + 资源分配

---

### 3.4 Envoy 统一代理

#### 3.4.1 端口分配策略

Redis 协议无 Host Header，**只能通过端口区分不同实例**：

| 端口范围 | 用途 | 说明 |
|----------|------|------|
| 16379-16399 | 业务读写端口 | 每个实例组分配一个，Envoy 自动读写分离 |
| 16400-16419 | 业务仅写端口 | 需要显式写主库时使用，也可用于管理命令（INFO/CONFIG/SLOWLOG 等） |

#### 3.4.2 代理模式

**模式 A：单端口业务视图（推荐默认）**

```
客户端 → Envoy:16379
         ├─ 写命令 → 主库 order-master:6379
         └─ 读命令 → 从库 order-replica:6379  (read_policy: REPLICA)
```

- 用户侧感知为"单点 Redis"，无需关心主从拓扑
- Envoy `redis_proxy` filter 的 `read_policy` 负责读命令路由；写命令仍走主库
- 此端口只面向业务读写命令，管理命令统一走 WriteOnly 端口
- 适合 90% 的业务场景

**模式 B：读写端口 + 显式主库端口**

```
客户端 → Envoy:16379  → 业务读写视图（读走从库，写走主库）
客户端 → Envoy:16400  → 主库 order-master:6379    (read_policy: MASTER)
```

- 需要强一致读或执行管理命令时使用 WriteOnly 端口
- WriteOnly 端口背后的写集群必须通过 ROLE 健康检查确保只有 master 健康
- 适合对一致性要求高、或需要 `INFO` / `CONFIG` / `SLOWLOG` / `CLIENT` 等管理命令的场景

#### 3.4.3 Envoy 配置片段

```yaml
# Envoy Redis Proxy 配置示例
static_resources:
  listeners:
  - name: redis-order-rw
    address:
      socket_address:
        address: 0.0.0.0
        port_value: 16379
    filter_chains:
    - filters:
      - name: envoy.filters.network.redis_proxy
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.redis_proxy.v3.RedisProxy
          stat_prefix: redis_order
          cluster: redis-order-cluster    # 单端口读写分离集群（模式A），写集群配置见附录C
          settings:
            op_timeout: 5s
          read_policy: REPLICA           # 读走从库

  clusters:
  - name: redis-order-cluster    # 业务读写集群；附录 C 为独立写集群配置
    type: STRICT_DNS
    health_checks:
    - timeout: 1s
      interval: 5s
      unhealthy_threshold: 2
      healthy_threshold: 1
      custom_health_check:
        name: envoy.health_checkers.redis
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.health_checkers.redis.v3.Redis
          key: healthcheck    # Redis 存活检查：PING/EXISTS，不判断 master/replica 角色
    drain_connections_on_host_removal: true
    load_assignment:
      cluster_name: redis-order-cluster
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: 10.0.1.10      # 主库
                port_value: 6379
        - endpoint:
            address:
              socket_address:
                address: 10.0.1.11      # 从库
                port_value: 6379
```

> **说明**：
> - `envoy.health_checkers.redis` 只负责 Redis 存活检查，不能判断 ROLE
> - 写集群的 master 判定必须使用附录 C 的 TCP `ROLE` 健康检查
> - `unhealthy_threshold: 2`：连续 2 次健康检查失败后摘除，避免单次网络抖动误判
> - `drain_connections_on_host_removal: true`：端点移除时优雅排空连接，防止正在执行的命令被中断

#### 3.4.4 管理命令

管理命令（`INFO`、`CONFIG GET/SET`、`SLOWLOG GET`、`CLIENT LIST` 等）应通过仅写端口（WriteOnly）执行，该端口使用 `read_policy: MASTER` 直连主库。

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
  - 从库执行 `BGSAVE`，完成后复制 RDB 文件到 backup 目录
  - 无从库的单点实例，在主库执行（安排在低峰期）
  - **如果开启了 AOF（appendonly: yes），备份时必须同时保存 AOF 文件**：
    1. 先执行 `BGREWRITEAOF` 生成精简 AOF
    2. 等待 `BGREWRITEAOF` 完成（`INFO Persistence: aof_rewrite_in_progress=0`）
    3. 备份 RDB 文件 + 重写后的 AOF 文件（appendonly.aof）
    4. 两者作为同一备份集，使用相同时间戳目录

- **Kvrocks 实例（engine: kvrocks）：**
  - 使用 RocksDB Checkpoint 创建一致性快照（不阻塞读写）
  - 从库执行 `ROCKSDB.CHECKPOINT` 生成快照目录，打包复制到 backup 目录
  - 无从库的单点实例，在主库执行 Checkpoint

**定时备份配置：**

备份调度由 Agent 内置 cron 驱动，配置写在 instances-state.yaml 的实例 `backup` 字段：

```yaml
backup:
  schedule: "0 2 * * *"   # cron 表达式，空字符串表示不启用自动备份
  retention: 7             # 保留份数
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

Agent 启动时读取所有本机实例的 `backup.schedule`，注册内部 cron。实例配置变更时，Agent 重新加载调度。

**备份轮转：**

```
# Redis 实例（RDB-only 实例）
/data/redis/{instance-name}/backup/
  ├── 2026-04-23T12:00:00.rdb
  ├── 2026-04-23T06:00:00.rdb
  ├── 2026-04-23T00:00:00.rdb
  └── ...
  保留策略：保留最近 N 份（默认 7），超期自动清理

# Redis 实例（开启 AOF 的实例 — RDB+AOF 联合备份）
/data/redis/{instance-name}/backup/
  ├── 2026-04-23T12:00:00/
  │   ├── dump.rdb              # RDB 快照
  │   └── appendonly.aof        # 重写后的 AOF
  ├── 2026-04-23T06:00:00/
  │   ├── dump.rdb
  │   └── appendonly.aof
  └── ...
  保留策略：保留最近 N 份（默认 7），超期自动清理

# Kvrocks 实例
/data/redis/{instance-name}/backup/
  ├── 2026-04-23T12:00:00.checkpoint.tar.gz
  ├── 2026-04-23T06:00:00.checkpoint.tar.gz
  ├── 2026-04-23T00:00:00.checkpoint.tar.gz
  └── ...
  保留策略：保留最近 N 份（默认 7），超期自动清理
```

**延迟从库（可选高级功能）：**

- 设置 `REPLICA_DELAY`（如 3600 秒），从库延迟 1 小时应用主库写入
- 用途：误删除数据的快速恢复（延迟窗口内提升延迟从库为主库）
- 需要额外从库资源，按需开启

#### 3.5.3 备份恢复流程

**Redis 实例恢复（优先 AOF → 回退 RDB）：**

```
1. 停止目标实例

2. 判断备份类型：
   ├─ 备份目录包含 AOF 文件（RDB+AOF 联合备份）：
   │   a. 将备份的 dump.rdb 复制到 data 目录
   │   b. 将备份的 appendonly.aof 复制到 AOF 目录
   │   c. 启动实例（Redis 自动优先加载 AOF，AOF 包含更完整的数据）
   │   d. 验证数据完整性（DBSIZE / 指定 key 抽查）
   │   → 恢复完成，无需额外操作
   │
   └─ 备份仅包含 RDB 文件：
       a. 将备份 RDB 文件复制到 data 目录，重命名为 dump.rdb
       b. 清空 AOF 目录（避免旧 AOF 与 RDB 不一致）
       c. 临时关闭 AOF：appendonly no
       d. 启动实例，从 RDB 加载数据
       e. 验证数据完整性
       f. 重新开启 AOF：CONFIG SET appendonly yes → BGREWRITEAOF
       → 恢复完成

3. 验证步骤：
   - 检查 INFO Server: uptime > 0
   - 检查 INFO Keyspace: keyspace 与预期一致
   - 检查 INFO Persistence: rdb_last_load_status=ok / aof_enabled=1
   - 可选：对关键 key 执行 GET 验证值正确性
```

**Kvrocks 实例恢复：**

```
1. 停止目标实例
2. 清空数据目录
3. 解压 checkpoint.tar.gz 到数据目录
4. 启动实例
5. 验证数据完整性（DBSIZE / 指定 key 抽查）
```

> **为什么优先使用 AOF 恢复？**
> - AOF 记录了所有写操作，数据完整性高于 RDB（RDB 是定时快照，可能丢失最后一次快照后的写入）
> - 联合备份时已通过 `BGREWRITEAOF` 生成精简 AOF，恢复效率与 RDB 相当
> - 仅在 AOF 文件损坏或备份中无 AOF 时，才回退到 RDB 恢复

---

### 3.6 资源清单

#### 3.6.1 设计目标

运维需要快速回答：
- **哪个端口对应哪个集群？**
- **这个实例做什么用的？**
- **当前一共分配了多少端口/容器？**
- **某台服务器上跑了哪些实例？**

#### 3.6.2 清单数据源

资源清单 **不单独维护**，由 `instances-state.yaml` + `pool-state.yaml` 实时派生，确保单一数据源（Single Source of Truth）。

#### 3.6.3 清单视图

**视图 A：端口-实例映射表**（面向网络/防火墙运维）

```yaml
# 由 Skill: redis-inventory 生成，可输出为 table / yaml / json
port_inventory:
  - envoy_port: 16379
    mode: readwrite
    instance_group: order
    engine: kvrocks
    category: persistent
    role: master→replica
    purpose: "订单持久化存储"
    backend_servers: ["server-a:6379(master)", "server-b:6379(replica)"]

  - envoy_port: 16380
    mode: writeonly
    instance_group: order
    engine: kvrocks
    category: persistent
    role: master only
    purpose: "订单写入（显式写主库）"
    backend_servers: ["server-a:6379(master)"]

  - envoy_port: 16381
    mode: readwrite
    instance_group: cache-1
    engine: redis
    category: cache
    role: standalone
    purpose: "用户会话缓存"
    backend_servers: ["server-c:6379(standalone)"]
```

**视图 B：服务器-实例分布表**（面向容量规划）

```yaml
server_inventory:
  server-a:
    ip: 10.0.1.10
    instances:
      - name: order-master
        engine: kvrocks
        container_port: 6379
        memory: 4Gi
        cpus: 2
        status: running
    allocated_memory: 4Gi
    total_memory: 64Gi
    utilization: 6.25%

  server-b:
    ip: 10.0.1.11
    instances:
      - name: order-replica
        engine: kvrocks
        container_port: 6379
        memory: 4Gi
        cpus: 2
        status: running
    allocated_memory: 4Gi
    total_memory: 64Gi
    utilization: 6.25%

  server-c:
    ip: 10.0.1.12
    instances:
      - name: cache-1
        engine: redis
        container_port: 6379
        memory: 2Gi
        cpus: 1
        status: running
    allocated_memory: 2Gi
    total_memory: 64Gi
    utilization: 3.125%
```

**视图 C：全局摘要**（面向管理层/日报）

```
┌──────────────────────────────────────────────────────┐
│ 资源清单摘要                                          │
├──────────┬────────┬──────────┬──────────┬────────────┤
│ 实例组    │ Engine │ 用途      │ Envoy端口 │ 状态       │
├──────────┼────────┼──────────┼──────────┼────────────┤
│ order    │kvrocks │ 订单持久化 │ 16379/80 │ ✅ running │
│ cache-1  │redis   │ 会话缓存   │ 16381    │ ✅ running │
├──────────┴────────┴──────────┴──────────┴────────────┤
│ 总计: 2 实例组 / 3 容器 / 3 服务器                     │
│ 已分配: 10Gi 内存 / 5 CPU                             │
└──────────────────────────────────────────────────────┘
```

#### 3.6.4 清单查询方式

| 查询场景 | 方式 | 说明 |
|----------|------|------|
| "端口 16379 是什么" | `redis-inventory --port 16379` | 按端口反查实例 |
| "server-a 上有什么" | `redis-inventory --server server-a` | 按服务器查实例 |
| "所有 kvrocks 实例" | `redis-inventory --engine kvrocks` | 按引擎过滤 |
| "导出完整清单" | `redis-inventory --format yaml` | 输出完整视图 A+B |
| "每日资源报告" | `redis-inventory --summary` | 输出视图 C |

---

### 3.7 Sentinel 高可用设计

Sentinel 用于 Redis/Kvrocks 主从实例组的自动故障转移。Sentinel 只负责判定主库故障、选举从库并执行主从切换；平台仍负责状态文件同步、补齐拓扑、刷新 Envoy 和审计记录。

#### 3.7.1 部署策略

Sentinel 不要求每台 Agent 都部署。默认策略：

| healthy Agent 数 | Sentinel 数 | quorum | 处理策略 |
|------------------|-------------|--------|----------|
| < 3 | 0 | - | 不启用自动故障转移，创建主从时返回 warning |
| 3-4 | 3 | 2 | 默认部署 3 个 Sentinel |
| >= 5 | 3 | 2 | 默认部署 3 个 Sentinel；核心业务可配置为 5 个 |
| >= 5 且 sentinel.replicas=5 | 5 | 3 | 高等级业务使用，容忍更多 Sentinel 节点故障 |

Sentinel 节点选择规则：

1. 只选择 `status=healthy` 的服务器
2. 优先选择不同 `zone` 标签的服务器
3. 优先选择 `role=production`，排除 `role=standby`，除非健康节点不足
4. Sentinel 数固定为奇数（3 或 5），避免偶数投票拓扑
5. 选中节点记录到 Server 派生配置中，不单独维护第二份状态

#### 3.7.2 Server 与 Agent API

Server 对 Agent 的 Sentinel 管理 API：

```
POST /sentinel/ensure
  确保本机 Sentinel 容器存在，并用请求体中的 master 列表重写 sentinel.conf 后重载/重启

POST /sentinel/remove-master
  从本机 Sentinel 配置中移除指定实例组

GET /sentinel/status
  返回本机 Sentinel 容器状态、已监控 master 列表、最近一次配置更新时间

POST /sentinel/event
  Agent 监听到 +switch-master 后上报 Server（事件加速路径，reconcile 仍是兜底）
```

`/sentinel/ensure` 请求示例：

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

如果 Redis/Kvrocks 实例启用了 `requirepass`，必须配置 `sentinel auth-pass`。否则 Sentinel 能检测端口存活，但无法执行 INFO、REPLICAOF、故障转移等命令。

#### 3.7.4 Podman 容器规范

Sentinel 容器推荐使用 host network，避免容器 NAT/端口映射导致 Sentinel 宣告错误地址、无法发现其他 Sentinel 或无法连接从库。

```bash
podman run -d \
  --name redis-sentinel \
  --network host \
  --restart on-failure:5 \
  -v /data/redis-sentinel/conf/sentinel.conf:/etc/redis/sentinel.conf:Z \
  -v /data/redis-sentinel/data:/data:Z \
  docker.io/redis:7 \
  redis-sentinel /etc/redis/sentinel.conf
```

如果不能使用 host network，必须显式配置 `sentinel announce-ip` / `sentinel announce-port`，并确保 Redis 实例的 replica announce 信息与实际可达地址一致。

#### 3.7.5 创建与删除时机

创建主从实例组成功后，Server 执行：

```
1. 从 pool-state 选择 Sentinel 节点
2. 从 instances-state 派生所有需要监控的 master 列表
3. 调用选中 Agent 的 /sentinel/ensure
4. 如果 healthy Agent < 3，跳过 Sentinel 并在创建结果中返回 warning
```

删除实例组时，Server 执行：

```
1. 调用所有 Sentinel 节点的 /sentinel/remove-master
2. 从 instances-state 删除实例组
3. 刷新 Envoy 配置
4. 写入审计日志
```

Sentinel 配置应由 Server 统一派生并下发；Agent 不应独立决定监控哪些 master。
Sentinel 的 `group` 必须来自 `instances-state.yaml` 中显式保存的实例组名，而不是当前 master 实例名；master 发生 failover 后，monitor name 仍保持不变。

#### 3.7.6 故障转移同步机制

第一版实现以 Server 定期 reconcile 为权威兜底：

```
每 5-10 秒：
  1. Server 查询任一健康 Sentinel：
     SENTINEL get-master-addr-by-name <group>
  2. 对比 Sentinel 返回的 master 地址和 instances-state 当前 master
  3. 如果不一致，调用 handleFailover(group, newMasterAddr)
```

Agent 监听 `+switch-master` 作为加速路径：

```
Agent SUBSCRIBE +switch-master
  → 收到事件后 POST /sentinel/event 到 Server
  → Server 仍调用同一个 handleFailover(group, newMasterAddr)
```

`handleFailover` 必须是幂等的：

1. 获取实例组编排锁
2. 校验新 master 地址属于当前实例组
3. 更新 `role` / `replica_of` / `replicas` / `status`
4. 如副本数不足，选择新服务器创建 replica
5. 重新生成并落盘 Envoy 配置
6. 写 `topology.failover` 审计日志
7. 释放编排锁

#### 3.7.7 失败处理

| 场景 | 处理策略 |
|------|----------|
| healthy Sentinel 数 < quorum | 标记自动故障转移能力降级，保留现有实例运行，触发告警 |
| `/sentinel/ensure` 部分节点失败 | 成功节点数 >= quorum 则继续并告警；否则回滚本次 Sentinel 配置并返回 warning |
| Sentinel 返回的新 master 不在 instances-state | 标记 `failover_conflict`，不自动改拓扑，触发人工介入 |
| 新主库再次故障 | 不主动抢占 Sentinel，等待下一轮 Sentinel failover；编排锁超时后允许重新同步 |
| Envoy 配置落盘失败 | 状态更新不得提交为 success，审计记录 failed，并保留旧配置 |
| 补齐 replica 失败 | failover 视为部分成功，记录 `degraded`，后续 reconcile 重试补齐 |

#### 3.7.8 验证矩阵

| 场景 | 期望 |
|------|------|
| Redis 主库容器停止 | Sentinel 提升从库，Envoy 写入口切到新 master，Server 状态最终同步 |
| Kvrocks 主库容器停止 | 行为同 Redis，需验证 `ROLE` / Sentinel failover / replication 状态兼容 |
| Redis/Kvrocks 配置 requirepass | Sentinel auth-pass 生效，failover 成功 |
| Sentinel 节点少于 quorum | 不发生自动故障转移，系统发出降级告警 |
| Envoy 重启 | 从落盘配置恢复，业务端口不变化 |

---

## 4. 关键流程

### 4.1 创建单点实例

```
用户: "创建一个缓存 Redis，2G 内存，服务器 server-c"

Skill: redis-create
  │
  ├─1. pool_query()
  │   → 确认 server-c 资源充足（2Gi 内存可用）
  │
  ├─2. port_allocate()
  │   → 分配实例端口 6379（本机无冲突）
  │   → 分配 Envoy 端口 16381
  │
  ├─3. state_update(instances-state, "cache-1", status=creating)
  │
  ├─4. agent_exec(server-c, "instance/create", {
  │       name: "cache-1",
  │       category: "cache",
  │       engine: "redis",
  │       port: 6379,
  │       memory: "2Gi",
  │       persistence: { rdb: false, aof: false },
  │       maxmemory-policy: "allkeys-lru"
  │   })
  │   → Agent: 创建目录 → 生成 redis.conf → podman run
  │
  ├─5. health_check(server-c, 6379)
  │   → 验证 PONG + INFO 验证持久化配置生效
  │
  ├─6. envoy_route_update(add, cache-1, server-c:6379, port=16381)
  │
  ├─7. state_update(instances-state, "cache-1", status=running)
  │   state_update(pool-state, server-c, allocated += 2Gi)
  │
  └─8. 返回结果: "缓存 Redis 已创建，连接地址 env-proxy:16381"
```

### 4.2 创建主从实例

```
用户: "创建一个订单 Redis，4G 内存，一主一从"

Skill: redis-create
  │
  ├─1. pool_query()
  │   → 选择 server-a（剩余 36Gi，zone: az-1）→ 主库
  │   → 选择 server-b（剩余 28Gi，zone: az-2）→ 从库（不同 zone）
  │
  ├─2. port_allocate()
  │   → 主库端口 6379，Envoy 端口 16379 (读写) + 16380 (仅写)
  │   → 从库端口 6379
  │
  ├─3. state_update(instances-state, "order-master", status=creating)
  │   state_update(instances-state, "order-replica", status=creating)
  │
  ├─4. agent_exec(server-a, "instance/create", {
  │       name: "order-master",
  │       category: "persistent",
  │       engine: "kvrocks",
  │       port: 6379,
  │       memory: "4Gi",
  │       persistence: null,
  │       kvrocks_config: {
  │         compression: "lz4",
  │         write_buffer_size: "256MB",
  │         max_write_buffer_number: 4
  │       },
  │       maxmemory-policy: "noeviction"
  │   })
  │
  ├─5. health_check(server-a, 6379)
  │
  ├─6. agent_exec(server-b, "instance/create", {
  │       name: "order-replica",
  │       category: "persistent",
  │       engine: "kvrocks",
  │       port: 6379,
  │       memory: "4Gi",
  │       replica_of: "10.0.1.10:6379",
  │       persistence: null,
  │       kvrocks_config: {
  │         compression: "lz4",
  │         write_buffer_size: "256MB",
  │         max_write_buffer_number: 4
  │       },
  │       maxmemory-policy: "noeviction"
  │   })
  │
  ├─7. health_check(server-b, 6379)
  │   → 验证 INFO replication: role=slave, master_link_status=up
  │
  ├─8. envoy_route_update(add, order, master=server-a:6379,
  │       replica=server-b:6379, rw_port=16379, w_port=16380)
  │
  ├─9. state_update(instances-state, "order-master", status=running)
  │   state_update(instances-state, "order-replica", status=running)
  │   state_update(pool-state, server-a, allocated += 4Gi)
  │   state_update(pool-state, server-b, allocated += 4Gi)
  │
  └─10. 返回: "订单 Redis 主从已创建
              读写地址: env-proxy:16379
              仅写地址: env-proxy:16380"
```

### 4.3 故障转移

#### 4.3.1 手动/Agent 上报触发

```
触发: Agent 上报 server-a 实例不可用 / 用户手动触发

Skill: redis-failover
  │
  ├─1. 识别故障实例
  │   → order-master (server-a) 不可用
  │   → order-replica (server-b) 存活
  │
  ├─2. 提升从库为主库
  │   agent_exec(server-b, "instance/promote", { name: "order-replica" })
  │   → Agent: 执行 REPLICAOF NO ONE
  │
  ├─3. 选择新服务器创建从库
  │   pool_query() → server-c（资源充足，不同服务器）
  │   agent_exec(server-c, "instance/create", {
  │       name: "order-replica-new",
  │       replica_of: "10.0.1.11:6379",   // 新主库
  │       ...
  │   })
  │
  ├─4. 更新 Envoy 路由
  │   envoy_route_update(update, order,
  │       master=server-b:6379,
  │       replica=server-c:6379)
  │
  ├─5. 更新状态
  │   state_update(instances-state, order-master, status=failed, server=server-a)
  │   state_update(instances-state, order-replica, role=master, server=server-b)
  │   state_update(instances-state, order-replica-new, role=replica, server=server-c)
  │
  └─6. 通知: "故障转移完成，主库已切换到 server-b"
```

#### 4.3.2 Sentinel 自动检测（推荐）

对于核心业务实例组，建议部署 Redis Sentinel 实现秒级故障检测：

```
架构:
  按 §3.7 从 healthy Agent 中选择 3 或 5 台运行 Sentinel（需 ≥ 3 台服务器，quorum=2/3）
  一个 Sentinel 实例可监控多个主库，每个主库用不同名字区分
  Sentinel 监控所有实例组的主库

检测流程:
  Sentinel(3) → quorum(2) → 判定主库客观下线(sdown → odown)

  0. redis-create 创建主从实例时自动管理 Sentinel：
     - 检查 pool-state 中 healthy 服务器数量
     - 服务器 ≥ 3：选择 3 或 5 台 Sentinel 节点，调用 /sentinel/ensure 下发完整 sentinel.conf
     - 服务器 < 3：跳过 Sentinel，返回警告"服务器不足3台，未启用自动故障转移"
     - redis-delete 删除实例组时执行 sentinel remove {instance-group}

  1. Sentinel 检测主库不可用
     → 投票判定 odown，自动执行 failover
     → 选择优先级最高的从库提升为主库

  2. Agent 监听 Sentinel 事件（+switch-master）
     → 收到事件后执行后续编排：
       a. 在 instances-state 中标记 failover_in_progress: true（编排锁）
       b. 根据事件中的 old-master/new-master 修正 instances-state 拓扑
       c. 在新服务器创建从库补齐拓扑（如当前从库数 < 目标副本数）
       d. 更新 Envoy 路由并重新生成/落盘 Envoy 配置
       e. 写入 topology.failover 审计日志
       f. 清除 failover_in_progress 标记

     instances-state 更新要求：
       - 旧主库：
         status: failed 或 unexpected_stopped
         role: replica 或 master_failed（实现可二选一，但查询输出必须清晰标识已失效）
         replicas: []
       - 被 Sentinel 提升的新主库：
         role: master
         replica_of: null
         replicas: [所有已重新挂载到新主库的从库实例名]
         envoy: 继承原实例组的 readwrite_port / writeonly_port
       - 新补齐的从库：
         role: replica
         replica_of: <新主库实例名>
         status: running
       - 其他存活从库：
         role: replica
         replica_of: <新主库实例名>

     Envoy 更新要求：
       - 业务读写端口保持不变，避免业务侧修改连接地址
       - 后端 endpoints 必须包含新主库和所有健康从库
       - WriteOnly 写集群继续通过 TCP ROLE health check 只保留当前 master
       - 生成的 Envoy 配置必须落盘，保证 Envoy/Server 重启后拓扑不回退

     审计要求：
       - action: topology.failover
       - level: critical
       - target 记录实例组、旧主库、新主库、故障服务器、新主库服务器
       - params 记录 Sentinel 事件、补齐从库名称、Envoy 端口
       - result 记录 success / failed / conflict

  3. 编排锁保护机制：
     - 收到 +switch-master 后，Agent 先在 instances-state 中写入
       failover_in_progress: true + failover_since: <timestamp>
     - 编排锁期间，忽略同一实例组的后续 +switch-master 事件
       （防止 Sentinel failover-timeout 内重试触发重复编排）
     - 编排锁超时：60 秒（超过后自动释放，允许重新编排）
     - 编排完成后必须清除锁标记

  4. 编排期间二次故障处理：
     - 如果编排过程中新主库再次宕机，Agent 不主动干预
     - 等待 Sentinel 发起新一轮 failover（编排锁超时后）
     - 将实例状态标记为 failover_conflict，触发告警通知人工介入

  5. 通知: "Sentinel 故障转移完成，主库已切换"
```

**Sentinel 配置要点**：

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| sentinel down-after-milliseconds | 5000 | 5 秒无响应判定主观下线 |
| sentinel failover-timeout | 30000 | 故障转移超时 |
| sentinel parallel-syncs | 1 | 同时可从新主库同步的从库数 |
| sentinel resolve-hostnames | yes | 支持主机名解析 |
| min-replicas-to-write | 1 | 主库至少有 1 个从库才接受写入，防止数据丢失 |

> **为什么用 Sentinel 而非纯 Agent 检测？**
> - Agent 上报依赖 Agent 自身存活 + 网络可达，存在单点盲区
> - Sentinel 采用分布式投票（quorum），对网络分区有更强容错
> - Sentinel 内置从库选举逻辑（优先级 → 复制偏移量 → Run ID），无需 Skill 重复实现

> **编排锁为什么重要？**
> - Sentinel 的 failover-timeout（30s）内可能重试 failover，导致 Agent 收到多个 +switch-master 事件
> - 没有锁保护时，多个编排流程并发执行会造成 Envoy 路由混乱、重复创建从库、状态文件冲突
> - 编排锁确保同一实例组同一时刻只有一个故障转移编排流程在执行

### 4.4 实例迁移

```
用户: "把 order-master 从 server-a 迁移到 server-c"

Skill: redis-migrate
  │
  ├─1. 在目标服务器创建从库
  │   agent_exec(server-c, "instance/create", {
  │       name: "order-master-new",
  │       replica_of: "10.0.1.10:6379",
  │       ...
  │   })
  │
  ├─2. 等待全量同步完成
  │   health_check → INFO replication: master_sync_in_progress=0
  │
  ├─3. 提升新从库为主库
  │   agent_exec(server-c, "instance/promote", { name: "order-master-new" })
  │
  ├─4. 更新旧从库的复制目标
  │   agent_exec(server-b, "instance/replicate", {
  │       name: "order-replica",
  │       replica_of: "10.0.1.12:6379"   // 新主库
  │   })
  │
  ├─5. 停止并删除旧主库
  │   agent_exec(server-a, "instance/stop", { name: "order-master" })
  │   agent_exec(server-a, "instance/delete", { name: "order-master" })
  │
  ├─6. 更新 Envoy 路由
  │   envoy_route_update(update, order,
  │       master=server-c:6379,
  │       replica=server-b:6379)
  │
  ├─7. 更新状态
  
  │   state_update(...)
  │
  └─8. 通知: "迁移完成，主库已从 server-a 切换到 server-c"
```

### 4.5 备份管理

```
用户: "备份订单 Redis"

Skill: redis-backup
  │
  ├─1. 确定备份源
  │   → 优先从库: order-replica (server-b)
  │   → 无从库时从主库备份
  │
  ├─2. agent_exec(server-b, "instance/backup", {
  │       name: "order-replica",
  │   })
  │   → Agent: BGSAVE → 等待完成 → 复制 dump.rdb 到 backup/ 目录
  │
  ├─3. 轮转清理
  │   agent_exec(server-b, "instance/backups/cleanup", {
  │       name: "order-replica",
  │       retention: 7
  │   })
  │
  └─4. state_update(instances-state, order-replica, last_backup=now)
```

---

## 5. Skills 设计

### 5.1 Skill 清单

| Skill 名称 | 触发场景 | 核心功能 |
|-------------|----------|----------|
| `redis-create` | "创建 Redis" | 资源分配 → 创建实例 → 健康检查 → 注册代理 → 更新状态 |
| `redis-delete` | "删除 Redis" | 停止实例 → 数据清理确认 → 删除容器 → 注销代理 → 释放资源 |
| `redis-config` | "修改 Redis 配置" | 配置校验 → 热更新 or 重启 → 验证生效 |
| `redis-scale` | "加从库 / 扩容" | 加从库：创建从库 → 同步 → 注册代理；扩容：调整内存/CPU |
| `redis-migrate` | "迁移 Redis" | 新从库 → 同步 → 提升 → 更新拓扑 → 清理旧实例 |
| `redis-failover` | "故障转移" / 自动触发 | 检测故障 → 提升从库 → 创建新从库 → 更新路由 |
| `redis-backup` | "备份 / 恢复 Redis" | 备份：BGSAVE → 复制 → 轮转；恢复：停止 → 替换 RDB → 启动 |
| `redis-diagnose` | "Redis 有问题" | 采集 INFO/SLOWLOG/MEMORY → AI 分析 → 建议 |
| `redis-status` | "查看 Redis 状态" | 池状态 / 实例状态 / 主从同步状态 |
| `redis-envoy` | "代理管理" | 查看路由 / 更新配置 / 重载 Envoy |
| `redis-inventory` | "资源清单" / "端口对应什么" | 从 instances-state + pool-state 派生端口/集群/用途映射表，支持按端口/服务器/引擎查询 |
| `redis-audit` | "操作记录" / "谁做了什么" | 查询审计日志，支持按时间/实例/操作级别过滤，校验日志完整性 |
| `redis-pool` | "添加服务器" / "移除服务器" | 维护 pool-state.yaml，注册/移除/更新服务器信息，纯本地操作 |

### 5.2 Skill 与 Tool 映射

```
redis-create     → pool_query, port_allocate, agent_exec(create), health_check,
                   envoy_route_update, state_update

redis-delete     → agent_exec(stop), agent_exec(delete), envoy_route_update, state_update

redis-config     → agent_exec(config), health_check, state_update

redis-scale      → pool_query, agent_exec(create/replicate), health_check,
                   envoy_route_update, state_update

redis-migrate    → pool_query, agent_exec(create), agent_exec(promote),
                   agent_exec(replicate), agent_exec(stop/delete),
                   envoy_route_update, state_update

redis-failover   → health_check, agent_exec(promote), agent_exec(create/replicate),
                   envoy_route_update, state_update

redis-backup     → agent_exec(backup), agent_exec(restore), agent_exec(cleanup),
                   state_update

redis-diagnose   → agent_exec(status), metrics_collect, AI 分析

redis-status     → state_read, agent_exec(status), pool_query

redis-envoy      → envoy_route_update, envoy_config_dump

redis-inventory  → state_read, pool_query, audit_log_read(只读)

redis-audit      → audit_log_read, audit_log_verify

redis-pool       → pool_add, pool_remove, pool_update, pool_query  (纯本地，不调用 Agent API)
```

---

## 6. 持久化目录结构

每台服务器上的标准目录：

```
/data/redis/
  ├── order-master/
  │   ├── conf/
  │   │   └── kvrocks.conf       # engine=kvrocks 时使用此配置
  │   ├── data/
  │   │   └── (RocksDB 文件: CURRENT, MANIFEST, *.sst, WAL)
  │   └── backup/
  │       └── 2026-04-23T12:00:00/  # Checkpoint 快照目录
  ├── order-replica/
  │   ├── conf/
  │   ├── data/
  │   └── backup/
  └── cache-1/
      ├── conf/
      │   └── redis.conf         # engine=redis 时使用此配置
      ├── data/
      │   ├── dump.rdb
      │   └── appendonlydir/
      └── backup/

Agent 配置与日志：
/opt/redis-agent/
  ├── agent.yaml           # Agent 自身配置
  ├── logs/
  │   └── agent.log
  └── templates/
      ├── redis.conf.tmpl    # redis.conf 模板
      └── kvrocks.conf.tmpl  # kvrocks.conf 模板

Sentinel 数据目录：
/data/redis-sentinel/
  ├── conf/
  │   └── sentinel.conf    # Sentinel 配置（由 Agent 生成）
  └── data/                # Sentinel 工作目录（存储状态文件）
```

---

## 7. Podman 容器规范

### 7.1 容器命名

```
Redis 实例:  redis-{instance-name}     例: redis-cache-1
Kvrocks 实例: kvrocks-{instance-name}  例: kvrocks-order-master, kvrocks-order-replica
```

### 7.2 运行参数

```bash
podman run -d \
  --name redis-{instance-name} \
  --memory {memory} \
  --memory-swap {memory} \            # 禁止 swap
  --cpus {cpus} \
  --restart on-failure:5 \            # 异常自动重启，最多 5 次
  -p {port}:6379 \
  -v /data/redis/{instance-name}/conf/redis.conf:/etc/redis/redis.conf:Z \
  -v /data/redis/{instance-name}/data:/data:Z \
  -v /data/redis/{instance-name}/backup:/backup:Z \
  docker.io/redis:7 \
  redis-server /etc/redis/redis.conf
```

**Kvrocks 实例：**

```bash
podman run -d \
  --name kvrocks-{instance-name} \
  --memory {memory} \
  --memory-swap {memory} \
  --cpus {cpus} \
  --restart on-failure:5 \
  -p {port}:6666 \
  -v /data/redis/{instance-name}/conf/kvrocks.conf:/etc/kvrocks/kvrocks.conf:Z \
  -v /data/redis/{instance-name}/data:/data:Z \
  -v /data/redis/{instance-name}/backup:/backup:Z \
  docker.io/apache/kvrocks:2.9 \
  kvrocks --config /etc/kvrocks/kvrocks.conf
```

### 7.3 资源限制

| 实例规格 | Engine | memory | cpus | 适用场景 |
|----------|--------|--------|------|----------|
| small | redis | 1Gi | 1 | 缓存、轻量会话 |
| small | kvrocks | 1Gi | 1 | 轻量持久化业务 |
| medium | redis | 4Gi | 2 | 队列、会话 |
| medium | kvrocks | 4Gi | 2 | 业务数据、订单 |
| large | redis | 16Gi | 4 | 大数据量、高频读写 |
| large | kvrocks | 16Gi | 4 | 核心业务、大数据集 |
| xlarge | redis | 32Gi | 8 | 超大缓存 |
| xlarge | kvrocks | 32Gi | 8 | 核心业务、大数据集 |

---

## 8. 安全设计

### 8.1 网络隔离

```
管理网络:  Agent API (8400) 仅对 GAL 可访问
业务网络:  Redis 端口 (6379) 通过 Envoy 代理暴露
备份网络:  备份目录仅 Agent 和运维可访问
```

### 8.2 认证

- Redis 实例启用 `requirepass`
- Agent API 使用 Token 认证
- Envoy 代理可配置 Redis AUTH

### 8.3 敏感信息

- 密码在状态文件中脱敏存储
- Agent 间通信使用 HTTPS
- 备份文件可配置加密

### 8.4 审计日志

#### 8.4.1 设计目标

所有管理操作必须留痕，满足：
- **可追溯**：谁在什么时间做了什么操作，结果如何
- **可审计**：支持按时间/操作类型/实例/操作人检索
- **防篡改**：日志追加写入，不可修改或删除
- **可保留**：按策略保留，到期归档或清理

#### 8.4.2 日志格式

```json
{
  "id": "audit-20260427-0001",
  "timestamp": "2026-04-27T10:30:00+08:00",
  "operator": "gal-agent",
  "action": "instance.create",
  "target": {
    "instance_group": "order",
    "engine": "kvrocks",
    "role": "master",
    "server": "server-a",
    "port": 6379
  },
  "params": {
    "memory": "4Gi",
    "cpus": 2,
    "category": "persistent"
  },
  "result": "success",
  "duration_ms": 3200,
  "detail": "容器 order-master 创建成功，端口 6379 已就绪"
}
```

#### 8.4.3 操作类型清单

| 操作分类 | action | 说明 | 审计级别 |
|----------|--------|------|----------|
| 实例生命周期 | `instance.create` | 创建实例 | 重要 |
| | `instance.delete` | 删除实例 | **关键** |
| | `instance.start` | 启动实例 | 普通 |
| | `instance.stop` | 停止实例 | 重要 |
| | `instance.restart` | 重启实例 | 重要 |
| 拓扑变更 | `topology.replicate` | 添加从库 | 重要 |
| | `topology.failover` | 故障转移 | **关键** |
| | `topology.migrate` | 实例迁移 | **关键** |
| 配置变更 | `config.update` | 修改实例配置 | 重要 |
| | `config.override` | 运行时配置覆盖 | 重要 |
| 备份恢复 | `backup.create` | 创建备份 | 普通 |
| | `backup.restore` | 恢复备份 | **关键** |
| 服务器管理 | `server.register` | 注册服务器 | 重要 |
| | `server.drain` | 服务器排空 | **关键** |
| 资源清单 | `inventory.query` | 查询资源清单 | 普通（只读不审计） |

> **关键**级操作：删除实例、故障转移、迁移、恢复备份、服务器排空 — 这些操作影响面大，审计日志必须保留更长时间。

#### 8.4.4 日志存储

| 维度 | 规范 |
|------|------|
| **存储位置** | `{data_root}/audit/audit-YYYYMMDD.jsonl`（每日一个文件，JSONL 格式追加写入） |
| **文件权限** | `0644`，仅 Agent 进程可写，运维只读 |
| **保留策略** | 普通操作保留 90 天；关键操作保留 365 天 |
| **归档方式** | 超期日志压缩为 `.gz` 归档，归档保留 1 年后自动清理 |
| **防篡改** | 每日文件末尾追加校验行（当日所有记录的 SHA256 摘要） |

#### 8.4.5 日志校验机制

```json
{
  "id": "audit-20260427-checksum",
  "type": "daily_checksum",
  "date": "2026-04-27",
  "record_count": 42,
  "sha256": "a1b2c3d4...（当日全部记录的哈希）",
  "generated_at": "2026-04-28T00:00:01+08:00"
}
```

- 每日 00:00 自动生成校验行
- 审计检查时重新计算哈希，与校验行比对，不一致则标记篡改告警

#### 8.4.6 查询方式

| 查询场景 | 方式 | 说明 |
|----------|------|------|
| "今天谁做了什么" | `redis-audit --today` | 当日操作概览 |
| "order 集群的变更" | `redis-audit --target order` | 按实例组过滤 |
| "所有关键操作" | `redis-audit --level critical` | 按审计级别过滤 |
| "某时间段的操作" | `redis-audit --from 2026-04-25 --to 2026-04-27` | 按时间范围 |
| "校验日志完整性" | `redis-audit --verify` | 验证每日校验和 |

---

## 9. 可观测性

### 9.1 指标采集

Agent 定期采集并缓存以下指标，供 `redis-diagnose` 和 `redis-status` 使用：

| 类别 | 指标 | 来源 | 适用 Engine |
|------|------|------|-------------|
| 内存 | used_memory, used_memory_peak, mem_fragmentation_ratio | INFO memory | redis / kvrocks |
| 连接 | connected_clients, blocked_clients | INFO clients | redis / kvrocks |
| 命令 | total_commands_processed, instantaneous_ops_per_sec | INFO stats | redis / kvrocks |
| 持久化 | rdb_last_bgsave_status, aof_last_write_status | INFO persistence | redis |
| 复制 | master_link_status, master_repl_offset, slave_read_offset | INFO replication | redis / kvrocks |
| 键空间 | keyspace_hits, keyspace_misses, expires / avg_ttl | INFO stats / keyspace | redis / kvrocks |
| 慢查询 | slowlog entries | SLOWLOG GET | redis / kvrocks |
| RocksDB | rocksdb_bytes_per_read, rocksdb_num_files_at_level, rocksdb_pending_compaction_bytes | INFO rocksdb | kvrocks |
| RocksDB 压缩 | rocksdb_compaction_pending, rocksdb_l0_slowdown, rocksdb_l0_stop | INFO rocksdb | kvrocks |

### 9.2 诊断流程

```
redis-diagnose Skill
  │
  ├─1. metrics_collect(all instances in group)
  │   → 从 Agent 拉取各实例 INFO 数据
  │
  ├─2. 数据聚合与异常检测
  │   → 内存使用率 > 80% ？
  │   → 主从延迟 > 10s ？
  │   → 慢查询突增 ？
  │   → 连接数异常 ？
  │
  ├─3. AI 分析
  │   → 将聚合数据提交给 LLM
  │   → LLM 生成诊断报告和优化建议
  │
  └─4. 输出诊断报告
      → 异常项 + 根因分析 + 优化建议
```

## 10. 约束与限制

### 10.1 RESP2 协议约束

- 所有 Redis 实例强制使用 RESP2（`proto 2`），确保 Envoy `redis_proxy` 兼容
- Envoy `redis_proxy` 的 `read_policy`（REPLICA/MASTER）路由功能依赖 RESP2 协议
- RESP3 的 `HELLO 3` 命令将被拒绝，客户端不得尝试升级协议
- 若未来 Envoy 支持 RESP3，需评估后全局切换

### 10.2 MULTI/EXEC 事务限制

- Envoy 读写分离代理下，`MULTI/EXEC` 事务必须路由到主库
- 事务内包含读命令（如 `GET`）时，仍由主库执行，不经过读副本
- 应用层应避免在事务中混用读写与纯读操作，以减少主库压力

### 10.3 读写一致性

- 写操作：经 Envoy 写集群路由到主库，强一致
- 读操作：经 Envoy 读集群路由到从库，存在复制延迟（通常 < 1ms）
- 对一致性要求极高的读操作，应直连主库或使用仅写端口（§3.4.4）

### 10.4 拓扑约束

- 每个实例组至少 1 主 1 从，`min-replicas-to-write 1` 确保无孤立主库
- 从库数 < 1 时主库拒绝写入，防止脑裂
- `min-replicas-max-lag 10`：从库复制延迟超过 10 秒不计入可用从库数
- `replica-serve-stale-data no`：从库断开复制后拒绝服务，客户端需处理读失败
- Sentinel 部署需 ≥ 3 节点，quorum ≥ 2

---

## 11. 实施路线

### Phase 1：基础能力（MVP）

- [ ] Agent 开发：实例 CRUD + 健康检查
- [ ] 状态文件设计：pool-state.yaml + instances-state.yaml
- [ ] 审计日志：操作留痕（JSONL 追加写入 + 每日校验和）
- [ ] Skills：redis-create / redis-delete / redis-status / redis-audit
- [ ] 验证：单点实例的完整生命周期 + 审计日志可查

### Phase 2：主从与代理

- [ ] Agent：主从复制管理（replicate / promote）
- [ ] Envoy Redis Proxy 集成
- [ ] 资源清单：从状态文件派生端口/集群/用途映射
- [ ] Skills：redis-config / redis-scale / redis-envoy / redis-inventory
- [ ] 验证：主从实例 + 读写分离代理 + 资源清单可查

### Phase 3：运维增强

- [ ] Agent：备份与恢复
- [ ] 审计增强：关键操作长保留 + 归档压缩 + 完整性校验
- [ ] Skills：redis-backup / redis-migrate / redis-failover
- [ ] 验证：备份恢复 + 迁移 + 故障切换 + 审计校验通过

### Phase 4：智能运维

- [ ] Agent：指标采集与缓存
- [ ] Skills：redis-diagnose
- [ ] 验证：AI 辅助诊断与优化建议

---

## 附录 A：术语表

| 术语 | 定义 |
|------|------|
| GAL | 本项目的 AI Agent 平台，通过 Skills 编排运维操作 |
| Agent | 部署在每台服务器上的管理守护进程 |
| Skill | GAL 中的编排单元，组合多个 Tool 完成运维场景 |
| Tool | 原子操作，如创建实例、更新配置 |
| Pool | 服务器资源池，统一管理多台服务器的资源分配 |
| 实例组 | 一个主库及其所有从库的集合 |
| 持久化 | Redis 数据落盘（RDB + AOF） |
| 故障转移 | 主库不可用时，自动/手动提升从库为新主库 |
| Kvrocks | 基于 RocksDB 的 Redis 协议兼容存储引擎，由 Apache 孵化 |
| RocksDB | Facebook 开发的高性能嵌入式 KV 存储（LSM-Tree），Kvrocks 的底层存储引擎 |
| LSM-Tree | Log-Structured Merge-Tree，RocksDB 使用的写入优化数据结构 |
| Compaction | LSM-Tree 后台合并操作，将多层 SST 文件合并以优化读性能和回收空间 |
| Checkpoint | RocksDB 原生的物理快照机制，用于 Kvrocks 备份 |
| SST 文件 | Sorted String Table，RocksDB 中的有序数据文件 |
| Write Ahead Log (WAL) | RocksDB 的预写日志，确保宕机后数据可恢复 |

## 附录 B：Redis 配置模板

```ini
# redis.conf 模板 — 由 Agent 根据参数生成

# 基础
port {{ port }}
bind 0.0.0.0
requirepass {{ password }}
timeout 300
tcp-keepalive 60
loglevel notice
databases 16

# 内存
maxmemory {{ memory }}
maxmemory-policy {{ maxmemory_policy | default("noeviction") }}

# 持久化 - RDB
save 3600 1 300 100 60 10000
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
dir /data
stop-writes-on-bgsave-error yes

# 持久化 - AOF
appendonly {{ aof | default("yes") }}
appendfilename "appendonly.aof"
appendfsync {{ aof_policy | default("everysec") }}
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# 复制
{% if replica_of %}
replicaof {{ replica_of }}
replica-read-only yes
replica-serve-stale-data no
replica-priority 100
{% endif %}

# 防脑裂
min-replicas-to-write 1
min-replicas-max-lag 10

# 协议
proto 2                        # 强制 RESP2，兼容 Envoy redis_proxy

# 安全
rename-command FLUSHDB ""
rename-command FLUSHALL ""
rename-command DEBUG ""

# 慢查询
slowlog-log-slower-than 10000
slowlog-max-len 128

# 客户端
maxclients 10000

# 自定义覆盖
{{ config_overrides }}
```

## 附录 B2：Kvrocks 配置模板

```ini
# kvrocks.conf 模板 — 由 Agent 根据参数生成

# 基础
bind 0.0.0.0
port {{ port }}
requirepass {{ password }}
timeout 300
tcp-keepalive 60
loglevel info
databases 16

# 工作目录（RocksDB 数据存放）
dir /data

# RocksDB 调优
rocksdb.compression snappy
rocksdb.block_size 16384
rocksdb.max_open_files 4096
rocksdb.write_buffer_size {{ write_buffer_size | default("64MB") }}
rocksdb.max_write_buffer_number 4
rocksdb.target_file_size_base {{ target_file_size_base | default("64MB") }}
rocksdb.max_bytes_for_level_base {{ max_bytes_for_level_base | default("256MB") }}
rocksdb.level0_slowdown_writes_trigger 20
rocksdb.level0_stop_writes_trigger 40
rocksdb.enable_pipelined_write yes

# Compaction
rocksdb.max_sub_compactions 2
rocksdb.compaction_readahead_size 0

# 复制
{% if replica_of %}
replicaof {{ replica_of }}
replica-read-only yes
replica-priority 100
{% endif %}

# 防脑裂
min-replicas-to-write 1
min-replicas-max-lag 10

# 慢查询
slowlog-log-slower-than 10000
slowlog-max-len 128

# 客户端
maxclients 10000

# 备份（RocksDB Checkpoint）
{% if backup_enabled %}
checkpoint-dir /backup
{% endif %}

# 自定义覆盖
{{ config_overrides }}
```

## 附录 C：Envoy 写集群 ROLE 健康检查配置

写集群必须确保仅 `role=master` 的端点接收写请求。Envoy 内置 Redis health checker 只能做 PING/EXISTS 存活检查，不能判断 Redis ROLE；因此写集群使用 TCP health check 发送 Redis RESP 编码的 `ROLE` 命令，并匹配 master 响应。

```yaml
# 写集群 — 仅主库接收写请求
clusters:
- name: redis-order-write-cluster
  type: STRICT_DNS
  health_checks:
  - timeout: 1s
    interval: 5s
    unhealthy_threshold: 2
    healthy_threshold: 1
    tcp_health_check:
      send:
        text: "*1\r\n$4\r\nROLE\r\n"
      receive:
      - text: "*3\r\n$6\r\nmaster\r\n"
  drain_connections_on_host_removal: true
  load_assignment:
    cluster_name: redis-order-write-cluster
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: 10.0.1.10      # 主库
              port_value: 6379
      - endpoint:
          address:
            socket_address:
              address: 10.0.1.11      # 从库（健康检查 ROLE=slave → 标记不健康 → 不接收写请求）
              port_value: 6379
```

> 如果 Redis/Kvrocks 启用了 `requirepass`，ROLE 健康检查必须先发送 AUTH，再发送 ROLE。RESP payload 示例：
>
> ```
> *2\r\n$4\r\nAUTH\r\n$<password_length>\r\n<password>\r\n*1\r\n$4\r\nROLE\r\n
> ```
>
> 健康检查期望响应仍匹配 `*3\r\n$6\r\nmaster\r\n`。如果不带 AUTH，受密码保护的实例会返回 `NOAUTH`，写集群会把所有端点判定为不健康。

> **ROLE 健康检查原理**：
> - TCP health check 发送 `ROLE` 命令
> - master 返回 `*3\r\n$6\r\nmaster\r\n`，健康检查通过
> - replica 返回 `*5\r\n$5\r\nslave\r\n`，健康检查失败，不接收写请求
> - 当主库故障切换后，新主库 ROLE 变为 master → 健康检查通过 → 自动接收写请求
> - 旧主库变为 slave → 健康检查标记为不健康（对写集群）→ 写流量停止
> - `drain_connections_on_host_removal: true` 确保端点移除时优雅排空连接
