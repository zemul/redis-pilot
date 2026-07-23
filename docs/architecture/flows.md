# Redis 多实例管理系统 — 关键流程与 Skills 设计

> 上一篇：[组件详细设计](components.md) · 下一篇：[部署与运维](deployment-ops.md)

---

## 4. 关键流程

### 4.1 创建单点实例

```
用户: "创建一个缓存 Redis，2G 内存，服务器 server-c"

Skill: redis-create
  │
  ├─1. node_list()
  │   → 确认 server-c 资源充足（2Gi 内存可用）
  │
  ├─2. port_allocate()
  │   → 分配实例端口 6379（本机无冲突）
  │   → 分配 Envoy 端口 16381
  │
  ├─3. state_update(instances, "cache-1", status=creating)
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
  ├─6. state_update(instances, "cache-1", status=running, envoy.auto_port=16381)
  │   → xDS 自动检测到 snapshot 变化并推送新配置到 Envoy
  │
  ├─7. state_update(instances, "cache-1", status=running)
  │   state_update(servers, server-c, allocated += 2Gi)
  │
  └─8. 返回结果: "缓存 Redis 已创建，连接地址 env-proxy:16381"
```

### 4.2 创建主从实例

```
用户: "创建一个订单 Redis，4G 内存，一主一从"

Skill: redis-create
  │
  ├─1. node_list()
  │   → 选择 server-a（剩余 36Gi，zone: az-1）→ 主库
  │   → 选择 server-b（剩余 28Gi，zone: az-2）→ 从库（不同 zone）
  │
  ├─2. port_allocate()
  │   → 主库端口 6379，Envoy 端口 16379 (读写) + 16380 (只读)
  │   → 从库端口 6379
  │
  ├─3. state_update(instances, "order-master", status=creating)
  │   state_update(instances, "order-replica", status=creating)
  │
  ├─4. agent_exec(server-a, "instance/create", {
  │       name: "order-master",
  │       category: "persistent",
  │       engine: "kvrocks",
  │       port: 6379,
  │       memory: "4Gi",
  │       persistence: null,
  │       config_overrides: {
  │         "rocksdb.compression": "lz4",
  │         "rocksdb.write_buffer_size": "256",
  │         "rocksdb.max_write_buffer_number": "4",
  │         "maxmemory-policy": "noeviction"
  │       }
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
  │       config_overrides: {
  │         "rocksdb.compression": "lz4",
  │         "rocksdb.write_buffer_size": "256",
  │         "rocksdb.max_write_buffer_number": "4",
  │         "maxmemory-policy": "noeviction"
  │       }
  │   })
  │
  ├─7. health_check(server-b, 6379)
  │   → 验证 INFO replication: role=slave, master_link_status=up
  │
  ├─8. state_update(instances, envoy.auto_port=16379, envoy.master_port=16380)
  │   → xDS 自动检测到 snapshot 变化并推送新配置到 Envoy
  │
  ├─9. state_update(instances, "order-master", status=running)
  │   state_update(instances, "order-replica", status=running)
  │   state_update(servers, server-a, allocated += 4Gi)
  │   state_update(servers, server-b, allocated += 4Gi)
  │
  └─10. 返回: "订单 Redis 主从已创建
              读写地址: env-proxy:16379
              只读地址: env-proxy:16380"
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
  ├─3. 更新状态（xDS 自动推送新路由到 Envoy）
  │   state_update(instances, order, current_master=order-replica,
  │       master=server-b:6379, replicas=healthy_replicas)
  │
  ├─4. 更新状态
  │   state_update(instances, order, current_master=order-replica)
  │   state_update(instances, order-master, status=failed, server=server-a)
  │   state_update(instances, order-replica, role=master, server=server-b)
  │
  └─5. 通知: "故障转移完成，主库已切换到 server-b"
```

#### 4.3.2 Sentinel 自动检测（推荐）

对于核心业务实例组，建议部署 Redis Sentinel 实现秒级故障检测：

```
架构:
  按 §3.7 提前规划并声明 3 或 5 台 Sentinel 节点（quorum=2/3）
  一个 Sentinel 实例可监控多个主库，每个主库用不同名字区分
  Sentinel 监控所有实例组的主库

检测流程:
  Sentinel(3) → quorum(2) → 判定主库客观下线(sdown → odown)

  0. redis-create 创建主从实例时同步 Sentinel 监控配置：
     - 检查 server.yaml 中是否声明 sentinel.nodes
     - 已声明 3 或 5 台 Sentinel 节点：Server 直连 26379 下发 SENTINEL MONITOR/SET
     - 未声明或数量不是 3/5：跳过 Sentinel，返回警告"未正确配置 Sentinel 节点，未启用自动故障转移"
     - redis-delete 删除实例组时执行 sentinel remove {instance-group}

  1. Sentinel 检测主库不可用
     → 投票判定 odown，自动执行 failover
     → 选择优先级最高的从库提升为主库

  2. Server Sentinel watcher 监听 Sentinel 事件（+switch-master）
     → Server 启动时为 sentinel.nodes 中每个 Sentinel 节点启动 1 个 watcher
     → 收到事件后只把它作为唤醒信号，不直接信任事件里的 new-master
     → Server 通过 SENTINEL get-master-addr-by-name <group> 再次确认当前 master
     → 确认后执行后续编排：
       a. 获取同组实例编排锁
       b. 使用确认后的 master 地址修正 `instances`/`instance_groups` 表拓扑
       c. 更新 Envoy 路由并重新生成/落盘 Envoy 配置
       d. 写入 topology.failover 审计日志
       e. 释放编排锁

     `instances` 表更新要求：
       - 实例组：
         current_master: <新主库实例名>
         topology_status: healthy 或 degraded
         envoy: 保持原实例组 master_port / auto_port 不变
       - 旧主库：
         status: failed 或 unexpected_stopped
         role: replica
         replica_of: <新主库实例名>
       - 被 Sentinel 提升的新主库：
         role: master
         replica_of: null
       - 其他存活从库：
         role: replica
         replica_of: <新主库实例名>

     Envoy 更新要求：
       - 业务读写端口保持不变，避免业务侧修改连接地址
       - 后端 endpoints 必须包含新主库和所有健康从库
       - RO 只读集群后端更新为当前所有健康从库
       - 生成的 Envoy 配置必须落盘，保证 Envoy/Server 重启后拓扑不回退

     审计要求：
       - action: topology.failover
       - level: critical
       - target 记录实例组、旧主库、新主库、故障服务器、新主库服务器
       - params 记录 Sentinel 事件和 Envoy 端口
       - result 记录 success / failed / conflict

  3. 编排锁与幂等保护机制：
     - 收到重复 +switch-master 或 reconcile 同时触发时，若当前 master 已同步则 no-op
     - 编排锁期间，同一实例组的后续 +switch-master 事件由锁保护或下一轮 reconcile 兜底
       （防止 Sentinel failover-timeout 内重试触发重复编排）
     - 编排锁超时：60 秒（超过后自动释放，允许重新编排）
     - 编排完成后必须释放锁

  4. 编排期间二次故障处理：
     - 如果编排过程中新主库再次宕机，Server 不主动抢占 Sentinel
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
> - Sentinel 的 failover-timeout（30s）内可能重试 failover，导致 Server watcher 收到多个 +switch-master 事件
> - 没有锁保护时，多个编排流程并发执行会造成 Envoy 路由混乱、状态文件冲突和重复告警
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
  ├─6. 更新状态（xDS 自动推送新路由到 Envoy）
  │   state_update(instances, order,
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
  └─4. state_update(instances, order-replica, last_backup=now)
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
| `redis-inventory` | "资源清单" / "端口对应什么" | 从 `instances`/`instance_groups` 表 + `servers` 表派生端口/集群/用途映射表，支持按端口/服务器/引擎查询 |
| `redis-audit` | "操作记录" / "谁做了什么" | 查询审计日志，支持按时间/实例/操作级别过滤 |
| `redis-node` | "添加服务器" / "移除服务器" | 维护 `servers` 表，注册/移除/更新服务器信息，纯本地操作 |

### 5.2 Skill 与 Tool 映射

```
redis-create     → node_list, port_allocate, agent_exec(create), health_check,
                   state_update (xDS 自动推送)

redis-delete     → agent_exec(stop), agent_exec(delete), state_update (xDS 自动推送)

redis-config     → agent_exec(config), health_check, state_update

redis-scale      → node_list, agent_exec(create/replicate), health_check,
                   state_update (xDS 自动推送)

redis-migrate    → node_list, agent_exec(create), agent_exec(promote),
                   agent_exec(replicate), agent_exec(stop/delete),
                   state_update (xDS 自动推送)

redis-failover   → health_check, agent_exec(promote), agent_exec(create/replicate),
                   state_update (xDS 自动推送)

redis-backup     → agent_exec(backup), agent_exec(restore), agent_exec(cleanup),
                   state_update

redis-diagnose   → agent_exec(status), metrics_collect, AI 分析

redis-status     → state_read, agent_exec(status), node_list

redis-inventory  → state_read, node_list, audit_log_read(只读)

redis-audit      → audit_log_read

redis-node       → node_add, node_remove, node_update, node_list  (纯本地，不调用 Agent API)
```

---

