# Redis 多实例管理系统 — 部署与运维

> 上一篇：[关键流程](flows.md) · 返回：[架构概述](overview.md)

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
  ├── agent.yaml           # Agent 自身配置（含 announce_ip: 本机宿主 IP，用于 replica-announce）
  ├── logs/
  │   └── agent.log
  └── templates/
      ├── redis.conf.tmpl    # redis.conf 模板
      └── kvrocks.conf.tmpl  # kvrocks.conf 模板

Sentinel 数据目录：
/data/redis-sentinel/
  ├── conf/
  │   └── sentinel.conf    # Sentinel 配置（由 Server 下发 MONITOR/SET 后由 Sentinel rewrite 持久化）
  └── data/                # Sentinel 工作目录（存储状态文件）
```

---

## 7. Podman 容器规范

### 7.1 容器命名

```
Redis 实例:  redis-{instance-name}     例: redis-cache-1
Kvrocks 实例: kvrocks-{instance-name}  例: kvrocks-order-1, kvrocks-order-2
```

### 7.1.1 实例命名规范

实例名一旦创建不可更改，但主从角色会因 failover 而切换，实例也可能迁移到不同服务器。**不要在实例名中编码角色或节点信息**（如 `master`、`replica`、`node1`、`redis01`），否则 failover 或迁移后名字与实际状态倒置，容易误导操作。

**推荐命名方式**（纯序号，不绑定角色和节点）：

| 场景 | 推荐 | 避免 |
|------|------|------|
| 数字序号 | `order-1` / `order-2` | `order-master` / `order-replica` |
| 字母序号 | `order-a` / `order-b` | `order-primary` / `order-standby` |
| 避免节点绑定 | `cache-1` / `cache-2` | `cache-node1` / `cache-redis01` |

当前主库信息由 group 状态的 `current_master` 字段维护，实例名无需重复表达角色或位置。

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
  <Server 下发的 engine_image> \
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
  -p {port}:6379 \
  -v /data/redis/{instance-name}/conf/kvrocks.conf:/etc/kvrocks/kvrocks.conf:Z \
  -v /data/redis/{instance-name}/data:/data:Z \
  -v /data/redis/{instance-name}/backup:/backup:Z \
  --entrypoint kvrocks \
  <Server 下发的 engine_image> \
  -c /etc/kvrocks/kvrocks.conf
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

> `cpus` 支持小数核（如 `0.5`、`1.5`），直接透传给 `podman --cpus`，用于精细化资源限制；上表为推荐档位。

---

## 8. 安全设计

### 8.1 网络隔离

```
管理网络:  Agent API (8400) 仅对 GAL 可访问
业务网络:  Redis 端口 (6379) 通过 Proxy 代理暴露
备份网络:  备份目录仅 Agent 和运维可访问
```

### 8.2 认证

- 后端实例无密码（`requirepass` 不写），鉴权只在客户端 -> Proxy 前端由 Proxy 校验
- 依赖主机层网络隔离作为后端安全边界
- Server API：用户体系（JWT 登录 / 页面签发 rpt_ API Token）；内部组件走静态 token（Proxy 的 `internal_token`/`server_token`）
- Agent API 使用 per-node Token 认证（`agent_token`）
- Proxy 前端可配置 AUTH 密码（快照 `ProxyListener.Password`）

### 8.3 敏感信息

- 前端密码在状态文件中脱敏存储
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

> **关键**级操作：删除实例、故障转移、迁移、恢复备份、服务器排空 — 这些操作影响面大，审计日志必须保留更长时间。

#### 8.4.4 日志存储

| 维度 | 规范 |
|------|------|
| **存储位置** | `pilot.db` 的 `audit_log` 表（SQLite，随实例/节点状态一起持久化，不再是独立 JSONL 文件） |
| **文件权限** | 随 `pilot.db` 继承 `0600`（仅属主可读写），数据目录 `0700` |
| **保留策略** | 普通操作保留 90 天；关键操作保留 365 天 |
| **归档方式** | 超期日志压缩为 `.gz` 归档，归档保留 1 年后自动清理 |
#### 8.4.5 查询方式

| 查询场景 | 方式 | 说明 |
|----------|------|------|
| "今天谁做了什么" | `redis-audit --today` | 当日操作概览 |
| "order 集群的变更" | `redis-audit --target order` | 按实例组过滤 |
| "所有关键操作" | `redis-audit --level critical` | 按审计级别过滤 |
| "某时间段的操作" | `redis-audit --from 2026-04-25 --to 2026-04-27` | 按时间范围 |

---

## 9. 可观测性

> 本节描述 P1 目标架构，当前实现仍以 Agent 缓存即时 INFO、Server 透传查询为主。详细设计见
> [可观测性架构](observability.md)。

### 9.1 数据存储边界

| 数据 | 存储位置 | 说明 |
|------|----------|------|
| 数值型时序指标 | Prometheus | Agent、Server、Proxy 通过 `/metrics` 暴露，由 Prometheus 拉取 |
| 慢查询数量趋势 | Prometheus | Counter 形式，不包含命令参数、Key、客户端地址等高基数信息 |
| 慢查询明细 | `pilot.db` | 保存时间、耗时、命令、脱敏参数和客户端信息 |
| 告警、任务、审计 | `pilot.db` | 保存具体事件和状态流转 |
| 运行日志 | 本地文件；后续可接 Loki | 不写入 SQLite |

`pilot.db` 不保存高频性能采样点。H5 通过 Server 查询网关访问 Prometheus，不直接连接
Prometheus，也不接受前端传入任意 PromQL。

### 9.2 指标采集

Agent 定期采集并通过 `/metrics` 暴露以下指标；默认 Prometheus 抓取周期为 30 秒：

| 类别 | 指标 | 来源 | 适用 Engine |
|------|------|------|-------------|
| 内存 | used_memory, used_memory_peak, mem_fragmentation_ratio | INFO memory | redis / kvrocks |
| 连接 | connected_clients, blocked_clients | INFO clients | redis / kvrocks |
| 命令 | total_commands_processed, instantaneous_ops_per_sec | INFO stats | redis / kvrocks |
| 持久化 | rdb_last_bgsave_status, aof_last_write_status | INFO persistence | redis |
| 复制 | master_link_status, master_repl_offset, slave_read_offset | INFO replication | redis / kvrocks |
| 键空间 | keyspace_hits, keyspace_misses, expires / avg_ttl | INFO stats / keyspace | redis / kvrocks |
| 慢查询趋势 | slowlog entries total | SLOWLOG GET 后转为受控 Counter | redis / kvrocks |
| RocksDB | rocksdb_bytes_per_read, rocksdb_num_files_at_level, rocksdb_pending_compaction_bytes | INFO rocksdb | kvrocks |
| RocksDB 压缩 | rocksdb_compaction_pending, rocksdb_l0_slowdown, rocksdb_l0_stop | INFO rocksdb | kvrocks |

累计计数器由 PromQL 使用 `rate()` 或区间增量计算。Key、客户端 IP、完整命令参数、Slowlog ID
和 Task ID 禁止作为 Prometheus label。

### 9.3 保留策略

| 数据 | 默认保留时间 |
|------|--------------|
| Prometheus 指标 | 30 天 |
| 慢查询明细 | 30 天，可配置 7～90 天 |
| 告警事件 | 180 天 |
| 普通任务和审计 | 90 天 |
| 关键任务和审计 | 365 天 |
| 运行日志 | 7～30 天 |

Prometheus 不可用时，性能趋势和指标告警标记为数据源不可用；实例、备份和拓扑等控制面操作
继续工作，不回退到 SQLite 保存指标。

### 9.4 诊断流程

```
redis-diagnose Skill
  │
  ├─1. metrics_query(all instances in group)
  │   → Server 查询 Prometheus 历史与即时指标
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

### 10.1 Proxy 协议约束

- Proxy 当前按 RESP2 工作，业务客户端经 Proxy 访问时不得尝试 `HELLO 3` 升级协议
- Redis/Kvrocks 实例配置不强制写入 `proto 2`；协议约束发生在业务客户端与 Proxy 之间
- 若未来 Proxy 支持 RESP3，需评估后全局切换

### 10.2 MULTI/EXEC 事务限制

- Proxy 读写分离代理下，`MULTI/EXEC` 事务必须路由到主库
- 事务内包含读命令（如 `GET`）时，仍由主库执行，不经过读副本
- 应用层应避免在事务中混用读写与纯读操作，以减少主库压力

### 10.3 读写一致性

- 写操作：经 Proxy 写集群路由到主库，强一致
- 读操作：经 Proxy 读集群路由到从库，存在复制延迟（通常 < 1ms）
- 对一致性要求极高的读操作，应使用 MASTER 端口直连主库（§3.4.4）

### 10.4 拓扑约束

- 每个实例组至少 1 主 1 从，`min-replicas-to-write 1` 确保无孤立主库
- 从库数 < 1 时主库拒绝写入，防止脑裂
- `min-replicas-max-lag 10`：从库复制延迟超过 10 秒不计入可用从库数
- `replica-serve-stale-data no`：从库断开复制后拒绝服务，客户端需处理读失败
- Sentinel 部署需 ≥ 3 节点，quorum ≥ 2

---
