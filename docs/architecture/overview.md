# Redis 多实例管理系统 — 架构概述

> 版本：v0.1
> 日期：2026-04-23
> 状态：草案

本篇是架构文档的入口，覆盖设计目标、范围、整体架构图与实施路线。详细内容拆分在以下几篇：

- [组件详细设计](components.md) — Server / Agent / CLI / xDS / 存储的详细设计
- [关键流程](flows.md) — 实例创建、故障转移等关键流程 + Skills 设计
- [部署与运维](deployment-ops.md) — 持久化目录结构、Podman 规范、安全设计、可观测性、约束与限制

---

## 1. 概述

### 1.1 目标

构建一套基于 GAL 的 Redis 多实例管理平台，实现：

- **单点 / 主从实例**的创建、配置、删除、查看
- **自动备份与备份管理**（创建、恢复、轮转、清理）
- **服务器节点化**，实例按资源状态智能分配
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
│  redis-pilot-cli node-list / node-add / node-remove / node-update│
│  redis-pilot-cli instance-create / delete / start / stop         │
│  redis-pilot-cli instance-config / promote / replicate           │
│  redis-pilot-cli backup-exec / restore / cleanup                 │
│  redis-pilot-cli backup set-schedule / get-schedule              │
│  redis-pilot-cli instance metrics                                 │
│  redis-pilot-cli sentinel status / sync                          │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP API (Token + HTTPS)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Server (状态管理层)                           │
│                                                              │
│  持有 pilot.db（SQLite，servers/instance_groups/instances 等表）│
│  所有状态读写串行化，实例级操作锁 + 内存读写锁                    │
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
   │ :16380       │  ← 订单从库（只读）
   │ :16381       │  ← 用户主库
   └──────────────┘
```

---
## 11. 实施路线

### Phase 1：基础能力（MVP）

- [ ] Agent 开发：实例 CRUD + 健康检查
- [ ] 状态文件设计：pool-state.yaml + instances-state.yaml
- [ ] 审计日志：操作留痕（JSONL 追加写入）
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
- [ ] 审计增强：关键操作长保留 + 归档压缩
- [ ] Skills：redis-backup / redis-migrate / redis-failover
- [ ] 验证：备份恢复 + 迁移 + 故障切换 + 审计可查

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
| Node | 服务器节点，统一管理多台服务器的资源分配 |
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
