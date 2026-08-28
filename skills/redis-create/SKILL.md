---
name: redis-create
description: 创建 Redis/Kvrocks 实例，支持单点和主从
---

# redis-create

创建 Redis 或 Kvrocks 实例。

## 流程

1. 创建实例（Server 自动调度节点）
   ```
   redis-pilot-cli instance create <name> \
     --engine <redis|kvrocks> \
     --category <cache|persistent> \
     --memory <memory> \
     --group <group> \
     --password <password>
   ```

2. 如果需要主从，创建从库。`--replica-of` 指向主库实例名，`--group` 可省略并由 Server 继承主库实例组。
   ```
   redis-pilot-cli instance create <replica-name> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <master-name>
   ```

3. 检查实例组状态
   ```
   redis-pilot-cli instance list --group <group>
   ```

4. 确认 Proxy 入口端口。实例创建成功后，Server 会分配并写入 `proxy.master_port` / `proxy.auto_port`；Proxy 通过长轮询 Server snapshot 自动发现拓扑变更。
   ```
   redis-pilot-cli instance status <name>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |
| engine | 否 | redis 或 kvrocks，默认 redis |
| engine-version | 否 | Redis 5 / 6.2 / 7 或 Kvrocks 2.15.0 |
| category | 否 | cache 或 persistent，默认 cache |
| memory | 否 | 内存限制，默认 1Gi |
| disk | 否 | 磁盘规划；Kvrocks 未指定时默认 20Gi，Redis 默认 10Gi |
| cpus | 否 | CPU 核数，默认 1 |
| group | 主库/单点必填 | 实例组名（主从共用） |
| replica-of | 从库必填 | 主库实例名或地址 |
| password | 否 | 访问密码 |
| type | 否 | standalone 或 replication，默认 standalone。声明实例组的拓扑规划：standalone 为单点，replication 表示主从架构（会预分配 proxy auto_port 用于读写分离）。添加从库时 Server 会自动将 standalone 升级为 replication |
| access-mode | 否 | 代理访问模式：standard 或 rw-split。默认行为：replication 类型默认 rw-split，standalone 类型默认 standard |
| config | 否 | 配置覆盖，格式 `k=v,k=v` |
| node-selector | 否 | 标签亲和调度，格式 `k=v,k=v`（如 `disk=ssd,zone=az-1`），只调度到标签全部匹配的节点 |

## 容量建议

按引擎特性选择规格，避免盲目堆资源或让字段取 0（0 会导致调度不计入容量账，存在超卖风险）。

### 内存（memory）

- **决定数据集上限的核心维度**，无论 Redis 还是 Kvrocks 都应按实际数据量规划。
- Redis 为纯内存，数据集受 `maxmemory` 限制，内存即容量上限；建议在预估数据量基础上留 20%-30% 余量给碎片和缓冲。
- Kvrocks 内存主要作为缓存/写缓冲，数据主要落盘，内存可远小于总数据量。

### CPU（cpus）

- **Redis：默认 1 核即可，一般无需调高。** Redis 命令处理是单线程的，多给核对常规负载几乎没有收益；只有 BGSAVE/AOF rewrite 的 fork、或显式开启 I/O 多线程时会用到额外 1-2 核。这与云厂商标准版 Redis「按内存卖、不让选 CPU」的做法一致。
- **Kvrocks：按需给 2-4 核。** Kvrocks 是多线程存储引擎（RocksDB compaction/flush 吃 CPU），写密集或大数据量场景多核有实际收益。

### 磁盘（disk）

- 磁盘用于调度容量核算（`Kvrocks 20Gi / Redis 10Gi` 默认兜底），**单位必须用 `Gi`**（如 `50Gi`）；不写 `GB`/`TB` 等单位。
- **Redis：磁盘仅用于持久化（RDB/AOF）和备份，默认值通常够用。** 需求约为内存的若干倍——persistent 类（RDB+AOF）比 cache 类（仅 RDB）占用更多，数据量大时可适当调高。
- **Kvrocks：磁盘是核心容量维度，务必按数据规模设置。** 数据落盘可远超内存，预计存 100G 数据就给 `100Gi` 以上。

> 通过 AI 助手对话创建（`create_instance` 工具）时默认值与本表一致，AI 会按上述引擎特性建议容量；CLI 与 AI 两条创建路径行为对齐。

### AI 追问策略

用户请求创建实例但信息不全时，**按引擎类型差异化追问，只问关键维度，其余用默认值**，不要把所有参数一股脑列表让用户确认：

1. **引擎未指定** → 先问「redis（纯内存缓存/持久化）还是 kvrocks（磁盘型存储，适合大数据量）？」
2. **确认 redis 后** → 重点问**内存**：「你的数据量大约多大？内存是 Redis 容量的硬上限。」CPU/磁盘用默认值即可，不必追问。
3. **确认 kvrocks 后** → 重点问**磁盘和 CPU**：「预计存多少数据（决定磁盘）？写入是否密集（决定 CPU 核数）？」内存可用默认值（kvrocks 内存只是缓存层）。
4. **用途（category）未指定** → 问「是缓存（可丢，可用淘汰策略）还是持久化（不可丢，需备份）？」

如果用户明确给出了规格（如「2Gi redis 缓存」），不要再反复确认已有信息——直接用给定值，缺什么补默认值，提议创建即可。追问的目标是**收集真正影响架构决策的信息**，不是走表格。

## 注意事项

- **实例命名规范：`<group>-<序号>`**，如 `galaxy-1`、`galaxy-2`、`order-1`、`order-2`。不要包含角色（master/replica）或节点（redis01）信息，因为角色会因 failover 切换，实例也可能迁移到不同服务器。当前主库由 group 的 `current_master` 字段维护。
- 创建主从时，先创建主库再创建从库
- group 名在 Sentinel/Proxy/审计中统一使用
- Redis 实例端口和 Proxy 端口均由 Server 统一分配，不从 CLI 传 `--port`
- Proxy 通过 `/api/v1/proxy/snapshot` HTTP 长轮询自动发现拓扑变更，不需要执行手动路由更新
