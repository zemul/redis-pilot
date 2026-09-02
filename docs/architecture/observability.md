# 可观测性架构设计

> 版本：v0.1  
> 日期：2026-08-13  
> 状态：P1 目标设计，尚未实现

本文定义 Redis Pilot P1 的指标、慢查询和告警数据边界。产品范围和后续阶段见
[产品优化路线图](../ROADMAP.md)。

## 1. 设计目标

- Redis Pilot H5 可以查询核心性能趋势，但不直接连接 Prometheus。
- 高频数值指标只进入 Prometheus，不写入 `pilot.db`。
- 慢查询数量可用于趋势和告警，具体慢查询明细可按条件检索。
- 控制面在 Prometheus 不可用时仍能执行实例、备份和拓扑操作。
- 严格限制 Prometheus label 基数，避免监控系统被 Key、命令参数等动态值压垮。

## 2. 数据所有权

| 数据类别 | 权威存储 | 示例 |
|----------|----------|------|
| 数值型时序指标 | Prometheus | QPS、内存、连接数、流量、复制 offset、Proxy 错误率 |
| 慢查询趋势指标 | Prometheus | 新增慢查询数、按受控命令名聚合的数量 |
| 慢查询明细 | `pilot.db` | 时间、耗时、命令、脱敏参数、客户端地址 |
| 控制面状态 | `pilot.db` | 实例、节点、实例组、期望拓扑、备份策略 |
| 事件记录 | `pilot.db` | 告警、任务、备份、配置变更、操作审计 |
| 运行日志文本 | 文件；后续可接 Loki | Redis/Kvrocks、Agent、Server、Proxy 日志 |

Prometheus 不保存完整命令、Key 名、客户端地址、Slowlog ID、Task ID 或错误详情。

## 3. 总体数据流

```text
Redis / Kvrocks -- INFO ----------> Agent collector
Redis / Kvrocks -- SLOWLOG GET ---> Agent slowlog collector
宿主机 / Podman ------------------> Agent collector

Agent  /metrics ----┐
Server /metrics ----+----> Prometheus <---- Prometheus HTTP API ---- Server query gateway ---- H5
Proxy  /metrics ----┘

Agent slowlog collector ----> Server slowlog ingest ----> pilot.db ----> Server query API ----> H5

Prometheus rules ----> Alertmanager ----> Server alert webhook ----> pilot.db ----> H5
Server task failures --------------------> Server alert service ----> pilot.db ----> H5
```

Prometheus 使用拉取模型。Agent 不主动向 Prometheus 写样本；Agent 只负责采集 Redis/Kvrocks
数据并通过 `/metrics` 暴露。

## 4. 指标采集

### 4.1 Agent 指标来源

| 类别 | 来源 | 示例 |
|------|------|------|
| 命令 | `INFO stats` | `total_commands_processed`、`instantaneous_ops_per_sec` |
| 内存 | `INFO memory` | `used_memory`、`maxmemory`、`mem_fragmentation_ratio` |
| 连接 | `INFO clients` | `connected_clients`、`blocked_clients` |
| Keyspace | `INFO stats` / `INFO keyspace` | hits、misses、expired、evicted、keys、avg_ttl |
| 网络 | `INFO stats` | input/output bytes |
| 复制 | `INFO replication` | role、link status、offset、last I/O |
| 持久化 | `INFO persistence` | RDB/AOF 状态和大小 |
| Kvrocks | `INFO rocksdb` 等 | pending compaction、SST/L0 状态、cache |
| 宿主机 | 系统接口 | CPU、内存、磁盘 |
| 容器 | Podman/cgroup | 容器状态和资源使用 |

累计值以 Prometheus Counter 暴露，当前值以 Gauge 暴露。QPS、流量速率、命中率等由
PromQL 使用 `rate()` 或区间增量计算，不能将累计值直接展示为速率。

### 4.2 Server 与 Proxy 指标

Server 至少暴露 Agent 在线状态、API 请求、调度、reconcile、备份任务和快照生成状态。
Proxy 至少暴露连接数、请求数、错误、路由失败、后端状态和快照版本。

延迟分位（P95/P99）由 Proxy 的 Histogram `redis_pilot_proxy_command_duration_seconds{group,kind}` 提供，
`kind` 取 read/write/other（由命令路由分类映射）。它观测的是一次后端 round-trip 耗时，
pipeline 批记为单次观测，因此是往返延迟而非严格的单命令延迟；PromQL 侧用
`histogram_quantile()` 计算分位。命令级流量（ops by command）由 Counter
`redis_pilot_proxy_command_total{group,cmd}` 提供，`cmd` 经命令白名单归一化（未知命令归入 `OTHER`）；
仅统计转发的业务命令，本地终结（AUTH/PING/CLIENT/INFO/CONFIG…）与被拒命令不计入。
命令级慢查询由 Counter `redis_pilot_proxy_slow_command_total{group,cmd}`
承接：仅当单次 round-trip 超过慢命令阈值（当前 10ms）时按命令 +1，`cmd` 经命令白名单
归一化（未知命令归入 `OTHER`）以约束基数。路由回落 master 由
`redis_pilot_proxy_read_fallback_master_total{group}` 记录，仅统计"无可用 replica 被迫回落"的情形，
master 因读权重被正常加权命中不计入。

发现链路（Proxy → Server 长轮询）健康由三项指标暴露：`redis_pilot_proxy_snapshot_fetch_total{result}`
（拉取成功/失败计数，failure 上升即链路劣化）、`redis_pilot_proxy_server_disconnect_total`（失联次数，
scalar 便于告警）、`redis_pilot_proxy_snapshot_applied_timestamp_seconds{version}`（当前已应用快照的
应用时刻，`time() - 该值` 即快照陈旧度；数值冻结说明链路卡死）。进程级兜底指标由
prometheus 的 Go/Process collector 提供（`go_goroutines`、`go_memstats_*`、`go_gc_duration_seconds`、
`process_resident_memory_bytes`、`process_open_fds` 等），因 Proxy 使用私有 registry，这些
collector 在初始化时显式注册。

> 注：`redis_pilot_proxy_backend_health` 已移除。Proxy 不主动健康探测后端，快照也不含每后端健康度，
> 该 gauge 无可靠数据源、长期为空会误导，故删除。后端健康信号改用
> `redis_pilot_proxy_backend_down_total`（down 事件）与 `redis_pilot_proxy_error_total`（转发失败）表达；若未来需要
> 每后端实时健康枚举，须单独引入 Proxy 主动健康探测能力。

### 4.3 Label 约束

允许使用数量受控的标签：

- `instance`
- `group`
- `node`
- `engine`
- `engine_version`
- 有限枚举的 `command` 或 `command_group`

禁止使用高基数或敏感标签：

- Key 名或命令参数
- 客户端 IP
- Slowlog ID、Task ID、请求 ID
- 完整错误文本
- 任意用户输入

## 5. 指标查询网关

H5 只访问 Server。Server 负责鉴权、业务指标到 PromQL 的映射、查询限制和响应归一化。

建议的内部抽象：

```go
type MetricsQuerier interface {
    QueryInstant(ctx context.Context, metric MetricQuery, at time.Time) (InstantResult, error)
    QueryRange(ctx context.Context, metric MetricQuery, from, to time.Time, step time.Duration) (RangeResult, error)
}
```

P1 只实现 Prometheus 查询后端，不实现 SQLite 指标后端。API 形态示例：

```http
GET /observability/metrics/query?group=order&metrics=qps,memory_usage&from=...&to=...&step=60
GET /observability/metrics/latest?group=order&metrics=connections,replication_lag
```

约束：

- 前端不能传任意 PromQL。
- Server 使用白名单指标目录生成 PromQL。
- 限制最大时间范围、最小 step、单次序列数和响应点数。
- Prometheus 超时或不可用时返回明确的指标源错误，不影响控制面 API。

## 6. 慢查询

### 6.1 采集与去重

Agent 每 10～30 秒执行受限的 `SLOWLOG GET`，读取比本地游标新的记录并上报 Server。
Redis 重启后 Slowlog ID 可能重新开始，因此唯一标识不能只依赖实例名和 Slowlog ID。

建议唯一键：

```text
(instance_id, instance_run_id, slowlog_id)
```

`instance_run_id` 优先取 Redis `run_id`；Kvrocks 若不提供等价字段，则使用能够区分进程启动
代际的稳定标识。Server 必须幂等写入，重复上报不得产生重复记录。

### 6.2 存储边界

`pilot.db` 保存慢查询明细，建议字段：

```text
slow_logs
  instance_id
  instance_run_id
  slowlog_id
  occurred_at
  duration_us
  command
  args_redacted
  client_addr
  client_name
  collected_at
```

进入数据库前完成参数脱敏。日志输出、审计详情和 Prometheus label 中不得出现原始参数。

Agent 同时暴露单调递增的慢查询 Counter，供 Prometheus 绘制趋势和触发告警。命令名标签
必须经过白名单或归一化，未知命令统一归入 `other`。

### 6.3 查询

```http
GET /observability/slowlogs/summary?group=order&from=...&to=...
GET /observability/slowlogs/query?group=order&instance=order-0&command=HGETALL&from=...&to=...
```

`summary` 可组合 Prometheus 趋势与数据库聚合；`query` 返回分页后的数据库明细。页面必须
说明 Redis 慢日志主要记录数据节点命令执行时间，不代表客户端端到端延迟。

## 7. 告警

指标告警由 Prometheus rules 计算，并通过 Alertmanager webhook 送入 Server；备份失败、
恢复失败、故障转移冲突等平台任务异常由 Server 直接产生。两者统一通过 IncidentService 写入
`health_incidents`（早期规划中的 `alert_events` 由该异常生命周期模型取代），供 H5 展示
产生、持续、确认和恢复状态。具体检测规则、防抖、关键时间线、表结构、API 和页面设计见
[异常记录功能设计](health-incidents.md)。

异常记录与操作审计保持清晰边界：实例不可达、复制偏移等“事实被发现”写入异常记录；
Sentinel 故障转移、reconcile 重新下发复制配置等“平台采取了状态变更动作”写入 `audit_log`。
同一处置链路通过 `correlation_id` 关联，周期性健康检查正常结果不进入审计，避免重复噪声。

P1 可先交付平台内置规则，不提供任意 PromQL 编辑器。首批规则包括：

- 实例不可达
- 主从链路断开或复制积压持续超阈值
- 内存、连接数持续超阈值
- 慢查询数量突增
- 宿主机磁盘不足
- 备份或关键任务失败

## 8. 保留策略

| 数据 | 默认保留时间 | 说明 |
|------|--------------|------|
| Prometheus 指标 | 30 天 | 默认抓取周期 30 秒；长期存储后续接 Thanos/Mimir/VictoriaMetrics |
| 慢查询明细 | 30 天 | 可配置 7～90 天，按时间分批清理 |
| 告警事件 | 180 天 | 聚合统计可保留更久 |
| 普通任务/审计 | 90 天 | 与现有审计策略一致 |
| 关键任务/审计 | 365 天 | 故障转移、恢复、删除等高风险操作 |
| 运行日志 | 7～30 天 | 根据组件和日志级别配置，不进入 SQLite |

## 9. 配置与部署

P1 将 Prometheus 设为指标页面的运行依赖。Server 配置至少需要：

```yaml
observability:
  prometheus:
    endpoint: http://127.0.0.1:9090
    query_timeout: 5s
  slowlog:
    collect_interval: 30s
    retention: 720h
```

部署包需要提供 Prometheus scrape 示例，覆盖 Server、所有 Agent 和所有 Proxy。认证、TLS、
服务发现和生产容量估算在实现前补充到 `deployment-ops.md`。

## 10. 降级与健康检查

- Prometheus 不可用：性能趋势和指标型告警标记为不可用；实例管理、备份和拓扑操作继续工作。
- 单个 Agent 指标过期：H5 显示最后采集时间并标记过期，不沿用旧值判断当前健康。
- 慢日志采集失败：保留游标并记录采集错误；恢复后继续增量采集，但 Redis 环形缓冲区已覆盖的
  记录无法恢复，应产生可观测的丢失风险事件。
- `pilot.db` 写入失败：慢日志和事件写入必须显式失败并计入 Server 自身指标，不能静默丢弃。
