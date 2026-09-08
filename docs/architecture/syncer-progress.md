# G6a-13：同步进度采样、上报与异常分类

## 本轮范围

在 [固定登记与上报](syncer-control-plane.md) 上增加可选进度采样：从同步内核读取一致的本地缓存快照，再独立读取源 Redis 的复制位置；通过现有心跳上报，由同步器和 Server 使用同一份分类逻辑生成观测视图。

这仍是隔离模式的**观测能力**，不是业务读写安全证明。本轮不启用回温、淘汰、限写、自动任务分配或故障切换，不修改 Redis/Kvrocks，不接入现有 Proxy 的请求路径；不新增 Dashboard 页面或告警通知。数据后端继续限制为 literal loopback、每任务独占冷 DB。

## 开启方式与成本

在现有 named-task 配置顶层显式开启，默认关闭，不改变已有配置的网络行为：

```yaml
progress:
  enabled: true
  interval: 5s
  timeout: 1s
```

- 只支持已有 `tasks` 模式，每进程最多 8 个采样任务；不自动给 legacy journal 分配任务 ID。
- `interval` 允许 1～10 秒，默认 5 秒；两次采样之间等待 interval 加最多 20% 抖动。
- `timeout` 允许大于 0 且不超过 2 秒，默认 1 秒，覆盖单次源连接与协议请求。
- 各任务独立串行采样，最多 8 个并行采样连接；不与 PSYNC 共用连接，不锁住同步任务去等待网络。
- 每次只执行可选 `AUTH` 和 `INFO replication`，然后关闭连接。需要现有源凭据有 INFO 权限；不读取业务 key，不向冷层发送请求，不做 PSYNC 或重试。
- RESP 响应最多 16 KiB，限制元素和嵌套；超限或格式错误记为未知，不采纳响应内容。凭据沿用私有文件、no-follow/nonblocking、属主与单链接校验。
- `-check-config`、构造函数、`GET /status` 和指标抓取均不触发采样。未配置控制面时可只查看同步器本地状态和指标。

默认频率只是原型的有界设置，不是性能验收结论。真实多任务的连接开销、INFO 权限和热写尾延迟仍需单独测试。

## 四个位置，三种差值

| 字段 | 精确定义 |
| --- | --- |
| `source.position` | 一次 `INFO replication` 返回的主库 `master_replid` 与 `master_repl_offset` |
| `local.captured` | 同步内核在一致快照中记录的、已经本地持久化的复制流位置；不是 socket 已读取位置 |
| `local.applied` | 冷层应用调用返回并由内核记录的位置；不是对冷库当前状态的主动探测 |
| `local.gc_confirmed` | manifest 中持久的日志清理确认位置；与冷层应用位置分开，不冒充实时冷层进度 |

位置均为复制流中最后一个字节的包含式 offset，必须连同复制历史标识一起读取。source 由任务已绑定的源连接采样，本地位置也只在匹配该源时对外返回；API 不暴露源/冷端点、日志路径、凭据或业务内容。

例如同一历史下 `source=1000, captured=900, applied=800, gc_confirmed=700`：

- `source_capture_bytes=100`：该样本中源端与本地持久捕获之间的复制流字节差。
- `captured_applied_bytes=100`：该样本中本地捕获与记录的冷层应用之间的字节差。
- `captured_gc_bytes=200`：本地捕获与日志清理确认之间的字节差，**不是冷层未应用字节数**。

这些不是 key 数、命令数、journal 文件大小，也不能换算成“落后几秒”。确认位置未知时省略对应差值，不能填零。API 中 offset、差值、日志字节量使用十进制 JSON 字符串，避免浏览器超过 `2^53` 后丢失精度；Prometheus 仍是浮点采样，不可作为恢复检查点。

## 样本配对与新鲜度

本地快照只在内核完成一组 captured/applied/gc/resource 观测时发布，独立于普通生命周期更新时间。网络重试、状态刷新不会给旧进度续期；非资源压力类采样错误也不刷新这份快照。

采样顺序固定为：**复制本地快照 → 独立读取源端 INFO → 发布这一对样本**。请求源端期间即使本地捕获继续推进，也不能把后来的本地位置换进这份旧源样本，否则会产生假“源端落后”。它不是跨 Redis、日志、Kvrocks 的原子快照或操作屏障。

每侧带 `sampled_at` 和 `age_ms`。本地时间戳表示缓存快照时间，不是冷层最后一次响应或最后一次写入时间；source 时间戳表示该次探测完成时间。重复取得同一个缓存时重新计算年龄，不重新盖采样时间。

新鲜度阈值为 45 秒，用于覆盖受限的采样和心跳周期。Server 不假设两个主机时钟同步，而使用上报的样本年龄，加上本地接收后的经过时间，再保守增加最多允许的 3 秒上报传输余量。Server 在等待存储锁之前记录接收时间，数据库排队也会使样本变旧。

`sampled_at` 是诊断时间，不是安全时钟证明；时钟异常、缺失年龄或过期时不计算差值。该观测协议仍信任经过认证的报告者，不能代替租约、分布式会话代际或 fencing。

## 分类规则

差值只有在任务运行、无已知安全错误、样本未过期、源是 master、必要位置存在、历史一致且位置顺序合理时才返回。数值未知时使用字段缺失，不返回一个伪造的 0。

| `condition` | 含义 |
| --- | --- |
| `unknown` / `initializing` | 未启用或尚无可比较的完整观测；不能认定已同步 |
| `catching_up` | 本次样本的源捕获差或冷层应用差大于 0 |
| `sample_aligned` | 本次样本中 source/captured/applied 相等；不承诺现在仍相等 |
| `observation_stale` | 采样过期、缺失年龄或时钟比较异常，省略差值 |
| `history_mismatch` | 源端、本地或确认位置属于不同复制历史，禁止相减 |
| `samples_not_aligned` | 位置顺序不合理，禁止相减；不自动修复或重建 |
| `source_unavailable` | 源连接/读取失败或内核处于源重试；新探测失败不会沿用旧成功位置 |
| `source_observation_error` | INFO 格式/权限、凭据或非 master 等观测错误，具体固定原因在 `source.error` |
| `cold_unavailable` | 内核已观察到冷层连接问题；不是持续的冷库健康探测 |
| `resource_pressure` | 内核报告磁盘/积压暂停或日志容量压力 |
| `journal_budget_warning` | 新鲜观测中引用的 journal 文件长度达到声明预算的 80%；不是精确物理磁盘占用或限流动作 |
| `needs_attention` | 日志/历史不一致、不确定提交、其他任务失败，需要人工核实 |
| `stopped` / `stopping` | 任务已停止或正在退出，不展示当前积压数值 |
| `invalid_observation` | 本地异常观测结构，省略数值；Server 对非法上报整体拒绝 |

Server 的进程 `stale`、重复 boot 的 `conflict` 或 `stopping` 优先覆盖任务进度视图，不能在一个失联或冲突的进程下面仍展示可用的“对齐”数值。

“运行中且样本对齐”不是“冷库健康”：空闲时没有新的应用调用，本轮不会主动探测冷库是否刚刚宕机。即便刚应用成功，之后还可能有新写入；任何一项位置都不能单独批准回温、冷读回落或热 key 淘汰。

## API 与指标

- 同步器 `GET /status`：每任务增加可选 `progress` 原始样本与 `progress_view` 派生结果。
- 心跳：在已有 `tasks[].progress` 上报样本，不上报客户端计算的健康结论。旧版不带 progress 的报告继续接受，显示未知。
- Server `GET /api/v1/syncer/runtime`：保存后的原始样本仍在 `observations[].tasks[].progress`；当前派生结果在 `observations[].task_progress[task_id]`，读取时动态考虑样本老化。原始 `age_ms` 是上报时年龄，不是页面读取时年龄。
- 进度变更是同一 boot 的观测更新，不改变任务绑定。重复心跳按实际 JSON 传输值判等，避免 time.Time 的内存单调时钟信息丢失后误报身份冲突。

开启后新增每任务指标：

- `redis_pilot_syncer_progress_condition{task_id,condition}`：固定类别的 one-hot 值。
- `redis_pilot_syncer_progress_lag_bytes{task_id,kind}`：只输出当前可比较的差值。
- `redis_pilot_syncer_progress_lag_known{task_id,kind}`：0 是未知，不是追平。
- `redis_pilot_syncer_progress_sample_age_seconds{task_id,side}`：样本年龄，不是秒级复制延迟。

标签只包含合法任务 ID 和固定枚举，不放复制历史、地址、路径或错误原文。已有 `redis_pilot_gateway_sync_*` 指标保持原契约；其中旧 `lag_valid` 仅表达位置可比，不含本轮的新鲜度判断，不能混用成健康信号。

## 停机与后续边界

一个失败任务不拖住其他任务采样；已结束任务不再发起下一次采样。停机取消所有正在进行的源连接，等待采样 goroutine 结束，再完成最终控制面报告；失败样本也不会触发任务重启、冷层写入或复制历史替换。

本轮是单次/缓存样本分类，没有实现积压持续增长分析、最后推进时间的可靠跟踪、停滞时长判定、告警阈值配置、通知或自动处置。它们需要结合真实写入速率与资源预算验收，不能把安静无写入误判为同步停滞。

下一步在这些观测之上设计 Gateway 保护与操作屏障，明确哪些错误必须暂停淘汰/冷读、内存压力如何限制写入。正式分布式归属、计划迁移、人工接管与自动接管仍按既定阶段独立推进。

## 验证边界

本轮本地测试使用受控 TCP/RESP、真实 HTTP/SQLite 与假同步任务：验证 INFO 只读采样、连接超时/取消、身份/位置解析、取本地快照的先后顺序、缓存老化、大整数精度、上报重试、Server 状态覆盖和多个采样任务的并发退出。既有同步内核和 CLI 子进程回归继续执行。

未做真实 Redis/Kvrocks 的本轮进度联测、Linux systemd 运行、扩容/故障接管、长测或热写延迟压测，也未部署。实际命令和结果见 [G6a-13 验证记录](benchmarks/g6-syncer-progress-20260908.log)。
