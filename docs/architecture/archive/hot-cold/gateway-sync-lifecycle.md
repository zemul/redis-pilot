# G6a-9：同步器优雅停止与进度观测

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

本阶段已提交 `ff2e6fe`。后续 [G6a-10 独立同步服务](syncer-service.md) 接入受控的单任务运行入口；下文范围与验证记录保留 G6a-9 的交付时点。

## 当前交付

基于已提交的 G6a-8（`19bc7b3`），新增显式 `shutdown(ctx)`、停止结果/时间观测，以及只读 Prometheus collector。本轮仍不连接 `Service`、YAML、Server 心跳或 Proxy 路由，不新增监听端口，也不启动长时间测试或部署开发机服务。

用户选择先继续完善功能，再安排长测。因此本轮验证以本地单测、并发检查和项目回归为主；新增真实 Redis → Kvrocks 的优雅停止用例留在显式 opt-in 集成套件中，远端复测和 4/24 小时持续运行验收后置。

## 两种停止方式

### 优雅停止

`shutdown(ctx)` 请求按以下顺序结束本次运行：

1. 先发布停止请求，再取消源复制连接。源捕获有独立的 context，即使主循环正在等待冷层 I/O，也可以及时停止源；此时不取消冷层操作。
2. 等待正在执行的冷层步骤返回，等待捕获及其 ACK 协程结束。
3. 从日志读取并固定最终持久化位置 `DrainTarget`。捕获取消期间最后一条完整记录仍可能完成 fsync，因此不能使用请求发出时或旧观测中的位置。
4. 对固定尾部完成应用与确认回收；执行有界孤儿清理。达到要求后返回 nil，状态为 `drained`。

对有增量的日志，要求完整的 Source/History/Offset 与目标精确一致，Applied 和 Confirmed 均到达目标。只比较 offset 数值或“至少到达”不够。

两个例外：从未持久化 snapshot 时，没有待同步的本地数据，可直接完成停止，但不代表冷库已初始化；仅有 snapshot 时，按既有 bootstrap 协议校验并应用到 Base，无需凭空制造 pending/GC 确认。对应缺失位置的指标保持无效，不输出伪造的零滞后。

**drained 只证明停止捕获后已经存在于本地日志的固定尾部处理完成。** 它不证明 Redis 上全部成功写入都已捕获，不阻止源继续写入，不是可淘汰热数据、无损切主或迁移完成的凭据。

### 强制中断

原有 `run` 的父 context 取消仍立即中断捕获和冷层操作。若中断发生在 EXEC 附近，结果可能不确定，必须保留错误并在下次明确启动时对账。

优雅停止的有效请求必须带一个未来且不超过 30 秒的 deadline。达到期限会强制中断本次运行，纯取消映射为 `shutdown_timeout`；调用方提前手动取消请求则为 `shutdown_aborted`。这两个类别均不是成功排空。

强制中断后仍等待捕获退出并释放运行锁，才向调用方返回。deadline 是触发强制中断的时间，不是严格的函数返回上限：本地文件操作不一定支持即时取消。

已经产生的 uncertain、日志损坏、历史不一致等终态优先于取消/超时，包括同时携带 context 错误的组合错误。不能为了让停止显示成功而隐藏恢复问题。

## 并发与重试边界

每次运行拥有独立 control、停止信号、完成信号、取消函数和结果。重复请求共同停止同一次运行；任一个已接受请求的期限届满或主动取消都可以触发强制中断。旧调用只能影响旧 control，不能停止下一次运行。

完成信号在捕获/ACK 回收、最终状态发布以及日志/runner 运行锁释放后关闭。返回后调用方才可关闭日志或重新启动。已完成的 control 返回该次原始结果，不会因新的等待 context 无 deadline 而改变旧结果。

尚未启动、正在校验且尚未发布 control、或校验失败时，返回 `errSyncRunnerNotRunning`。新运行校验期间会清除前次 control，不能误报前次排空成功；校验失败也会更新失败状态，不沿用旧的 drained 观测。参数无效的停止请求不产生停止副作用。

排空后不再重连源，也不受捕获准入水位阻挡；但冷层、文件系统错误仍需处理。冷层 open 阶段沿用有界重试；元数据/业务阶段错误和不确定提交仍终止，不自动重放。无法确认固定尾部时返回 `shutdown_incomplete`，不能当作停止成功。

## 进度与指标

`syncRunnerStatus` 新增：

- StartedAt、UpdatedAt、FinishedAt：本次启动、最近状态更新和结束时间。
- StopRequested、DrainTarget：是否请求优雅停止及捕获结束后的固定目标。
- ResourcesObserved、ResourceUpdatedAt：是否取得过资源观测及其时间。
- 资源状态增加 AvailableKnown：可用量采样成功时才为 true，区分真实 0 字节与采样失败。

保留 Captured、Applied、Confirmed 三种位置，不把 Redis ACK 当作冷层完成确认。UpdatedAt 是本地状态更新时间，不是 Redis/Kvrocks 最新健康检查时间；本轮仍没有独立的空闲冷层探测。

`newSyncRunnerCollector(r)` 由调用方显式注册到独立 registry，指标前缀为 `redis_pilot_gateway_sync_`。每次采集只复制一次内存状态，不访问日志、网络，不触发应用或恢复，不注册全局指标，也不启动 HTTP 或后台协程。

| 指标组 | 语义 |
| --- | --- |
| runner_present / state / last_error | 是否存在 runner、固定状态与固定错误类别；未知文字映射 unknown |
| timestamp_valid / timestamp_seconds | 四类事件时间及有效性；未知时间不写成 epoch 0 |
| stop_requested / position_valid / position_offset_bytes | 停止请求、四类位置及其有效性 |
| lag_valid / lag_bytes | 捕获到应用、捕获到确认、排空目标到应用的差值 |
| resources_observed / available_bytes_valid / resource_bytes | 缓存的资源观测，未知磁盘空间不输出伪造的 0 |
| open_views / capture_paused | 最近观测的视图数及资源暂停原因 |
| source_attempts / connection_retries | 每轮尝试/重试数；重启会重置，使用 Gauge 而不是 Counter |

滞后值只有在两端位置有效、历史/源相同且 offset 顺序合法时才输出；缺失或不可比较不表示“已追平”。字节差不是时间延迟、key 数或准确丢失量。指标使用浮点数，不能替代恢复 checkpoint。

不把 key、value、口令、源地址或复制历史字符串作为标签。一个 registry 对应一套这样的 collector；多分片标签与聚合方案、告警阈值仍待服务接入时设计。

## 验证与剩余工作

本地测试覆盖停止期间冷层操作不被提前取消、捕获尾帧与 join 顺序、并发请求、错误/超时优先级、确认精确匹配、空日志/snapshot 例外、锁释放与代际隔离，以及并发指标采集、缺失值和敏感信息隔离。真实集成套件增加“优雅停止确认固定尾部、停止后源写入留待下次续传”的用例，本轮未执行该远端用例。

本轮执行结果以 [验证记录](../../benchmarks/g6-lifecycle-20260908.log) 为准，不沿用 G6a-8 的十轮结果充当新版本集成验收。

最终本地目标测试三轮 race、网关全包 race、项目全量测试、vet 与 Linux 编译检查通过。该阶段后来提交为 `ff2e6fe`；没有操作远端环境或启动长测。

后续仍需：受控服务入口及指标注册、空闲健康/超时策略、长时间运行和真实故障验证。TTL、更多命令/大 RDB、冷热淘汰/回温、Cluster、所有权与高可用仍未实现；不得因支持优雅停止就开始降冷。
