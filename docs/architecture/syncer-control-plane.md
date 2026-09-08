# G6a-12：同步任务固定登记与状态上报

本文保留 G6a-12 的阶段边界。后续 [G6a-13 进度采样](syncer-progress.md) 已增加可选源端采样、位置上报与样本分类；下文“不上报偏移”指本阶段历史能力，自动处置与故障接管仍未实现。

## 本轮交付，不等同于自动分配

在 [静态多任务同步器](syncer-multitask.md) 上增加可选的控制面观测链路：同步器通过稳定 ID 上报本地配置中的 named tasks，Server 将第一次登记的任务归属持久保存到 `pilot.db`，后续报告不能覆盖归属。

这是**本地声明登记**，不是 Server 下发任务、批准执行、分布式锁或冷层写入隔离。任务仍由本地 YAML 决定；Server 不在线、不接受报告，均不会自动停止、迁移或新增同步任务。多个同步器仍必须由操作者保证不会同时访问同一个冷 DB，不能用本接口代替 fencing。

本轮不提供任务创建/迁移/删除管理接口、自动首次分配、动态任务拉取、故障接管或 UI 页面；也不改变 Agent、Gateway、Proxy 路由和既有服务部署。仍仅支持 loopback 的隔离数据后端，控制面地址允许经认证 HTTPS。

## 已确认的推进顺序

1. 固定归属、原地重启恢复、状态与滞后告警。当前完成登记与状态观测；已有内核负责本地日志恢复。完整滞后上报与告警规则/通知尚未完成。
2. 同步异常时 Gateway 保护协议：停止不安全淘汰，不从可能落后的冷值直接回落；内存达到阈值时限制写入。这些是后续设计与实现，当前 Gateway 未执行这些保护。
3. 计划迁移和人工故障接管：验证旧执行者已停止或被隔离，保留/恢复日志并验证复制历史，再交接任务。二者共用恢复机制，但故障场景不能假设旧执行者能够配合排空。
4. 明确可接受恢复时间、数据损失窗口并验证人工接管后，才实现自动故障接管。

正常进程重启不切换同步器。同步主机或磁盘故障仍需要处理，不能只考虑扩容。新增 Redis 主分片通常意味着新增同步任务，并不要求迁移旧任务；同步器容量不足或计划维护才涉及任务搬迁。slot 迁移与 Redis 主从故障切换另需复制历史和分片协调协议。

异步捕获有尚未持久化的写入窗口，自动接管不能消除它。Redis backlog 不足、本地日志丢失或复制历史改变时不得盲目续传；未来热层只是数据子集，不能拿热层全量快照直接替换冷库。具体恢复/重建流程待定。

## 身份与持久数据

| 字段 | 含义与约束 |
| --- | --- |
| `syncer_id` | 本地配置的稳定运维 ID，重启不变；不使用随机值或 PID |
| `boot_id` | 每次服务运行生成的随机进程身份，重启必须变化 |
| `node_id` | 操作者配置的节点标识，不代表 Server 已核验真实主机或 Agent |
| `task_id` | named task ID；在这份 Server 数据库中全局唯一，建议包含集群/分片语义，但不得包含秘密 |
| `identity_digest` | SHA-256(JSON 数组 `["sync-task-identity-v1", task_id, 规范化源端点, 规范化冷端点]`)，不含凭据、路径或资源预算；不是认证签名 |
| `sequence` | 单个 boot 的递增报告序号，不是 Redis 复制 offset |

`syncer_task_bindings` 保存 task → syncer/node/identity 的首次登记与 Server 接收时间；`syncer_observations` 保存 boot 报告与 Server 接收时间。一次心跳的所有绑定和报告在同一事务中写入，部分任务冲突时整体拒绝，不能留下半份登记。它们不修改 Proxy 的拓扑 generation。

重启后的 ID、节点和任务身份一致时沿用已有绑定。更换同步器、节点或源/冷地址产生的身份变化返回 409，哪怕原报告已经离线。没有自动释放、覆盖或重新分配；从 YAML 移除任务也不删除历史绑定。每同步器最多保留 64 个累计绑定，达到上限需先设计受控清理协议，不能删库绕过。

每 boot 内任务成员、身份、版本和节点不变，任务按 ID 排序上报。相同序号与相同内容的重试成功但不续期；旧序号或同序号不同内容返回 409。终态 boot 不得重新报告运行，应该使用新 boot。每同步器保留最多 8 个 boot，只淘汰超过离线阈值的历史观测，不删除任务绑定。

序号去重与终态保护适用于仍保留的 boot；已淘汰的 boot 没有永久重放防护，不能将这一有界观测历史当作安全租约或旧进程已失效的证明。正式执行授权需要独立的会话代际与写入隔离协议。

登记可发生在任务初始化期间；记录并不证明日志可恢复、冷 DB 独占或同步已成功。错误的初次配置也可能留下待核实的绑定；修复/迁移需另行实现明确协议，不能把改变 ID 或另开空目录当作恢复方式。

## 接口与认证

- `POST /api/v1/syncer/heartbeat/:name`：使用 Server `syncer_tokens` 中该 ID 的独立 Bearer token。缺失、短于 16 字节或不匹配始终拒绝，不复用 Proxy 的空 token 放行模式，也不接受 JWT 代替机器凭据。
- `GET /api/v1/syncer/runtime`：沿用用户 JWT/API Token 认证，viewer 可读；返回 `bindings`、`observations` 和 45 秒离线阈值，无写接口。

报告最多 16 KiB、1～8 个 named tasks，只接收已知字段和固定状态/错误类别，不接收原始错误、业务 key、密码或客户端时间。鉴权成功不代表获得冷层写权限；HTTP 200 只确认 Server 存下了这一条观测。

Server 配置片段（须替换为独立随机凭据，不要使用示例文本）：

```yaml
syncer_tokens:
  syncer-dev-01: "REPLACE_WITH_A_UNIQUE_RANDOM_SECRET"
```

同步器在现有 named-task YAML 顶层增加以下可选字段；不配置则保持纯本地模式：

```yaml
control_plane:
  server_endpoint: "https://pilot.internal.example"
  token_file: "/etc/redis-syncer/server-token"
  syncer_id: "syncer-dev-01"
  node_id: "redis01"
  heartbeat_interval: 10s
  request_timeout: 3s
```

凭据文件必须是当前服务用户拥有的私有普通文件、单硬链接、最多 4096 字节；不接受符号链接/FIFO/设备或多行秘密，令牌至少 16 个可打印非空白 ASCII 字符，可有结尾换行。读取采用 no-follow/nonblocking 打开后校验，`-check-config` 检查文件但不联系 Server、创建日志或后端。运行前再次读取，令牌仅在本次运行内存中使用；轮换后需有界重启服务。

端点只接受无路径/用户名/查询/片段的 HTTPS origin；明文 HTTP 仅允许字面量 loopback IP。禁用环境代理和 HTTP 重定向，避免转发凭据。报告串行发送、不建立持久队列，有超时、响应体/头部大小限制，失败后下一周期发新的观察值；不会把上报重试当成数据重放。

## 离线、冲突与退出

Server 用自身接收时间计算新鲜度，45 秒没有新报告标记 `stale`，未来时间也按 stale 处理。新鲜度只说明控制面连通性，不能证明源/冷数据库健康。`bindings` 是历史登记，必须结合 `observations.status` 和完整任务列表读取；移除、缺报、离线的任务不能因绑定存在就展示为运行。

同一同步器有两个未过期且非终态的 boot，标记 `conflict`。`stopping` 仍可能排空写冷库，不能忽略；所有任务已退出的 `stopped/failed` 不计入活跃重叠。判断只基于进程自报，即使冲突后来因超时消失，也不证明旧进程已被隔离。

上报 401/409/网络失败不会改动同步任务执行。409 的报告不会覆盖 Server 原记录；本地 `/status.reporting.last_result` 和指标记录冲突，操作者需检查对应同步器。未来正式授权执行协议须另外设计，不能把目前的可选上报开关当作安全门禁。

服务退出时先等待全部任务清理，再发送一次有界的最终报告；服务返回前等待报告 goroutine 结束。控制面终态报告失败时依靠离线阈值显露失联，不无限等待。开启上报后最终报告最多额外使用 `min(request_timeout, shutdown_timeout)`，沿用 30 秒 systemd 停止窗口时也须为任务、管理 HTTP 和文件清理预留时间。本轮不自动修改任何机器的 systemd 服务。

## 观测指标与未完成项

本地 `/status` 增加 `reporting.enabled/last_result/last_ack_at`。开启上报时增加：

- `redis_pilot_syncer_heartbeat_total{result="acknowledged|error|unauthorized|conflict"}`。
- `redis_pilot_syncer_heartbeat_last_ack_timestamp_seconds`；0 表示从未确认，不是同步滞后为零。

现有每任务指标继续保留。这里没有上报复制偏移、源历史、冷层确认屏障或可淘汰证明，也没有新增告警规则或通知渠道；“在线/运行”绝不等于“冷层追平/可安全回落”。

后续先补同源历史下的进度/滞后观测与保护协议，再实现 Server 权威任务配置、首次分配和受控交接。分布式所有权、自动故障接管、共享冷库、TTL/更多命令、回温与受控淘汰仍未完成。

## 验证边界

本地测试覆盖 Server API 认证/体积/输入约束、SQLite 原子登记与重开、重试不续期、离线不改归属、boot 冲突、报告上限、同步器报告与 SQLite 的重启闭环，以及网络失败/超时/重定向/凭据特殊文件处理。闭环使用真实 HTTP/SQLite 与假同步任务，不执行真实 Redis/Kvrocks 复制，不是生产故障接管验收。

最终命令和结果见 [本轮验证记录](benchmarks/g6-syncer-control-plane-20260908.log)。本轮不部署、不运行长测、不自动提交。
