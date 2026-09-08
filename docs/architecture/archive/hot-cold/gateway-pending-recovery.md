# G6a-5：单项待确认记录恢复

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

## 本轮结论

2026-09-08 新增 `sync_pending.go`，在 G6a-4 全状态核对原型旁实现独立的 `applyPending` 实验入口。

恢复时不再在 Go 中重建整个冷层状态，也不再扫描整个冷层；只核对最后一条持久化待确认记录及它涉及的 key。业务 value 仍是 Kvrocks 原生 String，新增的恢复信息保存在 **Kvrocks 元数据 key** 中，不写入 Server SQLite，也不要求 Redis 引擎改造。

当前“一批”固定为一个业务命令或一个复制控制记录。该限制让前态、后态和提交结果都可有界检查；不是任意事务/Lua/多 key 命令协议。

未接运行服务、未开启 TTL/淘汰/回温、未实现多网关所有权、未部署 Gateway。G6a-4 代码与测试保留作对照，两个协议不会自动互相升级或覆盖已有数据。

## 持久化数据

三个固定的实验元数据 key 均不得有 TTL：

- `__pilot_sync_v1__:{isolated}:v2-anchor`：绑定初始快照、源身份和初始位置的锚点。
- `__pilot_sync_v1__:{isolated}:v2-checkpoint`：版本 2 checkpoint，含 Source/History/Offset 和原始记录的 SHA-256 摘要链。
- `__pilot_sync_v1__:{isolated}:v2-pending`：最后一项的 previous/next checkpoint、原始复制记录、业务值 before/after。

before/after 显式区分“不存在”和“存在但为空”。数据使用 `[]byte` 的 JSON base64 编码，避免非法 UTF-8 被 JSON string 替换；key 从原始 RESP 字节重新解析，不以空 key 判断控制记录。恢复时重新根据原始命令和 before 计算 after，并与保存值校验，不直接相信元数据自带的 after。

pending **不会在提交后立即删除**。下一项准备前，必须先确认上一项已提交且 after 匹配，随后以条件事务覆盖 pending。checkpoint 已前进却找不到 pending 时拒绝恢复；仅允许 checkpoint 精确等于初始 anchor 的尚未开始状态没有 pending。

## 提交和恢复流程

### 初始化

源快照和命令形状预检完成后，WATCH 三个元数据 key，检查元数据不存在，并有界 SCAN 确认目标为空。然后通过 MULTI/MSET/EXEC，一次写入受限快照、anchor 与初始 checkpoint。

这仍依赖实验库独占。WATCH 能让旧初始化请求在新进度产生后中止，但不是针对外部任意写入者的全库锁。绝不使用缓存 DBSIZE 作为空库依据，不清库覆盖，也不将旧协议元数据自动迁移为新协议。

### 准备待确认项

1. WATCH checkpoint、pending、anchor 和当前业务 key。
2. 重读并精确校验当前元数据，读 key 的原生 String 值及 PTTL。
3. 计算 before/after，校验命令、数值、大小和位置。
4. 通过 MULTI/SET pending/EXEC 保存待确认记录。成功后才允许提交业务修改。

### 应用与恢复

再次 WATCH 同一组 key、重读并核对元数据和业务状态：

| 目标 checkpoint | 当前业务状态 | 行为 |
| --- | --- | --- |
| pending.previous | 等于 before | 可以提交原命令与 next checkpoint |
| pending.next | 等于 after | 已提交，跳过该项 |
| previous 但不等于 before | 数据可能超前或发生偏差 | 停止，要求明确恢复 |
| next 但不等于 after | 进度与数据不一致 | 停止，要求明确恢复 |
| 其他位置、缺失/错误 metadata | 无法确认同一历史与提交状态 | 停止 |

实际业务修改与 next checkpoint 在同一个 MULTI/EXEC 中提交。检查完整 EXEC 结果，并读回元数据和当前 key；意外错误或连接中断不自动重发。重新调用会重新验证日志与 pending，再按表中状态判断。

SET 同值、DEL 不存在的 key 可能 before=after，按最终状态与 checkpoint 判断；这不扩展到有额外副作用的命令。PING、SELECT 0、REPLCONF GETACK 也生成 pending 并推进真实字节位置，不能绕过之前的未确认数据修改。

### 为什么准备和初始化也需要 WATCH

关闭连接或重启同步器，不足以证明上一连接中的请求不会晚到。如果无条件 SET pending，旧请求可能覆盖新 pending，丢掉唯一恢复证据；旧 MSET 初始化也可能覆盖已推进的数据。

本实现的准备、业务提交和初始化都使用条件事务。测试中实际保留旧连接上的排队事务，待新连接完成恢复后才放行旧 EXEC，观察到旧事务中止，而不是仅模拟一个失败标志。

## 与 G6a-4 的保证差异

G6a-4 核对全库，因此能发现其他 key 的偏差。本轮只验证最近未确认项，**不能发现无关 key 被外部修改、遗漏或损坏**。测试特意修改无关 key，并确认这不被误称为全库审计。

正确性前提包括：冷库独占、严格单应用者、只使用本协议修改业务数据和元数据、无 TTL、底层同批提交与约定的 WAL 持久化成立。WATCH 是旧请求/竞争检测，不是生产多网关 fencing 或选主机制。冷层故障切换、回滚到旧副本、永久磁盘损失和定期全量对账仍需单独设计。

## 资源边界与仍未完成的“大容量”工作

- 单 key 和单 value 最多 64 KiB；编码后的 pending 不超过 512 KiB，并在写入前检查实际 RESP 长度能被当前 Reader 读回。
- 恢复/增量不再维护全库 map，也不再受 256 个活跃业务 key 的上限约束。真实测试追加 270 个 key 后再次恢复通过；这只是结构验证，不是容量或吞吐基准。
- 通过复制文件描述符建立固定长度的可靠日志视图，逐条解析，不把整个日志复制进内存；视图建立后新增记录留待下一轮，关闭原 journal 不会改变该视图边界。
- **启动仍两遍扫描完整日志**，并解析受限初始 RDB；尚无分段、定位索引、已确认段回收，不能宣称 O(1) 日志恢复。
- 初始快照仍沿用 G6a-4 的 RDB v11、无 LZF、无 TTL、DB0、最多 256 key/512 KiB 子集；本轮进一步要求初始单 key/value 不超过 64 KiB。大快照的分批导入和原子发布未实现。
- 增量预检保证命令形状和字节上限，不再重建所有历史 key 状态。INCR 的数值合法性在读取对应冷 key 时检查；如果此时失败，之前的合法记录可能已提交，程序停在对应进度，不自动跳过。
- 每项多一次 pending 持久化，增加冷层写入和往返；它不进入 Redis 热写确认链路，但冷层吞吐与积压能力需要后续实测，不能据此承诺性能。

## 验证

`sync_pending_unit_test.go` 覆盖命令与整数边界、非法 UTF-8、空 key/空 value/missing、元数据篡改与大小、视图 append/close 生命周期、CRC 和连续位置校验。代码经独立只读协议评审，测试开发与核心实现分开进行；评审不代替以下实测。

真实 Kvrocks 2.15.0 集成位于 `sync_pending_integration_test.go`：

- 初始化、INCR pending 准备、INCR 提交、控制记录准备、DEL 提交五个位置，分别注入 EXEC 前断线、成功后丢回复、旧 EXEC 延迟放行。
- 强杀一个真实同步子进程：它已持久化 pending，但业务 EXEC 被代理扣住；父进程重开日志并恢复，随后放行旧事务，旧事务中止且 INCR 不重复执行。
- 人工构造数据超前/进度超前，均拒绝继续；pending 缺失、错误类型、TTL、after 篡改均被拒绝。
- 非法 UTF-8、空 key/空串、重复删除、控制 pending 的恢复。
- 超过旧全状态原型的 key 数限制，重复恢复仍通过；无关数据不被扫描或修复。

采用与 G6a-4 相同的真实 Redis RDB/PSYNC fixture（第一段 counter=42，第二段 counter=43），故障及边界后缀由测试构造。没有把这些后缀冒充真实源捕获。本轮强杀的是同步测试进程，不是 Kvrocks，也不是主机掉电实验。

三轮最终远端输出、二进制校验值与本地测试结果见 [日志](../../benchmarks/g6-pending-20260908.log)。

## 环境与收尾

- 复用 redis03 上独占、前轮已清空并停止的 `redis-pilot-g6-apply-0eGQag`；网络 none、无对外端口、同步 WAL，现有 `kvrocks-test-kv` 等实例未触及。
- 新测试二进制 `/tmp/redis-pilot-g6-apply.0eGQag/redis-pilot-g6-pending-20260908.test`，只读挂载至 `/fixture`；前轮二进制保留。
- 各轮只清理明确的测试 key，结束后测试 DB 为空。fixture 文件、容器、测试二进制与引擎日志保留；实验容器已停止，未设置自启。
- 未提交 Git、未部署 Gateway/Server、未切流。

重跑入口（仅上述隔离容器启动后）：

```sh
podman exec --user 0 -e GATEWAY_TEST_KVROCKS_COMMIT=isolated -e GATEWAY_TEST_KVROCKS_ADDRESS=127.0.0.1:6666 -e GATEWAY_TEST_KVROCKS_PHASE=pending -e GATEWAY_TEST_SYNC_FIXTURE_DIR=/fixture redis-pilot-g6-apply-0eGQag /fixture/redis-pilot-g6-pending-20260908.test -test.run '^TestKvrocksCommitExperiment$' -test.v -test.count=3 -test.timeout=90s
```

后续 G6a-6 已增加独立的 [分段日志/定位/回收原型](gateway-segmented-journal.md)，本页描述的单文件参考实现保持不变。受控重连、物理磁盘与积压保护、TTL 协议、大 RDB、所有权和服务接线仍待完成，不能把有界 pending 恢复直接等同于完整冷热产品已完成。
