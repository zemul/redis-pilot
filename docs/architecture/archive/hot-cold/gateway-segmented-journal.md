# G6a-6：分段日志、checkpoint 定位与安全回收

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

后续进展：[G6a-7 资源保护与孤儿清理](gateway-sync-resources.md)、[G6a-8 隔离持续同步循环](gateway-sync-runner.md) 已扩展本实现。本页保留 G6a-6 的原始验收边界；下述“未实现”的磁盘水位、视图数量限制、孤儿清理和自动调度请结合后续文档阅读，运行服务接线仍未完成。

## 本轮范围

2026-09-08，在 G6a-5 单项 pending 协议旁增加独立分段日志和 `applySegments` / `pruneSegments` 实验入口。冷层仍使用相同的三个 v2 元数据 key、原生 String 与 WATCH/MULTI/EXEC 协议；没有增加 Server SQLite 的逐 key 状态，也没有修改 Redis/Kvrocks 引擎。

复制捕获入口抽取为 `syncCaptureLog`，单文件和分段实现都能接收真实 PSYNC 流。**没有接运行服务、没有开启 TTL、淘汰或回温，也没有部署 Gateway。** 原有单文件格式继续保留作对照，不会自动转换历史日志。

本轮解决的是日志记录不必从头扫描、已确认旧段可以正常回收，以及回收后如何保留恢复证据；不是完整生产同步器。

## 本地文件与持久化顺序

日志使用独占、权限 0700 的目录；文件权限 0600，拒绝最终路径符号链接。固定 `LOCK` 文件持有进程锁，manifest 替换不会换掉锁。段文件名为随机唯一 ID，通过独占创建避免覆盖旧文件。

- **anchor 文件**：初始 RDB 和 Source/History/Offset，永久保留；其摘要形成 base checkpoint。
- **segment 文件**：头部绑定段 ID 与上一 checkpoint；每条原始记录都有长度、CRC32C，checkpoint 继续使用 SHA-256 摘要链。
- **manifest**：规范 JSON、长度与 CRC，保存 anchor、各段 start/end/size、保留起点 floor 和已确认水位 confirmed。它用于定位和记录已持久化边界，不代表冷层自然完成了相同进度。

轮转顺序是：新段头写入并 fsync → 目录 fsync → 临时 manifest 写入并 fsync → rename 为 manifest → 目录 fsync → 向新 active 段追加命令。旧段中的命令在前面的 append 阶段已经逐项 fsync。

追加成功后才推进可向 Redis ACK 的捕获位置。每次追加不重写 manifest；重开时对有大小上限的 active 段扫描和 fsync，验证 manifest 记载的旧前缀，并恢复完整尾部。CRC 错误、半条记录、位置不连续均停止，不自动截尾。

manifest 引用的段缺失、sealed 段大小不匹配、anchor 损坏均拒绝打开。manifest 丢失但目录仍有其他历史文件时，不能把目录当新日志。未被 manifest 引用的文件不自动并回历史。

## 定位和应用

重开日志需要读取 manifest、anchor，打开并检查各段文件信息，以及扫描 active 段；**不是 O(1) 启动**，但不扫描全部 sealed 段的历史记录。

根据 checkpoint 的位置范围找到候选段，再从段头重算原始记录的 CRC、连续位置和摘要链，精确匹配完整 checkpoint。共享段边界优先校验产生该 checkpoint 的前一段；只有长期持久化的 retained floor 可以作为已回收前缀的校验起点。错误摘要但相同 offset 不能通过。

每个被访问的段先完整校验，再执行回调。应用器先只读预检未应用后缀，再提交业务命令。恢复核对 pending 原文、Previous/Next 与 before/after；索引不会绕过 G6a-5 的不确定结果处理。

已应用的早期 sealed 段不会为了恢复再被逐条读取。这也意味着**未访问旧段的损坏不是启动时全量审计的对象**。初始 RDB 只在 bootstrap 时解码成完整 map，仍保留原来的小快照限制。

视图固定文件长度并持有独立文件描述符。建立视图后的新追加留给下一轮；轮转、回收或关闭 journal 不会改变旧视图的读取边界。视图也保留目录锁，视图全部关闭前不能另开一个写入者。

## 安全回收

`pruneSegments` 是显式实验操作，尚无后台自动调度策略。它与同一 journal 的冷层应用串行执行，捕获仍可继续追加。

只有以下证据全部成立才允许回收：

1. 冷层 anchor 与本地一致，checkpoint 精确等于最后 pending 的 Next。
2. pending 的原始命令和 Previous → Next 能从本地保留段重新验证。
3. WATCH 元数据和相关 key 后重读，当前业务值符合 after 且无 TTL。
4. 只读的 MULTI/PING/EXEC 完成 WATCH 检查；回复不明也不能继续回收。

回收保留 **pending 原始记录所在的整个段及后续段**，不是简单删除到冷 checkpoint。

持久化顺序：先更新 manifest 中的 floor、confirmed 与保留段列表，完成 rename 和目录 fsync；随后才逐一 unlink 旧段，最后目录 fsync。

| 中断位置 | 重开后的判断 |
| --- | --- |
| 新 manifest 发布前 | 使用旧 manifest，旧段仍在 |
| 新 manifest 发布后、删段前 | 使用新 manifest；旧段是孤儿，不重新并入历史 |
| 已删部分旧段 | 新 manifest 不引用这些段；剩余旧文件仍只算孤儿 |
| 新 manifest 引用段缺失或损坏 | 停止，不回退到其他 manifest 或猜测拼接 |

持久化或删除失败后当前句柄停止使用，必须关闭并重开判断。不能因为一次错误返回，就假定 rename 或写入一定没有发生。

## 冷层回退保护

floor 是本地仍能提供证据的起点，confirmed 是本次 GC 已验证过的冷层确认水位；二者不同。

- 冷 checkpoint 早于 floor：已无完整重放证据，拒绝。
- 冷 checkpoint 仍在保留段内，但早于 confirmed：也拒绝，不能把冷层回滚误当未确认命令再次 INCR。
- checkpoint、pending、anchor 身份或摘要不匹配：拒绝。
- GC 后冷层元数据全丢，即使目标已经空库：拒绝用旧 anchor 自动初始化，避免漏掉已回收的历史。

confirmed **只在显式 GC 时持久化**，不能检测所有发生在该水位之后的冷层回滚。冷层副本切换、跨网关所有权与 fencing、灾备重建仍待设计。WATCH 不是分布式租约，本实验仍要求冷库独占、严格单应用者。

## 容量与未完成事项

- 段容量 1 KiB–16 MiB，最多 1024 个引用段，manifest 最多 1 MiB，总有效日志上限最多 1 GiB；单记录原始数据最多 8 MiB，并且必须放得进一个段。
- 总有效日志上限计算 anchor 和 manifest 引用段，**不是物理磁盘硬配额**。manifest/临时文件、崩溃孤儿和旧视图仍持有的已 unlink 文件不计入它。
- 正常 GC 删除已确认旧段；崩溃后剩余孤儿目前保留，不自动清理。物理磁盘水位、孤儿精确清理、视图数量/生命周期预算、告警及背压策略是下一步，不能声称崩溃后磁盘空间已经完全释放。
- 捕获容量满则停止该次捕获，不越过失败记录推进 ACK；受控重连、积压保护与后台回收调度未接线。
- RDB 仍仅 v11、无 LZF、无 TTL、DB0、最多 256 key/512 KiB；pending 单 key/value 64 KiB。常见命令扩展、TTL 延迟重放问题、大快照分批导入仍未解决。
- 每项 pending 和业务提交仍有独立的冷层持久化/往返开销。本轮不测吞吐，不据此承诺生产性能或零数据丢失。

## 验证与环境

单测位于 `sync_segments_test.go`，冷层实验位于 `sync_segment_integration_test.go`。验证覆盖分段/精确定位、正常回收、旧视图生命周期、损坏/缺失文件、容量及锁、轮转/GC 持久化阶段错误，以及真实 Redis PSYNC 续传。

真实 Kvrocks 实验复用 redis03 的专用 `redis-pilot-g6-apply-0eGQag`，网络 none、无对外端口、同步 WAL；仅操作列明的合成测试 key。既有 Redis/Kvrocks 实例与 Agent 不变。

实验包括真实 RDB/PSYNC fixture 的 42 → 43、回收后未确认 INCR 的断线/丢回复/晚到事务、after 偏差拒绝、冷层回退拒绝，以及在新 manifest 持久化后、旧段删除前实际 SIGKILL 回收子进程。强杀的是测试同步进程，不是 Kvrocks 或主机掉电。

最终执行结果和收尾状态见 [测试记录](../../benchmarks/g6-segments-20260908.log)。

最终结果：9 个新 Kvrocks 功能用例连续三轮通过，旧 pending 链路 24 个用例回归通过；真实 Redis 捕获及分段单测三轮 race 通过，网关全包 race、`go vet` 与项目 `go test ./...` 通过。首轮曾因测试清理顺序使用已关闭连接而整体失败，已修复后完整重跑，失败原因保留在记录中。

实验容器已停止（exited 0，network none），各轮仅清理明确的测试 key 并验证空库；fixture、二进制和引擎日志保留，既有实例与 Agent 状态不变。未提交 Git、未部署运行服务。
