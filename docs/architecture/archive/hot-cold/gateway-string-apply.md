# G6a-4：无 TTL String 的有界应用原型

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

## 本轮完成什么

2026-09-08 在不修改 Redis/Kvrocks 引擎、不封装业务 value 的前提下，串联了实际数据路径：

1. 独占 Redis 7.2.5 生成初始 RDB，接收器持久化快照及 PSYNC 增量。
2. 捕获器断开期间继续写源 Redis，重开日志并通过 PSYNC 续传。
3. 把两段真实捕获日志复制到隔离 Kvrocks 2.15.0 环境。
4. 应用器导入快照、执行增量；重新调用时恢复到真实复制位置，重复前缀不再次执行 INCR。

这是**离线转交真实捕获日志的串联验证**，不是同时运行的生产同步服务。代码为未导出的实验入口，未接 Gateway Service/config、Server 或 Agent；没有部署到正在运行的 Gateway，没有开启 TTL、淘汰、回温或 Cluster 路由。

## 代码与协议

- `sync_journal.go` 的 `applicationPrefix`：复制有界、完整的可靠日志前缀，校验连续位置，网络操作期间不持有日志锁。
- `sync_rdb.go`：RDB v11 的 String 子集解析，Redis CRC64/Jones 校验；支持普通字节串及整数编码。
- `sync_apply.go`：预检整个前缀，重建预期状态，管理目标 checkpoint，执行有界的一次应用。
- `sync_apply_test.go`：真实 Redis 捕获 fixture、解析/扫描测试和隔离 Kvrocks 应用测试。

checkpoint 包含协议版本、Source、History、真实复制字节 Offset，以及绑定快照和此前每条记录内容的 SHA-256 摘要链。不使用上轮测试中的人工递增序号。保留字前缀 `__pilot_sync_v1__:` 不允许出现在业务 key 中；元数据 key 为 `__pilot_sync_v1__:{isolated}:checkpoint`。

### 初始化

先完成整个输入前缀的格式、命令和状态上限检查。目标必须是专用、无其他写入者的空 DB0；不调用 FLUSH，不覆盖已有库。小快照的所有原生 String 值与初始 checkpoint 使用单条 MSET 写入，之后核对状态。

### 增量与恢复

- 支持无选项 SET、INCR、单 key DEL。INCR 可创建原先不存在的 key，但必须由日志重建状态确认不存在，而不是只凭一次冷层未命中。
- 每个业务记录与新 checkpoint 放入同一 MULTI/EXEC；前后校验结果，WATCH 检测 checkpoint/当前 key 的并发改变。复制控制记录只提交位置。
- 网络异常不自动重发，返回不确定状态。调用者重新调用时，重新连接、读取 checkpoint，并在当前完整日志中找到精确匹配的摘要和位置。
- **恢复时核对 checkpoint 对应的全量预期状态**：实际 key 集、String value、无 TTL 属性全部匹配才继续。如果数据先变但进度未变，或者进度先变但数据未变，均拒绝继续；不靠自动重试掩盖错误，也不自动修复。
- 正常确认需要收到预期 EXEC 结果，并校验当前数据及 checkpoint。返回的位置是本轮已确认的冷层位置，与捕获 ACK 分开；不能直接作为生产可淘汰证明。

这种全状态核对是当前小原型能保守恢复的基础，**不能省掉后仍沿用本轮正确性结论**。大容量方案必须重新设计有界的恢复证明/状态校验方式。

## 严格边界

| 项目 | 当前限制 |
| --- | --- |
| 调用方式 | 单次显式实验调用，无调度、自动重试或服务启动接线 |
| 目标 | 字面回环地址、独占单应用者、Kvrocks 2.15.0 主节点、非 Cluster；要求同步 WAL 且 WAL 未禁用 |
| 数据状态 | 最多 256 个业务 key，key/value 总字节不超过 512 KiB |
| 日志 | 应用前缀文件不超过 16 MiB，最多 4096 条记录；捕获器自身上限仍另行生效 |
| 快照 | RDB v11、DB0、无 TTL 的 String；支持整数编码，拒绝 LZF、其他类型、函数、模块及未支持 opcode；拒绝关闭校验和的 RDB |
| 命令 | SET 三参数、INCR、单 key DEL；SELECT 0、PING、REPLCONF GETACK 为允许的控制帧 |
| 未支持 | TTL、MULTI/EXEC 源事务、Lua、其他 DB、其他数据类型、多 key DEL、MSET 增量、迁移/切主 |
| 内存与性能 | 在 Go 中重建完整有界状态，重新调用会扫描完整前缀和冷层；没有吞吐/延迟承诺 |

源测试实例关闭 RDB 压缩以生成该子集，未修改任何已有开发或业务 Redis 配置。最终产品兼容范围没有因此被缩减；表中是实验边界。全状态驻内存和恢复扫描不符合大容量冷层的最终成本目标，后续必须替换，不能通过简单调大上限上线。

WATCH 不提供多网关所有权或 fencing；SCAN 不是并发快照。完整状态核对依赖实验目标独占。WAL 同步配置检查和已收到的响应不等于主机/磁盘永久故障、冷层切主或零 RPO 的证明。本轮没有重新做进程强杀，也不声称覆盖所有崩溃窗口。

## 实测与修正

真实源初始快照含 3 个 key，包括整数与含 NUL/CRLF 的二进制 key/value；第一段增量使 counter 从 41 到 42 并删除一项。捕获器断开期间再 INCR 和 SET，续传后的第二段最终 offset 为 **213**、counter 为 **43**。

Kvrocks 应用测试连续 3 轮通过：

- INCR 所在 EXEC 转发前断开，重新调用后只应用一次。
- INCR 所在 EXEC 成功但回复被丢弃，重新调用并核对状态后不重复加一。
- 再次应用同一前缀不重复执行；后续较长前缀接到 offset=213，值为 43。
- 二进制数据和删除结果正确。
- 数据被改成错误值、数据超前于 checkpoint、checkpoint 超前于数据，均拒绝恢复；没有继续写后面的 key。
- 新复制历史拒绝续接；包含 TTL 的前缀在写入前整体拒绝。
- 未初始化的非空目标库拒绝导入，原值保留且不产生 checkpoint。

首轮联调暴露并修正了 **DBSIZE 非实时** 的问题：Kvrocks 缓存 key 数量，不能据此证明空库或状态一致。早期实现会在快照导入后错误判定状态不一致，非空初始化检查也不可靠。已改成有界 SCAN，并增加“拒绝后没有生成 checkpoint”的断言。该首轮只作用于新建的独占实验容器，后续测试按明确 fixture key 清理；没有对业务库做试验。

原始输出、二进制/fixture 校验值与收尾记录见 [实验日志](../../benchmarks/g6-apply-20260908.log)。解析与扫描单测、真实 Redis 捕获/续传的 TestSync 用例完成 5 轮 race；另执行 Gateway 全包 race、vet 和短时 RDB 模糊测试，结果记录于日志。

## 环境与复现

- 本地源捕获导出：`/tmp/redis-pilot-sync-e2e-3975521347/phase1.log`、`phase2.log`，仅人工构造的测试数据，权限 0600。
- 新建开发机实验容器：redis03 的 `redis-pilot-g6-apply-0eGQag`，镜像 `docker.io/apache/kvrocks:2.15.0`，网络 none、无端口映射、内存 384 MiB。
- 独立 bundle：`/tmp/redis-pilot-g6-apply.0eGQag`；数据目录 `data` 归镜像 UID 999，挂载为 `/test-data`；fixture 和测试程序只读挂载至 `/fixture`。
- 测试只删除自己创建的明确 fixture key，最终 DB0 为空；快照文件、容器、测试二进制及引擎日志保留。容器停止且未设置开机自启，原有开发实例与 Agent 保持运行。
- 未部署服务、未修改路由、未提交 Git。

```sh
# 本地：仅新建自己的源 Redis，可选导出真实合成数据日志
GATEWAY_TEST_REDIS_SERVER=/opt/homebrew/bin/redis-server GATEWAY_TEST_SYNC_EXPORT=1 go test -race ./internal/gateway -run '^TestSyncApplyFixtureRedis$' -v

# 远端：仅在上述专用容器启动后执行，启动方式见本轮操作记录/容器配置
podman exec --user 0 -e GATEWAY_TEST_KVROCKS_COMMIT=isolated -e GATEWAY_TEST_KVROCKS_ADDRESS=127.0.0.1:6666 -e GATEWAY_TEST_KVROCKS_PHASE=apply -e GATEWAY_TEST_SYNC_FIXTURE_DIR=/fixture redis-pilot-g6-apply-0eGQag /fixture/redis-pilot-g6-apply-20260908.test -test.run '^TestKvrocksCommitExperiment$' -test.v -test.count=3 -test.timeout=60s
```

## 下一步待定

其中“替代全量 Go 状态重建与全库核对”已有独立的 [G6a-5 待确认项恢复原型](gateway-pending-recovery.md)。旧原型仍保留，新协议只证明最近待确认项，不再提供全库任意偏差检测；尚未实现日志分段/定位和大 RDB 导入。

- 用面向大容量的恢复协议替代全量 Go 状态重建与全库核对，同时保留异常提交拒绝恢复的保证。
- 大 RDB 的有界导入、压缩字符串支持和原子发布；不能靠扩大本轮 MSET 批量完成。
- TTL 历史回放、逻辑到期与物理回收的分离，仍未解决。
- 服务接线、写入积压保护、指标、自动重试策略、独占所有权/fencing、故障切换与 Cluster slot 迁移。

参考：[Redis 7.2 RDB 格式定义](https://github.com/redis/redis/blob/7.2/src/rdb.h)、[Redis CRC64](https://github.com/redis/redis/blob/7.2/src/crc64.c)、[Kvrocks DBSIZE 说明](https://kvrocks.apache.org/docs/faq/)。上述引用用于格式/接口依据，不代替本文固定版本的实测。
