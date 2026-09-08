# G6b-2a：内部降冷操作的复制标记实验

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

> 2026-09-08。已实现独立记录识别器，并在临时 Redis 7.2.5 上验证真实 PSYNC 内容和故障行为。
> **这是 G6b-2 的第一部分，不是完整淘汰协议。没有接 Gateway 或同步应用器，没有写 Kvrocks，没有实现精确冷层确认或 Gateway 持久恢复。**

前置：[单 key 操作屏障](gateway-key-barrier.md)。

后续 [G6b-2b 持久操作意图与准备标记](gateway-eviction-intent.md) 已补充只准备不删除的事件与本地操作日志恢复；冷层 ACK 和删除派发协议仍未实现。

## 1. 这一步解决什么

先回答一个可实验的问题：不改 Redis，能否在复制流里明确标识一次内部热副本移除，而不把同 key 后来的业务 `DEL` 一起忽略？

新增代码：

- [eviction_envelope.go](../../../../internal/gateway/eviction_envelope.go)：仅识别与调用方预期操作严格匹配的完整记录组，输出源位置与摘要，不执行网络或磁盘操作。
- [eviction_envelope_test.go](../../../../internal/gateway/eviction_envelope_test.go)：身份、顺序、偏移、残缺、改写、二进制 key 与边界测试。
- [eviction_envelope_integration_test.go](../../../../internal/gateway/eviction_envelope_integration_test.go)：仅测试文件内包含内部脚本，在测试自己创建的 Redis 中执行。

结论：在本轮 Redis 7.2.5 隔离环境中，这个候选记录格式可以被明确识别；错误和残缺情况可以拒绝。它并不证明冷层已有正确值，也不证明写入者有权发起淘汰。

## 2. 候选记录格式与实际复制内容

实验描述符包含：格式版本、task ID、128 位操作 ID、归属 epoch、源复制 history、二进制 key、预期值的 SHA-256。epoch 目前仅是测试字段，没有租约或 fencing 功能。

标记 key 使用测试保留前缀 `__pilot_sync_v1__:{g6b-probe}:evict:<task>:<operation>`；标记值为 `begin:<描述符>` / `complete:<描述符>`。描述符采用固定 JSON 结构，key 用 base64 保留空 key/非 UTF-8 内容。

测试脚本先检查：key 为 String、没有 TTL、值与预期字节完全相等；标记不存在。通过后依次 SET 开始标记、UNLINK 热 key、SET 完成标记。不清理标记。

实测正常操作的复制内容为：

```text
MULTI
SELECT 0            # 首次写入时实测出现；已选定 DB 0 时不出现
SET <marker> begin:<intent>
UNLINK <business-key>
SET <marker> complete:<intent>
EXEC
```

识别器只允许精确的五条或六条记录。六条时仅允许 `MULTI` 后出现一次 `SELECT 0`；不接受其他 DB、其他位置、额外命令或多 key UNLINK。五条形式的调用方必须已经确认复制上下文为 DB 0；当前不是通用多 DB 流解析器。

首轮真实测试暴露了 `SELECT 0` 插在事务内部的情况，原先假设固定五条的识别被测试拒绝；修正为上述两种精确形态后通过。没有通过“过滤所有 SELECT”等宽松处理掩盖差异。

Redis 官方说明效果复制会把脚本实际修改数据的命令组成事务发送给复制流，参见 [Scripting with Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)。脚本显式调用 `redis.replicate_commands()`；接口语义参见 [Lua API](https://redis.io/docs/latest/develop/interact/programmability/lua-api/#redisreplicate_commands)。本轮只实测 7.2.5，不将文档上的版本支持等同于项目 Redis 5/6.2/7 矩阵已通过。

## 3. 识别与确认必须分开

`inspectEvictionEnvelope` 接受独立提供的预期描述符、前一条源位置和完整记录组，检查：

- task、操作 ID、epoch、history、key、值摘要完全匹配；不是从收到的标记中自行生成“预期”。
- 固定命令结构和参数的原始 RESP 字节完全匹配。
- 同源、同 history，Redis 字节 offset 连续且不溢出；只在完整 EXEC 之后输出结果。
- 输出绑定起始位置的原始记录 SHA-256，以及描述符 SHA-256；不保留 key/value 缓冲区。

输入仅允许五或六条记录，key 最长 64 KiB；错误不返回部分结果或原始 key/value。当前是完整组识别函数，不承担流式缓存；以后组装器必须额外限制缓存/等待/日志保留预算。

这个结果刻意命名为源记录 evidence，不叫冷层 ACK：

- 它没有查询或写入 Kvrocks。
- 摘要不是签名，没有权限认证能力。
- 它没有核验真实租约、冷层 checkpoint、操作日志或数据持久化。
- 预期描述符目前来自测试代码，尚未有可信、持久、带鉴权的提供渠道。
- 有权限伪造相同保留 key/命令的写入者可以构造相同记录组；禁止业务访问保留空间与禁止后端绕写仍是上线前置条件。
- 脚本比较预期值原文，识别器比较描述符中的 SHA-256；两者一致性目前由测试构造保证，未来可信执行器必须自行计算，不能照收客户端摘要。

现有同步器没有调用此识别器。`syncPendingCommand` 继续拒绝 MULTI/EXEC、UNLINK 和保留命名空间写入，普通 `DEL` 仍按业务删除处理。没有把控制事务里的 UNLINK 单独跳过后继续回放。

## 4. 真实 Redis 实验结果

所有场景使用独立临时 Redis、随机 loopback 端口、测试密码与临时目录，禁止自动淘汰。RDB 定时保存/AOF 关闭；复制初始化仍会生成自己的快照。测试结束关闭 Redis，不操作现有实例或仓库中的 dump.rdb。

| 场景 | 实测结果 |
|---|---|
| 正常移除，首次复制写入 | 一组含 SELECT 0 的六条记录；热 key 不存在，完成标记存在 |
| 复制流已选 DB 0 | 一组五条记录，无重复 SELECT |
| 后端执行后丢失回复 | 客户端看到 EOF；测试侧确认后端完成；重复操作返回已移除，不再产生第二组删除 |
| 值已更新、带 TTL、非 String、已有冲突标记 | 删除前拒绝，没有新开始/完成标记或 UNLINK |
| 开始标记后注入脚本错误 | 热值仍在，开始标记保留，无完成标记；拒绝认定完整操作 |
| UNLINK 后注入脚本错误 | **热值已消失**，开始标记保留，无完成标记；拒绝认定完整操作 |
| 部分失败后再次尝试 | 标记冲突，拒绝盲目补做/清理 |
| 完成后 key 被重新创建，再重试旧操作 | 拒绝旧操作，不删除新值 |
| 同 key 随后发生普通 DEL | 实际复制到普通 DEL；原有逻辑转换仍产生业务删除，不因旧内部标记而忽略 |

两种中途错误都证明：不能把脚本报错当作没有副作用。Redis 的原子执行与事务回滚不是同一回事；相关背景见 [Redis 关于事务回滚的说明](https://redis.io/blog/you-dont-need-transaction-rollbacks-in-redis/)。

另外使用真实捕获的六条记录，在每个不完整前缀处关闭、重开本地 capture journal：恢复前缀均被识别器拒绝；追加完整余下记录才可匹配。这使用合成的快照锚点，仅验证本地日志记录的恢复边界，**不是源 Redis 强杀/AOF 恢复，不是 Gateway 持久意图恢复，也不是冷层恢复**。

## 5. 下一步的精确确认与持久恢复设计

不得将本实验脚本直接接为业务淘汰命令。它为观察复制行为而先删临时热数据，没有等待真实冷层确认。真实协议至少还缺：

1. **持久意图**：分配不可复用的操作 ID，记录目标 key、预期版本/值、源历史和归属，持久化后才能派发。进程重启先恢复意图再放行。存储格式/位置待定，不新增 Server 高频 per-key SQLite 表。
2. **删除前的准备屏障**：在 G6b-1 排空后产生可定位的准备事件，同步器按顺序应用先前业务写，并给出与准备事件绑定的冷层提交确认。当前 begin 标记与 UNLINK 同组，不能拿这个事后标记替代删除前确认；准备事件及元数据布局待定。
3. **验证冷层确认**：校验源 history、operation ID、归属、记录位置/摘要和冷层逻辑值；数据与确认记录必须满足已经验证的 pending/提交恢复协议。不得复用 G6a-13 的进度样本作为 ACK。
4. **完整组回放与恢复**：应用器在记录完整、意图可信且确认有效时，才把内部移除解释为“保留冷层逻辑数据，更新位置状态”；残缺/未知组应停在操作边界并保留证据。禁止把事务记录逐条当作业务命令执行。
5. **冷驻留及恢复后放行**：热副本消失后只能进入已验证的冷路径，不能回普通透传。begin-only、删除结果未知、源 history 改变和冷层提交不确定等状态都要分别核对。
6. **标记回收与迟到操作隔离**：当前测试标记永久保留，仅因为数据量极小。生产不能为每个冷 key 永久在 Redis 保存一个大描述符；目标应把临时标记限制在未完成操作集合。何时清除、清除后如何拒绝旧请求及操作 ID 复用，需要和持久驻留/归属协议一起解决，不能直接加 TTL。

仍待定/未测：多执行者和旧连接 fencing、分片切主与 FULLRESYNC 后冷数据保护、操作元数据预算、Redis 内存不足/磁盘故障/强杀、Kvrocks 提交故障、标记鉴权、Cluster 同 slot 元数据布局、TTL、复杂类型、多 key 与业务 Lua。固定 `{g6b-probe}` 前缀只用于本轮单实例实验，不能保证与任意业务 key 同 slot。

## 6. 验证方式

```sh
GATEWAY_TEST_REDIS_SERVER=/opt/homebrew/bin/redis-server go test -race ./internal/gateway -run '^(TestEvictionEnvelope|TestKeyBarrier)' -count=3
go test ./internal/gateway ./internal/syncer ./cmd/syncer
go vet ./internal/gateway
```

不设置 `GATEWAY_TEST_REDIS_SERVER` 时，真实 Redis 子测试会跳过；其余单元测试仍运行。上述路径是本机实际 Redis 7.2.5，不是要求开发服务器采用该路径。本轮没有远端部署、没有启动长测，也没有测量热写延迟。

本轮实际结果：真实 Redis 实验与屏障测试组合的三轮 race 通过（9.624 秒）；Gateway/同步器/同步器入口回归通过；`go vet ./internal/gateway`、`git diff --check` 通过。首次探针中固定五条的假设曾被实际 SELECT 0 记录击中，此处记录的是修正后的结果。
