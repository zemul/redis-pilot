# G6a-2：Kvrocks 冷层提交实验

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

## 结论

Kvrocks 2.15.0 的事务正常路径具备“数据与进度同批提交”的可用基础，但 **不能把任意变更命令和 checkpoint 拼进 MULTI/EXEC 就当作可靠同步协议**。执行期错误不会触发整笔事务回滚；Lua 也不自动提供错误回滚。

本轮新增的是故障/语义实验 `internal/gateway/sync_kvrocks_test.go`，不是冷层应用器。没有把 G6a-1 捕获日志接入 Kvrocks，没有开启回温、淘汰或 Cluster 路由。

## 实验环境

- 主机：redis03；只创建专用容器 `redis-pilot-g6-commit-nsXnBj`，未操作已有 `kvrocks-test-kv`。
- 镜像：本机已有 `docker.io/apache/kvrocks:2.15.0`，镜像 ID 前缀 `20133e9aeae5`，日志中的构建 commit 为 `ea617971`。
- `--network none`，不映射端口；测试进程通过容器内部 `127.0.0.1:6666` 访问。
- 独立数据目录：`/tmp/redis-pilot-g6-commit.nsXnBj/data`，不挂载任何已有实例目录。
- 容器内存 384MiB，workers 2，block cache 32MiB，write buffer 8MiB × 2；本轮不是压测，不据此推导性能。
- 实测前由测试程序校验 `rocksdb.write_options.sync=yes`、`rocksdb.write_options.disable_wal=no`。
- 测试 key 统一前缀 `__pilot_g6_commit_test__:{single-slot}:`。相同 hash tag 只是为未来同 slot 协议保留条件；本轮不是 Cluster 模式测试。

首次启动因新目录权限与镜像默认 UID 999 不匹配，在打开日志文件时失败。修正**专用测试目录**属主后启动成功，不涉及已有实例。测试结果均来自修正后实例。

## 源码依据

[cmd_txn.cc](https://github.com/apache/kvrocks/blob/v2.15.0/src/commands/cmd_txn.cc) 中 EXEC 调用 BeginTxn → ExecuteCommands → CommitTxn；注释明确执行期错误不阻止其他命令提交，与 Redis 的事务错误语义一致。
[storage.cc](https://github.com/apache/kvrocks/blob/v2.15.0/src/storage/storage.cc) 中事务累积到 WriteBatchWithIndex，CommitTxn 交给 RocksDB 写入。
这说明有底层同批提交基础，但不能推导出“所有业务命令成功，否则全部撤销”。

## 结果

| 场景 | 观测结果 | 意义 |
| --- | --- | --- |
| 正常 EXEC 回复被中间代理丢弃 | 客户端连接失败；重连读到 value=1、checkpoint=1 | 响应丢失时可以读回目标进度，不能盲目重发 INCR |
| MULTI 入队后、EXEC 前断开 | value=0、checkpoint=0 | 尚未执行的队列未提交 |
| INCR 输入为非整数，后接 SET checkpoint | EXEC 第一项报错，第二项成功；value 不变，checkpoint=1 | **反例：进度前进但修改没成功** |
| EXEC 内脚本先 INCR，再注入错误，之后才写 checkpoint | 脚本报错；value=1、checkpoint=0 | **反例：数据修改提交但进度没前进**；仅套 Lua 不足以解决 |
| WATCH 后由另一连接修改被观察 key | EXEC 中止，counter 不增加 | 可用作并发检查基础，不能替代写入 fencing |
| 64 次已确认提交后强杀 Kvrocks | `SIGKILL`，容器退出码 137、非 OOM；同目录启动后 value=64、checkpoint=64 | 验证启用同步 WAL 时该进程故障场景的恢复 |

强杀恢复后，测试读取 checkpoint=64，模拟重投第 64 项时不再发送 INCR，再执行第 65 项得到 value=65、checkpoint=65。
这是**读回进度后人工编码跳过重放的实验流程**，并非已经实现自动幂等应用器、复制 epoch 校验或 payload 防篡改校验。

语义用例连续 3 轮通过；外部强杀恢复执行两次，其中最后一次完整日志与语义复测一起归档于 [原始输出](../../benchmarks/g6-kvrocks-commit-20260907.log)。
测试二进制 SHA256：`9cbf9462e3e76ac7cdf9fdf8aa3d29c625defde6e3c5e542301244837659fe05`。

## 尚未证明的事情

- 不能将 SIGKILL 测试等同于主机掉电、磁盘满/损坏、写入中途故障、冷层副本切换或所有崩溃窗口的证明。
- 没有直接在 RocksDB 内部提交前后注入崩溃，也未验证真实存储设备的 fsync 行为。
- 没有完成 RDB 导入、增量转义/应用、绝对过期语义、事务边界保留、跨 slot、目标并发应用和错误后的自动恢复。
- 不允许仅凭 EXEC 收到一整个数组响应就更新本地进度；需要检查所有执行结果。但**检查回复本身也无法补救已经错误提交的 checkpoint**。

## 对实现路线的约束

保持 Redis 热写 + 异步冷层路线，不修改引擎源码；本实验否定的是“无条件拼装原命令 + checkpoint”的简化实现，不是否定所有事务用法。

继续优先验证 **Kvrocks 原生数据格式 + 受限命令事务 + 同事务保存进度**，不是立刻改成版本化 value 封装。执行期错误反例不等于正常路径必然失败，也不是修改 Redis/Kvrocks 的充分理由。

2026-09-08 已补充无 TTL 计数器的实际重试探针，并确认正常业务命令在延迟回放时存在独立的 TTL 语义问题，见 [G6a-3 回放边界验证](gateway-replay-validation.md)。这两件事必须分开：前者验证提交恢复，后者验证历史状态是否还能正确参与后续运算。不能因为重试探针通过就宣布包括 TTL 的同步完成。

下一步先以无 TTL 的受限 String 范围建立应用基础；**只是阶段实验范围，不是擅自取消最终产品的 TTL/常见命令目标**。TTL 的逻辑过期与物理回收分离、状态重建等候选方案需要单独评估。元数据命名、存储放大、直接读取冷层的语义及清理条件待定，尚不选择或实现版本化 value 封装，也不宣布 exactly-once。

## 重跑与收尾

测试默认跳过，必须显式设置 `GATEWAY_TEST_KVROCKS_COMMIT=isolated`、回环地址以及阶段：

```sh
# 仅在独立、无业务数据的 Kvrocks 测试环境中执行
GATEWAY_TEST_KVROCKS_COMMIT=isolated GATEWAY_TEST_KVROCKS_ADDRESS=127.0.0.1:6666 GATEWAY_TEST_KVROCKS_PHASE=semantics ./gateway.test -test.run '^TestKvrocksCommitExperiment$' -test.v
```

强杀实验必须按 `crash_prepare` → 外部 SIGKILL 专用 Kvrocks 进程 → 相同数据目录启动 → `crash_verify` 的顺序，不能在两个阶段之间清库。
测试容器已停止，没有设置开机自启；容器、专用数据目录和测试二进制保留以便复查，没有删除数据。现有开发实例未重启，Gateway/Server 未升级或切流。
