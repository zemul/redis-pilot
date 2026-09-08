# G6a-3：原生事务的重试与过期回放验证

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

## 结论与交付边界

2026-09-08 继续验证“Redis 热写 + 异步同步 + Kvrocks 原生事务与进度一起提交”。没有修改 Redis/Kvrocks 源码，没有改变业务 value 格式，也没有部署生产冷层应用器。

- **无 TTL 计数器的断线重试探针通过**：提交前断线，重新连接后应用；提交成功但回复丢失，重新连接后跳过同一项，计数器只增加一次。
- **TTL 历史回放需要单独处理**：正常命令、无执行错误，也可能因冷层按照当前时间过期而产生错误结果。事务只保证提交的组合，不能恢复已经被过期机制丢掉的旧值。
- 这不是对异步路线的否定，也不要求立即采用版本化 value 封装；但不能把“原命令 + checkpoint”直接推广到首批所有过期命令。

代码为测试探针：`internal/gateway/sync_replay_test.go` 与 `sync_capture_test.go`。未接入 Gateway 启动、RDB 导入、PSYNC 增量应用、Server、Agent、Cluster 路由、淘汰或回温。

## 无 TTL 重试探针

限制：单独测试分片、唯一应用者、预置的永久整数 counter 和永久 checkpoint，不允许外部修改。checkpoint 记录测试历史 ID、序号和 INCR 命令摘要；序号只是测试计数，**不是 Redis 字节 offset**。

每次调用都新建连接，不保留内存中的应用进度：

1. WATCH counter 与 checkpoint，读取已提交 token。
2. 相同 token 返回 duplicate；历史不符、序号缺口或同序号内容异常拒绝。
3. 只接受已有、无 TTL、规范十进制且不会溢出的整数；检查在 MULTI 之前进行。
4. MULTI 中执行 INCR 和 SET checkpoint；检查完整 EXEC 结果。

故障代理分别在 EXEC 转发前断开、或读完真实 EXEC 回复后丢弃回复。第二次调用真正重新执行上述判断，不再是上一轮测试中人工跳过一条 INCR 的流程。

| 场景 | 第二次调用结果 | 观测状态 |
| --- | --- | --- |
| EXEC 尚未转发就断开 | applied | 41 → 42，序号 1 |
| EXEC 已成功，回复丢失 | duplicate | 仍为 42，序号 1 |
| 再次重投同一 token | duplicate | 不重复增加 |
| 后续合法项 | applied | 42 → 43，序号 2 |
| 错历史、序号缺口、错误摘要 | 拒绝 | 不修改进度 |
| 非整数、非规范整数、溢出、带 TTL | 在事务前拒绝 | 不修改进度 |

限制不能省略：不是多实例 fencing；没有模拟冷层切主、主机掉电、磁盘故障、并发抢占或任意 EXEC 错误恢复。只保存最后一个摘要，不提供任意旧项的内容验证。意外 EXEC 错误仍停止，不能只看 checkpoint 自动恢复。测试没有证明泛化 exactly-once，也没有测量性能；每次新建连接和 WATCH 仅为正确性实验。

## 实际 Redis 复制流与正常过期反例

隔离 Redis **7.2.5** 在原到期时间之前执行：

```text
SET counter 41 PXAT T
INCR counter                # 42，保留原 TTL
PERSIST counter             # 删除到期时间，最终永久值 42
```

真实 PSYNC 捕获确认这三条修改按 SET/PXAT、INCR、PERSIST 传播，INCR 不自动转成 `SET counter 42`。另一个 key 执行 SET/PXAT、INCR、PEXPIRE；最后一项实际传播为 PEXPIREAT。

Kvrocks **2.15.0** 组件测试用同样的命令形态，将 T 设置为已过去的绝对时间，模拟消费这些记录时已超过 T：

```text
MULTI
SET counter 41 PXAT 已过期时间
INCR counter
PERSIST counter
SET checkpoint 3
EXEC
```

实测 EXEC 返回成功 SET、INCR=1、PERSIST=0、成功 checkpoint；最终 counter=1 且没有 TTL。**没有任何错误响应，数据和进度也一起提交，但与源端永久值 42 不同。**

把 PERSIST 换成未来时间的 PEXPIREAT，续期成功，值仍然是 1 而不是 42。因此问题并不依赖扩展命令 PERSIST；首批计划中的 PEXPIRE 也会遇到。不能通过检查是否报错或使用绝对到期时间单独解决。

这是两个相互对应的组件实验：本地捕获真实源传播，远端用已过去的时间重现目标回放；**不是已完成端到端导入/同步**，没有伪称将同一份捕获日志直接送到了 Kvrocks。测试 PASS 表示预期反例得到复现，不表示 TTL 同步通过。

## 下一步

下述第 1 项已推进为 [G6a-4 无 TTL String 应用原型](gateway-string-apply.md)。它使用真实 RDB/复制位置，但为小规模正确性验证保留全状态恢复核对，不代表大容量或服务接线完成。

1. 保留原生数据格式和事务提交路线，优先实现无 TTL、有限 String 的应用基础，接实际历史/字节位置、受控初始化与日志输入。
2. TTL 继续作为产品目标，在开放前验证逻辑到期与物理回收如何分离：不能在后续已捕获修改仍可能依赖旧值时，提前物理清掉它。是否需要独立过期元数据、是否允许直接访问冷层、回收水位、源过期 DEL 和全量导入如何配合，**待定**。
3. 保持源命令执行时间的状态重建也是候选，但时间信息、重建状态成本与可靠性尚未解决；不默认 PSYNC 提供完整执行时间或最终值。
4. 不通过将绝对时间简单改回相对 TTL 来掩盖差异；那会引入到期时间延后的语义变化。

这轮不改变完整产品需求，不启用不安全的 TTL 回放，不以额外封装格式替代未完成的协议设计。

## 验证环境与收尾

- Redis03 复用上一轮独占实验容器 `redis-pilot-g6-commit-nsXnBj`，网络 none、无对外端口、独立数据目录，未接触现有 `kvrocks-test-kv`。
- 仍校验 Kvrocks 2.15.0、同步 WAL 开启且 WAL 未禁用；未更改引擎配置。
- 本轮二进制 `/tmp/redis-pilot-g6-commit.nsXnBj/gateway-replay-20260908.test`，复制到容器 `/tmp/gateway-replay.test`；上一轮只读挂载的二进制未替换。
- 远端二进制 SHA256：`23dfcbbf620cbe4b123be543a7d987e5f4ef0e71c62885bad67d4743d4a90427`。其仅用于 Kvrocks 实验；随后本地补充了 Redis 测试清理修复。
- 新 replay 阶段连续 3 轮通过，原 semantics 阶段回归 1 轮通过。原始输出见 [实验日志](../../benchmarks/g6-replay-20260908.log)。本轮未重复强杀 Kvrocks。
- 本地真实 Redis 捕获过期命令 3 轮 race 通过。扩展 TestSync 回归中曾出现临时目录清理失败；根据原 helper 仅强杀 Redis 父进程和 PSYNC 触发后台保存的行为，推断 RDB 子进程仍写文件是可能原因，没有对子进程做现场追踪。增加测试专用 SHUTDOWN NOSAVE 清理，再由原 helper kill/wait 兜底，随后真实 Redis TestSync 全部 5 轮 race 通过。此修复只用于隔离测试，不修改产品停机流程。
- 最终再次执行真实 Redis TestSync 5 轮 race、Gateway 全包普通 race（未开启隔离外部依赖的用例会跳过）、`go vet ./internal/gateway` 与 `git diff --check`，均通过。
- 实验容器已停止，退出码 0，未设置自启；测试数据/二进制保留，未删除。原有开发容器保持运行，Agent active。未部署 Gateway/Server，未切流，未提交 Git。

重跑（只限上述隔离测试容器）：

```sh
podman exec -e GATEWAY_TEST_KVROCKS_COMMIT=isolated -e GATEWAY_TEST_KVROCKS_ADDRESS=127.0.0.1:6666 -e GATEWAY_TEST_KVROCKS_PHASE=replay redis-pilot-g6-commit-nsXnBj /tmp/gateway-replay.test -test.run '^TestKvrocksCommitExperiment$' -test.v -test.count=3 -test.timeout=45s
GATEWAY_TEST_REDIS_SERVER=/opt/homebrew/bin/redis-server go test -race ./internal/gateway -run '^TestSync' -count=5 -timeout=60s
```

参考：[Redis 过期语义](https://redis.io/docs/latest/commands/expire/)、[Redis 事务与 WATCH](https://redis.io/docs/latest/develop/using-commands/transactions/)。结论以本文固定版本实测为准，不泛化到未经验证的引擎版本。
