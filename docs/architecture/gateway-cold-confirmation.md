# G6b-2c：Kvrocks 精确提交确认

后续：[G6b-2d 跨进程隔离确认](gateway-confirmation-rpc.md) 已增加显式确认 HTTP 模式；本文“不提供 RPC”描述 G6b-2c 阶段边界。正常业务服务、持续同步任务的自动确认与热删除仍未接入。

> 2026-09-08。已完成隔离确认闭环：真实 Redis 捕获 → 准备标记前的业务应用 → Kvrocks 提交确认 → 本地确认落盘 → 重连/冷层重启后重新核对。
> 范围：单分片、单执行者、独占冷 DB、无 TTL String。**不是完整冷热系统；没有业务切流、热副本删除、确认 RPC 服务或分布式接管。**

前置：[持久操作意图](gateway-eviction-intent.md)、[操作屏障](gateway-key-barrier.md)、[内部移除记录](gateway-eviction-envelope.md)。

## 1. 具体怎样实现

入口为 [confirmEviction](../../internal/gateway/eviction_confirmation.go)，由隔离测试调用，不对外暴露 HTTP/业务命令。它锁定本地操作对象和对应日志的 apply/GC 通道，执行以下步骤。

### 1.1 从真实日志证明“在确认哪一次操作”

读取持久操作意图和准备记录，在绑定任务身份的分段复制日志里重新定位原始 SET 准备标记。校验源地址、history、原始 RESP、前后 byte offset 和日志链式摘要。

不能只传一个 offset 或信任标记里的自报字段。找不到原始记录、日志已经缺失、身份或字节不匹配，均不返回确认。

### 1.2 只应用准备标记之前的业务命令

将现有分段应用器提取为可指定精确终点的 `applySegmentView`，新增受校验的 `walkThrough`；普通应用仍以原来的完整视图结尾为终点。

确认路径的终点是准备标记的前驱 checkpoint。它先预检，再复用原来的 RDB 导入、pending 恢复和逐项应用，不重发已经应用的 INCR。准备标记和之后的命令不按普通业务写执行。

实际测试中 Redis 是 `counter=41`，捕获 INCR 后变成 `42`，再写准备标记和 `after-barrier`。确认完成后 Kvrocks 中 counter 为 `42`，但不存在 after-barrier；Redis 中 counter 仍为 `42`，没有执行淘汰。

### 1.3 在 Kvrocks 内核对，再提交确认

新建冷连接时要求 writable standalone Kvrocks 2.15.0，核验：

- `rocksdb.write_options.sync=yes`
- `rocksdb.write_options.disable_wal=no`

随后 WATCH 以下 key：目标业务 key、同步 checkpoint、pending、anchor、该任务的确认记录。读取并验证：

- checkpoint 必须等于本次准备事件的精确前驱；pending 必须已完成且能由本地源日志证明。
- 业务 key 存在、为 String、无 TTL；值的 SHA-256 必须等于意图中的预期摘要。
- 确认 key 要么不存在，要么完整内容与本次确认一致；不能覆盖另一操作的确认，确认本身也不能有 TTL。
- 如果本地已有确认记录，冷层对应记录必须仍存在，不能静默补建掩盖回退。

校验通过后执行 `MULTI → SET 确认记录 → EXEC`，只接受精确成功响应。

**确认事务只有一个修改命令 SET，不重新执行 INCR，也不拼接任意业务命令。** 业务数据已经由前一步按同步 WAL 提交；WATCH 将数据和进度的核对结果绑定到此次确认提交。如果核对期间有相关写入，事务中止，不返回确认。

这不是“把数据重写一遍和 ACK 凑到一起”，也不是依赖 Lua/任意事务自动回滚。

### 1.4 确认内容与两个进度

冷层确认 key：`__pilot_sync_v1__:{task:<task>}:eviction-receipt-v1`。每任务当前仅支持一个未清理操作。

| 字段 | 绑定内容 |
|---|---|
| `identity_sha256` | task、源和冷目标端点 |
| `intent_sha256` | 操作 ID、epoch、源 history、key、预期值摘要及意图版本 |
| `previous` | 准备标记之前的普通业务 checkpoint |
| `prepared` | 将精确准备标记纳入链后的屏障 checkpoint |

普通同步 checkpoint **保持在 previous**，不把还没有实现的冷热位置切换伪装成普通回放。确认记录自身保存 prepared 屏障位置。冷层这两者在本原型中含义不同，不能混用，也不能拿 G6a-13 的采样差值代替确认。

### 1.5 本地落盘后才返回成功

Kvrocks 提交得到确定成功后，把相同确认作为第三条可选记录追加到本地操作日志，并 fsync。整个调用只有在冷层提交与本地持久记录都确定时才返回确认。

沿用原日志 magic；初始意图和准备记录格式不变，旧的两条记录文件仍能恢复。新读者允许第三条确认，但拒绝第四条、残缺帧、错误摘要或不匹配意图；旧读者遇到扩展尾部会拒绝，不会误判成功。单文件仍限制 512 KiB。

本地持有确认只是历史记录，**不是永久有效的删除许可**。重新打开本地文件后再次调用确认入口，会重新验证源日志与冷层状态。

## 2. 回复丢失和重启怎么处理

| 情况 | 实现行为 |
|---|---|
| EXEC 未发送就断连 | 返回错误且不给确认；重连后重新检查并提交 |
| Kvrocks 已提交但回复丢失 | 返回错误且不给确认；重连核对已存在的相同确认，不重复 INCR |
| 原连接上的 EXEC 很晚才到 | 恢复连接在 WATCH 下重新 SET 相同确认，令旧事务失效；测试验证旧 EXEC 返回 null |
| 已有确认的重复核对发生同样晚到 | 同值 SET 仍使旧 WATCH 失效；后端重启验证阶段另做了此项测试 |
| 数据、TTL、类型、checkpoint 不一致 | 拒绝，不生成本地确认 |
| 确认属于其他操作或带 TTL | 拒绝，不覆盖为当前确认 |
| 本地确认存在，冷层确认丢失 | 拒绝，不能靠本地文件放行 |
| 冷层成功，本地写入/fsync 结果不确定 | 本地句柄失效，不返回确认；重开核验存活帧，再在线重验冷层 |
| Kvrocks 强杀后重启 | 重新校验持久数据、确认和日志链；不只读取一个“成功”标志 |
| 冷层无法连接或上下文取消 | 返回错误，不返回可用确认 |

实测 Kvrocks 的 WATCH 中止返回 `$-1`，原有测试也允许 `*-1`。最初新增晚到测试只期待后者而失败；补齐两种 null 回复的测试判定后通过。运行代码始终只接受明确的成功事务响应，null 从未被当成成功。

## 3. 与现有服务的隔离

- `syncColdTarget` / client 增加内部确认命名空间开关，只有新确认入口显式启用；未加入服务配置。默认应用器不认识确认元数据，不会静默接着跑。
- 普通 `syncPendingCommand` 继续拒绝准备标记、MULTI/EXEC 和 UNLINK。正常应用遇到准备标记仍停下；没有把保留空间写入放进业务白名单。
- 新确认路径复用原来的 pending 机制处理之前的业务写；不修改其提交格式、不开启自动日志回收。
- 本地操作互斥和源日志 apply/GC 互斥只负责单进程内串行化，**不是分布式所有权**。
- `testDial` 仅为故障测试替换连接的内部注入点，没有运行配置入口；实际绑定地址、日志身份与目标校验仍保持原值。
- 新 API 不发送 DEL/UNLINK，不提供自动删除、回温、标记回收或业务准入放行。

未来分进程运行时，同步器应持有已验证源日志并产生确认；Gateway 接收的确认还需鉴权、归属与时效校验。**当前没有 Gateway ↔ syncer 的确认 RPC/任务调度接线，不能把函数返回值直接当作已经上线的分布式协议。**

## 4. 实验与结果

redis03 专用容器：

- Kvrocks：`redis-pilot-cold-confirm-aDLff4`，镜像 `apache/kvrocks:2.15.0`，512 MiB、128 PID、2 workers，独立容器数据目录 `/tmp/cold-confirm-data`。
- Redis：`redis-pilot-hot-confirm-aDLff4`，镜像 `redis:7.2.5`，128 MiB，与专用 Kvrocks 共享隔离网络命名空间；无宿主机端口。
- 未操作原有 redis-sentinel、redis-galaxy-2、redis-test-v7、kvrocks-test-kv，没有修改 Server/Agent/Proxy/Gateway 服务或业务路由。

[真实后端测试](../../internal/gateway/eviction_cold_integration_test.go) 的 15 个场景连续三轮通过：正常、EXEC 前丢连接、回复丢失、晚到、WATCH 冲突、值偏差、TTL、类型错误、checkpoint 损坏、其他确认、确认缺失、确认 TTL、连接不可用、取消、本地 fsync 不确定。每次从独占空测试 DB 开始，仅清理本测试创建的明确 key；不是压测。

强杀步骤：先完成真实源捕获及确认，将源日志、本地意图保存在专用容器 `/tmp/receipt-restart`，再 SIGKILL 该 Kvrocks 容器。退出码 `137`，`OOMKilled=false`。使用同一容器数据目录启动后重验通过；最终版本又连续三轮验证重启后的相同确认和旧同值事务失效。这里是一轮实际 Kvrocks 强杀，三次恢复核对，不是三轮强杀。

本地真实 Redis + 源标记/意图/确认校验/屏障/元数据/分段日志三轮 race 测试通过（44.509 秒）；本地不运行真实 Kvrocks 用例。真实 Kvrocks 测试为 Linux 普通测试二进制，不宣称它们也做了 race。

`go test ./...`、相关包 `go vet`、`git diff --check` 通过；全量测试中需要单独环境的后端用例按原规则跳过，不能把它等同于全平台集成测试全覆盖。

最终 Linux 测试二进制 SHA-256：`fe49ddc2d7494c75d2e44a3e12749ebc2a3b95dbfaa5424d1b86cb4ef30fd053`。本地位于 `/tmp/redis-pilot-cold-confirm-build.4KizLN/gateway.test`，redis03 副本位于 `/tmp/redis-pilot-cold-confirm.aDLff4/gateway.test`。容器、专用测试数据和二进制保留供复查；收尾停止测试容器，不设置开机自启。

## 5. 仍然不能据此宣称什么

本阶段完成了**指定 key/指定意图的冷层确认**，不是证明冷层永远等于热层最新值。后续若该 key 又发生写入，旧确认不能授权删除；真正执行器仍需持续持有热 key 屏障并核验源历史和归属。

尚未实现：确认的跨进程传输与鉴权、分布式 fencing、确认后的持久删除/冷驻留发布、回温、标记/确认回收、多未完成操作、Cluster 扩容/切主、TTL、复杂类型、多 key 原子语义以及 FULLRESYNC 后冷层完整数据保护。

WATCH 只检测观察窗口内的变化，不隔离之后的直接写入；要求冷 DB 独占且所有权稳定，不能当作多写者/多 Gateway 协议。epoch/摘要是匹配字段，不是租约或签名。同步 WAL 配置不等于已经验证真实设备掉电、磁盘损坏、冷副本回退或所有故障窗口。

正常热写没有接入此新入口。本设计把确认成本放在冷热转换阶段，但尚未测量生产接线后的延迟或资源成本，不能据此宣布“无性能损耗”或“可以上线淘汰”。
