# 冷热缓存实现状态

更新：2026-09-12。mode: cache 开发实现，不代表正式发布。设计见 [冷热设计](hot-cold-gateway-design.md)。

## 已接入开发代码

- 固定单 Gateway + 热 Redis + 冷 Kvrocks，DB 0；独立前后端凭据、配置检查、预算、指标和心跳。
- invalidate / update_existing 两种写后策略；前者删除缓存，后者在已有完整缓存上安全增量更新，不再整对象重建。
- 五种类型分批完整回温；单对象大小/元素数上限及 per-type 大小配置已删除，默认 4 个加载；保留协议帧、内存预算和超时保护。带业务 TTL 的对象按剩余生命周期回温。
- 热层原始业务 key、原生类型，一个对象一个 key，无 data/complete、运行标识或热代次后缀。写冷、热失效/更新与同 key 回填串行；热失败先隔离，关闭旧服务端连接并清空专用热 DB 后恢复。冷结果未知仍用冷层固定 token CAS 隔离迟到写。
- Redis 自身 allkeys-lru；热层禁用持久化，不恢复旧缓存。无同步器、主动淘汰器、布隆过滤器或全量 key 目录。
- BLPOP/BRPOP 的有界独立冷连接，仅允许固定 cold_only_prefixes；这些 key 不会缓存。
- 新模式 Pipeline 按连接顺序逐条完成；普通 passthrough 行为保持不变。

## 当前命令矩阵

| 类别 | 允许命令与选项 |
| --- | --- |
| 连接 | AUTH、QUIT、PING、ECHO |
| 通用 | 单 key DEL/UNLINK/EXISTS/TYPE/TTL/PTTL、无条件 EXPIRE/PEXPIRE/PERSIST |
| String | GET、STRLEN、SET（EX/PX、NX/XX、KEEPTTL）、INCR/DECR/INCRBY/DECRBY、APPEND |
| Hash | HGET/HMGET/HGETALL/HLEN/HKEYS/HVALS/HEXISTS、HSET/HDEL/HINCRBY |
| List | LLEN/LINDEX/LRANGE、LPUSH/RPUSH/LPUSHX/RPUSHX、单元素 LPOP/RPOP、LSET/LTRIM/LREM |
| Set | SMEMBERS/SCARD/SISMEMBER、SADD/SREM、单元素 SPOP |
| ZSet | ZCARD/ZSCORE/ZRANK/ZREVRANK/ZCOUNT、按排名 ZRANGE/ZREVRANGE（可 WITHSCORES）、基础 ZADD/ZREM/ZINCRBY |
| 阻塞 | BLPOP/BRPOP，1～16 个冷层专用 key，timeout (0,60] 秒 |

上述白名单中的成功写命令均有更新路径（DEL/UNLINK 清理）：普通确定性局部修改直接更新，计数/浮点增量使用冷层最终结果，SPOP 按冷层选中成员删除，List 弹出核对返回值。热层缺失时不创建残缺对象；成功 SET 的完整新值可直接缓存，条件未满足不更新缓存。出现错误或无法安全更新时清理/隔离，绝不为了保持缓存而返回旧值。

未支持的命令、额外选项、内部保留 key 都明确拒绝。不是“Kvrocks 支持就直接透传”。原有透传模式有自己的白名单，不受此表扩展。

## 尚未完成

- 连接池、吞吐/延迟压测、长期稳定性与生产资源验收。
- 动态阻塞 key 的缓存绕过/恢复、无限阻塞、更多阻塞命令、队列前缀策略迁移。
- EXAT/PXAT、EXPIRE 条件选项、SET GET、Hash 字段过期、多 key、事务、Lua。
- 分片、扩缩容、多 Gateway、Cluster 路由/故障切换，冷库回滚或备份恢复后的重新建立边界。
- Server 自动分配 Gateway、discovery 路由接入、滚动升级；当前静态开发链路不等于控制面集成完成。

## 测试与环境边界

2026-09-12 增量更新与无大小准入验收：5000 字段、约 500KB Hash 分批回温后，HSET/HDEL/HINCRBY 验证原 key 完整驻留，未重新 HSCAN/HGETALL、DEL 或整对象重建；检查未修改字段和大于 2^53 的计数值。覆盖更新前 LRU 淘汰不创建残缺 Hash、所有白名单类型的增量效果（含随机 SPOP）、并发 HINCRBY 后 HGET 不倒退、热更新丢回复、分批回温中断清理。单对象限制配置从代码、示例和 redis03 运行配置中删除，旧字段显式拒绝。redis03 新 Gateway 已升级，实际 Proxy 链路两种策略的大 Hash 与故障恢复验收通过，默认 invalidate；通用协议帧和资源保护仍生效。

原 key 改造增补：热 Redis 的 DBSIZE/TYPE/原 key HGET 及内部前缀检查验证，一个对象只占一个原生 key、无 complete 标记；覆盖清空失败持续冷读写、迟到热回填被服务端断连隔离、恢复与冷写交错、发布错误清理，以及绝对过期时间防延迟回填。相关本地 cache race 与真实 Kvrocks 测试连续三轮通过。冷层 owner/cold-only-policy 两个固定 key 仍保留，不属于每对象缓存元数据。

2026-09-10 已在 redis03 部署常驻静态 Proxy → Gateway → Redis 7.2.5/Kvrocks 2.15.0，四个 systemd 单元均启用开机启动，默认 invalidate。两种策略通过端到端 Pipeline、并发计数、五种类型、小/大对象、阻塞队列、TTL、冷热服务中断、Gateway SIGKILL 重启和真实 LRU 压力验收；冷库在 Gateway 启动期间不可用 18 秒后恢复，Gateway 无需人工解锁自动恢复。详见 [部署记录](gateway-deployment.md)。未实测主机重启、性能或磁盘掉电恢复。

端到端测试发现并修复两项问题：systemd 的短时启动次数上限会阻止长故障恢复，现改为每 3 秒持续重试；Proxy 空闲独占连接在 Gateway 重启后可能残留失效连接，现借出前有限时 PING 探活，失败丢弃并新建，不重放业务请求。补充断连/无响应连接测试，相关 Proxy race 检查及全仓测试、静态检查通过。该 Proxy 修复已部署到 redis03 测试 Proxy，redis01 原 Proxy 未替换。

TTL 增补：两种策略下的 SET NX/XX、EX/PX、KEEPTTL、清除/保留/续期、EXPIRE/PEXPIRE/PERSIST、五种类型到期、非法 TTL 不改值、迟到回填保护，已在真实 Kvrocks 2.15.0 上连续三轮通过。本地 cache 测试连续三轮通过 race 检查。冷层时钟突跳、外部绕过 Gateway 改 TTL 不在保证范围。

本地 Redis 测试验证共享协议与协调算法；另在 redis03 的独立 Kvrocks 2.15.0 实例进行真实冷层测试，不把 Redis 替身结果冒充 Kvrocks 结果。五种类型、两种策略、整数精度、Pipeline、并发计数、迟到写/CAS、丢写回复、失效失败、冷连接故障与恢复、队列等待与超时均已连续三轮通过。本地 cache 测试通过 race 检查。

另有独立 Gateway 二进制 SIGKILL 后重启的测试，不仅是对象重建；测试在进程停止期间更改冷值，验证存活的旧热缓存不会被新进程复用。该测试不等于冷库宕机恢复、整机重启或在线故障切换验收。

全仓 go test ./...、go vet ./... 已通过；Gateway、Store、共享 API 类型的 race 检查通过。未进行吞吐/延迟压测，不能据此承诺热写性能。

Kvrocks 2.15.0 的脚本状态回复、阻塞超时 nil 回复与 Redis RESP2 存在形状差异，已在新模式转换并验证。

此前旧同步代码/阶段文档已在 c0a3a26 清理，历史可从 19d35ec 查回。旧 SQLite 观测表未 DROP；既有未跟踪 deploy/、dump.rdb、session/ 原样保留。redis01-03 原有服务、旧冷热数据和 Proxy 路由没有切换。
