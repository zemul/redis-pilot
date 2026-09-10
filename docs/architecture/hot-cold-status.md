# 冷热缓存实现状态

更新：2026-09-10。本轮实现新的 mode: cache；尚未提交，不代表正式发布或旧开发链路已切换。设计见 [冷热设计](hot-cold-gateway-design.md)。

## 已接入开发代码

- 固定单 Gateway + 热 Redis + 冷 Kvrocks，DB 0；独立前后端凭据、配置检查、预算、指标和心跳。
- invalidate / update_existing 两种写后策略；后者首版同步重建已有完整小缓存，不是原地镜像命令。
- 五种类型的有界完整缓存；默认 64KiB、256 个元素、4 个加载；大对象和带业务 TTL 的对象不缓存。
- 写冷、热失效/更新与同 key 回填串行；热失败换命名空间，冷结果未知用冷层 token CAS 隔离迟到写，重启启用新缓存代次。
- Redis 自身 allkeys-lru；热层禁用持久化，不恢复旧缓存。无同步器、主动淘汰器、布隆过滤器或全量 key 目录。
- BLPOP/BRPOP 的有界独立冷连接，仅允许固定 cold_only_prefixes；这些 key 不会缓存。
- 新模式 Pipeline 按连接顺序逐条完成；普通 passthrough 行为保持不变。

## 当前命令矩阵

| 类别 | 允许命令与选项 |
| --- | --- |
| 连接 | AUTH、QUIT、PING、ECHO |
| 通用 | 单 key DEL/UNLINK/EXISTS/TYPE/TTL/PTTL |
| String | GET、STRLEN、普通 SET（不带选项）、INCR/DECR/INCRBY/DECRBY、APPEND |
| Hash | HGET/HMGET/HGETALL/HLEN/HKEYS/HVALS/HEXISTS、HSET/HDEL/HINCRBY |
| List | LLEN/LINDEX/LRANGE、LPUSH/RPUSH/LPUSHX/RPUSHX、单元素 LPOP/RPOP、LSET/LTRIM/LREM |
| Set | SMEMBERS/SCARD/SISMEMBER、SADD/SREM、单元素 SPOP |
| ZSet | ZCARD/ZSCORE/ZRANK/ZREVRANK/ZCOUNT、按排名 ZRANGE/ZREVRANGE（可 WITHSCORES）、基础 ZADD/ZREM/ZINCRBY |
| 阻塞 | BLPOP/BRPOP，1～16 个冷层专用 key，timeout (0,60] 秒 |

更新已有缓存的写命令：SET、INCR/DECR/INCRBY/DECRBY、HSET/HDEL/HINCRBY、SADD/SREM、基础 ZADD/ZREM/ZINCRBY。其他写退回失效。复杂类型未缓存时不主动创建；普通 SET 允许建立小缓存。

未支持的命令、额外选项、内部保留 key 都明确拒绝。不是“Kvrocks 支持就直接透传”。原有透传模式有自己的白名单，不受此表扩展。

## 尚未完成

- 原地增量更新、连接池、吞吐/延迟压测、长期稳定性与生产资源验收。
- 动态阻塞 key 的缓存绕过/恢复、无限阻塞、更多阻塞命令、队列前缀策略迁移。
- 业务 TTL 写命令/选项、Hash 字段过期、多 key、事务、Lua。
- 分片、扩缩容、多 Gateway、Cluster 路由/故障切换，冷库回滚或备份恢复后的重新建立边界。
- 自动部署/切换旧开发链路；当前不允许直接替换旧 PSYNC 模式服务。

## 测试与环境边界

本地 Redis 测试验证共享协议与协调算法；另在 redis03 的独立 Kvrocks 2.15.0 实例进行真实冷层测试，不把 Redis 替身结果冒充 Kvrocks 结果。五种类型、两种策略、整数精度、Pipeline、并发计数、迟到写/CAS、丢写回复、失效失败、冷连接故障与恢复、队列等待与超时均已连续三轮通过。本地 cache 测试通过 race 检查。

另有独立 Gateway 二进制 SIGKILL 后重启的测试，不仅是对象重建；测试在进程停止期间更改冷值，验证存活的旧热缓存不会被新进程复用。该测试不等于冷库宕机恢复、整机重启或在线故障切换验收。

全仓 go test ./...、go vet ./... 已通过；Gateway、Store、共享 API 类型的 race 检查通过。未进行吞吐/延迟压测，不能据此承诺热写性能。

Kvrocks 2.15.0 的脚本状态回复、阻塞超时 nil 回复与 Redis RESP2 存在形状差异，已在新模式转换并验证。

此前旧同步代码/阶段文档已在 c0a3a26 清理，历史可从 19d35ec 查回。旧 SQLite 观测表未 DROP；既有未跟踪 deploy/、dump.rdb、session/ 原样保留。redis01-03 原有服务、旧冷热数据和 Proxy 路由没有切换。
