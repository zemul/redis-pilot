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
| 通用 | DEL/UNLINK/EXISTS（支持多个 key）、单 key TYPE；TTL/PTTL/EXPIRETIME/PEXPIRETIME；EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT（NX/XX/GT/LT）、PERSIST |
| 批量 String | MGET、MSET、MSETNX；保留参数顺序、重复 key 和原生整批原子语义，仍限定同一个冷后端 |
| 跨 key | RENAME/RENAMENX、SMOVE、LMOVE/RPOPLPUSH；SUNION/SINTER/SDIFF 及对应 STORE；ZUNION/ZINTER/ZDIFF 及对应 STORE、ZRANGESTORE、SINTERCARD/ZINTERCARD |
| 全库扫描 | SCAN（MATCH/COUNT/TYPE），固定冷层游标，过滤内部控制 key；KEYS 仅 Gateway 直接入口支持，Proxy 仍按安全策略拒绝 |
| 事务 | MULTI/EXEC/DISCARD/WATCH/UNWATCH，支持矩阵中非阻塞业务命令及 Lua；同连接排队，原生冷事务加自然过期兼容检查 |
| 脚本 | EVAL/EVALSHA/EVAL_RO/EVALSHA_RO；SCRIPT LOAD/EXISTS/FLUSH；声明全部 KEYS，受保护业务命令和 Lua 基础库，非任意管理命令透传 |
| String | GET/GETRANGE/SUBSTR/STRLEN；SET（EX/PX/EXAT/PXAT、NX/XX、GET、KEEPTTL）、SETNX/SETEX/PSETEX/GETSET/GETDEL/GETEX（EX/PX/EXAT/PXAT/PERSIST）；INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT、APPEND/SETRANGE |
| Hash | HGET/HMGET/HGETALL/HLEN/HKEYS/HVALS/HEXISTS/HSTRLEN；HRANDFIELD（count、WITHVALUES）；HSCAN（MATCH、COUNT）；HSET/HMSET/HSETNX/HDEL/HINCRBY/HINCRBYFLOAT |
| List | LLEN/LINDEX/LRANGE、LPOS（RANK/COUNT/MAXLEN）；LPUSH/RPUSH/LPUSHX/RPUSHX、LPOP/RPOP（可 count）、LSET/LTRIM/LREM/LINSERT |
| Set | SMEMBERS/SCARD/SISMEMBER/SMISMEMBER、SRANDMEMBER（可 count）、SSCAN（MATCH、COUNT）；SADD/SREM/SPOP（可 count） |
| ZSet | ZCARD/ZSCORE/ZMSCORE/ZRANK/ZREVRANK（可 WITHSCORE）/ZCOUNT/ZLEXCOUNT；ZRANGE（BYSCORE/BYLEX/REV/LIMIT/WITHSCORES）、ZREVRANGE、ZRANGEBYSCORE/ZREVRANGEBYSCORE、ZRANGEBYLEX/ZREVRANGEBYLEX（对应 LIMIT/WITHSCORES）；ZRANDMEMBER（count、WITHSCORES）、ZSCAN（MATCH、COUNT）；ZADD（NX/XX/GT/LT/CH/INCR）/ZREM/ZINCRBY、ZPOPMIN/ZPOPMAX（可 count）、ZREMRANGEBYRANK/BYSCORE/BYLEX |
| 阻塞 | BLPOP/BRPOP，1～16 个冷层专用 key，timeout (0,60] 秒 |

上述白名单中的成功写命令均有更新路径（DEL/UNLINK/GETDEL 清理）：普通确定性局部修改直接更新，计数/浮点增量使用冷层最终结果，SPOP/ZPOP 按冷层选中成员删除，List 单个/批量弹出核对二进制返回值。ZADD 条件/INCR 根据冷层修改后的对应成员分数更新，不重新扫描整对象。热层缺失时不创建残缺对象；SET/SETNX/SETEX/PSETEX/GETSET 成功写入的完整新字符串可直接缓存，条件未满足不更新缓存。出现错误或无法安全更新时清理/隔离，绝不为了保持缓存而返回旧值。

HSCAN/SSCAN/ZSCAN 始终走冷层，游标不会因热命中、回温、淘汰而切换引擎；扫描不是快照，COUNT 是提示，MATCH 能力遵循 Kvrocks 2.15 的实现，不能据此承诺所有 Redis glob 模式。TTL 与绝对过期查询也始终读冷层。随机查询允许每次结果不同。

验收基线为热 Redis 7.2.5 + 冷 Kvrocks 2.15.0，不能据此承诺 Redis 5 热层支持所有新增选项。新增版本、字段 TTL、位图/Bitfield/HyperLogLog、Kvrocks 自定义命令不属于这张矩阵。LMPOP/ZMPOP、更多阻塞变体等未列命令仍拒绝；事务仅接受本矩阵非阻塞业务命令。Lua 需要声明全部访问 key，不能访问管理命令、未声明 key 或内部控制 key；不是所有 Redis 模块/脚本扩展的兼容实现。

批量处理：锁桶去重后按固定顺序获取，和单 key 命令共用锁，避免重复 key、桶碰撞或反序请求死锁；等待失败释放已持有的部分锁。MGET 只有全部 key 可缓存且都命中时才用热侧一次 MGET；任何缺失或包含冷层专用 key 时整批读冷层一次 MGET，不拼接冷热结果，保留不存在/非 String 为 nil 和重复位置。读冷后仅对非 nil、可缓存 String 尝试回温。多 key EXISTS 始终查冷，不能把 LRU 未命中当作不存在。

MSET/MSETNX/多 key DEL/UNLINK 在 owner 校验的同一冷脚本中只执行一次原生命令，不循环 SET/DEL；MSET 清除旧业务 TTL，重复 key 以最终值为准；MSETNX 任一已存在则整批不写，返回 0 时不改缓存。成功后 invalidate 整批删缓存，update_existing 的 MSET/MSETNX 发布各 key 的完整 String 最终值；所有相关锁保持到全部缓存处理完或热层已隔离，单 key GET 也看不到处理中间态。多 key DEL/UNLINK 两种策略均删缓存。部分热更新/失效失败就隔离整个热层并自动恢复；冷写回复丢失返回结果未知，不重放。参数、保留 key 校验覆盖整批，不能先写前半批再发现后半批非法。当前不涉及跨分片/跨节点事务。

Kvrocks 2.15 对重复删除 key 的计数可能与 Redis 不同，DEL/UNLINK 在发送冷命令前按 key 去重；EXISTS 保留重复 key 的逐次计数，MGET 保留原始顺序及重复位置。冷层不可用时，全热命中的 MGET 仍可读；只要需要冷层，就整批报错，不返回半份结果。

Kvrocks 差异处理：浮点自增在冷脚本内用其返回字符串写回相同字段/字符串（保留业务 TTL），避免返回值与存储值格式不一致，热层使用同一结果；计算精度仍以冷引擎为准，不承诺与 Redis long-double 在所有极端输入下逐位相同。条件 EXPIRE 在冷侧固定脚本中原子判断，限制到 Lua 可精确比较的毫秒/绝对时间范围 ±(2^53−1)，超界明确拒绝，不静默取近似值。LPOS 索引、随机成员单值、带 count 的空 List 弹出及 WITHSCORE 空排名修正为原生 RESP2 形状。

未支持的命令、额外选项、内部保留 key 都明确拒绝。不是“Kvrocks 支持就直接透传”。原有透传模式有自己的白名单，不受此表扩展。

## 尚未完成

- 连接池、吞吐/延迟压测、长期稳定性与生产资源验收。
- 动态阻塞 key 的缓存绕过/恢复、无限阻塞、更多阻塞命令、队列前缀策略迁移。
- Hash 字段过期、位图/Bitfield/HyperLogLog、LMPOP/ZMPOP 等未列出的扩展，以及不同引擎所有极端输入的逐位兼容。
- 分片、扩缩容、多 Gateway、Cluster 路由/故障切换，冷库回滚或备份恢复后的重新建立边界。
- Server 自动分配 Gateway、discovery 路由接入、滚动升级；当前静态开发链路不等于控制面集成完成。

## 测试与环境边界

跨 key/事务/Lua 验收代码在 internal/gateway/cache_advanced_test.go：两种策略下与原生 Redis 对照 RENAME/RENAMENX、列表移动、集合运算/STORE；SCAN 不暴露控制 key；事务排队、WATCH 写冲突及自然过期、旧 owner EXEC 隔离、执行错误继续后续命令、NOSCRIPT 执行期错误；Lua 只读限制/源码缓存/沙箱越界、写后报错、私有写记录不受用户改表影响；脚本/事务并发批量读不混值、热更新失败和冷层已执行但丢回复不重放。5000 字段 Hash 的原地更新检查新增脚本 HSET 和事务 HSET，验证没有 HSCAN/HGETALL、DEL 或全量重建。实际 Proxy 验收脚本包含事务内 PING/ECHO/UNWATCH、WATCH 冲突、本地拒绝触发 EXECABORT，以及 Lua/集合运算/列表移动/SCAN。新补偿方案和边界见 [设计](hot-cold-gateway-design.md)。

本轮结果：真实 Kvrocks 2.15 全部 TestCache 多轮通过；本地全部 cache race、Proxy session/proxysvc race、全仓测试与静态检查通过。redis03 实际 Proxy 链路两种策略完整验收通过，覆盖命令、5000 字段 Hash、LRU、冷热中断、Gateway 重启和长冷故障恢复；Proxy 与 Gateway 已更新。Kvrocks WATCH 自然过期和嵌套脚本状态回复差异是在真实冷层测试中发现并修复，不将 Redis 替身结果代替 Kvrocks。Lua 有限小数返回按 Redis 向零截断；非有限或超 RESP int64 范围的 Lua 数值回复明确报错，不依赖不同 CPU 的浮点强转溢出结果。

基础批量验收：internal/gateway/cache_multi_test.go 覆盖两种策略的原生 Redis 对照、重复/空/二进制 key、MGET 非 String 和不存在为 nil、MSET 清除 TTL、MSETNX 全部不写与并发竞争、冷热混合/冷层专用 key、全热命中下冷故障可读、部分未命中时冷故障整批报错、锁桶碰撞与反序/取消释放、MSET/MGET 并发不混读、单 key GET 不穿过批量更新、部分热失败、冷写丢回复、旧 owner 批量写隔离与真实前端 Pipeline。真实 Kvrocks 批量专项连续三轮通过，补充边界后全部 TestCache 回归通过；本地 cache 竞态测试及新增批量专项、全仓测试和静态检查通过。redis03 实际 Proxy 链路六个批量命令、两种策略和原有故障恢复验收通过，恢复默认 invalidate，临时独立测试冷库容器清理，常驻业务冷库未清空。大 Hash 驻留测试的热操作等待放宽为 500ms，仅用于隔离竞态检测/编译负载导致的正常缓存绕过，不修改业务超时。

本轮单 key 命令扩展：新增原生 Redis 对照测试，在两种策略、热命中/热缺失下逐项比较返回值与后续对象内容；更新模式额外直接检查热对象，防止用“清缓存后读冷成功”冒充增量更新。覆盖 SET GET/NX/XX（包括旧值恰好是 OK 和 key 不存在）、浮点写回、条件/绝对 TTL、批量 POP 的二进制数据、ZADD 条件/INCR、空回复、错误后读、以及跨回温/淘汰的冷层游标。5000 字段 Hash 验收增加 HMSET/HSETNX/HINCRBYFLOAT，无整对象重建。五种类型分别注入热更新/失效失败，SET GET 注入冷层已执行但丢回复，均验证后续不读旧缓存。测试文件为 internal/gateway/cache_compat_test.go；实际 Proxy 验收脚本 scripts/verify-cache-dev.py 同步扩展。

最终验收：全仓 go test ./...、go vet ./...、本地 Redis 的全部 TestCache 竞态测试通过；真实 Kvrocks 全部 TestCache 连续三轮通过，测试夹具调整后又通过一轮；redis03 常驻 Proxy 链路两种模式通过扩展命令、大 Hash、并发、Pipeline、LRU、冷热中断、进程重启及长冷故障自动恢复验收，默认恢复 invalidate，四个服务 active/enabled、readyz 为 ok。首次本地同时执行多套测试出现 macOS 临时端口耗尽（含未改动 Server 测试）；已将扫描夹具改为批量写入并分开重跑，未改业务连接超时。旧 TTL 测试的正常读取窗口从 100ms 改为 500ms，并相应在 550ms 后验证过期，避免把测试调度延迟误判为功能故障。这些结果不代替吞吐或长期稳定性验收。

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
