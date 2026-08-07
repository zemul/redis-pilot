# Redis 命令路由分类参考

> 状态：设计阶段的**规格**（spec），非代码。它是 [proxy-design.md](proxy-design.md) §6
> 路由表的完整展开，供 M1 实现 `internal/proxy/route/` 时逐条誊写为 Go map + 表驱动
> 单测。本文结构刻意做成"一行 = 一条命令 + 分类"，到代码的翻译应是机械的。
>
> 命令集以 Redis 7.x（含 Stream、Functions）为准。Kvrocks 子集不额外列，按同名命令
> 归类即可。不在本表内的未知命令按 §2 兜底规则处理。

## 1. 分类枚举

| 分类 | 含义 | 路由动作 |
|------|------|---------|
| `WRITE` | 修改数据 | master 池 |
| `READ` | 只读 | 读池（master + replica 按 weight）；会话一致性窗口内 → master |
| `BLOCKING` | 可无限阻塞（含阻塞写、阻塞读） | **pin 独占** + 走 master |
| `TXN` | 事务控制 | **pin 独占** + 走 master |
| `SUBSCRIBE` | 进入/维持推送模式 | **pin（SUBSCRIBE 模式）** + 走 master |
| `MASTER_ONLY` | 语义上必须固定落主/落单点 | master 池（不 pin，除非另有阻塞语义） |
| `CONN_STATE` | 连接级状态 | proxy 终结（AUTH）或 pin（SELECT）；见 §5 |
| `ADMIN` | 服务器管理/信息 | master 池（部分可 DENY） |
| `DENY` | 拒绝执行 | 直接回错误，不转发 |

> 记号：分类是命令的**默认归类**；带参数的命令可能因参数改变归类（如 `XREAD` 带
> `BLOCK` 升级为 `BLOCKING`），这类在 §4 用"判定规则"覆盖，优先级见 §2。

## 2. 判定优先级与兜底规则

对一条命令 `argv`，按以下顺序判定，先命中先定：

1. **DENY 名单**（§8）→ `DENY`。
2. **CONN_STATE**（`AUTH`/`SELECT`/`HELLO`/`RESET`/`PING`…，§5）→ 按 §5 处理。
3. **SUBSCRIBE 族**（§7）→ `SUBSCRIBE`。
4. **TXN 族**（`MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH`，§6）→ `TXN`。
5. **参数触发的 BLOCKING**（§4.1：`XREAD`/`XREADGROUP` 带 `BLOCK`）→ `BLOCKING`。
6. **固有 BLOCKING**（§4.2：`BLPOP`/`BRPOP`/…/`WAIT`）→ `BLOCKING`。
7. **MASTER_ONLY**（§9：`EVAL`/`SCAN`/`INFO`/`PUBLISH`…）→ `MASTER_ONLY`。
8. **只读白名单**（§3.READ）→ `READ`。
9. **兜底**：其余一律 `WRITE`（保守默认安全——不确定就走 master，绝不把可能的写打到
   replica）。未知/未来新增命令天然落这里，安全。

> 关键不变量：**任何"可能改数据"的命令绝不能被判成 READ。** 只有明确在只读白名单里
> 的才 READ，其余全 WRITE。宁可牺牲一点读扩展，不可牺牲正确性。

## 3. 通用键命令

### WRITE（示例，非穷举，兜底规则保证遗漏项仍安全落 WRITE）
```
SET SETNX SETEX PSETEX SETRANGE APPEND GETSET GETDEL GETEX
MSET MSETNX DEL UNLINK RENAME RENAMENX MOVE COPY RESTORE
EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
INCR INCRBY INCRBYFLOAT DECR DECRBY
HSET HSETNX HMSET HDEL HINCRBY HINCRBYFLOAT
LPUSH LPUSHX RPUSH RPUSHX LPOP RPOP LSET LINSERT LREM LTRIM RPOPLPUSH LMOVE LMPOP
SADD SREM SPOP SMOVE SINTERSTORE SUNIONSTORE SDIFFSTORE
ZADD ZINCRBY ZREM ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX
ZPOPMIN ZPOPMAX ZMPOP ZRANGESTORE ZDIFFSTORE ZINTERSTORE ZUNIONSTORE
SETBIT BITOP BITFIELD
PFADD PFMERGE PFCOUNT(注: PFCOUNT 会改内部缓存, 归 WRITE)
GEOADD GEOSEARCHSTORE GEORADIUS(带 STORE) GEORADIUSBYMEMBER(带 STORE)
```

### READ（只读白名单，命中才判 READ）
```
GET MGET GETRANGE STRLEN SUBSTR
EXISTS TYPE TTL PTTL EXPIRETIME PEXPIRETIME OBJECT(ENCODING/IDLETIME/FREQ/REFCOUNT) DUMP
HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HSTRLEN HRANDFIELD HSCAN
LLEN LINDEX LRANGE LPOS
SCARD SISMEMBER SMISMEMBER SMEMBERS SRANDMEMBER SINTER SUNION SDIFF SINTERCARD SSCAN
ZCARD ZSCORE ZMSCORE ZCOUNT ZLEXCOUNT ZRANK ZREVRANK ZRANGE ZREVRANGE
ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZRANDMEMBER ZDIFF ZINTER ZUNION ZINTERCARD ZSCAN
GETBIT BITCOUNT BITPOS BITFIELD_RO
RANDOMKEY MEMORY USAGE
GEOPOS GEODIST GEOHASH GEOSEARCH GEORADIUS_RO GEORADIUSBYMEMBER_RO
```

> 注意成对的 `*_RO` 变体（`BITFIELD_RO`/`GEORADIUS_RO`/`SORT_RO`）是 READ；不带 `_RO`
> 的原命令可能写，归 WRITE。`SORT` → WRITE（可带 STORE）；`SORT_RO` → READ。

## 4. 阻塞命令（BLOCKING，一律 pin + 走 master）

### 4.1 参数触发（§2 规则 5）
```
XREAD        仅当带 BLOCK 参数 → BLOCKING；否则 → READ
XREADGROUP   仅当带 BLOCK 参数 → BLOCKING；否则 → 见 §10（消费组读，默认走 master）
```

### 4.2 固有阻塞（§2 规则 6，无论参数）
```
BLPOP BRPOP BLMOVE BRPOPLPUSH BLMPOP
BZPOPMIN BZPOPMAX BZMPOP
WAIT WAITAOF
```

> 说明：这些命令即便"读"意味（如 `XREAD BLOCK`）也一律走 master。理由（proxy-design §6）：
> 阻塞连接要 pin，走 master 只牺牲少量读扩展，换取正确性与实现简单。`WAIT`/`WAITAOF`
> 是复制/持久化屏障，必须 master。

## 5. 连接状态命令（CONN_STATE）

| 命令 | 处理 |
|------|------|
| `AUTH` | proxy 终结：按监听端口对应 group 的快照密码校验（D7）；不转发下游 |
| `SELECT` | v1 **pin 独占**（§proxy-design 5.6）；预留"追踪 DB + 按需补 SELECT"位置 |
| `HELLO` | 若带 `3` → 拒绝/降级应答，逼回 RESP2（D8）；不带版本 → 正常应答 RESP2 元信息 |
| `PING` | proxy 本地直接回 `PONG`（或带参回显），不必下发 |
| `ECHO` | proxy 本地回显 |
| `QUIT` | proxy 关闭该客户端连接 |
| `RESET` | 重置该客户端会话状态（清 mode/DB/订阅），proxy 本地处理 |
| `CLIENT` | 子命令区分见下表 |
| `COMMAND` | `MASTER_ONLY`：转发 master（客户端能力发现用）。`COMMAND DOCS`/`COMMAND INFO`/`COMMAND LIST` 同 |
| `MEMORY USAGE` | 已归 `READ`（§3）--纯查询，可走读池 |
| `MEMORY STATS` `MEMORY DOCTOR` `MEMORY PURGE` `MEMORY MALLOC-STATS` | `ADMIN`：走 master（管理/调试类） |
| `LATENCY` | `DENY`：诊断指令，不对外 |
| `ROLE` | `MASTER_ONLY`：避免 replica 返回 slave 角色使客户端误判拓扑 |
| `ACL` | `DENY`：v1 不支持 ACL（D7）。所有 ACL 子命令拒绝 |

> **CLIENT 子命令细分**：`GETNAME`/`SETNAME`/`ID`/`INFO`/`SETINFO` -> CONN_STATE（本地或
> master）；`LIST` -> MASTER_ONLY（返回后端连接信息，非客户端连接）；`KILL`/`PAUSE`/
> `UNPAUSE`/`NO-EVICT`/`NO-TOUCH`/`TRACKING`/`REPLY` -> DENY（破坏连接语义或多路复用）。

## 6. 事务命令（TXN，pin + 走 master）
```
MULTI EXEC DISCARD WATCH UNWATCH
```
- `MULTI` 触发 pin；`EXEC`/`DISCARD` 结束后解 pin 还池（proxy-design §7.2）。
- `WATCH` 也 pin（乐观锁依赖连接状态）；必须与其 `MULTI/EXEC` 在同一独占连接上。

## 7. 发布订阅命令（SUBSCRIBE / MASTER_ONLY）

| 命令 | 分类 | 说明 |
|------|------|------|
| `SUBSCRIBE` `PSUBSCRIBE` `SSUBSCRIBE` | `SUBSCRIBE` | pin 进推送模式，走 master |
| `UNSUBSCRIBE` `PUNSUBSCRIBE` `SUNSUBSCRIBE` | `SUBSCRIBE` | 在同一独占连接上处理；退完所有频道可解 pin |
| `PUBLISH` `SPUBLISH` | `MASTER_ONLY` | 不 pin，走 master（单 group 无分片，全落主天然互通） |
| `PUBSUB` | `MASTER_ONLY` | 查询类，走 master |

> 单主从场景下 Pub/Sub 极简：无分片，发布与订阅都落同一 master，跨 client 天然可达
> （proxy-design §6 附注）。

## 8. 危险命令（DENY，可配）

默认拒绝（回明确错误，不转发）：
```
MONITOR              # D12：刷屏 + 需独占
FLUSHALL FLUSHDB     # 清库
KEYS                 # 全量扫描阻塞
SWAPDB               # 换库
SHUTDOWN             # 停机
DEBUG                # 调试指令
CONFIG               # 改运行时配置（平台由 redis-pilot 管理，见 pkg/redisconf）
CLUSTER              # 本平台不用 cluster（注：部分客户端库会发 CLUSTER SLOTS/CLUSTER
                     # NODES 做拓扑探测。v1 可对这三个子命令 proxy 本地应答返回单主拓扑；
                     # 其余 CLUSTER 子命令 DENY）
REPLICAOF SLAVEOF    # 复制关系由平台管理，不容客户端改
PSYNC REPLCONF SYNC  # 复制协议命令，不允许客户端发起
FAILOVER             # 故障转移由平台/Sentinel 权威
ACL                  # v1 不支持 ACL（D7）
SWAPDB               # 换库
```
> DENY 名单可经 `proxy.yaml` 的 `limits.deny_commands` 覆盖/增删（迁移文档 §6）。
> 平台已有的 `rename-command`/保留字清单见 `pkg/redisconf/validate.go`，DENY 名单应与其
> 保持一致，避免两处口径冲突。

## 9. MASTER_ONLY（固定落主，不 pin）
```
EVAL EVALSHA EVAL_RO EVALSHA_RO FCALL FCALL_RO SCRIPT FUNCTION
SCAN                 # 游标一致性：钉 master（对标阿里云），不跨节点
INFO DBSIZE LASTSAVE ROLE
PUBLISH SPUBLISH PUBSUB   # 见 §7
COMMAND COMMAND DOCS COMMAND INFO COMMAND LIST  # 客户端能力发现
MEMORY STATS MEMORY DOCTOR MEMORY PURGE MEMORY MALLOC-STATS  # 管理/调试
```
> `EVAL_RO`/`FCALL_RO`/`EVALSHA_RO` 理论只读，v1 仍走 master（简单、安全）；后续若需把
> 只读脚本放 replica，需只读账号 + 显式开关（对标阿里云 `readonly_lua_route_ronode_enable`），
> 列为后续项，不在 v1。

## 10. Stream 命令完整归类（自研代理的核心诉求，必须全覆盖）

| 命令 | 分类 | 说明 |
|------|------|------|
| `XADD` | WRITE | |
| `XACK` | WRITE | |
| `XDEL` | WRITE | |
| `XTRIM` | WRITE | |
| `XSETID` | WRITE | |
| `XCLAIM` `XAUTOCLAIM` | WRITE | 转移 pending 所有权，改状态 |
| `XGROUP CREATE/DESTROY/CREATECONSUMER/DELCONSUMER/SETID` | WRITE | 组管理 |
| `XREAD`（不带 BLOCK） | READ | |
| `XREAD BLOCK` | BLOCKING | pin + master（§4.1） |
| `XREADGROUP`（不带 BLOCK） | MASTER_ONLY | 消费组读会更新组的 last-delivered-id（有写副作用），**走 master** |
| `XREADGROUP BLOCK` | BLOCKING | pin + master |
| `XRANGE` `XREVRANGE` `XLEN` `XINFO` `XPENDING` | READ | 纯查询 |

> `XREADGROUP` 即使不带 BLOCK 也归 MASTER_ONLY 而非 READ：它推进消费组游标，是有状态
> 副作用的读，落 replica 会导致组状态在从库上被改（错误）。这是 §2 规则 7 的一个显式
> 特例，务必在实现里硬编码，不能靠"只读白名单"兜底（它不在白名单，兜底会判成 WRITE →
> 也走 master，结果正确，但语义上应显式标 MASTER_ONLY 便于阅读与指标）。

## 11. 多键命令的说明（无分片场景）

本平台单 group 无分片，`MGET`/`MSET`/`DEL`/`UNLINK`/`EXISTS`/`TOUCH`/`SINTERSTORE` 等
多键命令**不需要拆分**（阿里云拆分是为跨 slot），按其读写性质整条路由即可：`MGET`/`EXISTS`
/`TOUCH` → READ，`MSET`/`DEL`/`UNLINK`/`*STORE` → WRITE。这是相对 cluster proxy 省掉的
一大块复杂度。

## 12. 到 Go 的映射（M1 实现指引，仍是文档）

M1 时 `internal/proxy/route/` 的形态（示意，非最终代码）：

- 一个 `map[string]Class` 承载 §3/§6/§7/§8/§9/§10 的**静态归类**（命令名大写归一）。
- 一个 `Classify(argv [][]byte) Decision` 函数，按 §2 优先级：先查 DENY/CONN_STATE/
  SUBSCRIBE/TXN，再处理**参数触发**（`XREAD`/`XREADGROUP` 扫 `BLOCK` token），再查静态
  map，最后兜底 WRITE。
- `Decision` 含 `Class` + 派生标志（`Pin bool`、`Target master|readpool`），供 session
  层直接消费。
- 表驱动单测：每条命令一个用例（命令 → 期望 Class/Pin/Target）；参数触发命令给
  带/不带 `BLOCK` 两个用例；兜底给一个"编造的未知命令 → WRITE"用例；DENY 给一个
  "被拒 → 错误"用例。
- 单测断言的正确性锚点：**只读白名单外的命令都不得判成 READ**（§2 不变量），可加一个
  遍历 map 的元测试守护。

## 参考
- [proxy-design.md](proxy-design.md) §6 路由表 / §7 独占升级 / §17 M1
- 平台保留字/改名清单：`pkg/redisconf/validate.go`（DENY 名单需与之对齐）
