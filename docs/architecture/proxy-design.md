# 自研 Redis 读写分离代理设计

> 状态：已实现。本文记录方案评估过程中拍板的决策、待定项，
> 以及连接核心的架构。实现落地后已同步更新
> [overview.md](overview.md) / [components.md](components.md)。

## 1. 背景与目标

平台原使用外部代理做读写分离，控制面是独立进程
（读 Server 快照，翻译成动态配置下发给代理），现已替换为自研 Proxy。原方案有两个与本平台场景不匹配的地方：

1. **阻塞/订阅/Stream 命令进不来。** 旧代理的命令白名单源于"按 key 哈希到分片 +
   上游连接池复用"的模型，阻塞命令（`BLPOP`/`XREAD BLOCK`/`BRPOP` …）、订阅
   （`SUBSCRIBE`）、Stream 消费组（`XREADGROUP`/`XGROUP`/`XINFO`）在这个模型下
   永远无法支持。这是原理性缺口，不是配置问题。
2. **cluster/分片能力本平台完全不用。** 平台是"单主从 + Sentinel"拓扑，不做
   Redis Cluster、不做 hash 分片、不做 prefix 路由。旧代理的绝大部分复杂度
   是为分片服务的，对本平台是纯负担。

**目标**：自研轻量代理替代了旧代理，在单端口上支持全部 Redis 命令
（含阻塞/订阅/Stream/事务），保留读写分离，去掉分片相关的一切。

**非目标（明确不做）**：
- hash 分片、Redis Cluster 拓扑发现、跨 slot 命令拆分与聚合、prefix 路由、Ketama。
- 热 key query cache、带宽限流、大 key 拦截（阿里云 proxy 有，本平台自用暂不需要）。
- TLS（v1 不支持；后续视平台内网加密需求再补）。

## 2. 已确定的设计决策（decision log）

| # | 决策 | 说明 |
|---|------|------|
| D1 | 用 Go 自研，不 fork Predixy | Predixy（C++，2017，0 release，无 Stream 支持）作为参考实现读其连接状态机，代码用 Go 从头写，与平台其余部分（Server/Agent/CLI）语言一致，命令表可随时补 Stream。 |
| D2 | 对标阿里云**读写分离**外部行为，非其内部实现 | 路由表、连接语义、健康处理照阿里云**公开文档**；内部实现（FIFO 解复用等）由 RESP 协议约束推导 + Predixy 源码佐证，非来自阿里云闭源代码。 |
| D3 | 池化 + 独占升级模型 | 常规命令多路复用共享上游连接；阻塞/事务/订阅命令把连接从池里"升级"为独占。连接模型 day1 定死（池化不是可后补的外挂，它动的是响应解复用这个核心）。 |
| D4 | 读池纳入 master | 读请求在 master + 所有 replica 间按 weight 分摊（master 读权重可调低，因其还要扛写）。 |
| D5 | 无会话一致性窗口 | 读写分离时读始终走读池（replica + master 按 weight），不保证 read-your-writes。需要强一致读的场景用 MASTER 端口。 |
| D6 | 读写分离按 group 显式开 | 不是全局默认。未开的 group 走"强制 master"语义。 |
| D7 | 认证：**仅 Proxy 前端鉴权，后端实例无密码** | 每个 group 的 client 面密码来自快照 `ProxyListener.Password`（= 该 group 的前端密码，存于 `InstanceGroupState.Password`，仅 Server 持有）。后端面密码来自快照 `ProxyCluster.Password`，在无密码方案下 Server 恒下发空串——实例本身不写 `requirepass`，Proxy→后端直连不发 AUTH。client 面与后端面**已解耦**（历史上曾共用同一密码）。proxy 靠"客户端连的哪个监听端口 -> 哪个 group -> 哪个前端密码"判定（与"端口即路由"一致）。AUTH 在 proxy **终结、不透传**（透传会破坏池化）。 |
| D8 | 仅支持 RESP2 | 绝大多数客户端默认 RESP2。`HELLO 3` 不应答 3 / 返回错误，逼客户端回落 RESP2。 |
| D9 | RESP 解析范围最小化 | 请求解析到命令名 + 第一个 key；响应解析到**帧边界**（能定位一条响应结束），不解释 value 内容。 |
| D10 | replica 全挂时读回落 master | 可用性优先，同时打指标告警。 |
| D11 | pipeline 化简：多条 inflight 整批钉 master | 避免跨后端乱序时需要的 per-client reorder buffer；只在"一问一答"时做读写分流。 |
| D12 | `MONITOR` 禁用 | 会刷屏且需独占连接；列入危险命令名单。 |
| D13 | per-client 连接 + 单请求体积 上限 | 对标公有云 proxy 保护机制。per-client 并发连接超阈值 reject 新连接防单客户端耗尽 fd/端口预算（§13）；单 RESP 帧超 `max_request_bytes` reject 防 OOM。均 `proxy.yaml` 可配。 |
| D14 | 优雅关闭（SIGTERM drain） | 收到 SIGTERM -> 停止 accept -> 等待 inflight 写完成或 `shutdown_timeout`（默认 10s）超时 -> 关闭。SHARED 连接等 FIFO 排空；EXCLUSIVE 连接等当前阻塞命令返回或超时。避免 kill -9 断 inflight 写导致 ambiguous error。 |
| D15 | 代理级策略：v1 reload-only | deny_commands / master_read_weight 在 `proxy.yaml` 中，变更需重启 proxy。拓扑变化通过快照热更新无需重启。后续可把策略搬进快照 `proxy_config` 字段实现 Server 在线下发（O4）。v1 接受重启频率低（策略稳定后不动）。 |
| D16 | 伪 Sentinel 用**单一独立端口**，不复用数据端口 | 客户端 sentinel 模式的服务发现是**跨 group** 的（一个端口回答任意 `masterName` 的地址），而数据端口按 D7「端口即路由」**绑定单个 group + 单个密码**。把跨 group 发现塞进单 group 数据端口会破坏 D7 不变式、且鉴权语义讲不清。故整个 proxy 进程加一个 Sentinel 协议端口（默认 26379），所有 group 共用；`get-master-addr-by-name <group>` 返回该 group 的 proxy 数据端口。详见 §19。 |
| D17 | group 级 `access_mode`（`standard`/`rw-split`），**仅用于伪 Sentinel 选择返回端口**；数据端口维持现状 | 对标阿里云「读写分离是实例级架构选型」（§18），但**不改端口分配、不做模式切换机制**（评估后放弃：单端口+切换要引入端口重分配/冷却回收/存量迁移，收益不抵复杂度）。数据端口照旧（replication 双口 auto+master、standalone 单口 master，都照常监听）。`access_mode` 只决定 `get-master-addr-by-name` 返回 auto 口还是 master 口：`rw-split`（主从默认）→auto，`standard`→master。直连客户端不受影响（两个口都在）。默认 `rw-split` 保持既有行为。详见 §20。 |

> D7 补充：前端密码来源是每 group 快照的 `ProxyListener.Password`（= `InstanceGroupState.Password`，
> 仅存于 Server，见 §11）。后端 `ProxyCluster.Password` 恒空——后端
> 实例无 `requirepass`。proxy 侧无任何密码配置，故不存在配置漂移；前端密码随快照更新
> （如轮换）在下一次快照生效，改一处即可，无需触碰实例/Sentinel/复制链路。v1 只支持
> `AUTH <password>`（requirepass 风格）单参形式，不支持 ACL 的 `AUTH <user> <pass>`。

## 3. 待定项（OPEN，未拍板）

以下各项直接影响连接核心的生命周期，**实现连接核心前必须补齐**。

- **O1 后端拓扑发现 + failover 信号源。** proxy 如何知道后端、master 何时切换、多快
  知道。候选：复用 Server 快照长轮询（已有，最省，切换延迟 = 轮询周期）／直接订阅真
  Sentinel 的 `+switch-master`（近实时）／两者结合。倾向：v1 先复用快照，见 §11。
- **O2 failover 时对 in-flight/阻塞连接的客户端语义。** 阻塞在 `BLPOP`/`XREAD BLOCK`
  的连接遇到 master 切换该得到什么。倾向：**直接报错断连让客户端重试**，proxy 不替它
  重放阻塞写命令（重放不安全）。见 §7.3。**已知与阿里云的差异**：阿里云 proxy 在
  failover 时缓冲 in-flight 写并重放到新 master，客户端无感知。v1 选择报错断连是保守
  立场（重放不安全），后续视客户端反馈可补有限重试。
- **O3 超时策略（已定，见 §15）。** EXCLUSIVE/阻塞连接不设 read 超时。TCP keepalive
  60s，建连超时 3s。Redis 实例模板须设 `timeout 0`。
- **O4 代理级路由策略在线变更。** 当前 deny_commands / master_read_weight
  在 proxy.yaml 中，变更需重启 proxy（D15）。拓扑变化靠快照热更新无需重启。后续可将策略
  搬进快照由 Server 在线下发。

## 4. 总体架构与数据流

```
业务应用 ──Redis 协议──> [ redis-proxy ] ──Redis 协议──> master / replica(s)
                              │
                              └── 从 Server 拉取 ProxySnapshot（后端拓扑 + 密码 + 端口）
```

- proxy 是**数据面**：只做连接管理、命令路由、字节转发，不做拓扑决策。
- 拓扑来源是 Server 的 `ProxySnapshot`（见 §11），proxy 只查"监听端口 -> 后端组"表。
- 管理流量（创建实例、failover 等）仍走 Server/Agent，与 proxy 无关。

一个 proxy 进程可服务多个 group，每个 group 对应一组监听端口（见 §11 端口映射）。

## 5. 连接模型（核心，day1 定死）

三类对象：

### 5.1 ClientSession -- 一条客户端连接
- 一个 reader goroutine 解析客户端的 RESP 命令流。
- 一个 auth 状态（见 §5.7）。
- `mode`：
  - `NORMAL`：命令逐条按路由表分发到共享池。
  - `EXCLUSIVE(conn)`：整条 client 钉死在某条独占上游，直到独占条件结束。
  - `SUBSCRIBE`：进入推送模式（独占的一种，基本不还池）。
- `db`：当前 SELECT 的 DB（见 §5.5）。

### 5.2 Backend -- 一个 redis 实例（master 或某 replica）
- `role`：MASTER / REPLICA。
- `health`：OK / DEGRADED / SYNCING / DOWN（见 §12）。
- `weight`：读分摊权重。
- `password`：该后端的 requirepass（来自快照）。
- 一个 UpstreamConn 池（含上限与空闲回收，见 §13）。

### 5.3 UpstreamConn -- 一条 proxy->redis 真实连接
- writer：串行把请求写上线；每写一条，把发起的 `ClientSession` 引用压进 **inflight FIFO**。
- reader goroutine：读响应，每读到一条完整响应，弹 FIFO 队头 -> 得到属主 client -> 转发。
- 断开时遍历 FIFO 回错误（见 §7.4）。
- `mode`：SHARED（在池中多路复用）/ EXCLUSIVE（钉给单个 client）。

### 5.4 支撑解复用的物理定律
单条 redis 连接上，**响应严格按请求发送顺序返回**（pipeline 保证）。所以一条 SHARED
上游连接配一个 FIFO 即可解复用：响应到达 -> 弹队头 -> 就是它的属主。这条定律是池化能
成立的全部地基。

### 5.5 跨连接顺序问题与化简（D11）
客户端用**一条连接**连 proxy，pipeline 发 `[SET a, GET b]`，期望响应按发送顺序回来。
若 SET 路由到 master-conn、GET 路由到 replica-conn，两条上游的响应可能乱序到达。

- **完整解**：每个 client 给请求打递增序列号，client 侧 reorder buffer 按序放行。
- **v1 化简（D11）**：同一 client 一旦有多条 inflight（即在 pipeline），整批钉到
  master 一条连接，顺序天然保证，无需 reorder buffer；只有单条 inflight（一问一答）
  才做读写分流。接口按完整解设计，实现先上化简版。

> 注：阿里云文档也建议"避免跨后端 pipeline"（其单连接场景 FAQ 指出同连接 pipeline 请求
> 相互耦合）。D11 与之同思路，不是本平台独创。

### 5.6 连接状态命令（SELECT / AUTH）
连接级状态命令会破坏多路复用。处理规则：
- `AUTH`：在 proxy 终结（D7），不进上游，不影响池。proxy 按客户端所连监听端口定位
  该 group 的密码（快照 `ProxyCluster.Password`），校验客户端 `AUTH <password>`；该
  group 后端无密码则不要求 AUTH（语义与直连 requirepass 实例一致）。proxy 用同一密码
  维护到后端的上游连接鉴权，但不转发客户端的 AUTH 命令。
- `SELECT n`：v1 采用**触发 pin**（一旦 SELECT 该 client 升级为 EXCLUSIVE，永久 pin，
  详见 §5.8）。结构里预留"追踪每 client DB + 路由时按需补 SELECT"的位置，供后续需要
  频繁多 DB 时切换。

### 5.7 AUTH 状态机（D7）

每个 client 有一个 auth 状态：

- **UNAUTHENTICATED**：初始状态。只允许 `AUTH`/`PING`/`HELLO`/`QUIT`/`RESET`，其余回
  `-NOAUTH Authentication required`。
- **AUTHENTICATED**：AUTH 成功后切换，所有命令可用。

group 后端无密码（Password 空）时 client 直连即 AUTHENTICATED；收到 AUTH 回 OK。有密码：
`AUTH <password>` 匹配快照密码成功 -> AUTHENTICATED；失败 `-ERR invalid password`；
`AUTH <user> <password>` 回 `-ERR single parameter AUTH only`（v1 不支持 ACL）。

> auth 状态持久化在 ClientSession，与后端无关（AUTH 不转发上游）。

### 5.8 SELECT 永久 pin（v1，关联 §7.2）

v1 语义：**一旦 SELECT，该 client 永久 pin 为 EXCLUSIVE 直到连接关闭**。被 pin 的上游
连接不还 SHARED 池（DB ≠ 0 复用会给其他 client 意外的 DB）。整个生命周期不再读写分流
--可接受：用 SELECT 的 client 通常有连接状态依赖，不做读扩展。后续多 DB 需求频繁时
再设计「按需补 SELECT」池化版（§5.6 预留位置）。

## 6. 命令路由表（route 包）

`route` 包是纯函数：命令名 -> 去向分类。它是整个代理最该先钉死、最能独立单测的部分。

> 完整命令清单（Redis 全命令 + Stream 全家 + 阻塞/事务/订阅 + DENY 名单 + 判定优先级 +
> 到 Go 的映射）见 **[proxy-command-routing.md](proxy-command-routing.md)**。下表为摘要。

| 分类 | 去向 | 命令（示例，非全量） |
|------|------|------|
| WRITE | master 池 | SET/DEL/HSET/LPUSH/INCR/EXPIRE/XADD/XACK/XDEL/XTRIM/XCLAIM… |
| READ | 读池（master+replica 按 weight）；会话窗口内->master | GET/MGET/HGET/SMEMBERS/ZRANGE/XRANGE/XLEN/XINFO/EXISTS… |
| BLOCKING | **触发 pin**，走 master | BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/WAIT |
| BLOCKING-READ | **触发 pin**，走 master（保守） | XREAD BLOCK / XREADGROUP BLOCK |
| TXN | **触发 pin**，走 master | MULTI/EXEC/DISCARD/WATCH/UNWATCH |
| SUBSCRIBE | **触发 pin（SUBSCRIBE 模式）**，走 master | SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE + 对应 UNSUB |
| MASTER-ONLY | master 池 | EVAL/EVALSHA/FCALL/SCAN/INFO/PUBLISH/COMMAND |
| CONN-STATE | proxy 终结 / pin | AUTH（终结）/SELECT（pin，见 §5.6）/HELLO（见 §10）/PING/ECHO |
| DENY | 拒绝 | MONITOR（D12）；可配禁用 KEYS/FLUSHALL/FLUSHDB/CONFIG/SHUTDOWN/ACL/CLUSTER… |

**Stream 命令归类要点**（这是自研代理的核心诉求，必须全覆盖）：
- 写：`XADD` `XACK` `XDEL` `XTRIM` `XSETID` `XCLAIM` `XAUTOCLAIM` `XGROUP CREATE/DESTROY/CREATECONSUMER/DELCONSUMER/SETID`
- 读：`XRANGE` `XREVRANGE` `XLEN` `XINFO` `XPENDING`
- 阻塞读（触发 pin）：`XREAD [BLOCK]` `XREADGROUP [BLOCK]`

判定原则（保守默认安全）：不在明确只读集合里的命令，一律当写 -> 走 master。阻塞命令
无论读写一律 pin + 走 master（`XREAD BLOCK` 走 master 仅牺牲少量读扩展，换取正确性）。

## 7. 独占升级生命周期

### 7.1 触发 pin
收到 BLOCKING / TXN（`MULTI`）/ SUBSCRIBE / `SELECT` 时：
1. 为目标后端（阻塞与事务一律 master）取一条上游连接（从池取或新建），标 EXCLUSIVE，
   移出多路复用。
2. `client.mode = EXCLUSIVE(conn)`；此后该 client 所有命令 1:1 直通这条连，**不再进
   FIFO 解复用**（它已独占，天然有序）。

### 7.2 解 pin 还池
- 事务：收到 `EXEC` / `DISCARD` 且响应已发。
- 单条阻塞命令：响应返回即可还池。
- 订阅：直到 `UNSUBSCRIBE` 完所有频道或连接关闭。
- `SELECT`：**v1 永久 pin，不还池**（见 §5.8）。

### 7.3 failover 时的独占连接（关联 O2）
后端标 DOWN 时，其池内 EXCLUSIVE 连接被 close。倾向策略：**对应 client 收到错误并断连，
让客户端重试**，proxy 不重放阻塞写命令。最终策略待 O2 拍板（v1 不做透明重试）。

### 7.4 上游连接断开（SHARED + EXCLUSIVE）

**SHARED 池连接断开**（EOF / RST）：reader 收到 EOF 后遍历 inflight FIFO，对每个 entry
对应的 client 回 `-ERR upstream connection closed`（RESP 错误帧）；Backend health ->
DEGRADED；新请求走其他池连接或新建。

**EXCLUSIVE 连接断开**：对应 client 回错误并断连（同 §7.3 failover 行为）；不重放 inflight
写命令（保守，与 O2 一致）。

### 7.5 启动：首次快照就绪前不开监听

proxy 启动后先从 Server 拉取首次快照，**快照就绪前不 open 监听口**，避免 client 连入后
无拓扑可路由。首次快照失败则重试（策略见 O1），不提供服务。快照就绪 -> 解析拓扑 ->
open 监听端口 -> 开始 accept。

### 7.6 快照拉取失败时保持最后快照

后续快照拉取失败（Server 不可达、超时、非 304）不丢弃旧快照：以最后一次成功的快照保持
后端拓扑继续服务。旧快照可能过期（master 已切换但快照不知），inflight 写会收到 RST ->
§7.4 路径触发 -> client 断连重试。持续失败超阈值（默认 5min）打告警「proxy 与 Server
失联」。

“失败”不包括合法空快照。已成功鉴权和解析、且版本发生变化的空快照是 Server 的权威
全量状态（例如最后一个组被删除），Proxy 必须应用并撤销旧监听与后端。否则旧路由会永久
残留。只有网络/HTTP/解析失败才保留最后快照。

## 8. 读写分离（D4/D5/D6）

- 读写分离**按 group 显式开**（D6）；未开的 group 所有命令走 master。
- 开启的 group：写命令走 master 池；读命令直接进读池（master + replica 按 weight，D4）。
- **不保证 read-your-writes**（D5）：读写分离后读可能命中尚未同步的 replica。需要
  read-your-writes 语义的客户端应使用 MASTER 端口（直连主库）。
- AUTO 端口适合读多写少、容忍最终一致性的场景（缓存、统计、报表等）。

## 9. 读分摊与 weight（D4/D10）

- 读池成员 = master + 所有 `health==OK` 的 replica。
- 按 weight 加权随机选择；master 读 weight 默认调低（可配），避免读流量挤占写。
- `health==SYNCING`（全量同步中）的 replica weight 归零，不参与（见 §12）。
- 所有 replica 不可用（DOWN/SYNCING）时，读回落 master（D10），打告警指标。

## 10. 协议（D8）

- 仅实现 RESP2。
- `HELLO`：不应答协议版本 3（返回错误或降级应答），逼客户端回落 RESP2。go-redis 等
  库在 `HELLO 3` 失败时会自动降级。
- RESP 解析范围见 D9：请求到命令名 + 首 key；响应到帧边界，不解 value。

## 11. 后端发现与快照复用（关联 O1）

v1 倾向**复用 Server 现有 `ProxySnapshot`**，不新造输入侧。

现有类型（`pkg/apitypes/types.go`，已核实）：
```go
type ProxySnapshot struct { Version, GeneratedAt string; Listeners []ProxyListener; Clusters []ProxyCluster }
type ProxyListener struct { Name, Group, Mode /*master|auto*/, Bind string; Port int; Cluster, ReadCluster, Password, ReadPolicy string; ... }
type ProxyCluster  struct { Name, Password string; Endpoints []ProxyEndpoint }
type ProxyEndpoint struct { Address string; Port int }
```

映射到 proxy 所需信息（每 group）：
- **master 端点**：该 group `Mode==master`（或 auto listener 的 `Cluster`）指向的
  `ProxyCluster.Endpoints`。
- **replica 端点**：auto listener 的 `ReadCluster` 指向的 `ProxyCluster.Endpoints`。
- **密码**：`ProxyCluster.Password` / `ProxyListener.Password`--即该 group 后端的
  requirepass。按 D7，这同一个密码同时用于 client 面鉴权与后端面鉴权；为空则该 group
  两面都不鉴权。
- **监听端口**：
  - `Mode==master` 的 listener `Port` -> proxy 上"强制 master"监听口。
  - `Mode==auto`  的 listener `Port` -> proxy 上"读写分离"监听口（全命令支持）。

复用方式：
- 长轮询客户端可**直接搬用** `internal/proxy/discovery/client.go` 的 `FetchProxySnapshot(ctx,
  knownVersion, wait)`--它已处理 304、长轮询降级、鉴权。**注意**：`FetchProxySnapshot`
  是 `serverClient`（unexported 类型）的方法，搬用时需连同 `serverClient` +
  `newServerClient` 一起搬到 `internal/proxy/discovery/`（`poll_test.go` 跟搬）。
- Server 端 `/api/v1/proxy/snapshot`（`proxy_handler.go`）与 `buildInstanceGroups`
  （`proxy_group.go`，已按 `IsServing()` 过滤健康实例、按 `CurrentMaster` 定 master）
  **无需改动**即可服务 proxy。

后续清理（非 v1 必须）：快照原为旧代理形状，含死字段（`ReadPolicy` 恒为 `MASTER` 但
自研 proxy 不读它、`StatPrefix` 是旧代理指标前缀、master/auto 双 listener 拆分）。
可在 proxy 稳定后把快照简化为 proxy 原生形状。

failover 感知：v1 靠快照长轮询（master 变化 -> 快照 `Version` 变 -> proxy 收到新拓扑 ->
对旧 master Backend 执行 §12 的 close/rebuild）。若实测切换延迟不可接受，再加 Sentinel
`+switch-master` 订阅（O1）。

## 12. 健康状态与降级（后端状态 -> 连接生命周期）

Backend 的 `health` 状态机驱动连接动作，是设计里的一等公民：

| 状态 | 触发 | 连接动作 |
|------|------|---------|
| master 切换 | 快照 master 变化（O1） | 旧 master Backend -> DOWN，**主动 close 其池内所有连接**（含 EXCLUSIVE）；新 master 建池。手动 failover 场景旧 master 可能还活着，不主动 close 会导致写打到旧主报 `-READONLY` 或数据被覆盖--这是正确性硬要求。 |
| SYNCING | replica `INFO replication` 的 `master_sync_in_progress=1` | weight 归零，停止分配新读；同步完成恢复。Agent 健康检查加采一个字段即可。 |
| DEGRADED | replica 连续连接失败/异常 | 降 weight 而非直接摘；连续失败超阈值才转 DOWN（比二值 `IsServing()` 更平滑，对标阿里云）。 |
| DOWN | 失败超阈值 / 快照移除 | 移出读池，drain 连接。 |

## 13. 连接规模与端口预算

无池模型（或池未收敛时）proxy->后端是**出站连接**，吃临时端口（ephemeral port），
这是比 fd 更硬的天花板：

- 一个 proxy（单源 IP）-> 一个后端（单 dst IP:port）的并发出站连接上限 ≈ `ip_local_port_range`
  大小。Linux 默认 `32768-60999` ≈ **28000/后端**。
- **每后端独立预算**（四元组 dst 不同）：到 master 和到某 replica 各算各的。
- 调 `net.ipv4.ip_local_port_range = 1024 65535` -> ≈ **64000/后端**。
- 长连接下 TIME_WAIT 不是主要矛盾（活连接占 ESTABLISHED），`tcp_tw_reuse` 帮助有限。

放大链条：`客户端到 proxy 连接数 = app 实例数 × 每实例连接池大小`；无池时上游到 master
连接数 ≈ 该值。缓解顺序（便宜->贵）：
1. 调 `ip_local_port_range` 到 64k。
2. 多 proxy 分摊（本就要 3+ 做 HA，客户端分散后每 proxy 扛 C/N，端口压力除以 N）。
3. proxy 主机多绑源 IP（每源 IP 又一份预算）。
4. 池化收敛（D3 的池真正启用；这也是池化存在的根本原因）。

`fd`：`LimitNOFILE=65536` 起，可上调；比端口好办。

**池上限与空闲回收**：每 Backend 连接池设上限 `pool_size`（默认 64），超限时 SHARED 请求
排队而非无限新建；空闲连接超 `idle_timeout`（默认 5m）关闭回收。防止池膨胀撞端口预算。

## 14. 可观测性

v1 至少暴露（按 group / 按后端维度）：
- `active_client_conns`、`active_upstream_conns`（监控连接放大，逼近端口预算 60% 告警）。
- 独占连接数（`exclusive_conns`），观察阻塞/事务/订阅占用。
- 读回落 master 事件计数（§9/§12 触发）。
- 错误计数、后端健康状态。
- per-group / per-backend QPS 计数（`proxy_qps`），容量规划需要。

> 指标以 **Prometheus exposition format** 暴露在 `/metrics` endpoint（对标
> Prometheus 输出）。

per-command 指标 v1 降级为"先记录不细分"，后续按需补。

## 15. 超时策略（O3，已定）

- **EXCLUSIVE/阻塞上游连接不设 read 超时**--`XREAD BLOCK 0` / `BLPOP key 0` 是无限
  阻塞，统一 read deadline 会误杀合法阻塞。这是 day1 必须落实的红线。
- **TCP keepalive 60s**：上游连接显式设 `SetKeepAlive(true)` + `SetKeepAlivePeriod(60s)`，
  不能依赖 OS 默认的 2 小时。死连接 ~90s 内被探测发现。
- **建连超时 3s**：proxy -> 后端 TCP 建连阶段设 3s 超时。
- **Redis 实例 `timeout 0`**：实例配置模板须设 `timeout 0`（服务端不主动断空闲连接），
  由 proxy `idle_timeout`（§13，默认 5m）统一管理连接生命周期。
- SHARED 池连接可设 read 超时（非阻塞场景）；建连阶段设超时。

### 15.1 优雅关闭（D14）

收到 SIGTERM：停止 accept -> 等待 inflight 写完成或 `shutdown_timeout`（默认 10s）
超时 -> 关闭所有连接。SHARED 连接等 FIFO 排空；EXCLUSIVE 连接等当前阻塞命令返回或
超时。超时后强制关闭。避免 kill -9 断 inflight 写导致 client 收到 ambiguous error。

## 16. 模块划分（Go 包）

```
internal/proxy/
  session/    ClientSession：client 连接、RESP 解析、mode 状态机、auth 状态、（预留）reorder buffer
  backend/    Backend：角色/健康/weight；UpstreamConn 池（pool_size/idle_timeout）；inflight FIFO 解复用
  route/      路由决策：命令分类表、读写判定、weight 选择（纯函数，易单测）
  resp/       RESP2 解析/编码（解析到判定所需，不全量解 value）
  discovery/  快照拉取（长轮询客户端）+ 后端状态维护
  proxysvc/   组装：listener、失败重连、指标、优雅关闭、/metrics endpoint
cmd/proxy/    进程入口
```

依赖方向：`proxysvc -> {session, backend, route, discovery} -> resp`。

## 17. 分阶段实现计划

对标终态设计接口，分阶段填实现，避免"全写完才能跑"：

1. **M1 骨架**：`route` 包命令分类表 + 单测（最先钉死、零依赖）。
2. **M2 单后端直通**：写死一组 master/replica，每 client 一条 SHARED 上游（池大小=客户端
   数，退化不收敛），跑通 `XREADGROUP … BLOCK 0`、`MULTI/EXEC`、`SUBSCRIBE` 的独占路径。
   验收线 = **旧代理不支持的那些命令在这里能跑通**。
3. **M3 读写分离**：接读池 weight、回落 master。
4. **M4 快照接入 + failover**：搬 `discovery`，接 Server 快照，实现 master 切换 close/rebuild。
5. **M5 池收敛 + 指标**：SHARED 连接真正多路复用（打开 FIFO 解复用 / reorder 或 D11 化简），
   补 §14 指标。
6. **M6（择机）**：伪 Sentinel 客户端发现（设计见 §19）、Sentinel 直订阅（O1）、快照原生化简。

> **迁移 cutover 注记**：测试环境可 M4 验收；**生产 cutover 需 M5**（无池化收敛会撞端口预算）。

## 18. 公有云 proxy 对标补充

以下能力对标阿里云 Tair Proxy / 腾讯云 CKV Proxy（排除 Cluster），补充到设计中：

| 能力 | 设计落点 | 状态 |
|------|---------|------|
| per-client 连接限额 | proxy.yaml `max_connections_per_client`；超限 reject 新连接 | D13 |
| 单请求体积上限 | proxy.yaml `max_request_bytes`；RESP 帧超限 reject | D13 |
| 优雅关闭（SIGTERM drain） | §15.1 | D14 |
| 池上限 + 空闲回收 | proxy.yaml `pool_size`/`idle_timeout`；§13 | D13 |
| QPS 指标 | §14 per-group/per-backend | 新增 |
| Prometheus /metrics | §14 | 新增 |
| AUTH 状态机 | §5.7 UNAUTH/AUTHENTICATED | 新增 |
| 上游断开 inflight 处理 | §7.4 | 新增 |
| 启动 / 快照降级 | §7.5 / §7.6 | 新增 |
| SELECT 永久 pin | §5.8 | 新增 |
| 路由规格补全（COMMAND/ACL/MEMORY/ROLE 等） | proxy-command-routing.md | 新增 |
| CLUSTER 兼容应答 | proxy-command-routing.md §8 | 新增注记 |
| failover 写重试 | O2，v1 不做（报错断连） | 已知差异，后续视反馈 |
| 只读端点（走 replica，拒写） | v1 不做；`auto` 端口配 `master_read_weight=0` 近似 | 后续加第 3 端口 `readonly` |
| 只读账号 | D7，v1 不支持（单密码模型） | 已知差异 |
| TLS | 非目标（v1 不支持） | 后续视需求 |
| 配置在线热更新 | D15，v1 reload-only | O4 后续 |
| per-command 指标 | §14，v1 降级 | 后续 |
| 慢查询日志 | v1 不实现 | 后续 |
| 伪 Sentinel 服务发现（sentinel 模式客户端零改代码接入） | §19；单一 Sentinel 端口 | 已设计，M6 实现 |
| 读写分离作为 group 级选型（standard/rw-split） | §20 `access_mode`；仅决定 sentinel 返回哪个口，双口维持 | D17，已设计 |

## 19. 伪 Sentinel 服务发现（D16）

> 状态：设计已定稿，实现排期在 §17 M6。目标是让**已经用 Sentinel 模式的客户端零改代码**
> 接入 Proxy——把 Proxy 自己伪装成一个 Sentinel。
>
> 实现代码见 `internal/proxy/sentinel/` 包。

### 19.1 背景与目标

存量业务用的是 sentinel 模式客户端（配置的是「sentinel 地址列表 + masterName」，
运行时先问 Sentinel 要 master 地址再连数据）。若要它们改用 Proxy，常规做法是把客户端
从 sentinel 模式改成单点模式、填 Proxy 地址——这需要改客户端代码/配置，成本高。

**目标**：客户端保持 sentinel 模式与代码不变，仅把 sentinel 地址列表指向 Proxy，
即可让业务流量走 Proxy（继续享受读写分离），且故障转移对客户端透明。

**明确不能做的（澄清一个常见误区）**：不能"把 Proxy 地址配到真 Sentinel 上"。真
Sentinel 的 `get-master-addr-by-name` 返回的是它**实际监控到的 master ip:port**，没有
"返回一个我指定的假地址"的配置项；若把 monitor 目标写成 Proxy，Sentinel 会把 Proxy 当
真 redis 去做故障判定/failover，破坏平台**依赖真 Sentinel 做故障转移**的机制。故正确方向
是反过来——**让 Proxy 假扮 Sentinel**。

### 19.2 端口模型（D16）

- 整个 Proxy 进程加**一个**独立的 Sentinel 协议监听端口（默认 `26379`），**所有 group 共用**。
- 不复用数据端口：发现是跨 group 的（一个端口回答任意 `masterName`），数据端口按 D7
  「端口即路由」绑定单 group + 单密码；混用会破坏 D7 不变式。见 D16。
- 数据端口（16379+ auto / 16500+ master）沿用现状，不新增。**净增仅此一个 Sentinel 端口。**
- 数据端口分配维持现状（replication 双口 auto+master、standalone 单口 master，均照常监听）。
  group 的 `access_mode`（D17，§20）**仅决定伪 Sentinel 广告这两个口中的哪一个**，不改变
  端口分配、不关闭任一 listener。

### 19.3 发现语义（核心）

Sentinel 发现按 `masterName` 查，约定 **`masterName` == group 名**（与平台 group 命名一致）。

- `SENTINEL get-master-addr-by-name <group>` → 返回**该 group 的 Proxy 数据端口**，按其
  `access_mode`（D17，§20）选择：
  - `access_mode==rw-split`（主从默认）→ 返回 **auto 端口**（`ProxyListener.Mode==auto`）。
    sentinel 客户端把该地址当"master"，读写都发到 auto 端口，由 Proxy 内部做读写分离。
  - `access_mode==standard` → 返回 **master 端口**（`Mode==master`），读写都走主
    （replica 仅作 HA 热备）。
  - 兜底：目标端口缺失时回退到另一个存在的端口（standalone 只有 master 口，天然返回 master）。
  - 返回的 host 见 §19.5（announce 地址）。
- `SENTINEL slaves <group>` / `SENTINEL replicas <group>` → **返回空列表**。
  这样 sentinel-aware 客户端不会绕过 Proxy 自己去连后端 replica 做读——全部流量收敛到
  Proxy 的 auto 端口，读写分离由 Proxy 统一决策（对齐 §8）。**后端真实拓扑对客户端隐藏。**
- `SENTINEL master <group>` / `SENTINEL masters` → 返回一条最小 master 描述（name/ip/port/
  flags=master），满足会读该信息的客户端；ip:port 同 `get-master-addr-by-name`。
- `SENTINEL sentinels <group>` → v1 返回**空**（客户端用自己配置的 sentinel 列表做 HA）。
  多 Proxy 互相通告 peer 列表留待后续（需要 Proxy 感知同伴，快照当前不含该信息）。

### 19.4 failover 透明化与 pub/sub

- Proxy 返回给客户端的地址是 **Proxy 自己、永不变**。真 master 切换发生在 Proxy 背后
  （Proxy 靠快照感知并 rebuild 后端连接，见 §11/§12），客户端看到的地址不变——**failover
  对客户端完全透明，无重连风暴**。这是伪 Sentinel 相比"客户端直连真 Sentinel"的最大收益。
- `SUBSCRIBE` / `PSUBSCRIBE` 到 `+switch-master`、`+sdown`、`-sdown`、`+odown` 等频道：
  **接受订阅**（不报错，避免打断客户端的标准 sentinel 流程）。因 Proxy 地址稳定，v1
  **不强制推送 `+switch-master`**（推了地址也没变）。可选：后端 master 变化时 best-effort
  发一条 `+switch-master <group> <proxyip> <port> <proxyip> <port>`（新旧地址相同）以兼容
  某些坚持重新解析的客户端；不作为正确性依赖。

### 19.5 返回地址（announce）

Proxy 需知道自己对客户端可达的 IP（`bind 0.0.0.0` 时无法可靠自推，NAT 场景更甚）。

- announce host 完全由 Server 通过 `/api/v1/proxy/snapshot` 快照下发（`ProxySnapshot.AnnounceIP`，
  来自 `server.yaml` 的 `proxy_public_endpoint`），Proxy 侧**不提供**本地配置项，不做兜底覆盖。
  Server 是唯一权威来源，避免 Server/Proxy 两处配置独立维护、手动同步出现漂移。
- 若 Server 未配置 `proxy_public_endpoint`（快照 `AnnounceIP` 为空），回退为**客户端连接命中的
  本地地址**（请求的 local addr），单网段部署下够用；跨 NAT / 多网卡务必在 `server.yaml` 显式配置。
- 端口来自快照对应 group 的数据端口（§19.3），不需额外配。

### 19.6 鉴权

- 伪 Sentinel 端口只暴露"group → Proxy 地址"的拓扑，不暴露数据、不泄露后端 requirepass
  （数据端口仍按 D7 用各自 group 密码鉴权）。
- v1 伪 Sentinel 端口的鉴权：新增可选 `sentinel_password`（对标真 Sentinel 的
  requirepass）。留空 = 发现端口不鉴权。它与数据面/后端密码彼此独立。

### 19.7 命令支持范围

伪 Sentinel 是**只读发现**，不是真 Sentinel 管理面：

- 支持：`SENTINEL get-master-addr-by-name|master|masters|slaves|replicas|sentinels`、
  `PING`、`SUBSCRIBE`/`PSUBSCRIBE`/`UNSUBSCRIBE`（sentinel 频道）、`INFO`（最小）、
  `HELLO`（按 D8 逼回 RESP2）、`AUTH`（若配了 `sentinel_password`）、`QUIT`。
- **拒绝**：`SENTINEL failover|monitor|remove|set|reset` 等管理/变更命令
  （回 `-ERR unsupported`）——故障转移权威仍是平台真 Sentinel，伪 Sentinel 不接管。

### 19.8 快照映射（复用现有 ProxySnapshot，仅新增一个向后兼容字段）

伪 Sentinel 的输入来自现有 `apitypes.ProxySnapshot`（§11）：

- 遍历 `Listeners`，按 `Group` 聚合；每 group 按 `access_mode` 选"客户端入口端口"：
  `rw-split`→`Mode==auto` 的 `Port`；`standard`→`Mode==master` 的 `Port`；缺失回退另一个。
- 该端口 + announce IP 即 `get-master-addr-by-name <group>` 的返回值。
- 快照热更新时映射随之更新；新增/删除 group 自动反映到发现结果。
- **快照新增一个向后兼容字段** `access_mode`（承载在 `ProxyListener` 上，D17/§20）；旧 proxy
  忽略未知字段，不影响兼容。除此之外无 schema 改动。

### 19.9 配置（proxy.yaml）

```yaml
# ─── 伪 Sentinel 服务发现（可选） ───
sentinel_listen: ":26379"      # 伪 Sentinel 监听地址；空 = 关闭该特性
sentinel_password: ""          # 伪 Sentinel 端口鉴权密码；空 = 不鉴权
# announce host 完全由 Server 通过快照下发（server.yaml 的 proxy_public_endpoint），
# 本文件不提供 sentinel_announce_ip 配置项。
```

### 19.10 部署与接入

- 客户端**唯一改动**：把 sentinel 地址列表指向各 Proxy 的 `sentinel_listen` 端口，
  `masterName` 保持 = group 名。客户端代码不变。
- 平台真 Sentinel 继续**纯内部**用于故障转移，不对客户端暴露。
- 若想做到客户端**零改动**：把 Proxy 的伪 Sentinel 端口部署在客户端当前 sentinel 配置
  已填的那个 host:port 上（真 Sentinel 挪到内网另一端口）——属部署编排取舍，非协议层要求。

### 19.11 模块落点

- 在 `internal/proxy/` 下新增 `sentinel/` 包：Sentinel 协议应答（复用 `resp` 编解码），
  持有从 `discovery` 快照派生的 `group → (announce_ip, port)` 映射。
- `proxysvc` 在 `sentinel_listen` 非空时额外起该 listener，与数据 listener 同生命周期
  （随快照更新映射、SIGTERM 一并优雅关闭）。

### 19.12 本特性的非目标

- 伪 Sentinel 之间的 peer 自动通告（`SENTINEL sentinels` 返回空）。
- 任何 Sentinel 管理/变更命令（monitor/failover/set/remove）。
- RESP3（同 D8）。

## 20. Group 访问模式 access_mode（D17）

对标阿里云「读写分离是**实例级架构选型**」（§18），把「sentinel 客户端把哪个口当 master」
做成 **group 级配置**。**本轮只做最小闭环**：不改端口分配、不做模式切换机制——`access_mode`
仅供伪 Sentinel 决定返回哪个已存在的端口。

> 为什么不做单端口分配 + 模式切换：那会引入端口重分配、释放口冷却回收、存量双口迁移等有状态
> 逻辑（原 Plan B），边界多、收益（省一个端口）不抵复杂度。评估后放弃，维持双口现状。

### 20.1 access_mode 语义

group 级新增字段 `access_mode`，**只影响伪 Sentinel `get-master-addr-by-name` 的返回**：

| access_mode | sentinel 返回 | 客户端效果 |
|---|---|---|
| `rw-split`（主从默认） | auto 口（16379+） | 把 auto 口当 master，读写分离由 Proxy 内部做 |
| `standard` | master 口（16500+） | 读写都走主，replica 仅作 HA 热备 |

- 数据端口分配**完全不变**：replication 仍同时分配并监听 auto+master 两个口，standalone 仍
  只有 master 口。`access_mode` 不关闭任何 listener。
- `standalone` 恒等价 `standard`（只有 master 口，兜底天然返回 master）。
- `replication` 默认 `rw-split`（保持既有行为）。
- 直连数据端口的客户端不受影响——两个口都在，想连哪个连哪个。`access_mode` 纯粹是给
  sentinel 发现用的"把哪个口当 master 广告出去"的提示。

### 20.2 改动落点（thin slice）

- `pkg/apitypes`：`InstanceGroupState` 增 `AccessMode string`；`ProxyListener` 增 `AccessMode string`
  以随快照下发（空值视为 `rw-split`，兼容存量与旧 proxy）。
- `internal/server/instance_handler.go`：create 时设默认（replication→`rw-split`，standalone→
  `standard`）；可选 CLI 参数 `--access-mode` 覆盖。**不需要独立的切换 handler**——即便日后要改，
  因两个口都在，改字段即可（下次快照生效、sentinel 返回另一个口），无端口重分配。
- `internal/server/proxy_handler.go`：生成快照时把 group 的 `access_mode` 填进该 group 的
  `ProxyListener`。
- `internal/proxy/discovery`：`ParseSnapshot` 把 `access_mode` 读进 `GroupTopology`。
- `internal/proxy/sentinel`：`resolve()` 按 `GroupTopology.AccessMode` 选 auto/master 口
  （缺省 `rw-split`，兜底回退另一存在端口）。
- `cmd/cli`：create 支持 `--access-mode`；status/list 展示（可选）。

### 20.3 本特性的非目标（本轮）

- **模式切换机制**：不做端口重分配、冷却回收、切换 handler（原 Plan B）。字段先支持起来，
  切换动作日后需要再评估（因两个口都在，未来做切换也很轻，仅改字段 + 客户端重连）。
- **单端口分配 / 省端口**：不做，维持双口。
- 三态 `both`（分离读 + 强一致读打主独立入口）：不做。
- 集群（分片）架构：不支持。

## 参考

- [命令路由规格](proxy-command-routing.md)
- [架构总览](overview.md) / [组件](components.md) / [流程](flows.md)
