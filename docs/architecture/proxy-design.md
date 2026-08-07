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
- 客户端侧的伪 Sentinel 服务发现（本轮暂停，后续单独设计；见 §12）。

## 2. 已确定的设计决策（decision log）

| # | 决策 | 说明 |
|---|------|------|
| D1 | 用 Go 自研，不 fork Predixy | Predixy（C++，2017，0 release，无 Stream 支持）作为参考实现读其连接状态机，代码用 Go 从头写，与平台其余部分（Server/Agent/CLI）语言一致，命令表可随时补 Stream。 |
| D2 | 对标阿里云**读写分离**外部行为，非其内部实现 | 路由表、连接语义、健康处理照阿里云**公开文档**；内部实现（FIFO 解复用等）由 RESP 协议约束推导 + Predixy 源码佐证，非来自阿里云闭源代码。 |
| D3 | 池化 + 独占升级模型 | 常规命令多路复用共享上游连接；阻塞/事务/订阅命令把连接从池里"升级"为独占。连接模型 day1 定死（池化不是可后补的外挂，它动的是响应解复用这个核心）。 |
| D4 | 读池纳入 master | 读请求在 master + 所有 replica 间按 weight 分摊（master 读权重可调低，因其还要扛写）。 |
| D5 | 会话一致性默认开，窗口 500ms | 客户端发生写后，其读在窗口内强制走 master，避免读己之写的 stale read。 |
| D6 | 读写分离按 group 显式开 | 不是全局默认。未开的 group 走"强制 master"语义。 |
| D7 | 认证：**按实例（group）来，密码全部来自快照，proxy 不配任何密码** | 每个 group 的 client 面密码 = 该 group 后端自身的 requirepass（快照 `ProxyCluster.Password` 已携带）；client 面与后端面**共用这同一个密码**。后端未设密码则该 group 两面都不鉴权。proxy 靠"客户端连的哪个监听端口 -> 哪个 group -> 哪个密码"判定（与"端口即路由"一致）。AUTH 在 proxy **终结、不透传**（透传会破坏池化）。 |
| D8 | 仅支持 RESP2 | 绝大多数客户端默认 RESP2。`HELLO 3` 不应答 3 / 返回错误，逼客户端回落 RESP2。 |
| D9 | RESP 解析范围最小化 | 请求解析到命令名 + 第一个 key；响应解析到**帧边界**（能定位一条响应结束），不解释 value 内容。 |
| D10 | replica 全挂时读回落 master | 可用性优先，同时打指标告警。 |
| D11 | pipeline 化简：多条 inflight 整批钉 master | 避免跨后端乱序时需要的 per-client reorder buffer；只在"一问一答"时做读写分流。 |
| D12 | `MONITOR` 禁用 | 会刷屏且需独占连接；列入危险命令名单。 |
| D13 | per-client 连接 + 单请求体积 上限 | 对标公有云 proxy 保护机制。per-client 并发连接超阈值 reject 新连接防单客户端耗尽 fd/端口预算（§13）；单 RESP 帧超 `max_request_bytes` reject 防 OOM。均 `proxy.yaml` 可配。 |
| D14 | 优雅关闭（SIGTERM drain） | 收到 SIGTERM -> 停止 accept -> 等待 inflight 写完成或 `shutdown_timeout`（默认 10s）超时 -> 关闭。SHARED 连接等 FIFO 排空；EXCLUSIVE 连接等当前阻塞命令返回或超时。避免 kill -9 断 inflight 写导致 ambiguous error。 |
| D15 | 代理级策略：v1 reload-only | deny_commands / consistency 窗口 / master_read_weight 在 `proxy.yaml` 中，变更需重启 proxy。拓扑变化通过快照热更新无需重启。后续可把策略搬进快照 `proxy_config` 字段实现 Server 在线下发（O4）。v1 接受重启频率低（策略稳定后不动）。 |

> D7 补充：密码来源是每 group 的快照 `ProxyCluster.Password`（由 master 实例的
> requirepass 而来，见 §11）。proxy 侧无任何密码配置，故不存在配置漂移；password 随快照
> 更新（如轮换 requirepass）在下一次快照生效。v1 只支持 `AUTH <password>`（requirepass
> 风格）单参形式，不支持 ACL 的 `AUTH <user> <pass>`。

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
- **O4 代理级路由策略在线变更。** 当前 deny_commands / consistency 窗口 / master_read_weight
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
- `lastWriteAt`：会话一致性时间戳（见 §8）。
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

## 8. 会话一致性（D4/D5/D6）

- 读写分离**按 group 显式开**（D6）；未开的 group 所有命令走 master。
- 开启的 group：写命令更新 `client.lastWriteAt`；读命令若 `now - lastWriteAt < 500ms`
  强制走 master，否则进读池（master + replica 按 weight，D4）。
- 窗口时长 500ms 可配（按 group）。
- 局限：会话一致性只保证**同一 client 连接**读己之写；跨连接不保证（与 Redis 主从异步
  复制的固有语义一致）。

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
  route/      路由决策：命令分类表、读写判定、会话一致性、weight 选择（纯函数，易单测）
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
3. **M3 读写分离 + 会话一致性**：接读池 weight、500ms 窗口、回落 master。
4. **M4 快照接入 + failover**：搬 `discovery`，接 Server 快照，实现 master 切换 close/rebuild。
5. **M5 池收敛 + 指标**：SHARED 连接真正多路复用（打开 FIFO 解复用 / reorder 或 D11 化简），
   补 §14 指标。
6. **M6（择机）**：伪 Sentinel 客户端发现、Sentinel 直订阅（O1）、快照原生化简。

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

## 参考

- [命令路由规格](proxy-command-routing.md)
- [架构总览](overview.md) / [组件](components.md) / [流程](flows.md)
