# 冷热网关设计与第一阶段实现计划

> 日期：2026-09-07
> 状态：G1～G3 网关基础已实现（G3 采用保守工作区预算）；G4 双层集成、开发环境版本验证与短基准已完成，持续压力/资源成本验收待做（§17）。尚无冷热能力。
> 前置文档：[冷热混合存储预研](hot-cold-storage-research.md)。
> 仅供隔离开发测试；已在 redis01 部署独立 Gateway 和专用测试 Redis，不改变现有 Proxy 路由。

控制面接入进展：[G5 接入设计](gateway-control-plane.md)。G5a 已实现 Server 配置草稿及管理 API；G5b 已完成开发机独立部署；G5c 最小登记/心跳观测闭环已实现但未部署，配置应用确认和 Proxy 切流未实现。正式冷热产品目标为 Cluster，单后端模式仅用于开发验证。

当前实现与运行方式见 §14～§16（分阶段记录）。下文配置草案与完整阶段目标不等于全部已经实现，以最新交付记录为准。

G6a-1 已增加独立测试调用的复制捕获、日志恢复与 PSYNC 续传原型，见 [异步同步路线与验收](gateway-async-sync.md)。G6a-4 已在隔离实验中把真实 RDB/增量日志应用到 Kvrocks，见 [无 TTL String 应用原型](gateway-string-apply.md)；仍未接入运行服务，尚无可部署的冷热能力。

G6a-5 增加 [单项 pending 恢复](gateway-pending-recovery.md)，G6a-6 增加 [分段定位与安全回收](gateway-segmented-journal.md)。这些仍是隔离同步原型；TTL、淘汰/回温、所有权、物理磁盘水位和运行服务接线未完成。

G6a-7 已在隔离原型内补充 [磁盘水位、积压和孤儿清理](gateway-sync-resources.md)。仍未接自动调度或运行服务，生产阈值与长期同步故障时的热层限写策略待确认。

## 1. 目标与阶段边界

最终目标是在不改 Redis / Kvrocks 引擎源码的优先预研方向下，由网关承担冷热协调。
完整同步、回温、安全降冷、Cluster 和高可用尚未完成协议验证，不属于第一阶段交付。

第一阶段交付一个独立、可测量的单分片纯转发骨架：

- 一个测试监听入口绑定一个逻辑服务、一个测试分片、一个 Redis 主节点。
- RESP2 请求检查、受控转发、响应顺序、pipeline、超时与关闭。
- 静态配置、生命周期、健康探测、基础指标和结构化日志。
- 使用隔离测试入口和测试数据；不接管原有实例组。
- 评估增加网关的延迟、连接数和内存成本。

明确不做：Kvrocks 业务写入、异步同步、回温、淘汰、过滤器、精确冷热目录、分布式锁、选主、Cluster 重定向、读写分离、动态拓扑、业务写请求去重。
Redis 端不得预先降冷；所有被验证的业务数据均完整存在于测试 Redis。
禁止用透传模式承载只有部分数据的热层，防止冷 key 被当作不存在。

## 2. 控制面与数据面职责

| 组件或链路 | 分类 | 职责 |
|---|---|---|
| Server API | 控制面 | 配置、实例和分片规划、网关归属、任务编排、审计 |
| Server → Proxy / 网关 | 控制面通信 | 未来发布路由和策略；不是逐请求审批 |
| Agent | 管理执行侧 | 仅 Redis/Kvrocks 容器部署、启停、健康与资源采集，不管理 Gateway |
| 运维部署 + systemd | 生命周期管理 | 独立 Gateway 二进制安装、升级、启停 |
| 客户端 → Proxy | 数据面入口 | 认证、接入、连接管理、选择网关 |
| Proxy → 网关 → Redis / Kvrocks | 数据面 | 业务执行、后端路由、未来冷热协调 |
| Redis → 同步模块 → Kvrocks | 数据面复制链路 | 传递数据变化、可靠应用和确认 |

Server 决定归属和策略，网关执行实时协调。每次 GET、回温和候选淘汰不经过 Server API，不把全量 per-key 高频状态放入 pilot.db。
冷热实例后端选择统一归网关，前置 Proxy 不再同时选择 Redis 主从。
普通实例继续 Proxy → Redis，不强制经过网关。

已确认的第一版访问策略：冷热实例使用单个业务入口，不提供 auto / master 双端口；热读、热写都由网关路由到对应 Redis 主节点，不开放副本读。
Redis / Kvrocks 副本仍作为后续高可用部署的一部分，副本数量与切换协议待定；这不扩大当前纯转发骨架的交付范围。
现有普通主从实例的 auto / master 端口及读写分离行为保持不变。
副本读只在未来压测证明收益且一致性协议完成后另行评估，不是第一版待开启的默认功能。

第一阶段使用静态配置绕开控制面接入，并非否定最终的 Server 管理模式。

## 3. 代码复用评估

以下基于本仓库实际读取的代码，不是完整安全审计；列出的缺口需要测试确认和实现前处理。

| 代码 | 观察 | 复用建议 |
|---|---|---|
| [resp/resp.go](../../internal/proxy/resp/resp.go) | ReadCommand 返回原始帧及 argv，ReadFrame 读取 RESP2 完整响应；Writer 可编码或原样发送 | 首选复用候选，但必须先补长度、结构和响应预算测试 |
| [backend/upstream.go](../../internal/proxy/backend/upstream.go) | SHARED FIFO 解复用、EXCLUSIVE 直接收发；SendBatch 中 Flush 返回值未检查 | 复用思路，不原样用于新网关；先验证错误传播与生命周期 |
| [backend/pool.go](../../internal/proxy/backend/pool.go) | RoundTrip 在 channel 等待，无 context 参数；共享连接数限制不等于在途请求限制 | 第一阶段不复用共享池；将来单独设计取消、队列、字节预算 |
| [session/session.go](../../internal/proxy/session/session.go) | 同时绑定认证、读池、路由分类、事务/订阅与指标 | 不整套实例化或复制为网关 Session |
| [route/route.go](../../internal/proxy/route/route.go) | 读写分类、独占连接选择；未知命令兜底写主库 | 可参考命令归类，不能用作冷热 key 依赖识别；网关先采用明确准入列表 |
| [proxysvc/proxysvc.go](../../internal/proxy/proxysvc/proxysvc.go) | listener、会话跟踪、发现、伪 Sentinel、连接池集中组装 | 参考启动和关闭方式，新建轻量网关服务，不引入发现及伪 Sentinel |
| [metrics/metrics.go](../../internal/proxy/metrics/metrics.go) | 私有 Prometheus registry，固定 proxy 命名空间 | 复用采集模式，不直接使用 proxy 命名空间 |
| [cmd/proxy/main.go](../../cmd/proxy/main.go) | YAML/flag 合并、统一日志初始化、服务启动 | 复用约定及 internal/logger，不复用 Proxy 全部配置 |
| [apitypes/types.go](../../pkg/apitypes/types.go) | 当前 ProxyCluster 是端点集合，未含网关归属/slot 状态 | 第一阶段不改；后续设计独立配置版本和兼容迁移 |

### 3.1 RESP 复用前必须补齐的检查

当前读取路径存在以下待覆盖项：

- 请求聚合 maxBytes 检查并不覆盖所有分配前预算；bulk 分配、元素数量和整数溢出需校验。
- ReadFrame 未复用请求的字节限制，响应数组按计数预分配，递归嵌套需深度和大小上限。
- bulk 尾部 CRLF、空数组/null 参数、截断帧和非法长度的处理需测试。
- inline 当前是按空格拆分，不能当作完整的引用/转义解析；第一阶段建议只接受 RESP 数组命令。
- 大于内部行缓冲的合法行与错误行的行为要明确；不能仅加外层帧长度检测便宣称防 OOM。

建议先直接通过窄适配层引用现有 resp 包，不为“未来通用”立即搬迁全部代码。
需要修正共用解析行为时，作为独立、带回归测试的变更提交；如行为不能兼容旧调用方，先设计独立受限 API。
新网关自身的资源限制不能依赖请求发到 Redis 后再报错。

### 3.2 基线检查记录

本轮执行 `go test ./internal/proxy/...`，退出码为 0，已有测试包通过（输出为缓存结果）。
其中 resp、session、config 当前没有独立测试文件；其他包通过不代表这些边界已验证。
未运行真实 Redis / Kvrocks 集成测试、性能压测或故障注入。

## 4. 第一阶段的进程与连接模型

### 4.1 部署

已新增 `cmd/gateway` 启动入口；`make gateway` / `make gateway-linux` 构建 `bin/redis-gateway`。采用独立二进制 + systemd，不接入 Agent 或普通 `make deploy`；步骤见 [部署说明](gateway-deployment.md)。
配置默认仅监听回环地址，通过人工测试配置独立启动，不修改现有 server.yaml / proxy.yaml。
第一阶段只允许一个服务、一个分片、一个非 Cluster 主节点；遇到副本或 Cluster 后端不 ready。

未来一个进程可管理多个分片，但现在不引入通用分片调度器。
未来每分片单活需要可靠所有权和隔离；第一阶段没有此能力，不能用多进程负载均衡宣称高可用。

### 4.2 连接建议：每条入站连接独占一条后端连接

生命周期内不换后端，不跨会话共享，不回池。会话退出时关闭后端连接。
好处是减少 FIFO 跨客户端解复用、连接状态泄漏与取消处理；代价是连接数较多，必须限额并压测。
这不意味着单线程处理整个分片，不同会话可以并行。

重要边界：网关看到的“入站连接”可能是 Proxy 的共享后端连接，不一定对应真实业务客户端。
未来事务、WATCH、订阅等依赖会话的功能，需要验证 Proxy 全链路连接固定与状态传递；单纯网关 1:1 连接不能自动解决。

### 4.3 同一连接内的顺序

- 一个有界读取/准入过程分配会话内序号，只用于响应排序，不是业务幂等 ID。
- 普通命令按序写入同一后端连接，单个响应读取者按顺序关联响应。
- pipeline 允许多个未完成请求，但限制条数、总请求字节和响应暂存字节。
- 本地拒绝或本地 AUTH 响应必须占据原请求的顺序位置，不能插到较早请求响应前。
- 同一客户端断连后取消其会话，不自动将已发命令投递给新连接。
- partial write、Flush 错误和后端协议错误使当前后端连接不可复用，终止会话。

并发实现可使用有界队列；goroutine 数量、队列实现及缓冲策略待编码评审，不承诺采用现有 Pool。

## 5. 模块与内部契约

建议先使用 `internal/gateway` 内少量文件，避免为尚未实现的冷热功能建立大量空包。

| 模块 | 输入 | 输出 / 契约 |
|---|---|---|
| Config | 静态配置、显式 CLI 参数 | 校验后的不可变配置，拒绝未知字段与无效限制 |
| Service | 配置、生命周期 context | listener、健康端点、会话注册、drain |
| Session | 一条入站连接及固定分片配置 | 保序处理、认证、准入、关闭，不做重试 |
| Protocol adapter | 字节流、预算 | 经验证的帧与 argv；内存/深度/长度上限 |
| Command admission | argv、当前会话状态 | forward / local / reject；暂不产出冷热 key 列表 |
| Upstream session | 固定 Redis 地址、独占连接 | 连通及身份检查、发送/读取、错误分类 |
| Metrics / logging | 低基数事件 | 独立 registry、无 key/value/password 日志 |

错误契约至少区分：发送前失败、已开始发送但结果不确定、完整后端响应、客户端交付失败。
不能把一个普通 error 丢掉阶段信息后统一“重试一次”。

后续扩展边界而非当前接口承诺：

- Backend routing：静态目标替换为带版本的 slot 路由与重定向处理。
- Cold coordinator：命令执行前的数据就绪检查、回温和冲突协调；未知依赖不得默认 ready。
- Sync observer：基于明确源历史和操作屏障查询可靠进度；不能只返回一个 synced=true。
- Ownership guard：接管与真正的后端隔离；本地 epoch 字段不等于 fencing。

以上扩展暂不定义 Go 方法签名、持久化表和 HTTP API，避免把未验证的同步协议固化进代码。

## 6. 配置草案

以下是拟议字段，不是可直接运行的配置。数值仅为测试起点，最终默认值由压测决定。

| 字段 | 示例 / 规则 |
|---|---|
| gateway_id | 静态非空标识，禁止直接用其宣称分布式唯一 owner |
| mode | 仅 `passthrough`；其他模式启动失败，不做占位假实现 |
| service_id / shard_id | 各一个固定标识，用于隔离、日志和指标 |
| listen | `127.0.0.1:17379`，与现有入口分离 |
| admin_listen | `127.0.0.1:19200`，提供 live/ready/metrics |
| redis.address | 隔离环境非 Cluster 主节点地址 |
| frontend_password_file | 独立测试入口凭据，避免与内部后端凭据混用 |
| redis.password_file | 独立后端凭据；ACL 用户名支持待定 |
| connect_timeout | 候选 3s |
| request_timeout | 候选 5s，从准入到完整后端响应，包含排队 |
| client_write_timeout | 候选 5s，限制慢客户端 |
| shutdown_timeout | 候选 10s |
| max_connections | 候选 256；到上限拒绝新会话 |
| max_request_bytes / max_response_bytes | 候选 16 MiB / 32 MiB；业务大对象兼容需单独评估 |
| max_inflight_per_session | 候选 64 |
| max_inflight_bytes_per_session | 候选 16 MiB，不能只按条数限制 |
| total_buffer_budget | 必须配置或有保守默认；所有会话共用硬预算，具体值待定 |
| max_frame_depth / max_array_elements | 显式受限，值待定；编码前确定测试覆盖 |

空密码仅在显式测试许可且回环监听时允许，不能静默暴露公网入口。
未知字段、冲突监听、超限长度、无效时间值启动失败；不输出凭据内容。
配置优先级沿用“默认 → 文件 → 显式 CLI”；动态热更新、Secret 管理和证书方式后续再设计。
达到会话或全局预算后停止准入/读取，保留有界响应处理空间，不无限创建等待 goroutine。

## 7. 命令准入与请求流程

### 7.1 第一阶段建议测试集合

- 本地会话：AUTH（独立入口口令）、QUIT；具体参数校验与响应需测试。
- 后端执行：PING、ECHO、GET、SET、INCR、DECR、APPEND、DEL、UNLINK、EXISTS、TYPE、TTL、PTTL、EXPIRE、PEXPIRE、PERSIST。
- 在仅有一个完整数据后端的测试中加入 MGET/MSET，验证数组响应和 pipeline，不代表未来跨 slot 支持。
- SET 等可选参数交给固定版本 Redis 执行校验，但测试必须覆盖选定参数，不能宣称所有版本兼容。
- 第一阶段拒绝 HELLO/RESP3、事务、Lua、阻塞、Pub/Sub、SELECT、CLIENT 状态修改、管理/复制/Cluster 命令和未知命令。

这是暂定的转发验收范围，不是最终冷热兼容清单。常见客户端握手命令需记录并逐项放行，不能为了连接成功全面放开未知命令。
普通 Proxy 现有透传功能不受此测试入口准入限制影响。

### 7.2 请求顺序

1. 会话认证、连接准入和全局预算检查。
2. 解析完整 RESP 数组命令，严格校验长度与结构。
3. 建立序号槽位，进行命令准入，本地结果同样进入保序出口。
4. 需要后端执行时，使用当前会话的独占连接；发送前确保初始化成功。
5. 按序发送并读取完整 RESP2 响应，后端错误也是一个正常响应帧，不视为传输错误。
6. 更新指标并按序交付，释放预算。

第一阶段不解释 GET 的 nil，不访问冷层，不执行额外业务写入。
收到 ASK/MOVED 等意外响应时不自动跟随；记录不支持的拓扑状态并使相应就绪状态失效，已收到的错误帧按序转交。

## 8. 超时、断连与关闭

| 场景 | 行为 |
|---|---|
| 后端建连/认证失败，尚未发送业务命令 | 返回明确错误或关闭；不循环重试，指标标记 before-send |
| 后端返回业务错误 | 原样按序转发，不改写成网关故障 |
| 开始写命令后发生超时/断连 | 结果不确定，关闭后端和入站会话，不重放 |
| 响应只传了一部分 | 关闭连接，不拼接一条错误帧冒充完整响应 |
| pipeline 中某条请求超时 | 关闭整条独占会话；后续请求可能已执行，不承诺未执行 |
| 完整后端响应已收到，但客户端写失败 | 不重新执行业务命令，只记录交付失败 |
| 客户端断连 | 关闭独占后端，释放资源；不得据此宣称已发送命令被撤销 |
| 超大请求或非法帧 | 分配前拒绝并关闭；可安全发送错误时才发送，不继续猜帧边界 |
| 超大响应 | 关闭会话并报告指标，不能让无界分配导致进程 OOM |

SIGTERM：ready=false → 停止新连接和新请求准入 → 有界排空已准入请求 → 超时关闭残留连接。
退出超时不是事务回滚，也不是安全重试信号。
探测 Redis 的连接独立于业务连接，不能往业务连接插入会破坏 FIFO 的探测命令。

## 9. 健康、指标与日志

- `/livez`：进程内部基本运行状态，不因为一次 Redis 不通就作为存活失败触发重启风暴。
- `/readyz`：监听已准备、未 drain、配置有效、最近后端检查成功且是允许的主节点/模式。
- 后端探测周期和失败阈值待定，探测时检查身份；第一阶段不自动切主。
- `/metrics`：独立命名空间 `redis_pilot_gateway`。

指标至少包含会话数、后端连接数、请求/错误计数、排队与后端耗时、inflight 条数/字节、预算拒绝、超时阶段、未知结果计数、drain 耗时。
命令标签归一化，未知命令统一 OTHER；不将 key、任意错误文本、动态请求 ID 作为 Prometheus 标签。
日志只记录服务/分片、阶段、错误类别和路由信息，不记录 value、AUTH 或完整命令参数。
采样日志与慢请求记录策略待定。

## 10. 与现有 Proxy / Agent / Server 的接入顺序

1. 首先测试客户端 → 网关 → 隔离 Redis，不接现有 Server。
2. 再启动独立测试 Proxy，用静态主后端指向网关，关闭读写分离，测试双层 pipeline 和错误传播。
3. 不把独立网关测试入口加入现有生产实例快照，不修改普通实例的监听器。
4. 完成基准后再设计 Server 的新实例模式、网关资源与路由快照；字段和 API 待定。
5. 提供独立二进制和 systemd 部署模板，再实现 Gateway 直接向 Server 注册、拉取配置与健康上报。Agent 不参与网关部署、回温或业务请求协调。

当前 Proxy 会终结 AUTH，后端池与用户会话存在复用关系；Proxy→网关内部身份如何传递与授权需单独设计。
双层测试阶段仅使用回环/隔离网络及明确测试凭据，不由“Proxy 已认证”推断网关入口天然安全。
未来高级会话命令必须做端到端状态测试，不能只验证客户端直连网关。

## 11. 测试与验收

### 11.1 不依赖真实 Redis 的测试

- 配置：未知字段、非法上限、配置覆盖、密码脱敏、端口冲突。
- 协议：二进制参数、空值/空字符串、整数/错误/数组响应、部分读写、错误 CRLF、极大长度、整数溢出、嵌套超深和截断帧。
- 会话：并发连接互不串响应；pipeline 中混合成功/本地拒绝/后端错误严格保序。
- 假后端注入：接收完整写后断连、响应部分发送后断连、Flush 错误、慢响应与主动取消。
- 资源：慢客户端、满队列、全局预算、重复关闭、drain 超时，无无限 goroutine/连接增长。
- 断言不自动重放：假后端记录每个请求接收次数，不能用 GET 最终结果代替验证。
- 协议与状态机 fuzz、相关包 race 检查。

### 11.2 隔离 Redis 集成与压测

必须使用专用容器或临时端口和数据目录，不碰仓库现有 dump.rdb 或用户实例。

对比四条路径：直连 Redis、现有 Proxy→Redis、网关→Redis、独立测试 Proxy→网关→Redis。
固定硬件、Redis 版本、持久化配置、连接数、pipeline 深度、key/value 大小及读写比例。
记录吞吐、P50/P95/P99、CPU、RSS/Go heap、GC、连接数、带宽及超时/拒绝率。
指标验收阈值待业务目标和基准后确定，不预先宣称增加一跳没有性能影响。

正确性门槛：

- 选定命令返回和最终状态与直连一致，TTL 在允许的时间误差内。
- pipeline 顺序正确，本地拒绝不能提前越过已发请求。
- 无故障时不丢响应；故障时不伪造成功、不自动重复执行、不跨连接串响应。
- 达到限制后受控拒绝，解除压力后恢复，不因超大帧 OOM。
- 优雅关闭及超时关闭均能结束，连接和 goroutine 回到可解释的基线。

### 11.3 不能由第一阶段宣称的结论

不能证明冷热一致性、复制 exactly-once、完整命令兼容、无感迁移、高可用或零丢失。
纯转发性能也不代表加入回温/同步后的最终性能。

## 12. 开发顺序与后续门槛

| 批次 | 代码工作 | 验收 |
|---|---|---|
| G1 | 受限 RESP 适配和测试、配置、日志、启动/健康骨架 | 不影响既有 Proxy，协议分配受限 |
| G2 | 单会话独占后端、准入与认证、顺序转发 | 基础命令与错误分类通过 |
| G3 | 有界 pipeline、全局预算、取消/drain、指标 | 故障注入及资源测试通过 |
| G4 | 隔离 Redis 集成、双层 Proxy 测试、基准报告 | 确认独立网关部署成本是否可接受 |
| 后续 | 单向同步协议原型，禁止降冷 | 去重/续传/分类验证后才接冷热闭环 |

建议每批独立评审，不在第一批夹带原有 Proxy 大规模重构。

## 13. 待定项与实施前检查

| 编号 | 项目 | 是否阻塞骨架 |
|---|---|---|
| G-O1 | 固定 Redis 版本、构建与隔离测试运行方式 | 阻塞真实集成，不阻塞协议单测 |
| G-O2 | 帧/数组/嵌套、会话和全局预算的初始值 | 编码时需给保守上限，压测后定正式值 |
| G-O3 | 前后端认证、未来 Proxy 身份与会话传递、TLS | 回环原型可受限运行，阻塞共享网络上线 |
| G-O4 | 完整命令和参数矩阵、客户端握手 | 第一阶段暂用明确列表，不允许未知兜底 |
| G-O5 | Server/Gateway/CLI API、独立部署模板、动态路由 | 不阻塞静态原型 |
| G-O6 | 同步分类、去重、历史交接与可靠提交 | 不阻塞骨架，阻塞冷热功能 |
| G-O7 | 选主、状态恢复、fencing | 不阻塞隔离单节点原型，阻塞高可用上线 |
| G-O8 | 写成功承诺、RPO/RTO、延迟/吞吐预算 | 不阻塞骨架，阻塞最终选型和生产验收 |

最终部署是否维持独立网关进程，应结合双层转发成本与运维复杂度评估。
若合入 Proxy，也应保留冷热协调的逻辑边界，不能恢复为多个 Proxy 各自无协调降冷。

后续实现完成时更新本文件的状态、实际字段、测试报告和预研文档中的决策；本草案没有任何已部署生产网关的含义。

## 14. G1 当前交付记录

本节记录 G1 skeleton 模式，G2 新增能力见 §15；两种模式均保留。

实现文件：[入口](../../cmd/gateway/main.go)、[配置](../../internal/gateway/config.go)、[受限 RESP2 解析](../../internal/gateway/resp.go)、[服务骨架](../../internal/gateway/service.go)、[测试](../../internal/gateway/gateway_test.go)。

当前行为：

- 仅支持 `mode: skeleton`，不接受 passthrough/hybrid；不包含 Redis 地址、认证与转发配置。
- 仅允许字面量回环 IP 监听；数据入口固定返回 ERR 并关闭，不解析或执行业务请求。
- `/livez` 提供进程状态，`/readyz` 始终 503，`/metrics` 提供独立的骨架拒绝计数。
- 支持 SIGTERM/取消、管理 HTTP 超时、数据入口有界拒绝和监听失败清理。
- YAML 严格字段校验、单文档限制、64KiB 配置大小上限；无配置时使用隔离默认值。
- 解析器可供后续 Session 接入，限制整帧字节、整帧元素总数与嵌套深度，校验 CRLF、整数溢出和数组结构；拒绝 inline 命令。
- 没有修改既有 Proxy 解析器。复用评估后选择独立的窄解析实现，避免新安全限制改变旧客户端行为；后续如抽公共包需独立评审。

实际配置示例：[gateway-skeleton.yaml](../examples/gateway-skeleton.yaml)。这里只提供 `-config` 与 `-check-config`，尚未实现业务配置的 CLI 覆盖。

```sh
go run ./cmd/gateway -config docs/examples/gateway-skeleton.yaml -check-config
go run ./cmd/gateway -config docs/examples/gateway-skeleton.yaml
```

第二条命令占用示例中指定的回环端口；自行启动测试后使用 Ctrl-C 停止。不得将其接入正常业务入口。

验证命令：

```sh
go test ./internal/gateway ./cmd/gateway ./internal/proxy/...
go test -race ./internal/gateway
go test ./internal/gateway -run '^$' -fuzz FuzzRESP -fuzztime=3s -parallel=2
go vet ./internal/gateway ./cmd/gateway
```

未实现的 G1/G2 衔接工作：统一日志文件配置、完整进程指标、后端检查与凭据、单会话转发、pipeline 与全局缓冲预算。
当前限制只保障骨架/单帧解析，不应描述为完整网关的资源隔离或性能验收。
没有启动真实 Redis/Kvrocks 做集成或压测；业务正确性与后端故障恢复需要 G2 之后验证。

## 15. G2 单连接转发交付记录

本节为 G2 历史状态，串行执行与立即关闭等行为已在 G3 更新，见 §16。

新增 [session.go](../../internal/gateway/session.go)、[会话测试](../../internal/gateway/session_test.go)、[独立 Redis 集成测试](../../internal/gateway/integration_test.go)。现有 Proxy 代码不变。

### 15.1 当前行为

- 新增 `mode: passthrough`，仅回环监听、单个固定 Redis 后端，不允许 hybrid。
- 每个入站会话独占后端连接；完成前一响应后才处理下一请求。同连接 pipeline 能保序，但不做批量发送，性能不等同 G3。
- 前端 AUTH 在网关终结；后端使用独立文件中的口令，可为空。两者不共享凭据，也不转发前端 AUTH。
- 未配置前端口令时必须显式 `allow_unauthenticated: true`，仅用于隔离环境。口令文件单行，最多 4096 字节，支持尾部换行，文件相对路径以进程工作目录为准。
- AUTH 仅支持单密码形式；ACL 用户名、HELLO、CLIENT 握手等尚不支持，正式 Proxy 内部认证仍待设计。
- 首个准入业务请求前建立后端连接，执行可选 AUTH 和 INFO all；要求 role=master、cluster_enabled=0 且存在 redis_version。
- 后端身份检查不等于 fencing，也不是固定版本兼容保证；后端切主或模式变化需要后续专门协议。
- 明确准入 §7 的普通命令，未知/高级命令本地拒绝；本地响应与后端响应严格保持请求顺序。
- 建连/初始化失败时业务未发送，返回明确错误并关闭；发送后网络、协议或超时错误直接关闭，不重连、不重放、不伪造业务失败响应。
- 后端业务错误原样转交；意外 ASK/MOVED 原样返回后关闭，不实现跳转。
- 最大活跃会话数 `max_connections`；超限直接关闭新连接。所有会话逐帧受 limits 约束；完整全局字节预算仍属 G3。

### 15.2 健康与关闭

passthrough 每 2 秒完成一轮独立后端初始化探测（实际周期另加探测耗时），不往业务连接插入探测命令。
ready 要求服务正在运行、未退出且最近探测成功；启动探测未完成前是 503。探测可连接不表示业务时刻绝不会失败。

G2 退出时停止准入并关闭活跃会话，中止的已发请求结果可能不确定；不是在途写入排空。G3 再实现完整 drain。
客户端读帧受 client_idle_timeout 限制，后端初始化/请求及客户端响应写入受有限超时约束；尚非跨所有阶段统一的端到端 deadline。
客户端在等待后端时断连，最晚由当前请求超时结束后端会话；G3 再细化快速取消。

新增 `redis_pilot_gateway_upstream_result_unknown_total` 统计发出后未取得完整后端响应的请求（含读），不承诺可自动重试。
G1 skeleton 专用拒绝计数仍保留；完整连接、延迟、拒绝指标待 G3 完善。

### 15.3 运行

示例：[gateway-passthrough.yaml](../examples/gateway-passthrough.yaml)。运行前自行准备专用、数据完整的测试 Redis，示例地址是 `127.0.0.1:16389`，不会自动创建后端。

```sh
go run ./cmd/gateway -config docs/examples/gateway-passthrough.yaml -check-config
go run ./cmd/gateway -config docs/examples/gateway-passthrough.yaml
```

check-config 校验配置和口令文件，不检查后端连通性。passthrough 是真实转发入口，不能指向只有部分热数据的实例。
本阶段没有修改服务部署或让实例自动接入新网关。

### 15.4 验证结果与剩余范围

已通过普通测试、race、vet 和现有 Proxy 回归测试。
假后端测试覆盖：认证分离、未认证不发业务请求、pipeline 本地拒绝保序、并发会话隔离、副本/Cluster/错误后端密码拒绝、写后断连、部分响应、超时与取消不重放。

真实集成在本地 Redis 7.2.5 上通过：测试自动启动临时回环实例，关闭持久化，使用独立临时目录和测试口令；结束时停止测试进程。
覆盖 GET/SET/INCR、过期与取消过期、删除、pipeline、AUTH 和 ready 检查。不等于项目所有 Redis 版本均验证通过。

```sh
# 可选集成测试；只指定可执行文件，不传入已有 Redis 地址。
GATEWAY_TEST_REDIS_SERVER=/absolute/path/to/redis-server go test -race ./internal/gateway ./cmd/gateway
go test ./internal/proxy/...
go vet ./internal/gateway ./cmd/gateway
```

尚未验证：正式 Proxy→网关双层集成、性能压测、部分 socket 写入的精确注入、所有参数组合、真实切主、Kvrocks 与冷热协议。
下一阶段 G3 优先补有界批量 pipeline、全局字节预算、在途排空和完整指标；不能把当前串行转发的测试通过视为生产上线许可。

## 16. G3 有界 pipeline、预算与排空交付记录

新增 [pipeline.go](../../internal/gateway/pipeline.go)、[metrics.go](../../internal/gateway/metrics.go)、[G3 测试](../../internal/gateway/pipeline_test.go)。
原 session.go 保留后端初始化、命令列表、凭据读取和探测；原 Proxy 代码未改。

### 16.1 有界批处理

- 每个会话一个解析协程、一条独占后端连接；输入队列容量为 1。
- 从已到达的请求中机会式组批，不为了凑满批次额外等待；批次数由 `max_pipeline` 限制，原始请求总字节由 `max_batch_bytes` 限制。
- `max_batch_bytes` 必须不小于 `limits.max_bytes`，因此单条合法请求也能落入批次上限。
- 已解析但放不进当前批次的请求保留到下一批；输入队列、读取中帧和 pending 也计入工作区预算。
- AUTH/本地拒绝与普通命令按输入顺序判定；批次内部只发送准入的业务请求，响应按原槽位输出。
- QUIT 之后的预读请求不发送；不增加事务、脚本或阻塞命令支持。
- 后端写入使用 32KiB 缓冲批量 Flush，并与读取响应并行，避免写完所有大请求才读大响应造成相互阻塞。
- 一批完成才开始下一批；没有跨批次无限在途队列。实际批次大小受入站节奏和调度影响，G4 需要测量，不承诺总能填满配置条数。
- 部分写入、Flush 错误、异常响应或超时终止连接，不重放。批次未取得完整响应的业务槽位保守计为结果不确定，可能包含实际尚未送达后端的请求。

### 16.2 全局缓冲准入预算

采用“先预留每会话最坏情况工作区，再读取业务请求”，而非逐字节动态争抢。
这样会话等待响应时不必再申请响应内存，避免所有会话占满请求预算却没有空间读响应的死锁。
默认预算不足时直接关闭新会话，已有会话不被挤占；会话关闭后归还，可再次准入。
后台探测另有一份固定预留，启动配置至少能容纳探测与一个会话。

当前预留公式（字节）：

- 会话：`8 × (max_batch_bytes + 4 × limits.max_bytes) + 64 × limits.max_elements × (max_pipeline + 3) + 65536`。
- 探测：`8 × limits.max_bytes + 64 × limits.max_elements + 16384`。

预留覆盖批次、预读/pending、响应、参数描述和 I/O 缓冲的保守容量。
它不是实际 heap/RSS 计量，也不包含操作系统 socket 缓冲、Go GC 尚未回收的垃圾和所有运行时开销；进程仍需额外资源余量。
较大的帧上限会明显降低预算允许的会话数；示例使用 1MiB 帧和批次上限，不把 16MiB 默认帧限制当作所有业务最佳配置。

新增实际字段：`max_pipeline`（默认 8，上限 64）、`max_batch_bytes`（默认 16MiB，上限 64MiB）、`buffer_budget_bytes`（默认 1GiB，上限 8GiB）。
`max_connections` 和预算同时生效，先达到任一限制就停止新增会话。
配置示例见 [gateway-passthrough.yaml](../examples/gateway-passthrough.yaml)。
细粒度动态记账、按租户预算、并发公平性和更紧凑解析分配可作为后续压测优化，不声称本版实现了精确堆上限。

### 16.3 限时排空

退出时：ready=false → 停新连接 → 停各会话读取/准入并停止探测 → 处理已准入批次及有限队列 → 到 shutdown_timeout 强制取消残留连接。
读取停止与准入之间用会话锁协调；未完整接收/未准入的数据不再执行。
正常排空期间允许后端结果送回客户端；强制结束时已经发出的操作仍可能已执行，不能宣称回滚或安全重试。
后端网络 deadline 和客户端写 deadline 仍生效；目前不是覆盖建连、预读、排队和所有响应的统一端到端 deadline。

### 16.4 指标与测试

新增低基数指标：active_sessions、reserved_buffer_bytes、inflight_requests、admission_rejected_total（原因）、transport_errors_total（阶段）、responses_delivered_total（来源）、request_duration_seconds、batch_requests、drain_duration_seconds，统一 `redis_pilot_gateway_` 前缀。
请求耗时从完整解析结束到响应交付，包含排队，不含客户端上传整帧时间；reserved_buffer_bytes 表示容量预留，不表示实际分配。
未添加 key/value/请求 ID 等高基数标签；完整运维告警阈值待 G4。

新增测试覆盖：后端必须接收多条请求才响应、局部拒绝的保序、预算耗尽/复用、批次 Flush 失败、成功排空、超时取消、退出后会话/预算/在途归零。
延续 G2 认证、并发隔离、超时不重放、真实 Redis 集成和 Proxy 回归测试。
真实集成使用临时 Redis 7.2.5，持久化关闭，不触及已有实例或仓库 dump.rdb。

G4 尚需：Proxy→网关双层集成、实际批次分布与吞吐/P99、慢客户端持续压力、内存/GC 与保守预算成本，以及目标部署版本矩阵。
G1～G3 是冷热网关的基础传输与资源控制，不包含同步、回温、降冷、Cluster 迁移或网关高可用协议。

## 17. G4 第一轮验证（2026-09-07）

后续已完成[分负载持续复测与原生工具交叉检查](gateway-g4-benchmark.md)：Proxy 纯读写不止 1.5 万 QPS；首次数字对应交替读写的特定负载。下方本地短测保留为历史记录，不作容量上限结论。

测试入口：[g4_test.go](../../internal/gateway/g4_test.go)。本节是最新验证状态，前面各批次的“尚未验证”描述保留为当时记录。

### 17.1 双层链路与故障测试

- 真正运行现有 Proxy 二进制，连接内嵌测试网关和临时 Redis，验证 `Proxy → gateway → Redis`。
- 前端与 Redis 使用不同测试口令；正确认证可转发，错误前端口令拒绝。
- 同连接 pipeline：SET → INCR → 被拒绝的 EVAL → GET，响应保序且 Redis 最终值正确。
- 假后端已收到 INCR、尚未回响应时退出网关：客户端收到错误或断连；观测到的后端业务请求为一次。本测试验证该故障点不重放，不代表所有网络故障均具备 exactly-once。
- 非读取客户端使用无缓冲 net.Pipe 确定性阻塞响应，连续 8 个会话验证写超时释放 active、inflight 和工作区。它不是 TCP 慢客户端长期压力测试。
- 本地 Redis 7.2.5 集成、gateway race、vet 和现有 Proxy 回归通过；没有修改原 Proxy 实现。

### 17.2 开发环境验证

按 Makefile 的目标连接 redis01；未执行会重启现有服务的 `make deploy`。
在隔离 Podman 容器中使用 Linux/amd64 静态测试二进制与 Proxy 二进制，分别通过以上三项测试：

| 本地已有镜像 | 实测 Redis 版本 |
| --- | --- |
| redis:5 | 5.0.14 |
| redis:6.2 | 6.2.22 |
| redis:7 | 7.4.9 |

容器 `--network none`、不发布端口、只读挂载测试二进制目录；限 1 CPU、512MiB、128 PID，测试总超时 60 秒。
Redis 在容器内部临时目录启动，禁用 AOF/RDB；测试容器已自动删除。没有挂载已有实例数据，没有改 Server/Agent/Proxy 配置。
临时二进制保留在 redis01 的 `/tmp/redis-pilot-g4.GYN3Uw`，未安装为服务。
此矩阵仅覆盖本节列出的测试，不等于 Redis 全命令、所有客户端或全部版本已兼容。

### 17.3 四路径短基准

环境：本地 Darwin/arm64、Go 1.25.1、Redis 7.2.5；无 race。
8 个客户端，每轮 4096 条命令；每客户端独立 key，SET/GET 各半并逐响应校验；value 128B/4096B，pipeline 1/8，各两轮并轮换路径顺序。
网关使用测试配置：64KiB 帧、256KiB 批次、max_pipeline=8、max_connections=32、预算 1GiB。
压测器和网关在同一 Go 测试进程，Proxy/Redis 为子进程；共享开发机资源、未单独预热。QPS 包含建连/认证耗时，以下仅是探索结果，不能当生产容量。

下表列出 pipeline=8 的两轮范围，QPS 单位千条/秒；P99 是**整批 8 条完成耗时**，不可除以 8 当单条延迟。

| value | 路径 | QPS（千/秒） | 整批 P99（ms） |
| --- | --- | --- | --- |
| 128B | Redis 直连 | 109.1～114.2 | 1.61～1.71 |
| 128B | Proxy → Redis | 13.8～15.0 | 6.29～7.31 |
| 128B | gateway → Redis | 14.4～18.4 | 7.81～64.34 |
| 128B | Proxy → gateway → Redis | 5.4～7.1 | 15.86～28.53 |
| 4096B | Redis 直连 | 61.1～93.0 | 1.43～3.24 |
| 4096B | Proxy → Redis | 13.2～14.4 | 6.64～7.64 |
| 4096B | gateway → Redis | 16.8～20.6 | 5.82～8.31 |
| 4096B | Proxy → gateway → Redis | 6.8～7.1 | 14.43～14.68 |

全场景网关批次直方图：40955 批、65601 请求（含认证），其中 18492 批为 1 条，20344 批为 2 条，2114 批为 3～4 条，5 批为 5～8 条；平均约 1.60 条/批。
该直方图混合了 pipeline=1/8 和直连网关/双层场景，不能当作单独 pipeline=8 的平均批次。

结论：当前双层 pipeline 吞吐明显损耗，不能宣称“多一跳成本可忽略”。机会式组批实际偏小；但没有 profile，不能把全部损耗归因于它。
单轮 P99 抖动较大；需分离压测器、预热、延长测试并采集 CPU/heap/GC profile，才能判断调度、分配、组批与原 Proxy 转发各占多少。

复现：

```sh
GATEWAY_TEST_REDIS_SERVER=/absolute/path/to/redis-server go test -race ./internal/gateway ./cmd/gateway
GATEWAY_TEST_REDIS_SERVER=/absolute/path/to/redis-server GATEWAY_TEST_G4_LOAD=1 go test ./internal/gateway -run '^TestG4Comparison$' -count=1 -v
```

测试默认自行编译 Proxy；跨编译运行可设置 `GATEWAY_TEST_PROXY_BINARY` 指向匹配目标平台的二进制，无需目标机安装 Go。
输出 `G4_RESULT` 包含各轮 QPS 与批次 P50/P95/P99，`G4_BATCH_HISTOGRAM` 为该次运行合并的网关组批分布。

### 17.4 未完成项与下一步

G4 **尚未完成性能/资源验收**。优先继续：

1. 已补独立进程、2 秒预热/10 秒测量、两轮 8/32 并发的分负载复测，见[报告](gateway-g4-benchmark.md)；仍需独立资源/跨机器压测、更多并发和 TCP 慢读长期压力，以及逐进程 CPU/RSS、heap、GC 与预算拒绝/恢复。
2. 按路径和 pipeline 分开记录组批直方图、采集 profile，再决定组批/内存分配优化；保持有界性、保序和不重放不退化。
3. 确定可接受吞吐损耗、P99、连接规模和资源预算（待业务目标），再判断双层部署是否可接受。

本轮未接入 Kvrocks，未实现 Cluster slot 路由，也未启动冷热同步/回温/淘汰。不能据此进入生产或认为冷热一致性难题已经解决。
