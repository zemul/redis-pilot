# 冷热缓存实现状态

更新：2026-09-18。mode: cache 开发实现，不代表正式发布。设计见 [冷热设计](hot-cold-gateway-design.md)。

## Redis 自定义分片接入（2026-09-18，开发环境验收）

- 单份规格创建支持 `engine: redis`，必须省略 `cache`；原 Kvrocks 无缓存、删除缓存、双写更新模式保持兼容。手工登记 Redis 集群须显式 `cache_enabled: false`。历史章节中的“Redis 引擎适配未开始”不再代表当前代码。
- Redis 分片使用新专用用途 `redis-storage`：standalone、persistent、75% 容器内存上限、noeviction、RDB、AOF 和 `appendfsync always`，重写配置不丢失用途；禁止覆盖持久化与淘汰策略。不是 Redis Cluster 协议，也不是可自动淘汰的纯缓存集群。同步落盘增加写延迟，不代表副本高可用或磁盘损坏零丢失。
- 注册、扩缩容目标按集群引擎验证，禁止混入另一引擎。Gateway 配置增加 `cache.cold_engine`（省略仍为 kvrocks），配置导出和控制面身份校验包含引擎；runtime.db 绑定引擎，禁止用同一状态目录改引擎。Redis 启动/恢复、迁移准入检查主节点、引擎、加载状态、AOF 健康、同步策略和 noeviction；新迁移目标必须无业务数据。
- 页面沿用实例管理与现有创建弹窗，增加 Redis/Kvrocks 引擎选择；Redis 隐藏额外缓存选项，父子记录按实际引擎展示图标、名称和内存指标。CLI `custom-cluster provision --file` 使用同一规格，无新服务。
- 本轮不增加副本/Sentinel、多 Gateway 接管、引擎在线转换、跨节点事务或新的命令白名单。兼容旧接口，规格里的 `cold` 仍表示权威存储分片，不要求真的存在冷热两层。
- 本地验证：创建幂等/Server 重启续跑和导出使用模拟 Agent；真实本地 Redis 7.2.5、Gateway 与 Proxy 子进程覆盖受控扩缩容、并发 Hash 写、五类型和 TTL、迁移中 SIGKILL Gateway 原盘恢复、控制面中断/确认丢失、源资源释放与地址复用；另验证 Redis SIGKILL 后从原 AOF 恢复，Gateway 原盘恢复后值不丢失、不重复自增。引擎混用、AOF 非同步/淘汰策略和有数据目标拒绝测试通过，全仓 Go 测试、go vet、专项竞态测试、前端测试及构建通过。
- 开发环境：已升级 redis01 Server 和三台 Agent，独立创建 redis-shards-dev。真实 Redis 7.4.9 暴露 setfenv 缺失导致的事务/Lua 兼容问题，已修复并仅更新新 Gateway。API 幂等创建、Proxy 五类型/TTL/批量/同分片事务/Lua、2→3→2 分片迁移及 1,587 次并发写后读检查、Gateway SIGKILL 自动重启和 Redis 分片正常重启后校验通过。旧集群拓扑/入口未改变，详情见 [部署记录](gateway-deployment.md#redis-自定义分片开发验收2026-09-18)。不是生产性能、整机掉电、多副本故障或浏览器完整操作流程验收；新版沙箱本轮未在真实 Kvrocks 上重跑。

创建规格示例（API `/api/v1/custom-clusters/create`，CLI `custom-cluster provision --file`）：

```json
{"id":"redis-orders","engine":"redis","gateway_id":"gateway-redis-orders","node_id":"redis01","cold":{"count":2,"memory":"1Gi","cpus":1,"servers":["redis02","redis03"]}}
```

Gateway 标识须先配置机器凭据；创建完成后仍要导出配置、独立部署和初始化 Gateway、登记 Proxy 入口，不是自动安装 Gateway。

## 页面集群聚合与创建（2026-09-18）

本轮接入现有实例管理页面：逻辑集群聚合为一条可展开记录，冷热成员不再在普通实例区重复显示。后续页面修订将单点、主从和分片集群合并为同一张列表，共用搜索/筛选/创建入口，移除独立自定义集群侧栏；该修订 bb25b54 已部署 redis01。管理员可提交单份规格、查看持久创建进度/重试原因、导出初始 Gateway YAML、登记 Proxy 入口；只读账号仅查看。资源完成与业务就绪分开判定。没有新增 Agent 启停 Gateway、分片副本、在线策略切换或自动服务安装能力。字段、权限与验证范围见 [实例管理设计](instance-management-ui.md#自定义集群聚合与创建2026-09-18)。代码 e080240 已推送 dev 并部署 redis01 Server 与内嵌页面；下文“页面未完成”的记录描述此前阶段。部署验证见 [部署记录](gateway-deployment.md#页面集群聚合部署2026-09-18)。

## 本轮真实创建与接入验收（2026-09-17）

新增 spec-update、spec-invalidate、spec-nocache 三个隔离开发集群：真实单份规格创建 → 真实 Agent/Podman 配置和数据库预检 → 导出配置 → 独立 systemd Gateway → 原 Proxy 接入。两轮五类型、多 key、TTL、大 Hash 缓存语义和重启检查通过，共 1200 次并发写后读校验，无旧值或请求错误。新 Gateway/专用后端和防火墙已配置开机启动；未实际重启宿主机。本轮不修改旧集群数据、不覆盖原 Gateway，保留三个新集群。部署位置、凭据私有路径、备份和日志见 [部署记录](gateway-deployment.md#单份规格到真实-proxy-验收2026-09-17)。

此结果补充下文各实现阶段的测试替身验证；不是页面一键安装、无人值守部署器、性能或长稳验收。Server 及 redis02/03 Agent 已更新；redis01 Agent 本轮没有新数据库任务，未更新。新增安装/验收脚本未自动提交或推送。

## 自定义集群：缓存可选（2026-09-17）

- 新建 Kvrocks 集群可显式设置 API `cache_enabled: false`，不配置 hot_instance；Gateway 对应 `cache.enabled: false`，删除 redis_address/backend_password_file。关闭后不连接热库、不做热库清空/回温/失效，仍复用原有命令协调、HRW 分片、迁移日志与恢复协议，不是无保护的透传。旧配置和旧数据库记录省略开关时仍启用缓存。engine 默认 kvrocks，Redis 自定义分片当前拒绝。
- 新增 `/api/v1/custom-clusters` 及详情、tasks、ingress 同语义路由，原 cold-clusters API 保留。CLI `custom-cluster` 是原 cold-cluster 的别名。前端“自定义集群”只读聚合展示存储引擎、缓存开关、成员、统一入口、实时 Gateway 观测与迁移任务，不把 ingress.ready 或过期心跳当成实时健康。
- 开关目前是创建时配置，**不是在线切换能力**。已存在的 Gateway 持久化状态绑定热地址，不能通过改配置直接开启/关闭缓存；控制面模式不匹配也不会确认入口或接受任务。无缓存模式保留原命令支持边界和阻塞命令前缀约束，不新增 Redis Cluster 协议或副本切换。
- 本地新增配置/状态兼容、存储登记/迁移、API 鉴权测试通过；无缓存五类型、批量读写，以及含原两种缓存模式的配置式迁移/重启 race 测试通过。redis03 隔离真实 Kvrocks 进程通过无缓存五类型和迁移/重启测试，日志 `/tmp/pilot-no-cache.MuQLpJ/result.log`。未重启或改写 managed-dev。前端单测、构建及本地模拟数据页面交互检查通过，未将新页面部署到开发 Server。
- 后端及 CLI 已支持从单份规格自动创建资源、登记集群和导出 Gateway 初始配置，独立服务的开发环境整链路已验收。**未完成**：产品化配置自动安装，以及页面创建/操作流程。当前页面不提供假一键创建按钮；Gateway 仍独立部署。一个分片对应实例组及未来副本模型仍需继续落实，现阶段仍为专用 standalone 实例。Redis 引擎适配、Sentinel 接入未开始。

## Gateway 初始配置导出（2026-09-17）

- 管理员 `POST /api/v1/custom-clusters/:cluster/gateway-config`，输入 server_endpoint、listen、admin_listen、state_dir、token_file；CLI `custom-cluster gateway-config NAME --file deployment.json` 输出纯 YAML。字段含义和部署步骤见 [部署说明](gateway-deployment.md#从已创建集群生成初始配置)。API 返回 cluster_id、node_id、revision、yaml。
- 从已登记集群读取真实地址、节点身份、冷热开关与写策略，远端后端仅放行精确私网 IP（/32 或 /128）。输出用 Gateway 自己的严格解析器再次验证。监听必须为回环固定端口；控制面要求 HTTPS 或本机回环 HTTP。配置不包含机器 token 内容、业务密码或后端密码，不读取 token_file。
- 只允许 revision=1 且没有迁移任务的集群；旧手工登记且启用缓存但未声明 write_policy 的集群不猜策略，继续使用原 YAML。配置仅用于首次安装，不能用于丢失路由状态后的重建或多 Gateway 接管。导出不锁定后续拓扑，使用前需重新核对；真正初始化仍须数据库空库检查。
- 此接口无部署副作用，不调用 Agent、不写 Gateway 状态、不初始化冷库、不创建/确认 ingress。机器凭据仍由运维安全安装，Gateway 仍独立二进制/systemd 启动。首次初始化只执行一次，systemd 正常启动不得附带初始化参数。无需修改现有业务认证方式。
- 未默认启用阻塞命令前缀（需显式规划 cold_only_prefixes），连接/帧/预算采用现有分片示例的保守值，不代表压测选型。导出不验证端口是否被其他进程占用、主机防火墙和路径权限，也不代表服务 ready。
- 验证覆盖三种缓存配置的 YAML 往返、精确 IPv4/IPv6 放行、非法路径/公开监听/零端口/迁移状态拒绝、管理员权限、机器凭据不泄露、规格创建后直接导出，以及 CLI 纯 YAML 输出。未将新创建→导出→真实服务启动链路部署至开发机。

## 单份规格创建入口（2026-09-17）

`POST /api/v1/custom-clusters/create` 或 CLI `redis-pilot-cli custom-cluster provision --file cluster.json` 接收以下规格，不必先手动创建资源子任务：

```json
{
  "id": "orders",
  "gateway_id": "gateway",
  "node_id": "redis01",
  "cold": {
    "count": 2,
    "memory": "2Gi",
    "cpus": 1,
    "disk": "50Gi",
    "servers": ["redis02", "redis03"]
  },
  "cache": {
    "memory": "4Gi",
    "server": "redis02",
    "write_policy": "update_existing"
  }
}
```

- 当前只支持 Kvrocks 存储，冷分片 1–128 个，可选一个 Redis 缓存。省略 cache 表示无缓存；提供 cache 时 write_policy 默认 invalidate，可选 update_existing。策略写入集群记录，Gateway 配置必须匹配；不是在线切换。旧手工登记记录未指定策略时保持原行为。
- 每个资源必须提供 memory；CPU 默认 1。cold.servers 可省略自动调度，或提供与 count 等长的列表（空项自动调度）；cache.server 可省略。支持 node_selector 标签约束和 engine_version。自动调度按剩余容量选节点，不保证分片跨机分散；disk 仍是容量预算，不是磁盘配额。
- 集群 ID 最长 32 字符，生成资源名 `<id>-cold-1` 等、可选 `<id>-hot`，分片 ID 为 s1、s2 等。Gateway 所在节点需已登记，机器凭据需预配置；不在此规格中设置业务密码。远端数据库节点需为私网 IP，并仍需正确配置网络访问控制。
- 资源/端口预留、全部子任务、父任务及请求回执在同一个 SQLite 事务提交。资源不足、名字或 Gateway 冲突时整体回滚，提交前不调用 Agent。事务内复核选中节点的地址、容量、标签和状态，变化则拒绝本次预留，可重新提交。
- 返回 HTTP 202，含 task_id、cluster_id、status、phase；通过 `/tasks/{id}` 查询。相同集群 ID、相同归一化规格重复提交返回原父任务（并发或 Server 重启后同样有效）；同名不同规格返回冲突，不静默修改或追加实例。当前无删除重建该回执的工作流。
- Server 继续执行专用子任务、验证数据库、组装集群。成功仅表示 cluster_registered，**不表示业务入口可用**：可继续调用上述配置导出接口，但不会自动安装配置、部署服务或发布 ingress，也未新增页面创建表单。已有手动登记 create 和子任务组装 provision API 保留。
- 验证包括无缓存及两种缓存策略、六个并发相同提交、关闭/重开数据库后续跑、成功后重复提交不再调用 Agent，以及容量不足、后续成员重名、最终父任务冲突时整批回滚。Agent 使用测试替身，不能据此宣称真实三机一键部署已验收。
- 本轮全仓 `go test ./...`、`go vet ./...`、Server/Store/CLI 创建链路专项 race 检查通过；另验证节点容量在调度快照之后变化会拒绝并回滚预留。未部署到 redis01–03，未自动提交或推送。

## 专用实例配置基础（2026-09-17）

- 创建实例 API 增加不可变 `profile`：`redis-cache` 要求 Redis/cache，`kvrocks-storage` 要求 Kvrocks/persistent；两者均限 standalone、关闭普通 Proxy 入口、无实例业务密码。省略 profile 保持普通实例行为，不自动转换已有实例。
- Redis 缓存模板关闭 RDB/AOF，采用 allkeys-lru，maxmemory 为容器内存预算的 75%；允许 Gateway 所需 FLUSHDB，仍禁用 FLUSHALL。剩余 25% 是开销预留，不是不会 OOM 的保证。不得绕过 Gateway 访问专用后端。
- Kvrocks 存储模板启用 WAL 和同步写，workers=2；block cache 为内存预算的四分之一、单 write buffer 为十六分之一（4 个 buffer），至少配置 64Mi。max-db-size=0，不误把 RAM 额度当数据库容量限制；disk 字段仍是容量规划信息，并非磁盘配额。同步写不等于副本高可用或任意故障零丢失。
- 用途写入 SQLite，Agent 生成配置中保留用途标记，配置采用临时文件、同步与原子替换；重写、重启后仍保留用途。禁止移除用途、原地改用途、修改专用组引擎/类型或向组追加副本。新 Server 使用带 profile 的 Agent 路径；旧 Agent 返回不支持，不能忽略字段后创建普通实例。
- 专用创建调用失败不自动删除后端：超时可能已创建成功，状态保留为待核对，不能当作确定不存在。自动核对与续跑已接入下述 Server 单实例创建任务。专用 Kvrocks 在线内存调整暂拒绝，避免只变更容器额度而未协调引擎 buffer；CPU 调整保留。
- 当前是配置和状态基础，不代表一键创建、Gateway 自动部署或 Sentinel 已实现；未替换开发环境常驻服务。
- 验证：全仓 Go 测试、go vet 通过；专用模板重复重写、Agent 重建后用途保护、SQLite 关闭重开、拓扑变更拒绝和旧 Agent 拒绝测试通过。此轮尚未用新模板创建真实 Redis/Kvrocks 容器，不把模板断言当作真实引擎启动验收。

## 可恢复创建的 Agent 基础（2026-09-17）

新增内部鉴权接口 `POST /instance/ensure-profile`，在专用创建参数外要求固定 `operation_id`。Agent 在实例锁内，先持久化操作编号与请求摘要，再通过 Podman label 标记容器归属；创建后重新检查容器运行状态和身份。相同操作、相同参数重试不重复创建，也不重写已完成实例的配置。回复丢失、Agent 重启后可根据本机记录和容器 label 接认原容器；同名陌生目录/容器、参数变化、身份不符、已完成容器消失或被替换均拒绝接认，不覆盖、不删除，也不隐式启动停止的容器。Podman 查询失败不当作“不存在”。普通创建接口不能覆盖带此创建记录的实例。

此接口已接入下述 Server 自动续跑循环；容器 Running 仍只代表进程状态，Server 另调用 verify-profile 检查数据库后才完成子任务，不等于 Gateway 就绪。创建记录写入前后宿主机掉电、真实 Podman 故障及整链路恢复尚未验收；容器操作测试使用运行时替身。

专项测试覆盖：创建回复丢失后 Agent 重启接认、创建发出前失败后的续跑、8 个并发相同请求只创建一次、参数/操作编号变化拒绝、陌生资源拒绝、已完成容器消失/替换/停止时不重建、普通创建接口不能覆盖。专项 race 检查通过。

## Server 持久化专用实例创建任务（2026-09-17）

- `POST /instance/create` 指定非空 profile 时改为异步：事务内完成调度、实例组/实例登记、端口与资源预留、`tasks` 中 `profile.create` 登记，然后返回 HTTP 202，data 含 task_id/instance/status。普通实例创建保持原有同步接口。通过 `GET /tasks/{id}` 或 `/tasks?instance=...` 查询；请求回复丢失后不要换实例名重建。重复提交同名实例仍返回冲突，可查询原任务。
- 专用请求默认 CPU=1，显式指定服务器也校验资源与标签；任务持久化解析后的镜像、版本、端口、完整参数和 Agent 地址。Agent 地址在预留事务中再次核对，活动任务期间不能换地址。CPU/内存预留不因 reconcile 暂时观测不到容器而释放。
- 复用 Server 进程和 SQLite，无新服务。独立于备份任务跟踪的单 worker 每 5 秒检查任务，失败至少等待 30 秒后用同一 operation_id 重试；拉镜像等调用不阻塞备份任务跟踪。Server 启动后恢复 pending/running 任务。只支持当前单控制 Server 模型，不是多 Server 分布式任务租约。
- 活动任务是持久保护，不随临时锁超时消失；即使传入同一个会话编号也不能停库、删库、改配置或变配。SQLite 触发器同时保护实例、组、节点地址及集群认领；已有资源不能在创建未完成时加入冷热集群。
- 创建完成后删除资源时，Agent 先在数据根目录的 `.provision-retired/<operation_id>/` 持久化退役记录，再删除容器和实例目录；迟到的旧创建请求即使跨 Agent 重启也会被拒绝。退役记录不能随实例目录一起清理，目前不做自动 GC；它按资源创建操作计数，不是逐业务 key 元数据。新操作编号仍可重用已释放的实例名。
- Agent 返回匹配的操作编号、非空容器 ID，核验容器归属与 Running 后，Server 还须调用 verify-profile 确认数据库就绪、关键运行配置与空库；回复必须属于同一容器。全部确认后才将子任务置 success/database_ready；任务终态、实例观测、解锁在同一事务提交。确认结果写幂等审计，不自动发布业务入口。
- 网络错误、旧 Agent 不支持、错误回包会保留 retry_wait 和原因，自动重试，不触发删除或重放新任务。永久参数/资源冲突也保留占用和可见原因，不能靠盲目删除解决；安全取消/终止尚未开放。这只限制正在创建的资源，不停止已有集群服务。
- 本轮覆盖持久任务重启续跑、相同操作/参数重试、事务回滚、过期锁与同会话绕过拒绝、资源占用、旧 Agent、任务查询和删除后的迟到请求隔离；创建期间派生状态保持 creating，不因临时 missing 报成最终失败，也不提前当作可接入实例。真实 Podman/宿主机故障尚未验收。父任务组装和数据库预检见下节；Gateway 初始配置交付、页面创建流程仍未完成。
- 验证结果：全仓 Go 测试、go vet、Agent/Server/Store/状态投影的专项 race 检查通过；注入完成事务失败时，任务终态与解锁均回滚。未部署到 redis01–03，不将测试替身当作真实 Podman 验收。

## 数据库就绪与集群父任务组装（2026-09-17）

- Agent 内部 `POST /instance/verify-profile` 要求同一创建操作的原始参数；先确认容器归属，再从本机回环端口读 PING、INFO、CONFIG GET、DBSIZE，最后再次确认容器身份。连接与整体探测有超时，RESP 回复有大小/深度限制。只读检查，不写测试 key、不清库、不自动修复配置。
- 检查实际 Redis/Kvrocks 引擎、master、cluster_enabled=0 和非加载状态；Redis 检查 save 空、appendonly=no、allkeys-lru、正 maxmemory；Kvrocks 检查 WAL 开启、同步写、workers=2、max-db-size=0。DB 0 必须为空，INFO 中若有其他非空 DB 也拒绝。预检不是持续一致性锁：专用后端必须禁止旁路写入，Gateway 初始化时仍要自己的空库与配置检查。
- `POST /api/v1/custom-clusters/provision` 接收 `{"spec": <ColdClusterSpec>, "child_tasks": [<创建任务ID>...]}`。先用带 profile 的 `/instance/create` 生成子任务，再把每个资源对应的 task_id 提交给此接口；每个冷分片一个 Kvrocks 子任务，可选一个 Redis 缓存子任务。返回 202 和父 task_id，经通用 `/tasks/{id}` 查询。**不是一次请求自动创建所有子资源**，也没有新增页面创建按钮。
- 父任务 `cluster.provision` 与成员占用一起落库。通过当前实例来源记录绑定子任务，拒绝任意同名实例、重复成员、错误用途、缺失成员或已被其他集群占用的资源；旧容器级 success 且无 database_ready 证明的任务不能直接组装。
- 一个子任务失败/暂不可达时，其他已成功资源保留并受父任务保护，Server 重启后继续等待与重试。子任务全部成功后，父任务重新预检全部数据库，不能仅凭之前的就绪结果接管可能已被修改的实例。完成时在同一事务登记 HRW 集群、建立集群资源占用、置父任务 success/cluster_registered，并释放父任务的临时成员占用。成员从创建保护转交集群保护，中间没有可删除窗口。
- 父任务完成时不创建 ingress、不部署 Gateway、不宣布业务就绪。临时故障自动重试；非空数据、错误运行配置、身份冲突不会被自动清理。安全取消/失败资源回收、Gateway 初始配置交付与页面操作仍待实现；单份规格预留入口见上节。
- 测试覆盖有缓存/无缓存两种父任务、部分子任务失败后 Server 重启续跑、禁止提前登记、已成功成员不能被删除、最终保护转交，以及探测的错误引擎/副本/加载中/非空库/错误 LRU/WAL/认证/异常回复。真实 Podman 与真实 Kvrocks 的本轮新链路尚未验收。
- 本轮全仓 Go 测试、go vet、Agent/Server/Store 专项 race 检查通过。本地独立真实 Redis 进程通过空库就绪检查；写入测试 key 后预检拒绝，且原值仍可读取，确认探测不清库。未改动 redis01–03 常驻服务，未提交或部署本轮变更。

## 已接入开发代码

- 单有效 Gateway + 一个热 Redis + 单个或可选 HRW 多冷 Kvrocks，DB 0；独立前后端凭据、配置检查、预算、指标和心跳。默认本机回环，可通过 backend_networks 显式开放私网跨机后端。
- invalidate / update_existing 两种写后策略；前者删除缓存，后者在已有完整缓存上安全增量更新，不再整对象重建。
- 五种类型分批完整回温；单对象大小/元素数上限及 per-type 大小配置已删除，默认 4 个加载；保留协议帧、内存预算和超时保护。带业务 TTL 的对象按剩余生命周期回温。
- 热层原始业务 key、原生类型，一个对象一个 key，无 data/complete、运行标识或热代次后缀。写冷、热失效/更新与同 key 回填串行；热失败先隔离，关闭旧服务端连接并清空专用热 DB 后恢复。冷结果未知仍用冷层固定 token CAS 隔离迟到写。
- Redis 自身 allkeys-lru；热层禁用持久化，不恢复旧缓存。无同步器、主动淘汰器、布隆过滤器或全量 key 目录。
- BLPOP/BRPOP 的有界独立冷连接，仅允许固定 cold_only_prefixes；这些 key 不会缓存。
- 新模式 Pipeline 按连接顺序逐条完成；普通 passthrough 行为保持不变。

## 常驻真实控制面验收（2026-09-16）

后续短时稳定性：`scripts/soak-managed-dev.py` 经正式 Proxy 入口，在默认 invalidate、a+b 拓扑下运行 600 秒；两个客户端各用独立 String/Hash，串行 SET/GET/HSET/EXPIRE/HGET，每轮主动等待 100ms。完成 9349 轮，请求错误、读值不匹配、readyz 失败均为 0；结束删除本轮 4 个随机前缀 key，无服务重启或拓扑调整。五条命令整轮耗时 p50=26.87ms、p95=35.31ms、p99=44.65ms，不是单命令延迟或最大吞吐。采样 Gateway RSS 约 19MiB、Proxy 约 9.6–12.2MiB；日志在 redis01 `/opt/pilot-managed-stage.JwUn22/soak-600s.log`。只覆盖当前删除缓存模式的短时 String/Hash 写后读，不替代更新模式长稳、大对象压力、宿主机重启或生产容量验收。

8020d47 已部署到 redis01 常驻 Server、主 discovery Proxy、redis-gateway，正式集群 managed-dev 使用 redis02 热 Redis/冷 a、redis03 冷 b；入口 10.70.100.67:16508，业务密码在 root 私有文件，不在文档中公开。此次不再使用 HTTP 控制夹具。拓扑、路径、专用后端初始化调优和开机配置见 [部署记录](gateway-deployment.md)。

scripts/verify-managed-dev.py 从真实 Proxy 入口连续完成两轮 invalidate/update_existing 验收：五种类型、1000 字段 Hash 回温和增量缓存保留、持续字段写后立即读及未修改字段校验，跨机扩容/缩容、控制 Server 重启和 Gateway 重启。两轮各模式分别完成 229/568、229/565 次持续写后读循环，非吞吐测量。第二轮还验证从已有双冷拓扑重新开始；最终 revision=12、a+b、任务 complete、默认 invalidate，自己的随机前缀测试 key 已删除。

Server runtime API 可见当前 Gateway serving/backend_ready，旧 boot 记录过期为 stale 属正常历史观测。旧普通 Proxy listeners/clusters 与升级前一致，redis03 旧 cache 栈和旧数据库数据未改。Server/Proxy/Gateway 及专用防火墙、后端开机启动单元均 enabled；主机实际重启、长时间稳定性和性能仍未验收。

日志在 redis01 `/opt/pilot-managed-stage.JwUn22/acceptance-ready.log`、`acceptance-repeat.log`。首次脚本曾仅等 Proxy PING 就开始写，因 Gateway 尚未监听而失败；修正为 /readyz + 真实转发 EXISTS 后两轮通过。这是验收就绪判断修正，没有因此改动缓存一致性逻辑。

## 跨主机后端验收（2026-09-16）

Server 按资源所属主机解析私网端点；Gateway 通过 backend_networks 显式放行，动态任务与落盘恢复同样校验。Gateway 所在节点即使没有数据库也受资源删除/改地址保护。Proxy → Gateway 仍本机回环，业务密码只在 Proxy；本轮不是多 Gateway、热层分片或副本切换。

真实三机隔离测试：redis01 (10.70.100.67) 运行测试 Gateway，redis02 (10.70.100.63) 运行临时 Redis 7 和 Kvrocks 2.15.0 A，redis03 (10.70.100.68) 运行临时 Kvrocks 2.15.0 B。数据库仅绑定回环，由只接受 redis01 源 IP 的临时测试转发入口提供跨机连接。控制 HTTP 使用真实 store 的测试夹具，不是常驻 Server；本轮没有重跑 Proxy 路径，也没有替换任何常驻服务。

invalidate、update_existing 各通过两轮扩容→缩容→Gateway 进程重启。持续 HSET 后立即 HGET，并检查未修改字段；启动 YAML 仍只列旧 A，重启从落盘目标 B 恢复。最终轮补验迁移前后 PEXPIRETIME 相同、迁移期间过期 key 不复活，分别 11.11s / 10.91s。测试耗时不是性能指标；未模拟跨机网络分区、宿主机掉电、运行时时钟突跳。

新增地址白名单、远端资源、节点保护、时钟解析/偏差拒绝单测。全仓 go test / go vet 通过，真实本地 Redis 的缓存/迁移/分片 race 测试及 store Cold race 测试通过。跨机迁移依赖 NTP，并增加预检及拷贝/提交前 250ms 时钟准入，不能据此宣称任意时钟条件下零过期偏差。

复验日志在 redis01 `/tmp/pilot-crosshost.M2133y/final-invalidate.log` 与 `final-update_existing.log`；测试二进制 `gateway-final.test` SHA256 为 `9aac226ec8fa4aa6b84439d11d83dacf503a96a07ceebfc32f0290e92b3753d0`。临时容器与转发入口完成后清理，仅保留配置/二进制/日志供复查。

## 跨机故障回归（2026-09-16）

在上述三机隔离布局上增加测试专用连接转发器，主动关闭已有冷热连接、拒绝新请求，并注入一次“INCR 已执行、回复丢失”。不是重启数据库来代替断网。控制面仍为真实 store + HTTP 夹具，业务走真实 Gateway 子进程与远端 Redis/Kvrocks，未替换常驻 Server/Proxy/Gateway。

最终故障用例 invalidate / update_existing 分别 27.92s / 27.69s，通过以下断言：

- 热连接断开后写冷成功、立即读到新值；恢复时 Gateway 不返回旧值，最终直接检查专用热 Redis 已缓存新值，证明不是永久绕过缓存。隔离期间热库里的旧字节允许暂存，但不能经 Gateway 返回。
- 冷连接断开时非幂等写及未缓存读返回错误，不把异常当作 nil；恢复后未发送的 INCR 没有生效。
- 已执行 INCR 的回复被丢弃后请求报错，恢复读取计数恰为 1，没有自动重放成 2。
- 扩容目标断连后，等待迁移日志 failures>0，确认 worker 真实遇到故障；未迁移到目标的 Hash 持续成功写后读，未修改字段保持。集群版本不能提前完成。
- 杀掉 Gateway 后恢复目标网络，在控制 HTTP 仍持续 503 时重启；先确认重启前成功写入仍可读，再等本地迁移完成，最后恢复控制面补报。无人工删标记或强改任务状态。
- 随后缩容、再次重启，并复验写后读、原绝对 TTL、过期不复活。

本轮无生产业务代码修改。全仓 go test / go vet、真实本地 Redis 的 Cache/Migration/Shard/RemoteFaultLink race 测试通过；转发器断连/丢回复自测另重复三次通过。网络静默黑洞、单向分区、宿主机掉电、磁盘损坏、长时间稳定性仍不在此次验收结论内。

日志保留于 redis01 `/tmp/pilot-fault.lUuoQA/verified-invalidate.log`、`verified-update_existing.log`；`gateway-verified.test` SHA256 为 `efdac5f3d41c8859110d33556cb019b139b76d23b1ae6f1a362042d53b0c4b3d`。该测试二进制对热库恢复的内部字节检查比最终测试源更严格；最终源允许隔离期旧字节暂存，Gateway 不读旧值及最终恢复缓存的断言不变。临时实例/转发服务用后清理，仅保留复查资料。

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

## 2026-09-16：Proxy 入口与动态冷端点

Server/API/CLI 增加独立 ingress 登记：自动分配不与普通实例冲突的 Proxy 端口，密码只用于客户端→Proxy，查询不返回明文。Gateway 确认监听地址、拓扑和无前端密码后发布入口；Proxy 增加 node_id，只接受同机回环 Gateway 路由。普通单点/主从保持原有快照行为；冷热入口为 standard 模式，不读写分离，不回退直连数据库。

托管迁移从任务自动接收新冷端点，校验回环地址、节点身份和前置条件，不再改 YAML 或重启。动态节点表并发访问加锁；正式路由仍由持久化任务控制。重启从已接受任务源/目标快照恢复端点，Server 不在线也能恢复，新端点无需写回 YAML。

节点登记仅保留当前/任务所需节点；完成缩容后，释放的端口可被新分片 ID 再次使用，无需重启。仍在使用或未完成回收的节点不允许换地址/身份。启动支持新旧拓扑的节点并集，不能把每个拓扑的 128 节点上限误当成迁移期间端点并集上限。

仍限定单有效 Gateway，Proxy 入口同机；后续跨机后端增补见部署文档的“跨主机后端”。入口不提供在线改地址/改密码/注销，ready 是接入确认而非持续健康检查。首次部署/初始化仍需运维，不自动启动二进制、改数据库配置或创建新实例。以下“手动补 YAML/无 discovery 接入”是上一阶段记录，以本节为准。

验证：全仓测试、go vet、差异格式检查通过；本地 Redis 的全部 Cache/Migration/Shard 竞态回归、Store/Server/CLI 入口专项和 Proxy 节点过滤检查通过。redis03 使用真实 Proxy/Gateway 子进程、Redis 7.2.5 与 Kvrocks 2.15，最终两种策略各连续三轮通过：无 YAML 变更扩容、持续 Hash 字段写后立即读、Server 断连、Gateway SIGKILL 与离线恢复、缩容、旧端口以新分片 ID 再扩容/缩容、旧冷进程退出后的恢复及最后成功值核对。HTTP 故障夹具调用真实 store 并提供快照；Server HTTP handler/快照生成/鉴权另由 Server 测试覆盖，不能冒充整套常驻 Server 部署验收。

日志保留 redis03:/tmp/pilot-cold-ingress-verify.RMPIXD/reuse.log，最终测试二进制 SHA256 为 5af5814ece53f359916f9261d9b0fd4ad62cf589afc8da4fbfa092265a77cc2c。SSH 观察连接中断后测试继续，已通过容器退出码 0 和完整三轮 PASS 日志核对。常驻 Agent/Gateway/Proxy 保持 active，未替换或重启；这是功能/故障验证，不是压测、掉电或跨主机验收。

## 2026-09-16：Server 任务入口

新增 cold-cluster create/get/list/migrate CLI 及 HTTP API。Server pilot.db 保存集群、任务、资源占用；Gateway 本地 runtime.db/handoffs.db 保存实际执行与交接状态，控制面断连不停止已落盘任务。相同任务 ID 和相同参数可重试，版本冲突/并发第二任务拒绝，进度不倒退。只有 complete 才释放缩容源；published 不允许删除源实例。

本版限定一个有效 Gateway、同主机回环、独立单点 Redis/Kvrocks，无后端密码；普通实例可用 disable_proxy/CLI --disable-proxy 创建专用后端。登记拒绝已有直连 Proxy 入口的实例，避免绕过 Gateway；被占用实例的停止、删除、复制拓扑或配置变更由存储层保护。健康观测与备份仍允许。

新目标须先加入本地 YAML 端点注册表并重启 Gateway；缺失时任务 waiting_configuration，普通业务继续。迁移前检查真实 Kvrocks 主节点、非 Cluster、sync=yes、disable_wal=no 与新增节点无业务数据；不自动改配置、不清库。Kvrocks 2.15 不支持通过运行时 CONFIG SET 改 rocksdb.write_options.sync，需要配置文件和重启。

尚无注册删除/更新、任务取消/回滚、页面操作、配置自动发布或 discovery Proxy 自动接入。下面较早阶段的“控制面未接入”属于历史验证记录，以本节为准；不将单机验证等同于跨主机、掉电或生产可用性验收。

验证：全仓 go test ./...、go vet ./... 通过；本地 Redis 的 Cache/Migration/Shard/Cold 竞态回归及 Server 专用后端创建/控制 API 竞态检查通过。redis03 独立 Kvrocks 2.15 容器中，两种缓存模式各连续三轮通过“缺端点等待、持久性检查失败后重试、进度回复丢失、Server 断连、Gateway SIGKILL/原盘恢复、扩容后连续缩容、complete 后释放旧资源、旧冷进程退出后再启动”验证。HTTP 夹具使用真实 store；真实 Server handler 鉴权另由 Server 测试覆盖，不冒充整套常驻部署验收。

最终日志保留 redis03:/tmp/pilot-cold-control-verify.EJzo7J/shrink.log，测试二进制 SHA256 为 21d61635a81d507ef906691edef2b3eb01037b940fb0dbd64f0d061afed0847f。最初故障注入因 CONFIG SET 不受支持失败，修正为改独立测试配置并重启后通过；初轮失败日志同目录保留。常驻 Agent/Gateway/Proxy 仍 active，未替换、未重启。这不是压测、整机掉电或跨主机验证。

## 尚未完成

- 连接池、吞吐/延迟压测、长期稳定性与生产资源验收。
- 动态阻塞 key 的缓存绕过/恢复、无限阻塞、更多阻塞命令、队列前缀策略迁移。
- Hash 字段过期、位图/Bitfield/HyperLogLog、LMPOP/ZMPOP 等未列出的扩展，以及不同引擎所有极端输入的逐位兼容。
- HRW 分片的 Server/API/CLI 任务入口、冷端点自动下发和同机 Proxy 接入已接通；独立热拓扑和自动部署尚未接入。不依赖 Redis Cluster slot 协议。
- 多 Gateway 多活/跨主机接管、Redis Cluster 协议路由、冷库复制/故障切换，以及冷库回滚或备份恢复后的重新建立边界，不属于本次 HRW 版本承诺。
- Server 自动分配 Gateway、跨主机接入、滚动升级；当前控制面及回环入口开发测试不等于生产部署验收。

## 下一版本：内部请求链路、迁移与未完成范围

### 2026-09-16 多冷节点配置与本地启动恢复

增加 cache.sharding（节点 ID/地址/独立凭据文件、私有状态目录、迁移限速/超时），保留原单冷配置。显式 -init-sharding-state 仅为专用空冷库创建初始 HRW 拓扑；正常启动不自动初始化。runtime.db 持久化服务身份、源拓扑和当前任务引用，交接明细仍在 tasks 下的 handoffs.db；已完成任务保留目标快照，不能靠修改 YAML 回滚归属。恢复自动区分 active/published/complete，继续搬迁或回收；缩容 complete 后不要求旧节点在线/保留凭据。配置新增节点只登记端点，不会自动扩容。

本轮新增配置校验、状态互斥/损坏/缺失/身份不匹配、敏感信息不落盘和孤立端点不改变路由测试；真实进程测试通过 YAML/NewService/Run 启动，不再手工组装 sharding runtime，覆盖两种缓存策略、五类型、稳定运行/迁移中/发布后的 SIGKILL、自动继续任务、旧节点退出后启动及已成功更新的数据不回退。任务引用在测试中经内部登记接口创建，不能将此描述成 Server/API/CLI 已支持发起扩缩容。

验证：本地专项显式启用 Redis 的竞态测试连续三轮通过（18.115 秒）；TestCache/TestMigration/TestShard 竞态回归通过（82.428 秒）。redis03 独立无宿主网络容器，热 Redis 7.2.5、冷 Kvrocks 2.15.0（workers=2、sync=yes、WAL 开启），配置/恢复专项连续三轮通过，6 个顶层用例×3，共 18 次通过，另有 3 次 subprocess helper 顶层跳过，约 8.3 秒，退出码 0。远端日志和二进制保存在 `/tmp/pilot-shard-config-verify.uHzRO0`，SHA256 `067fb60ab26cd66809c9e8cf68e4474418fa2f3fb743aab0b09950886ad25139`；未更改常驻 Agent/Gateway/Proxy。

分片日志包全量竞态测试、全仓默认测试、go vet 和差异格式检查通过；远端测试容器已删除，日志保留。冷库持久性前置检查、跨主机连接/时钟保护、控制面任务入口和常驻服务部署仍待完成；本轮初始化的空库检查不等于生产持久性验收。

### 2026-09-14 用户扫描兼容性修复

内部分片 SCAN/HSCAN/SSCAN/ZSCAN 已不再强制要求 run_id。Redis 保留 run_id 判断；Kvrocks 使用 PID＋秒级启动时间作为只读游标的重启提示。INFO server 后在同一条不自动重连的连接上调用 TIME 核对采样仍在同一秒，跨秒/时间倒退最多重采样三次，避免直接允许 ±1 秒差值导致身份漂移；缺失/重复/负数/溢出字段拒绝。当前页扫描继续使用同一连接，页间不保留连接，不创建内部 key，不写磁盘，也不增加常驻服务。

后端连接、身份检查或页面处理失败会删除本次使用的旧游标；后端恢复后必须从 0 重扫，不假报成功结束。成功页面的游标仍可重读，应对前端回复丢失。命令、原 key、选项、拓扑、节点和 owner 的绑定、有界容量/有效期、内部 key 与旧副本过滤保留；对象扫描不触发热回温。

边界：PID＋秒级启动时间不是唯一进程标识；同秒重启且 PID 复用等情况可能漏判，不承诺跨重启的完整遍历或快照。此提示不参与普通写入、缓存一致性、迁移放行或阻塞排空；这些路径继续使用既有后端屏障、交接日志和连接确认。后台迁移扫描与用户游标仍是两条独立路径。

验证：本地扫描专项显式启用 Redis 后竞态三轮通过（21.990 秒），TestCache/TestMigration/TestShard 竞态回归通过（40.171 秒），分片日志包完整竞态通过（4.041 秒），全仓默认测试、go vet 及差异格式检查通过。真实 Kvrocks 2.15.0、workers=2 的扫描专项 9 个顶层用例连续三轮、共 27 次通过（约 38.1 秒）。覆盖二进制 Hash/Set/ZSet 各 700 元素逐项核对、游标重读/绑定/异常作废、MATCH/TYPE、扩容与缩容分页间交错迁移、Gateway SIGKILL 后旧游标拒绝、Kvrocks 原盘原端口快速重启后旧游标拒绝及重扫保留新写值；跨秒/时钟回退与非法字段使用可控协议样本测试，不声称对真实主机调时做过验证。

远程为 redis03 独立无宿主网络测试容器，热 Redis 7.2.5、冷 Kvrocks 2.15.0，sync=yes/WAL 开启/lua-strict-key-accessing=yes；日志与二进制保存在 `/tmp/pilot-scan-verify.9AmJqK`，`scan.log` 对应扫描三轮，`full.log` 对应 `^TestMigration|^TestShard` 完整回归：51 个顶层用例通过、0 失败、1 个仅由子进程调用的 helper 顶层跳过，约 57.5 秒，退出码 0。此前因 run_id 缺失而失败的扫描与阻塞路径均在本轮矩阵中通过；仍不是所有生产场景验收。二进制 SHA256 为 `fa20eb9eb867ddab3c13e3d66594ad2e88223b91f2b06694fa50b73d2f8fd223`。没有替换常驻服务或数据库，也没有开放公开配置/控制面。

### 2026-09-14 阻塞请求兼容性修复

内部分片 BLPOP/BRPOP 不再要求 INFO server/run_id。新登记使用 v2，只记录随机连接名字、CLIENT ID 和涉及的 key；旧 v1 登记仍可读，但旧 run_id 不再作为跳过排空的理由。在同一条不自动重连的直连控制连接上查询名字、核对唯一 ID、关闭并反复确认名字消失，之后才清理登记和继续迁移。同名不同 ID/重名/列表异常/连接中断保留记录，后续自动重试；旧 ID 被其他名字复用时不会误杀。没有新增服务，也没有为阻塞路径引入启动时间推算。

测试补充：INFO server 不可用且 INFO all 不提供 run_id 的兼容路径、旧 v1 run_id 不同但连接仍存活、ID 被其他连接名复用、同名 ID 不符/重名、关闭前断网、关闭 ACK 丢失、确认列表回复丢失。增加真实 Kvrocks 强杀重启后的阻塞未知结果、登记自动恢复和后续队列读写测试。保留迟到原生发送、Gateway SIGKILL 后恢复、登记 ACK 丢失不发送弹出、弹出回复丢失不重复消费、容量与两种缓存策略测试。

真实 Kvrocks 2.15.0 的异步 KILL 可能等待原生阻塞超时才实际释放连接。该 key 的交接必须等待，不影响无关普通请求；等满原生 timeout 后的正常空数组允许返回，连接错误不能冒充空数组。已在迁移返回时立即核对服务端连接和登记均消失，避免把“最终请求结束”误当成“迁移前已排空”。

验证：redis03 隔离容器，热 Redis 7.2.5、冷 Kvrocks 2.15.0，workers=2、sync=yes、WAL 开启、lua-strict-key-accessing=yes。最终 `^TestShardBlocking` 的 13 个顶层用例连续三轮、共 39 次通过（约 68.9 秒）；包含真实冷进程及 Gateway 进程 SIGKILL，子进程 helper 顶层跳过不计入业务用例。日志与二进制保存在 `/tmp/pilot-blocking-verify.1nveP0`，`final.log` 对应退出码 0；`initial.log` 保留首轮失败（测试误禁用建连所需 INFO all，以及未区分 Kvrocks 正常阻塞超时的旧断言），不作为通过结果。最终二进制 SHA256：`7b7ebd50c65e11897d85856dee08d08640970d2520e673a713addaff4e0796b4`。未替换常驻服务或数据库。

本地显式设置 GATEWAY_TEST_REDIS_SERVER 后，阻塞专项竞态三轮通过（16.258 秒），TestCache/TestMigration/TestShard 竞态回归通过（59.449 秒），分片日志包完整竞态测试通过（3.763 秒）；全仓默认测试、go vet 与差异格式检查通过。最初未设置环境变量的默认测试跳过了引擎集成用例，不计为引擎验证。

范围：这一轮先修复阻塞路径；其后完成的 SCAN/HSCAN/SSCAN/ZSCAN 适配见上节。公开配置/控制面、跨主机时钟和完整扩缩容验收仍待完成，专项通过不代表整个 HRW 功能已经完成或可发布。

### 2026-09-12 开发环境 Kvrocks 验证（历史失败记录，后续修复见上文）

环境：redis03，独立测试容器，热层 Redis 7.2.5、冷层 Apache Kvrocks 2.15.0（git ea617971），两个冷进程各自临时数据目录；容器仅有内部回环网络，无宿主端口映射，内存上限 1536MiB。Gateway 测试二进制交叉编译运行，远程不是 race 构建，也不是吞吐压测。常驻 Server/Agent/Proxy/Gateway 和已有数据库未替换、未重启。

已通过范围：五大类型完整逻辑搬迁（含二进制、大对象分批和 TTL）、两种缓存策略、Hash 局部更新、跨节点批量命令、同节点事务/Lua、前台写与后台搬迁交错、失败后重试、发布/标记回收、扩容→缩容→再扩容，以及 Gateway 在后台扫描或已发布清理途中实际 SIGKILL 后原盘恢复。新增冷库进程测试在目标 ready、提交并写入新值、源清理等阶段强杀并重启 Kvrocks，核对 700 个 Hash 字段、更新值和原绝对过期时刻；不将进程强杀等同于整机掉电、磁盘损坏或主从故障切换。

本次阻断项：真实引擎的内部分片 BLPOP/BRPOP 路径及对外 SCAN/HSCAN/SSCAN/ZSCAN 未通过。Kvrocks 2.15.0 的 INFO server 没有 Redis run_id，实测仅有 process_id、server_time_usec、uptime_in_seconds 等信息；现有 shardScanRunID 返回空，扫描及阻塞登记在保护处拒绝。阻塞未能走到原生发送，因此其迁移排空/重启登记恢复也不能算验收通过。后台迁移扫描不使用这套用户游标身份，不能把两类 SCAN 的结果混为一谈。未关闭身份检查、未用 PID 或模糊启动时间冒充唯一进程标识；下一步应设计可靠的连接/进程身份适配，再重跑失败矩阵。[Kvrocks 2.15 GetServerInfo 实现](https://github.com/apache/kvrocks/blob/v2.15.0/src/server/server.cc)可核对这些字段。

冷测试进程明确设置并读取确认 rocksdb.write_options.sync=yes、disable_wal=no、workers=2、lua-strict-key-accessing=yes。官方默认 sync=no，普通 ACK 不能直接外推为整机掉电不丢；本轮没有改动常驻冷库配置，也未实现管理入口强制检查这些持久性前提。[Kvrocks 2.15 配置说明](https://github.com/apache/kvrocks/blob/v2.15.0/kvrocks.conf)解释 WAL 同步选项。跨主机时钟保护、真实阻塞排空和公开任务控制面仍待完成。

测试入口：新增 GATEWAY_TEST_KVROCKS_SERVER，将 TestMigration/TestShard 的两个隔离冷后端换为真正 Kvrocks，热后端仍由 GATEWAY_TEST_REDIS_SERVER 提供；每个测试自动创建专属目录/端口并销毁进程，不能指向常驻数据库。GATEWAY_TEST_KVROCKS_STRICT=no 可另测非严格模式，本轮使用默认严格模式。测试夹具新增实际源端离线确认：Kvrocks 不接受原 Redis 关停方式，现通过持有的测试进程句柄终止，并检查端口已不可连接，避免误报旧节点离线验收。

远程测试二进制与日志保存在 redis03 的 /tmp/pilot-hrw-verify.PT2VX4。full.log 对应首次完整矩阵（文件名前缀 pilot-hrw-verify-PT2VX4-），12 个顶层用例失败，集中于阻塞/对外扫描；repeat2.log 为修正离线夹具后的迁移及普通请求范围，29 个顶层用例连续三轮、共 87 次通过，约 65.5 秒，不包含阻塞/对外扫描，不能称全量通过。crash.log 保留了关停夹具未生效的失败，repeat.log 是主动中断的中间运行，均不作为通过证据。最终测试二进制 SHA256 为 97a741f335e7b79b053ea98783a40b5acdbe98a239b9cb05d7c86cbe0ca34158。本地 Redis 迁移/分片竞态回归、全仓测试及静态检查通过；此前下方各轮“本地 Redis 通过”的结果仍只说明 Redis 替身验证。临时容器运行结束后清理，日志和测试二进制保留，常驻冷热服务与 Agent 检查均为 active。

详细约定统一维护在 [冷热设计](hot-cold-gateway-design.md) 的“自主管理分片与扩缩容”一节，不另建阶段文档。

路由基础：internal/gateway/sharding 的 hrw-sha256-v1、稳定标签解析、不可变拓扑快照校验/编解码、新旧目标比较与扫描源选择；internal/gateway/sharding_commands.go 复用当前命令和 Lua KEYS 提取，规划批量 key 原始位置、拒绝跨目标原子命令/事务（包含 WATCH key），显式标出待交接 key。规划函数本身不发后端请求或证明目标就绪，运行时由下述准入层协调。

交接日志增补：internal/gateway/sharding/journal.go 使用 Gateway 本机独立 SQLite WAL/FULL 日志（不是 Server pilot.db），保存任务新旧拓扑与临时 key 状态，支持准备、提交、源清理确认、归属查询、分页读取和同盘重启恢复。覆盖幂等准备/确认、阶段顺序与任务身份校验；发布最终路由前已提交记录即使源已清理也不删除，防止归属退回。打开已有日志不会自动重建丢失/损坏文件。日志模块本身不向后端写 key，也不做实际复制/后端防迟到操作；内部完成/回收现已由下述运行时结束流程接入，数据库日志的恢复不能单独冒充整条迁移链路的恢复。

逐 key 搬迁增补：migration_fence.go / migration_copy.go 增加源冻结、五种类型逻辑分批复制、目标 ready、日志提交、目标 serving、源清理的调用链。后端脚本检查节点 owner、任务/key 绑定、复制尝试和批次；复制响应未知不盲重放，未提交可换尝试重拷，已提交只补开放/清理。源和目标各使用临时迁移 Hash（仅测试后端创建，未在现有开发部署创建），原业务 key 不改名。TTL 传递原绝对截止时间，同机验证期间过期不复活。

本轮请求链路增补：sharding_backends.go / sharding_admission.go / sharding_runtime.go 将普通五类型命令、热读/回温/更新、同节点事务及 Lua 接入共同准入和后端脚本保护。节点有独立凭据及 owner；内部切换拓扑先等待已准入请求结束，归属变化的请求先交接再执行业务。迁移与请求共用 key 锁；已提交归属不退回源，待清理时访问目标不依赖源可达。正常迁移保留完整热对象，update_existing 后续局部写不整删 Hash。跨节点 MGET/MSET/DEL/UNLINK/EXISTS 拆分汇总，保留重复位置与 Pipeline 顺序；写失败可能部分成功，不假报整体成功、不重放结果未知命令。跨节点原子命令/事务/Lua 预先拒绝；WATCH 遇任何拓扑版本变化保守使 EXEC 返回空结果。

启用边界（本段原记录为内部装配阶段，配置进展见 2026-09-16 小节）：默认单冷能力和常驻开发部署不变。普通请求的扫描及同节点冷层专用队列 BLPOP/BRPOP 已接入；分片模式仍拒绝 KEYS、跨节点阻塞及事务/脚本中的扫描/阻塞。后台扫描、核查、限速、进度、最终发布/回收、内部下一轮任务及同盘恢复已接入；真实 Kvrocks 的扫描、阻塞排空专项与部分进程崩溃恢复已按上文验证。仍缺 Server 任务持久登记/下发、全部迁移崩溃窗口验证、热层独立扩缩容；跨主机时钟、整机掉电/存储故障及旧未登记阻塞连接升级处理未验收，不能启用为完整在线扩缩容。

后台执行器增补：migration_worker.go 随 Gateway Service 启停，不是新服务。先恢复待处理交接，再扫描迁移、从 0 核查；每 key 与业务共享准入，失败继续其他 key/源并自动退避重试。后台限制交接次数与主要通信字节（允许一帧突发），不限制前台按需交接。进度存同一 handoffs.db，旧格式逐级事务升级到 v3；未发布任务重启重扫但不重拷已提交 key。只有两轮成功且屏障下无待处理交接才写 awaiting_finalize；它不是任务完成，后续新增待处理交接会唤醒恢复，由下面的结束流程决定发布/回收。计数是累计尝试，不是唯一 key 数或完成百分比。详细参数边界见设计文档。

任务结束增补：migration_finalize.go / sharding/finalize.go 增加 active→published→complete。内部 Finalize 选项开启后，屏障下重查就绪及待处理记录，轮换参与节点 owner，再持久发布目标路由；清理期间新旧业务 key 都只走最终目标，不回旧源。逐 key 确认并删除精确绑定的两端迁移标记，再回收本地 key 记录；失败/丢回复自动重试，无关读写继续，不清业务对象。最后再次轮换 owner 隔离迟到清理，再确认 complete，允许下一任务。恢复 published 不依赖选项开启，恢复 complete 只检查最终节点，移除节点离线不阻断启动。发布结果未知且日志不可读时先拒绝未确认路由，自动重读，不猜回滚。保留任务快照与汇总进度；不自动删除目录或收缩 SQLite 文件，控制面活动任务指针/归档仍待接入。

结束流程验证：本地 Redis 上，两种策略完成扩容→缩容→再次扩容并持续 HINCRBY/HGET；保留完整热 Hash。覆盖清理失败和清理已执行但丢回复自动恢复、旧冻结/旧清理凭据拒绝、发布结果未知的已提交/未提交两条恢复路径、日志阶段约束、v2→v3 升级和损坏状态拒绝。独立 Gateway 子进程在 published 且标记清理未完成时实际 SIGKILL，原盘重启后清完并保留期间新写入；随后关闭已移除冷节点，再次启动成功并读写。专项竞态连续三轮通过（Gateway 9.867 秒、日志 2.762 秒），全部 TestCache/TestMigration/TestShard 竞态回归通过（34.289 秒），全仓 go test ./...、go vet ./...、分片包完整竞态检查和差异格式检查通过。这不是实际 Kvrocks、整机掉电或所有执行指令窗口的验收；未部署。

后台专项验证：本地独立 Redis + 真实 Gateway TCP，覆盖两种策略的五类型分批内容核对、90KB String 在小帧预算下搬迁、保留完整热 Hash、连续 HINCRBY/HGET、不在源扫描中的过期/空源待恢复项、失败 key 不阻断其他 key、缩容扫描断连自动重试、就绪后新待处理项自动恢复、物理源残留不能假报就绪、限速取消与单执行器约束。独立 Gateway 子进程使用 Service 自动执行器，实际 SIGKILL 打断扫描，原盘重启后推进剩余记录，已提交目标的更新值不被旧源覆盖。日志测试另覆盖进度代次/计数防倒退、v1 升级与损坏 v2 拒绝。后台专项竞态测试连续三轮通过（Gateway 12.544 秒、日志 4.507 秒），全部 TestCache/TestMigration/TestShard 竞态回归通过（46.852 秒），全仓 go test ./...、go vet ./... 和差异格式检查通过。以上不是实际 Kvrocks、整机掉电、全部复制阶段 SIGKILL 或跨主机时钟验收；未部署。

阻塞增补：sharding_blocking.go 实现同节点 BLPOP/BRPOP 的“共同准入内登记连接、释放 key 锁后原生等待、迁移前后端断连并核对消失、自动清理遗留登记”。不阻塞生产者、不重放未知弹出。每冷节点一份按需创建的内部 blocking Hash，最多 256 个未清理连接记录，不为每个业务对象建 key；Gateway 重启后由既有探测循环继续清理，不新增服务或本地状态库。KILL 返回值不是排空证明，已按 Kvrocks 2.15 异步关闭实现增加 CLIENT LIST 确认；固定直连后端、稳定 worker 配置等前提见设计。

阻塞专项验证：TestShardBlocking 连续三轮竞态测试通过（12.694 秒），覆盖两种缓存策略、二进制队列、多 key 顺序、生产者唤醒、有限超时、跨节点/非法参数拒绝、迁移打断、登记后迟到原生发送、KILL 报成功但连接仍存在时拒绝搬迁且无关读写继续、登记回复丢失不发弹出、弹出回复丢失不重复消费、登记容量限制与损坏格式拒绝。独立 Gateway 子进程实际 SIGKILL 后冷层登记保留，重新启动同盘 Gateway 后自动清理并继续消费。补充身份严格检查后，全部 TestCache/TestMigration/TestShard 竞态回归通过（42.538 秒），全仓测试及 go vet ./... 通过。该子进程验证仅覆盖阻塞登记恢复，不是整个迁移任务的崩溃恢复，也不是实际 Kvrocks 排空验收；未部署。

扫描增补：sharding_scan.go 接入普通 SCAN/HSCAN/SSCAN/ZSCAN。十进制游标绑定请求/拓扑/节点/owner/后端进程，Gateway 仅保留有界临时页面位置（1024 项、5 分钟），不建业务 key 目录、不落盘；丢失或失效明确要求从 0 重扫。对象扫描只走权威冷层，拓扑变化拒绝旧游标。全库扫描在迁移期间按“全部旧节点，再全部目标节点”遍历，允许重复；逐 key 核对日志，过滤旧副本、未提交目标和内部标记。它是用户查询能力，不是自动搬迁执行器。

扫描验证：新增 TestShardScan 竞态测试连续三轮通过（13.449 秒），覆盖两种策略下 Hash/Set/ZSet 的 700 个二进制成员/值逐项核对、旧游标重读、key/选项绑定、拓扑切换和进程身份变化拒绝、有界容量/过期、全库扩容与缩容分页交错搬迁、MATCH/TYPE、旧副本/半成品过滤、后端断连不变成成功结束、游标状态丢失后要求重扫。该阶段全部 TestCache/TestMigration/TestShard 竞态回归通过（23.585 秒）；补充对象扫描不回温断言后专项复跑通过，全仓测试及 go vet ./... 通过。仅本地 Redis，后端 run_id 与扫描语义还需真实 Kvrocks 验收；后续阻塞排空进展见上文。

本轮请求验证：TestShardRuntime/TestShardAdmission 通过真实 Gateway TCP 前端连接一个热 Redis、两个独立冷 Redis，竞态测试连续三轮通过（20.148 秒）。两种策略覆盖五类型、不同节点密码、空/二进制 key、重复批量位置及 Pipeline、同节点 Lua/事务/WATCH、跨节点拒绝；5000 字段 Hash 搬迁保留热对象，更新模式局部修改仍完整；持续 HINCRBY/HGET 与迁移交错、迟到旧脚本拒绝、跨节点冷写已执行但丢回复后读取不返回旧缓存。原有 TestCache 与 TestMigration 竞态回归通过（20.877 秒）。这是本地 Redis 验证，不是新链路的真实 Kvrocks 或远程部署验收，也未覆盖完整 Gateway 进程在搬迁中的 SIGKILL。

搬迁专项验证：本地两个独立 Redis 进程，新增 TestMigration 竞态测试连续三轮通过；比较五种类型全部内容（包括分批、二进制、空 String/空源），覆盖持久/有限 TTL、确定性注入复制途中到期、目标 ready 但日志未提交时拒绝开放、并发保护写与冻结交错、丢分批 ACK/延迟分批、重开日志后重拷、提交后开放或清理失败恢复、新写/删除后迟到旧操作、重复 Begin 不删分批、错序分批、错误后尝试隔离、旧节点 owner 拒绝、日志提交前取消不开放目标。全仓测试及 Gateway 静态检查通过，原有全部 TestCache 在本地 Redis 的独立竞态回归通过（15.752 秒）。此轮未测试真实 Kvrocks、跨主机时钟、原生阻塞迁移或真实 Gateway 进程在复制途中的 SIGKILL；此前日志 SIGKILL 测试不能冒充完整搬迁故障验收。

日志验证：专项竞态测试连续三轮通过，覆盖空/二进制 key、并发重复准备、分页、禁止状态跳跃/倒退、任务/拓扑不匹配、进程独占打开、日志缺失/空文件/损坏/未知版本、取消与 SQLite 写入失败、不将读取错误当成未迁移。独立子进程在准备/提交/清理确认落盘后分别 SIGKILL，以及在归属更新事务未提交时 SIGKILL，重开日志验证已确认状态保留、未提交更新不生效。全仓测试、Gateway 包竞态检查及相关静态检查通过；本轮没有启用真实后端集成测试，未验收数据搬迁、磁盘满或整机掉电恢复，也未部署到 redis01-03。

本轮基础验证：新增测试覆盖确定性参考算法、二进制/空 key、标签边界、节点顺序/地址/版本不改变评分、快照编解码与调用方修改隔离、并发零分配查找、3→4/3→6/10→11/10→20 分布与不发生旧节点间无谓搬迁、移除中间 ID、混合增删的扫描源、非法/重复地址与源地址复用拒绝、批量重复位置、五类单 key 提取、事务/WATCH/Lua 跨目标拒绝及迁移版本不匹配。新路由/规划竞态测试、全仓测试及静态检查通过；已有全部 TestCache 在本地独立 Redis 上的完整竞态回归通过（18.349 秒）。首次与其他构建/测试并行运行时，已有兼容性/热故障注入/冷故障缓存读取三项失败，单独重跑及完整复跑通过，未更改业务超时或测试夹具；不能仅据复跑把原因确定为负载。此轮未跑真实 Kvrocks 或远程部署，也未测试任何真实搬迁。

仅算法微基准（本地 Apple M1、200ms 采样，非 Proxy/Gateway 端到端压测）：3/10/32/100 节点分别约 0.55/1.64/5.20/15.79 微秒每次 Owner 查找，均为 0 分配。此结果不承诺服务器 QPS，且不包含标签外的命令解析、计划构建、连接、缓存或数据库耗时。

总体约定与后续范围（部分已由上述内部链路实现）：

- 等权重 HRW，稳定逻辑节点 ID；不维护永久 slot/key 索引、环或虚拟节点。节点数量预期不大，算法开销后续实测。
- 五大类型按顶层 key 分片；保留两种缓存策略、完整大对象回温和已有单 key 能力，不新增对象大小准入上限。
- 冷热独立扩缩容、单集群一次一个拓扑任务：扫描迁移，再扫描核查，支持限速、进度、自动恢复；仅归属变化且开始交接的 key 保存临时持久化记录，安全完成后回收。
- 跨冷节点 MGET/EXISTS 拆分汇总，无整体快照；MSET/DEL/UNLINK 无整体原子性、允许部分成功，但不返回假成功、不遗漏已执行子写的缓存处理。当前单后端矩阵的原子语义不因此变更。
- MSETNX、RENAME、移动/集合运算、事务与 Lua 等保留同冷节点执行，跨节点产生业务副作用前拒绝；相同 `{业务标签}` 可稳定共同路由，迁移期间仍须协调本次请求涉及的全部 key。WATCH 相关归属变化计划保守中止。
- 单有效 Gateway，不做通用分布式事务或多活。日志与后端写入之间的恢复协议、后端迟到操作隔离、阻塞队列迁移、跨节点/跨迁移游标、TTL 与分批完整性仍需技术设计和真实引擎验证；日志盘丢失/回滚与跨主机接管不在当前保证内。这些不能被当前测试结果覆盖。

## 默认单冷模式的既有测试与环境边界

以下为分片开发前的验收记录；真实 Kvrocks 和 redis03 的结果仅适用于默认单冷冷热链路，不代表上述新分片模式已在真实 Kvrocks 或开发机验收。

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
