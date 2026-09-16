# Gateway 部署与运维

## 常驻正式接入开发栈（2026-09-16）

已将 redis01 的 Server、主 discovery Proxy、既有 redis-gateway.service 升级为 8020d47；Gateway 从旧隔离透传配置切换到新建的 managed-dev 冷热集群。旧透传 Redis 和 redis03 既有 cache 测试栈保留，未复用或清空它们的数据。普通 Proxy listeners/clusters 在升级和验收后与升级前一致。

| 位置 | 常驻组件/资源 | 接入地址 |
| --- | --- | --- |
| redis01 | Server；主 Proxy 的 cold:managed-dev 入口 | HTTP 8080；业务 10.70.100.67:16508 |
| redis01 | redis-gateway.service，gateway-managed-dev | 127.0.0.1:17379；管理 19200 |
| redis02 | pmdev-hot，Redis 7 专用热缓存 | 10.70.100.63:6383 |
| redis02 | pmdev-cold-a，Kvrocks 2.15.0 分片 a | 10.70.100.63:6384 |
| redis03 | pmdev-cold-b，Kvrocks 2.15.0 分片 b | 10.70.100.68:6382 |

三个后端经真实 /instance/create API 创建，disable_proxy=true，无数据库密码；cold-cluster 注册和 ingress 下发走真实 Server，迁移经真实 CLI 发起。业务入口需要密码，仅保存在 redis01 的 root 私有文件 `/opt/pilot-managed-stage.JwUn22/ingress.json`，不写入仓库。Gateway 默认 invalidate，两个冷分片保留供后续测试；热层容器额度 128Mi，实际 maxmemory 32Mi、allkeys-lru、AOF/RDB 关闭；各冷容器 256Mi，WAL 开启并同步刷盘。

Agent 通用模板默认禁用 FLUSHDB。此部署仅在新建专用热实例配置中解除 FLUSHDB 禁用，FLUSHALL/DEBUG 仍禁用；冷实例配置显式设置 rocksdb.write_options.sync=yes、disable_wal=no，并缩小缓存/写缓冲。这些是登记集群前后的专用初始化调优，没有放宽普通实例模板或通用配置覆盖校验。配置原件保存为各专用实例 conf 下的 *.before-managed。当前仍非一键自动配置：不要通过通用模板重新生成专用配置，否则可能丢失必要选项；集群资源登记后的通用配置/拓扑操作由资源保护限制。

Gateway 的配置/机器凭据在 /etc/redis-gateway，私有运行状态在 /var/lib/redis-gateway，systemd managed.conf 增加 StateDirectory 和写入权限。Server、Proxy、Gateway 均 enabled；redis02/03 的 redis-pilot-managed-backends.service 只负责开机启动这三个现有容器，日常数据库操作仍由 Agent 管理。当前 Podman 不支持 update --restart，因此没有依赖该选项，也没有改其他容器启动策略。

数据库端口由独立 nft 表 pilot_managed_dev 在 DNAT 前限制，仅允许 redis01 的 10.70.100.67 外部访问；本机 Agent 管理不受影响。redis-pilot-managed-firewall.service 开机加载，后端启动单元 Requires/After 它。已实测 redis03 到 redis02 热端口不可连接，Gateway 正常访问；白名单文件 /etc/redis-pilot-managed-dev.nft。未修改通用防火墙策略或既有实例端口。

升级前的 Server/Proxy/Gateway 二进制、配置和一致性 SQLite 备份保存在 redis01 `/opt/pilot-managed-stage.JwUn22/backup`。不要直接恢复这份旧 pilot.db 来回滚已经创建的集群；它不包含新注册资源或后续迁移。原有 Gateway 后端数据未删除。

[正式链路验收脚本](../../scripts/verify-managed-dev.py) 在 redis01 运行，参数为上述 ingress.json 路径。它只用于 managed-dev：两种模式、五种基本类型、完整 Hash 增量缓存、持续 HSET 后立即 HGET、扩缩容、Server 重启和 Gateway 重启；每轮清理自己的随机前缀测试 key，最终保留 a+b、恢复原写策略。不清空冷库。ready 等待同时检查 Gateway /readyz 和经 Proxy 的 EXISTS，不能只检查 Proxy 本地响应的 PING。脚本会切换策略、改变此开发集群拓扑并重启指定服务，不是只读检查。

两轮完整验收日志为 `/opt/pilot-managed-stage.JwUn22/acceptance-ready.log`、`acceptance-repeat.log`，最终 revision=12、a+b、任务 complete。本次配置并检查了开机启动，未执行三台主机实际重启，也未开展长时间稳定性或性能压测。

## 跨主机后端（2026-09-16）

Proxy 与 Gateway 仍同机回环连接；一个 Gateway 可连接多台服务器上的专用热 Redis / 冷 Kvrocks。不是多 Gateway HA，也不含副本自动切换。

- Server 集群 node_id 表示 Gateway/Proxy 所在机器，不再要求所有数据库同机。按实例 server 解析：同机 127.0.0.1，异机使用 servers.endpoint 的字面私网 IP，IPv6 使用标准方括号地址。暂不支持 DNS、公网或链路本地地址。
- Gateway 的 cache.backend_networks 显式列出允许的私网 CIDR，例如 `["10.70.100.63/32", "10.70.100.68/32"]`。未配置仍仅本机；新增端点、启动配置和落盘恢复都校验。缩小白名单可能使既有拓扑无法恢复，须先迁出对应节点。
- 业务密码仍仅在 Proxy。数据库无密码的前提是专用网络访问控制：绑定私网 IP、防火墙仅允许指定 Gateway 访问，禁止客户端旁路访问。白名单限制 Gateway 向外连接，不是数据库入站防火墙，也不提供 TLS。
- 冷层迁移保留绝对过期时间。各机器须维持 NTP/chrony 同步，避免运行时手动跳时钟。远端任务预检，以及跨地址逐 key 拷贝前、提交前检查 TIME；相对 Gateway 的时差或测量不确定性超过 250ms 时自动重试迁移。该检查不保证零时差，也不能防止检查后时钟突变；已准备的 key 可能暂时 TRYAGAIN，不能绕过交接状态强行回旧节点。热缓存回温使用冷层剩余 TTL 和热层 TIME 换算，扣除请求耗时。
- 网络错误沿用既有恢复语义：热层不可用则绕过缓存，清理完成后才恢复；冷写结果未知不自动重放。Server 断连不停止已经持久化的本地任务。迁移故障不自动改路由到另一份数据。

冷实例仍需单点主库、同步 WAL；首次初始化仅允许新空冷库。热层仍只有一个 Redis，本轮未增加热层分片。

### 跨机故障验收方法

`TestColdRemoteAcceptance` 支持显式 `GATEWAY_TEST_REMOTE_FAULTS=1`，同时设置 `GATEWAY_TEST_FAULT_BIND` 为测试 Gateway 主机私网 IP；其余 `GATEWAY_TEST_REMOTE_HOT/A/B` 指向专用空测试实例，`GATEWAY_TEST_REMOTE_POLICY` 分别为 invalidate、update_existing。不能指向现有业务库；初始化会拒绝非空冷库。每轮重新创建隔离实例，不靠清理业务标记来恢复。

测试转发器一条连接对应一条真实后端连接，仅允许本机同 IP 连接；故障包括断开存量连接并拒绝新请求，以及冷端已执行 INCR 后只丢失一次回复。迁移用例等到日志记录失败，确认目标断连确实影响过 worker，再杀掉 Gateway，恢复目标连接，在控制 HTTP 持续 503 时启动 Gateway；检查本地任务完成、已确认写入仍可读，再恢复控制面补报。

验收不等于所有故障模型已经覆盖：连接重置与丢回复不代表长时间静默黑洞、单向分区、宿主机掉电、磁盘损坏或多 Gateway 竞争。成功回复丢失时客户端看到错误不意味着写入没发生；非幂等写不能由调用方直接无条件重试。

更新：2026-09-16。当前提供开发透传、单冷 cache 及可选 HRW 多冷节点启动/恢复，不自动升级旧冷热链路。设计见 [冷热设计](hot-cold-gateway-design.md)，实现边界见 [状态](hot-cold-status.md)。

## 进程职责

Gateway 是独立宿主机二进制，由 systemd 管理并开启开机启动。Agent 只管理 Redis/Kvrocks 容器，不负责部署或启停 Gateway。Server 的草稿和心跳不等于自动下发配置或 Proxy 切流。

同步器已从源码和部署示例移除。不要对旧冷热实例直接安装本轮二进制；新模式要求冷层保存完整数据，不能把部分热数据集当成权威存储。

## 新 cache 模式（隔离开发）

参考 [配置示例](../examples/gateway-cache.yaml)，redis03 当前独立测试栈使用这些地址（见下表）。准备独立 Redis/Kvrocks 和三个独立凭据文件（前端、热后端、冷后端）。冷库需配置并验证 WAL/刷盘和备份策略；Gateway 成功返回不等于任意宕机下零丢写。

- 热 Redis 必须配置 maxmemory > 0、allkeys-lru、appendonly no、save 空，且只允许 Gateway 修改。热库配置不合格或不可用时自动绕过，不修改数据库配置来强行启动。
- 冷层必须独占，业务不能旁路访问。Gateway 启动会在冷层保留前缀下绑定 owner token 与冷层专用前缀策略；这是正常运行元数据，不是复制日志。
- lock_file 父目录需服务账号持有、0700；启动创建 0600 锁文件。[新模式 systemd 模板](../examples/redis-gateway-cache.service) 已配置 StateDirectory=redis-gateway-cache、0700 和独立路径，避免 ProtectSystem=strict 下无法写锁；安装并验收后 enable，不能覆盖旧服务。
- 热层现在使用原始 key，一个对象一个正式缓存 key。Gateway 启动或热层异常后会先隔离旧操作，CLIENT KILL TYPE normal SKIPME yes，再 FLUSHDB SYNC，确认后恢复。热 Redis 必须整个实例专用，不能混入其他数据或接入会自动重连重放的中间代理。该流程会断开热 Redis 的运维普通连接，清空仅限热 DB 0，绝不对冷库执行 FLUSHDB。
- write_policy 为 invalidate 或 update_existing，其他值拒绝。加载并发、缓存 TTL、hot_timeout、阻塞并发及通用协议/资源预算可配置；单对象 max_object_bytes、max_object_elements、cache.types 已移除，旧配置需删除这些字段。参数约束在 -check-config 检查，检查本身不创建锁或修改后端。
- cold_only_prefixes 为 BLPOP/BRPOP 队列范围，该范围所有读写永久绕过缓存。前缀集合绑定在冷库，原地修改会拒绝启动；不能删元数据绕过旧阻塞请求的生命周期检查。
- 正常进程重启使用同一锁路径即可，无 -init-state、人工清理、全量扫描或同步追赶。新进程采用新 token，自动清空专用热缓存后再启用热读写；不支持不同主机/锁路径部署重叠写者。
- 新模式每连接 Pipeline 保序执行，不做多后端流水线并发。不支持的命令明确拒绝。

启动检查 /livez、/readyz 和 /metrics。ready 主要反映冷层可用性；热层异常允许降级服务。指标 redis_pilot_gateway_cache_events_total 的固定 outcome 包含 hit、cold_request、filled、load_skipped、load_limited、invalidated、updated、update_miss、hot_bypass、cold_result_unknown、epoch_ready、blocking_completed、blocking_unknown，不含 key 标签。加载跳过可能是并发预算、帧限制、超时或回填期间到期，不存在单对象大小准入阈值。

2026-09-12 单 key 及基础批量命令扩展已部署至 redis03 的独立 cache 栈，未修改 redis01 原 Proxy 或旧冷热数据库。scripts/verify-cache-dev.py 在真实 Proxy 17480 → Gateway 17479 → Redis/Kvrocks 上同时验收 invalidate/update_existing 的新增命令、MGET/MSET/MSETNX、多 key DEL/UNLINK/EXISTS、原生缓存内容、重复 key、随机弹出结果、条件 TTL、重启和故障恢复；执行后恢复默认 invalidate 并清理本轮测试 key，不清空冷库。完整命令、引擎版本及边界见 [状态矩阵](hot-cold-status.md)。

同日继续部署跨 key 运算、RENAME、列表移动、SCAN、事务和受保护 Lua。Proxy 同步修复事务连接保持、PING/ECHO 排队和本地拒绝必须中止事务的问题。完整故障验收两种策略通过；verify-cache-dev.py 新增 --commands-only 可只复验命令矩阵，仍切换两种策略并在收尾恢复 invalidate、清理本轮业务测试 key。临时 pilot-cache-advanced-cold（19492）仅用于独立 Kvrocks 测试，不属于常驻部署；验收结束停止并自动删除容器。未更改冷层持久化/备份设置，也未做生产性能或长期稳定性背书。

此前进程内验收使用临时容器 pilot-cache-cold-test-20260910，现已停止。2026-09-10 进一步部署独立常驻栈，和旧冷热数据库隔离：

| redis03 服务 | 回环端点 | 运行方式 |
| --- | --- | --- |
| redis-proxy-cache.service | 127.0.0.1:17480；指标 19401 | 独立 Proxy 二进制，静态 master 指向 Gateway，无 replica |
| redis-gateway-cache.service | 127.0.0.1:17479；管理 19400 | 独立 Gateway 二进制，默认 invalidate |
| redis-cache-dev-hot.service | 127.0.0.1:19490 | pilot-cache-dev-hot 容器，Redis 7.2.5，32MiB allkeys-lru，无持久化 |
| redis-cache-dev-cold.service | 127.0.0.1:19491 | pilot-cache-dev-cold 容器，Kvrocks 2.15.0，独立 pilot-cache-dev-cold-data 卷 |

四个单元均启用开机启动。Gateway Restart=on-failure、RestartSec=3s、StartLimitIntervalSec=0，冷库长时间不可用时持续有限频率重试，避免启动限流后必须人工 reset-failed。主机重启本身尚未实测。代理和 Gateway 均为主机二进制，不由 Agent 部署或启停。

二进制在 /opt/redis-gateway-cache，配置和三个独立随机凭据在 /etc/redis-gateway-cache，Gateway 私有锁目录为 /var/lib/redis-gateway-cache。静态 Proxy 的现有 password 配置同时用于前端与 Gateway 认证；冷热数据库密码独立。端点全部为回环，需在 redis03 本机或通过 SSH 转发接入，未开放到公网。没有修改 redis01 的 discovery Proxy 或 Server 路由快照；本轮验收覆盖实际 Proxy 数据路径，不覆盖控制面自动分配 Gateway。

[首次安装脚本](../../scripts/deploy-cache-dev.py) 在 redis03 以 root 运行，参数为含 Linux redis-gateway/redis-proxy 二进制、gateway-cache.yaml 和 redis-gateway-cache.service 的暂存目录；发现同名安装目录或容器会拒绝，不支持覆盖升级。容器显式指定入口，避免镜像默认入口重复执行或降权后无法读取私有配置。

[开发升级脚本](../../scripts/upgrade-cache-dev.py) 仅用于现有独立栈，参数为新 Gateway 二进制路径；核对固定冷热地址，停止 Gateway，删除已移除的单对象准入配置，校验并安装二进制后启动。不变更冷数据。升级会使热库按正常启动恢复流程清空，再按原 key 回温。

[端到端验收脚本](../../scripts/verify-cache-dev.py) 只用于这套隔离开发栈，会切换写策略、启停冷热服务、强制结束 Gateway 并制造 LRU 压力。生成带随机 acceptance 前缀的业务测试 key，正常收尾删除本轮测试 key、恢复 invalidate 和服务；Gateway 恢复会清空专用热库，冷库从不 FLUSH，也不删除数据卷。异常中断可能留下该轮测试 key，不能直接对其他数据库运行。性能和生产持久性不由此测试背书。

## 可选多冷分片启动与恢复

参考 [多冷配置示例](../examples/gateway-cache-sharded.yaml)。默认本机回环，可显式允许私网跨机后端；一个有效 Gateway、一个专用热 Redis、多个独立冷节点，不需要 Redis Cluster。redis03 的旧 cache 栈与新 managed-dev 栈互相独立。

1. 准备专用**空**冷库、专用热库及 Gateway→Server 机器凭据，托管模式不设置数据库密码；验证热缓存配置及冷库 WAL/持久性，创建服务账号持有的 0700 父目录。不要预创建 sharding.state_dir 本身。
2. 以服务账号运行 `redis-gateway -config /etc/redis-gateway-sharded/gateway.yaml -check-config`，只检查配置及凭据，不证明状态或数据库可用。
3. 停止业务接入，首次执行同一命令加 `-init-sharding-state`（不与 -check-config 同用）。冷库非空或状态目录已存在均拒绝；不会清空冷数据。初始化失败留下的目录不自动删除，请检查原因，不要拿此命令修复已有集群。
4. systemd 正常 ExecStart 只带 `-config`，不能带初始化参数。StateDirectory/ReadWritePaths 必须覆盖 lock_file 父目录及 sharding.state_dir。重启使用持久化路由与任务日志恢复，不重新按 YAML 分配数据。
5. 托管模式从已接受任务快照恢复端点，新节点自动随任务下发，不必修改 YAML。手工模式仍需保留正在使用的节点登记。新增 YAML 登记本身不会扩容。

`state_dir/runtime.db` 与 `state_dir/tasks/<task-id>/handoffs.db` 是不同职责的本地 SQLite：前者存集群路由/任务引用，后者存逐 key 交接及完成阶段；不是 Server pilot.db。两者需要保存在同一受保护的持久化状态目录，不随二进制发布删除。活动库不能仅复制 .db 忽略 WAL；备份/恢复必须同时考虑业务冷库、状态库与交接日志，当前不承诺任意历史回滚或换机接管。

## 当前透传部署

1. 使用隔离、数据完整的单后端实例；当前入口只监听 loopback。
2. 本机 make gateway，Linux amd64 使用 make gateway-linux，生成 bin/redis-gateway；该目标不加入普通 make deploy。
3. 创建专用无登录账号 redis-gateway，安装二进制至 /opt/redis-gateway/redis-gateway。
4. 参考 [透传配置](../examples/gateway-passthrough.yaml) 或 [小预算配置](../examples/gateway-systemd.yaml)。配置前后端独立凭据；凭据文件由服务账号持有、0600，不能加入仓库或命令行。按机器余量配置连接/RESP/缓冲预算。
5. 以服务账号执行 redis-gateway -config /etc/redis-gateway/gateway.yaml -check-config；校验不代表后端连通或就绪。
6. 安装 [systemd 模板](../examples/redis-gateway.service)，校验配置后启动并 enable。检查 /livez、/readyz、/metrics，再验证认证和测试命令。进程 active 不等于 ready。
7. SIGTERM 会排空请求；systemd 停止超时须大于 shutdown_timeout。写入结果未知不能自动重放。

Gateway 可直接向 Server 上报观测，参见配置的 control_plane 字段。Server 使用 gateway_tokens 独立鉴权；心跳失败不阻断数据请求。配置草稿、心跳不授权自动部署或多写者接管。

## HRW 控制面操作

当前支持一套 Gateway、一个专用热 Redis 和若干专用单点 Kvrocks，后端可在显式放行的不同私网主机。使用 instance create --disable-proxy 创建后端，不设置 --password；已有直连 Proxy 入口的实例不能直接登记。先确认冷配置 rocksdb.write_options.sync=yes、disable_wal=no；Kvrocks 2.15 修改前者需要配置文件并重启，不支持运行时 CONFIG SET。

Server 配置 gateway_tokens，Gateway 使用对应 token_file 和 control_plane.manage_shards: true，cluster_id 必须等于 service_id。参考 [多冷配置](../examples/gateway-cache-sharded.yaml)。首次初始化只允许新空冷库；后续启动不能重复执行初始化。

集群文件示例（实例名称替换为平台真实资源；地址由 Server 解析）：

```json
{"id":"sharded-dev","gateway_id":"gateway-sharded-dev","node_id":"redis03","hot_instance":"hot-1","shards":[{"id":"cold-a","instance":"cold-1"}]}
```

执行 redis-pilot-cli cold-cluster create --file cluster.json；用 cold-cluster get sharded-dev 查看状态。扩容前先创建专用空 cold-2，地址由任务自动下发，无需改 YAML 或重启；不能改变已有节点 ID/地址。迁移文件示例：

```json
{"id":"expand-001","expected_revision":1,"shards":[{"id":"cold-a","instance":"cold-1"},{"id":"cold-b","instance":"cold-2"}]}
```

执行 redis-pilot-cli cold-cluster migrate sharded-dev --file task.json。请求超时可用完全相同任务 ID/参数重试；下一任务使用新 ID 和最新 revision。缩容同样提交完整目标列表，仅移除目标节点。只有任务 complete 且集群版本更新后才能停用旧实例；published 还在清理，不等于可下线。

端点身份冲突或非法地址时 waiting_configuration；持久性或空库检查失败时 preflight_failed。修复原因后自动重试，不自动清库或改数据库配置。Server 暂时不可用不影响已落盘任务执行。pages/attempts 是累计工作量，不是完成百分比。

业务入口：Gateway 保持回环监听，配置 allow_unauthenticated: true 并移除 frontend_password_file；只有同机 Proxy 可访问。Proxy 使用 discovery 模式，增加 node_id: redis03（或 -node-id redis03），必须与集群 node_id 一致。其他节点的 Proxy 和未配置 node_id 的旧 Proxy 不会收到冷热入口，普通实例快照保持不变。

入口文件用 0600 权限保存，示例：

```json
{"gateway_address":"127.0.0.1:17579","password":"替换为业务密码"}
```

执行 redis-pilot-cli cold-cluster ingress sharded-dev --file ingress.json；不带 --file 查询端口及 ready。Server 分配业务端口，目标 Gateway 确认地址/无前端密码/拓扑匹配后 ready=true，随后同机 Proxy 自动开端口；客户端连接该主机业务端口并使用上述密码。密码不下发到 Gateway 或数据库，查询不返回明文。首次确认前不发布入口；已有入口的地址/密码不可原地修改，相同请求可安全重试。ready 表示已完成接入确认，不是实时健康状态。不要绕过 Proxy 访问数据库。

尚不自动部署 Gateway/数据库，也无集群注销、入口修改/注销、任务取消/回滚接口。新增注册不修改已有普通实例入口。网络隔离仍必须保证后端只被该 Gateway 访问；本机非受信进程不在该隔离模型内。

## 升级与旧环境保护

- 旧 hot_cold、state_dir 及 -init-state 已移除。旧 YAML 必须报错，不支持自动降级或旧状态迁移。
- 不把旧热 Redis 当成完整后端，不删除复制日志/驻留标记来绕过校验。
- 不在旧 Redis 先写的环境直接开启 LRU：未同步数据可能仍是唯一副本。
- 安装前保留旧二进制、配置和数据备份；新缓存实现完成后再制定并验证迁移和回滚流程。
- 既有 pilot.db 同步器历史表保留但不再读写，不自动删除历史观测数据。

## 既有开发部署与退役记录

redis01 曾部署独立 redis-gateway.service，数据/管理地址为 127.0.0.1:17379/19200，隔离 Redis 后端 127.0.0.1:16389。相关 [后端单元](../examples/redis-gateway-dev-backend.service) 和 [开发依赖](../examples/redis-gateway-dev-backend.conf) 保留；该测试后端禁用持久化，开机启动不代表保留测试数据。

redis03 旧冷热部署由以下单元组成，全部为回环开发端点：

| 单元 | 端点 |
| --- | --- |
| redis-pilot-hotcold-gateway.service | 数据 17379，管理 19200 |
| redis-pilot-hotcold-syncer.service | 确认 RPC 19301 |
| redis-pilot-hotcold-hot.service | Redis 16390 |
| redis-pilot-hotcold-cold.service | Kvrocks 16666 |

历史二进制在 /opt/redis-pilot-hotcold/，配置和独立凭据在 /etc/redis-pilot-hotcold/。Gateway 状态在 /var/lib/redis-pilot-hotcold-gateway/state-live，同步器日志及操作状态在 /var/lib/redis-pilot-hotcold-syncer/ 下。

2026-09-10 经用户确认退役旧计算链路：核查 Server 当前 Proxy 路由快照未引用 17379/19301，旧 Gateway 仅监听回环且无当前接入连接后，先停止并禁用 redis03 的 redis-pilot-hotcold-gateway.service 和 redis-pilot-hotcold-syncer.service。用户明确无需回退后，核查无其他服务引用且冷热容器未挂载这些路径，永久删除两个 service 文件及上述四个专用目录（/opt/redis-pilot-hotcold、/etc/redis-pilot-hotcold、/var/lib/redis-pilot-hotcold-gateway、/var/lib/redis-pilot-hotcold-syncer），包含历史二进制、验证脚本、专用配置凭据和运行状态，不留回退副本。daemon-reload 后两个服务均为 not-found/inactive。

旧热 Redis、冷 Kvrocks 和 Agent 仍正常运行；冷热容器、数据卷及其 service 文件未删除，Proxy 路由未修改。共享系统日志、系统账号和 pilot.db 历史观测表未清理。此退役操作不代表新 cache 链路已经部署。

后续部署新缓存版本需独立配置和验收；旧冷热数据库是否复用或删除需另行确认，不能默认旧冷库具有完整权威数据。
