# Gateway 部署与运维

更新：2026-09-10。当前提供单后端开发透传与固定端点的 cache 开发模式，不自动升级旧冷热链路。设计见 [冷热设计](hot-cold-gateway-design.md)，实现边界见 [状态](hot-cold-status.md)。

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
- 正常进程重启使用同一锁路径即可，无 -init-state，无全量扫描、清缓存或同步追赶。新进程采用新 token；不支持不同主机/锁路径部署重叠写者。
- 新模式每连接 Pipeline 保序执行，不做多后端流水线并发。不支持的命令明确拒绝。

启动检查 /livez、/readyz 和 /metrics。ready 主要反映冷层可用性；热层异常允许降级服务。指标 redis_pilot_gateway_cache_events_total 的固定 outcome 包含 hit、cold_request、filled、load_skipped、load_limited、invalidated、update_attempt、hot_bypass、cold_result_unknown、epoch_ready、blocking_completed、blocking_unknown，不含 key 标签。加载跳过不一定是错误，可能是大对象或回填期间已到期的对象。

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

## 当前透传部署

1. 使用隔离、数据完整的单后端实例；当前入口只监听 loopback。
2. 本机 make gateway，Linux amd64 使用 make gateway-linux，生成 bin/redis-gateway；该目标不加入普通 make deploy。
3. 创建专用无登录账号 redis-gateway，安装二进制至 /opt/redis-gateway/redis-gateway。
4. 参考 [透传配置](../examples/gateway-passthrough.yaml) 或 [小预算配置](../examples/gateway-systemd.yaml)。配置前后端独立凭据；凭据文件由服务账号持有、0600，不能加入仓库或命令行。按机器余量配置连接/RESP/缓冲预算。
5. 以服务账号执行 redis-gateway -config /etc/redis-gateway/gateway.yaml -check-config；校验不代表后端连通或就绪。
6. 安装 [systemd 模板](../examples/redis-gateway.service)，校验配置后启动并 enable。检查 /livez、/readyz、/metrics，再验证认证和测试命令。进程 active 不等于 ready。
7. SIGTERM 会排空请求；systemd 停止超时须大于 shutdown_timeout。写入结果未知不能自动重放。

Gateway 可直接向 Server 上报观测，参见配置的 control_plane 字段。Server 使用 gateway_tokens 独立鉴权；心跳失败不阻断数据请求。配置草稿、心跳不授权自动部署或多写者接管。

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
