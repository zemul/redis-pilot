# Gateway 部署与运维

更新：2026-09-10。当前提供单后端开发透传与固定端点的 cache 开发模式，不自动升级旧冷热链路。设计见 [冷热设计](hot-cold-gateway-design.md)，实现边界见 [状态](hot-cold-status.md)。

## 进程职责

Gateway 是独立宿主机二进制，由 systemd 管理并开启开机启动。Agent 只管理 Redis/Kvrocks 容器，不负责部署或启停 Gateway。Server 的草稿和心跳不等于自动下发配置或 Proxy 切流。

同步器已从源码和部署示例移除。不要对旧冷热实例直接安装本轮二进制；新模式要求冷层保存完整数据，不能把部分热数据集当成权威存储。

## 新 cache 模式（隔离开发）

参考 [配置示例](../examples/gateway-cache.yaml)，示例地址不是已部署服务。准备独立 Redis/Kvrocks 和三个独立凭据文件（前端、热后端、冷后端）。冷库需配置并验证 WAL/刷盘和备份策略；Gateway 成功返回不等于任意宕机下零丢写。

- 热 Redis 必须配置 maxmemory > 0、allkeys-lru、appendonly no、save 空，且只允许 Gateway 修改。热库配置不合格或不可用时自动绕过，不修改数据库配置来强行启动。
- 冷层必须独占，业务不能旁路访问。Gateway 启动会在冷层保留前缀下绑定 owner token 与冷层专用前缀策略；这是正常运行元数据，不是复制日志。
- lock_file 父目录需服务账号持有、0700；启动创建 0600 锁文件。[新模式 systemd 模板](../examples/redis-gateway-cache.service) 已配置 StateDirectory=redis-gateway-cache、0700 和独立路径，避免 ProtectSystem=strict 下无法写锁；安装并验收后 enable，不能覆盖旧服务。
- write_policy 为 invalidate 或 update_existing，其他值拒绝。缓存字节数、元素数、加载并发、缓存 TTL、hot_timeout 和阻塞并发可配置；参数约束在 -check-config 检查，检查本身不创建锁或修改后端。
- cold_only_prefixes 为 BLPOP/BRPOP 队列范围，该范围所有读写永久绕过缓存。前缀集合绑定在冷库，原地修改会拒绝启动；不能删元数据绕过旧阻塞请求的生命周期检查。
- 正常进程重启使用同一锁路径即可，无 -init-state，无全量扫描、清缓存或同步追赶。新进程采用新 token；不支持不同主机/锁路径部署重叠写者。
- 新模式每连接 Pipeline 保序执行，不做多后端流水线并发。不支持的命令明确拒绝。

启动检查 /livez、/readyz 和 /metrics。ready 主要反映冷层可用性；热层异常允许降级服务。指标 redis_pilot_gateway_cache_events_total 的固定 outcome 包含 hit、cold_request、filled、load_skipped、load_limited、invalidated、update_attempt、hot_bypass、cold_result_unknown、epoch_ready、blocking_completed、blocking_unknown，不含 key 标签。加载跳过不一定是错误，可能是大对象或回填期间已到期的对象。

本轮真实冷层验收使用 redis03 新建的独立测试容器（Kvrocks 19491），测试运行器内启动一次性 Redis；不是新 Gateway 的常驻 systemd 部署，没有更新旧服务。测试容器在验收后停止，保留用于排查；旧服务和业务数据不变。

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

## 既有开发部署盘点（历史记录，不是本轮探测）

redis01 曾部署独立 redis-gateway.service，数据/管理地址为 127.0.0.1:17379/19200，隔离 Redis 后端 127.0.0.1:16389。相关 [后端单元](../examples/redis-gateway-dev-backend.service) 和 [开发依赖](../examples/redis-gateway-dev-backend.conf) 保留；该测试后端禁用持久化，开机启动不代表保留测试数据。

redis03 旧冷热部署由以下单元组成，全部为回环开发端点：

| 单元 | 端点 |
| --- | --- |
| redis-pilot-hotcold-gateway.service | 数据 17379，管理 19200 |
| redis-pilot-hotcold-syncer.service | 确认 RPC 19301 |
| redis-pilot-hotcold-hot.service | Redis 16390 |
| redis-pilot-hotcold-cold.service | Kvrocks 16666 |

历史二进制在 /opt/redis-pilot-hotcold/，配置和独立凭据在 /etc/redis-pilot-hotcold/。Gateway 状态在 /var/lib/redis-pilot-hotcold-gateway/state-live，同步器日志及操作状态在 /var/lib/redis-pilot-hotcold-syncer/ 下。旧服务此前设为 enabled；本轮没有核查、停止、升级或卸载任何远端服务，也没有修改 Proxy 路由。

后续切换需先核查实际状态与数据，受控停止旧链路后再部署新缓存版本。不能将本次源码清理当作远端旧服务已下线。
