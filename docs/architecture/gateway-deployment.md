# 冷热组件部署与运维

现行运行入口，更新：2026-09-09。架构与支持边界见 [现行设计](hot-cold-gateway-design.md)，能力状态见 [实现进度](hot-cold-status.md)。本轮在 redis03 部署专用开发冷热链路；redis01 原有 Gateway 和三台机器的业务实例未替换。

Gateway 是独立数据面进程，与 Proxy 一样由运维部署二进制、由 systemd 管理生命周期。
Agent 仅管理本机 Redis/Kvrocks 容器，不安装、启停或采集 Gateway 专属状态。
Server 保留配置草稿；Gateway 登记及健康上报的最小闭环已有代码，实际部署版本须另查，配置拉取和已应用版本确认尚未实现。
默认仍为单后端透传。配置 `hot_cold` 后启用无 TTL String 的降冷/回温模式；自动内存水位调度需要另行显式开启，不是 Cluster。

## 从配置启用手动冷热模式

参考 [Gateway 配置](../examples/gateway-hot-cold.yaml)、[同步器运行配置](../examples/syncer-hot-cold.yaml) 和 [一次性同步引导配置](../examples/syncer-hot-cold-bootstrap.yaml)。依次执行：

1. 准备独占的本机 Redis/Kvrocks，Redis 使用 noeviction，Kvrocks 使用同步 WAL；配置和端口不能指向现有业务实例。Redis/Kvrocks 数据应使用持久卷。生成独立前端密码、管理 Token、同步器 Token，生成前 `umask 077`，文件归对应账号、0600；不把凭据放进参数或仓库。
2. **先引导同步，再固定历史编号。** 在尚无 Gateway 流量时，以同步器账号运行 `redis-syncer -config .../syncer-hot-cold-bootstrap.yaml`，使用最终任务 ID、端点和日志目录。确认首次捕获成功，正常停止并确认任务 drained。需要验证初始冷应用时，在专用源写入并删除一个测试 String，等待冷进度追平；空快照尚无后续命令时冷元数据可能尚未创建。
3. 用引导后 Redis 的 `INFO replication` 中 `master_replid` 填入两份运行配置的 `binding.history`；与已捕获历史一致。**不要使用 Redis 首次建立 backlog 之前的编号**。运行配置固定 source/cold/task/gateway/syncer/epoch，不在重启时自动更换绑定或重建 journal。
4. 给 Gateway 创建空的 0700 状态目录（账号持有），配置 `state_dir`。以同一账号先 `redis-gateway -config .../gateway.yaml -check-config`，再仅执行一次 `-init-state`。初始化只写本地身份/启用状态，不启动监听或访问后端。非空目录、重复初始化均拒绝；正常启动始终恢复，丢失状态不当作首次运行。
5. 给同步器创建独立的 0700 `operation_dir`，不能与 journal/Gateway 状态目录混用。用 `-confirmation-config .../syncer.yaml -check-config` 核对配置和凭据，再启动长期运行的 live 确认服务。
6. 正常启动 Gateway，无需测试程序注入配置。启动会核对 Redis 角色、固定复制历史和 noeviction；冷层短暂不可用不作为热业务启动门槛。检查 `/readyz` 后再使用开发流量，不修改生产 Proxy 路由。

可复用 [Gateway 单元](../examples/redis-pilot-hotcold-gateway.service)、[同步器单元](../examples/redis-pilot-hotcold-syncer.service) 以及对应 [Redis](../examples/redis-pilot-hotcold-hot.service)/[Kvrocks](../examples/redis-pilot-hotcold-cold.service) 开发容器单元。Gateway/同步器仍是独立二进制，systemd 的 `StateDirectory` 允许在 `ProtectSystem=strict` 下写状态，不能把状态放在只读 `/opt`。模板启用失败退避重启，保留原身份和日志，不删除状态来实现“恢复”。

手动降冷：向管理端 `POST /internal/v1/hot-preparation/evict`，Bearer 使用独立管理 Token；请求只需 `{"key":"<原始 key 的标准 base64>"}`，允许普通 JSON 空格和字段顺序。响应及 `X-Operation-ID` 返回编号；结果不确定可携带该编号重试。省略编号时，进行中的同 key 操作复用编号，已冷 key 返回 already_cold，新操作从持久上次操作的 128 位编号递增，不使用时钟或随机 UUID。回温后再次省略编号表示新一次降冷，不提供业务级 exactly-once API 承诺。

`-check-config` 不创建状态，也不证明后端连通。删除 `hot_cold` 而保留 `state_dir` 会被拒绝，内部状态也有启用锁；不能通过移除整个配置、换目录或旧二进制绕开检查后接管部分热数据集。当前仍需独占 Gateway 入口，跨机、TTL、Cluster 及源 Redis 重启后的历史接续尚未验收；受限自动调度见下一节。

## 构建与安装

### 自动降冷配置与观测

在上述已有冷热部署中设置 `hot_cold.auto_eviction.enabled: true`，检查配置后正常重启 Gateway；不重新初始化状态，不重启同步器或数据库。关闭此子开关只停止自动调度，保留冷热驻留检查与回温，不能删除整个 `hot_cold` 配置。

全部字段见 [配置示例](../examples/gateway-hot-cold.yaml)。默认高/低水位 0.8/0.7、批间隔 5s、批耗时 3s、采样 64、最多尝试 4 个 key、最短空闲 1m、最小 key 内存 8192 字节、最大同步落后 1MiB。时间和数量都有配置校验上限；扫描回复仍受 RESP 总字节/元素预算限制。`SCAN COUNT` 是提示，不保证精确返回数量；只保留有界候选并推进游标，异常回复暂停本批，不做全量缓存索引。

候选是无 TTL String、key 长度不超过 256 字节、非内部标记；`MEMORY USAGE` 上限也保守限制在 64KiB。该方式通过跳过小对象避免元数据成本抵消大部分收益，不等于精确成本模型。开始降冷后持续到低水位；每次删除前重新读取水位，UNLINK 异步释放不按估算大小伪造内存下降。

`/metrics` 新增以下固定名称，标签不包含 key：

- `redis_pilot_gateway_auto_eviction_memory_ratio`：最近成功观测的扣除不计入淘汰部分后的内存占比。
- `redis_pilot_gateway_auto_eviction_pressure`：是否已进入高低水位滞回区间，不代表冷库健康。
- `redis_pilot_gateway_auto_eviction_cycles_total{outcome=...}`：低水位、无候选、同步/冷不可用、操作推迟、批完成等结果。
- `redis_pilot_gateway_auto_eviction_keys_total{outcome=...}`：完成/跳过/推迟的尝试；完成不代表 key 此后一直冷，也不是已释放字节数。
- `redis_pilot_gateway_auto_eviction_last_success_timestamp_seconds`：最近一次成功自动降冷尝试的时间。

没有开启自动模式时不注册这些指标；不通过 `/readyz` 将冷库故障升级为热业务不可用。源 noeviction 仍是前提。长期无法安全降冷时，写入可能触达 Redis OOM，需要容量告警；默认不丢弃尚未同步的数据强行腾空间。

### 二进制安装

本机 `make gateway`，Linux amd64 开发机使用 `make gateway-linux`，产物为 `bin/redis-gateway`。
独立目标不属于 `make all`、`make deploy`；默认构建目标仍为 `all`。
当前只提供手动部署步骤，不自动选择主机或覆盖现有服务。

首次部署前，操作人员需要：

1. 确认使用隔离测试 Redis，数据完整，不能是只有部分 key 的热层。
2. 检查监听端口空闲、主机资源充足。当前只能回环监听，与测试 Proxy 同机；跨机入口支持待定。
3. 创建无登录权限的专用系统账号 `redis-gateway`，准备 `/opt/redis-gateway` 和 `/etc/redis-gateway`。二进制由 root 持有，服务账号仅可读取/执行。
4. 将构建产物安装到 `/opt/redis-gateway/redis-gateway`，权限 0755。将 [配置示例](../examples/gateway-passthrough.yaml) 复制为 `/etc/redis-gateway/gateway.yaml` 并调整后端、端口和容量预算。
5. 设置 `allow_unauthenticated: false`，配置独立的 `frontend_password_file`；后端有密码时设置 `backend_password_file`。凭据文件使用绝对路径，服务账号持有、权限 0600；配置目录 root:redis-gateway、0750，配置文件 root:redis-gateway、0640。不要把真实凭据加入仓库或命令行。
6. 以服务账号运行 `redis-gateway -config /etc/redis-gateway/gateway.yaml -check-config`，确认本地配置和凭据可读。此检查不代表后端连通、角色正确或业务就绪。
7. 安装 [systemd 模板](../examples/redis-gateway.service) 到 `/etc/systemd/system/redis-gateway.service`，执行 `systemctl daemon-reload`、`systemctl start redis-gateway`。确认保留的部署验收后执行 `systemctl enable redis-gateway`，满足开机启动要求；一次性测试实例不设置自启动。

示例 buffer_budget 是应用层保守预留预算，不是实际 RSS 上限。应按测试连接数、最大命令和机器余量调整；本模板不施加压测用的 CPU 配额。生产资源预算与 systemd 内存限制待容量测试后确定。

## 检查、升级与停止

- 使用 `systemctl status redis-gateway`、`journalctl -u redis-gateway` 检查进程与日志；通过配置中的管理地址检查 `/readyz` 和 `/metrics`。进程 active 不等于后端 ready。
- 用独立测试客户端验证认证、SET/GET，以及明确的错误返回；不修改现有 Proxy 快照。
- 停止使用 `systemctl stop redis-gateway`：SIGTERM 触发网关排空，systemd 超时必须大于配置中的 `shutdown_timeout`。
- 升级前保留旧二进制与配置，检查新配置，停止服务，再替换二进制并启动、验收；失败恢复旧版本。当前不提供滚动升级、自动切流或无损重试保证，客户端遇到不确定写入不能盲目重试。
- Server 草稿创建、修改、删除均不会启停该服务，草稿 revision 也不是进程已应用版本。

G5c 由 Gateway 直接与 Server 通信，配置与边界见 [登记与心跳](archive/hot-cold/gateway-heartbeat.md)；不通过 Agent 转发，配置应用确认仍待后续实现。
多网关归属、分片 fencing、高可用与切流仍待实现，不能把复制 systemd 服务理解为完成高可用。

## 独立同步器：当前可运行范围

- 通过 `make syncer` / `make syncer-linux` 构建 `bin/redis-syncer`；独立二进制，不由 Gateway 或 Agent 启动，不加入普通部署目标。
- 普通隔离任务使用 `redis-syncer -config /绝对路径/syncer.yaml`，先用同一参数加 `-check-config` 检查配置与凭据。参考 [单任务示例](../examples/syncer-isolated.yaml) 与 [多任务示例](../examples/syncer-multitask.yaml)；检查成功不证明后端/持久状态可用。
- 源/冷目标需符合当前 loopback 隔离限制，每任务独占冷 DB，日志目录与身份固定。不得通过清空目录、删除冷元数据或更换 task_id 绕过恢复检查。
- 使用独立账号与 [systemd 模板](../examples/redis-syncer.service)，安装前核对路径、权限、日志磁盘预算与停止超时；保留部署验收后开启自启动。模板当前 `Restart=no`，仅开机启动不等于故障自愈；自动恢复策略仍待完成，不能直接宣称自动接管。
- `/status`、`/livez`、`/metrics` 用于观测；可选 Server 心跳不下发任务、不授权写入，也不改变任务归属。增减静态任务目前需要受控重启。
- `-confirmation-config` 是固定单任务/单未完成转换的 live 确认入口，与普通配置互斥。配合上面的 Gateway 热冷配置使用；准备边界已有保留热数据收尾，但不等于自动淘汰。

已有显式 `hot_cold` YAML 开关，仅用于受限开发部署；默认透传仍不能接入部分热数据集。不进行现有业务切流，不启动自动淘汰，生产资源和可用性验收待完成。

配置字段和早期实验细节保留在 [同步器阶段归档](archive/hot-cold/syncer-service.md) 与 [静态多任务归档](archive/hot-cold/syncer-multitask.md)，现状优先以实现进度为准。

## 开发机部署记录（2026-09-07）

代码版本 `efbf187` 已在 redis01 独立部署，未部署 Server/Agent 的新版本，未修改 Proxy 路由。

| 项目 | 实际值 |
| --- | --- |
| 服务 | `redis-gateway.service`，专用无登录账号 `redis-gateway` |
| 二进制 | `/opt/redis-gateway/redis-gateway` |
| 配置 | `/etc/redis-gateway/gateway.yaml`，来源为 [小预算部署示例](../examples/gateway-systemd.yaml) |
| 入口 / 管理 | `127.0.0.1:17379` / `127.0.0.1:19200` |
| 凭据 | 随机生成，保存在 `/etc/redis-gateway/frontend.secret`，0600；不入库、不入仓库 |
| 隔离后端 | `redis-gateway-dev-backend`，Redis 7.4.9，仅映射 `127.0.0.1:16389` |
| 后端资源 | 容器内存 128MiB，Redis maxmemory 64MiB，noeviction；禁用 AOF/RDB，不存业务数据 |
| 自启动 | Gateway 和 `redis-gateway-dev-backend.service` 均已 enable、active；通过开发专用 drop-in 声明后端启动顺序 |

二进制 SHA256：`3ca12802520cfb49f5c3ef9ee67d10d5e8ec3fe4b1534e736e6f8feb311ae8f8`，本地与远端一致。
已验证 `/readyz`、前端认证、SET/INCR/GET/DEL（41→42），未认证 GET 返回 NOAUTH；测试 key 已删除。
另已验证 Gateway 停止后重新启动、`/metrics` 可访问、仅回环监听和凭据权限；后端 DBSIZE 为 0。现有 Server/Agent/Proxy 保持 active，未重启。
systemd 单元校验通过；系统同时提示既有 cloudmonitor 单元的告警，该服务未改动。
首次 start 返回后立即请求健康端口曾遇到连接拒绝，随后服务完成启动并返回 ok；不能把 systemctl start 成功等同于就绪。

该部署只用于功能验证，不是生产发布，也没有开启冷热同步、回温、淘汰或 Server 状态上报。

### 开机自启动补充

开发后端使用 [独立 systemd 单元](../examples/redis-gateway-dev-backend.service) 管理已有容器，不重建、不删除容器。
[开发依赖 drop-in](../examples/redis-gateway-dev-backend.conf) 安装至 `/etc/systemd/system/redis-gateway.service.d/dev-backend.conf`，仅适用于此隔离部署，不属于通用 Gateway 后端绑定。
两服务已执行 `systemctl enable --now`；停止后端/网关后重新启动验证通过，健康检查在后端就绪前短暂返回 503，随后返回 ok。
启动顺序不等于 Redis 就绪承诺，调用方仍需检查 `/readyz`。未实际重启开发机验证，未影响现有 Server/Agent/Proxy。
测试 Redis 仍禁用持久化：开机自动启动不代表重启保留测试数据。

## 手动冷热开发部署（2026-09-09，redis03）

本次使用 `dd7f504` 基础上的未提交工作区构建，版本标识 `dd7f504-hotcold-config-dev`；不是已发布版本。Gateway 和同步器为独立宿主机二进制，数据库为专用 Podman 容器。未替换 redis01 的旧 Gateway，未修改 Proxy 路由或现有实例。

| 组件 | systemd 单元 | redis03 回环地址 |
| --- | --- | --- |
| Gateway | `redis-pilot-hotcold-gateway.service` | 数据 `127.0.0.1:17379`，管理 `127.0.0.1:19200` |
| 同步器 | `redis-pilot-hotcold-syncer.service` | 确认 RPC `127.0.0.1:19301` |
| Redis 7.2.5 | `redis-pilot-hotcold-hot.service` | `127.0.0.1:16390` |
| Kvrocks 2.15.0 | `redis-pilot-hotcold-cold.service` | `127.0.0.1:16666` |

四个单元均 active、enabled。新 Gateway/同步器使用 `Restart=on-failure`，与前述普通同步器模板不同；冷库停止不会通过 systemd 依赖联动停止 Gateway。二进制位于 `/opt/redis-pilot-hotcold/`，配置与独立凭据位于 `/etc/redis-pilot-hotcold/`。前端及管理接口鉴权，后端仅本机回环、显式允许无密码。凭据权限 0600，不记录凭据内容。

运行状态目录：Gateway `/var/lib/redis-pilot-hotcold-gateway/state-live`；同步日志 `/var/lib/redis-pilot-hotcold-syncer/journal`；转换状态 `/var/lib/redis-pilot-hotcold-syncer/operations-live`，目录权限 0700。

首次空库引导发现 Redis 创建复制积压缓冲区时 history 发生变化，因此按“先引导，再读取 history，最后初始化”的顺序部署。此前尚未接收业务的初始 `state` / `operations` 目录原样保留，没有删除状态绕过恢复检查。当前固定 history 为 `2ab1e3074bd7c8e0251b6f040e804258540c6f04`，任务/分片 `hotcold-dev-0`、epoch 1。此新空库初始化处理不适用于已有业务数据的重新初始化。

资源：Redis 容器 256MiB、maxmemory 128MiB、noeviction、AOF everysec；Kvrocks 容器 512MiB、同步 WAL、block cache 32MiB；同步日志预算 128MiB。两库使用独立持久卷，没有增加压测 CPU 配额。

真实进程验收已覆盖：认证、写入同步、无 operation_id 的手动降冷、重复降冷、32KiB String、重启后回温、INCR pipeline、冷库停机时热读继续和冷写明确未执行、冷库恢复、Gateway/同步器 SIGKILL 后自动重启。三个专用测试业务 key 已通过 Gateway 删除并确认同步到冷库；内部标记保留。详见 [验收记录](benchmarks/hotcold-config-deploy-20260909.log)。

限制：仍是单源、回环、无 TTL String 的手动冷热验证；没有自动 LRU、Cluster 扩容或 Proxy 切流。未重启整台开发机；Redis 源重启/故障转移后的 history 衔接尚未支持，AOF 与单元 enabled 不代表整条冷热链路已能在源重启后自动恢复。

### 同日追加：水位自动降冷

redis03 的同一 Gateway 已升级为 `dd7f504-auto-eviction-dev`（未提交工作区），同步器和数据库二进制未变。原 Gateway/配置保留为 `/opt/redis-pilot-hotcold/redis-gateway.before-auto` 与 `/etc/redis-pilot-hotcold/gateway.before-auto.yaml`，持久状态目录没有更换或初始化；Proxy 路由不变。

Gateway SHA256：`8bddcbadb1f7dd737535c8a6ca45b2f87df6fbf468e65443d6e56bb9a941fddb`，本地与远端一致。

最终运行配置保留 `auto_eviction.enabled: true`，80%/70%、批间隔 5s、批总耗时 3s、最多尝试 4 key、采样 64、min_idle 1m、min_memory_bytes 8192、max_sync_lag_bytes 1MiB。测试期间临时降低的 Redis maxmemory 已恢复 128MiB；97 个测试业务 key 经 Gateway 删除且冷层确认，内部标记保留。四个单元继续启用开机启动；只重启 Gateway 应用默认采样参数，不重建状态。

真实测试用 96 个 16KiB String 制造内存压力，52 个自动降冷后 Redis 计入淘汰的内存占比从约 85.4% 降到 69.6%，低水位停止与访问回温验证通过。实际停掉 Kvrocks 时热读写仍成功、自动降冷暂停；恢复 Kvrocks 后自动继续。串行确认仍导致部分批次超时和较慢的下降速度，不代表降冷吞吐或热写延迟达标。完整结果见 [自动降冷验收](benchmarks/auto-eviction-20260909.log)。
