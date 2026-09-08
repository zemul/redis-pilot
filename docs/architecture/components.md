# Redis 多实例管理系统 — 组件详细设计

> 上一篇：[架构概述](overview.md) · 下一篇：[关键流程](flows.md)

---

## 3. 组件详细设计

### 3.0a 冷热扩展组件边界（隔离开发中）

Gateway 与同步服务同仓库、不同二进制和 systemd 进程：Gateway 负责业务请求和未来冷热协调；同步服务负责 Redis 复制捕获、本地持久日志、Kvrocks 应用与确认。两者属于数据面，不把逐请求处理放到 Server，不由 Agent 自动部署或启停。

当前 Gateway 是单后端隔离透传；G6a-11 同步服务可运行 1～8 个静态隔离任务，每任务一个源与独占冷目标，尚无正式冷热业务链路。部署模板和约束见 [独立同步服务入口](syncer-service.md)、[静态多任务管理](syncer-multitask.md) 与 [网关独立部署](gateway-deployment.md)。

G6a-12 增加可选的同步器 → Server 状态上报：Server 在 `pilot.db` 持久记录本地任务声明的固定归属，重启不变、离线不重新分配；用户可查询进程冲突与失败状态。此链路不参与热写、不授权冷库写入、不下发配置或启停二进制。鉴权使用独立的 per-syncer token；完整边界与后续保护/接管顺序见 [同步器控制面登记](syncer-control-plane.md)。

G6a-13 可显式开启独立的源端 INFO 采样，并上报与本地缓存配对的同步进度。采样不复用 PSYNC、不触碰冷层、不位于业务请求的同步调用路径；仍会消耗源端连接与 CPU，实际影响待压测。Server 按样本年龄与复制历史派生差值，这不是当前冷库健康或安全淘汰证明。字段、成本与测试边界见 [同步进度观测](syncer-progress.md)。

单进程多任务、独立元数据和日志身份绑定由 G6a-11 提供；未来 Server 根据主分片拓扑分配任务，仍是待实现能力。当前静态本地运行不具备分布式归属/fencing、故障接管和扩容协调。Gateway 的安全淘汰需与同步器建立明确确认协议，当前不得开始降冷。

G6b-2d 提供 [显式跨进程隔离确认](gateway-confirmation-rpc.md)：同步器独占停止捕获后的已有源日志，Gateway 适配器持有自己的操作日志和屏障，经固定绑定/启动身份校验后请求冷确认。此确认模式与普通持续同步互斥；未接 Server 路由发现、实际业务准入、热删除或冷驻留发布，不是运行中的完整冷热服务。

G6b-2e 增加 [持续同步确认模式](gateway-live-confirmation.md)，同一单任务进程可继续 PSYNC 捕获、在准备位置保持冷应用并处理确认。屏障后不回收源证据，达到积压/磁盘阈值暂停捕获，重启保留待确认/需重验状态；仍不属于普通多任务模式的默认配置，也没有业务准入或热删除。

G6b-2f-1 增加 [Service/Pipeline 内部请求屏障接线](gateway-request-admission.md)，支持当前转发命令的 key 提取、多 key 原子准入及未知结果隔离。仅内部安装/隔离测试，默认服务不启用；持久启动保护与准备管理入口尚未完成，不提供自动淘汰。

### 3.0 Server（状态管理层）

部署在控制节点，是 CLI 和各服务器 Agent 之间的中间层，负责全局状态管理和并发控制。

#### 职责

| 职责 | 说明 |
|------|------|
| 状态存储 | 持有 pilot.db（SQLite 单文件，servers / instance_groups / instances / audit_log 等表），所有读写经过 Server 串行化 |
| 并发控制 | 内存读写锁（sync.RWMutex）+ 实例级操作锁（业务层），防止多 GAL 会话并发冲突。Server 为单进程部署，`WithInstances`/`WithServers` 通过 `DB.mu`（`sync.Mutex`）将"读快照 → 修改 → 全量重写"过程串行化，避免并发事务基于同一旧快照互相覆盖，无需文件级锁 |
| API 网关 | 暴露与 CLI Tools 一一对应的 HTTP API，转发操作到各服务器 Agent |
| 审计日志 | 记录所有管理操作到 pilot.db 的 audit_log 表 |

#### API 定义

```
节点管理（纯本地，不调用 Agent）
  GET  /node/list              查询节点状态
  POST /node/add                注册新服务器
  POST /node/remove             移除服务器
  POST /node/update             更新服务器信息

实例管理（转发到目标服务器 Agent）
  POST /instance/create         创建实例
  POST /instance/delete         删除实例
  POST /instance/start          启动实例
  POST /instance/stop           停止实例
  POST /instance/config         更新配置
  POST /instance/labels         整体替换或清空实例组标签列表（仅分类/筛选，不参与权限和调度）
  POST /instance/failover       触发故障转移（Sentinel 托管组：转发 SENTINEL FAILOVER；未托管组：直接操作 Agent，等价于旧版 promote）
  POST /instance/replicate      设置复制目标（Sentinel 托管组上拒绝，避免与自动拓扑管理冲突）
  GET  /instance/list           列出所有实例
                               可重复传 label=标签内容，多个条件按 AND 匹配实例组
  GET  /instance/status         实例详细状态
  GET  /instance/bigkeys        兼容同步大 Key 扫描（同实例存在异步扫描时拒绝）
  POST /instance/bigkeys        提交异步大 Key 扫描（返回 task_id）
  GET  /instance/key/get        受限 K/V 浏览器：查看单个 key（元信息 + 分页值）；仅 admin
  POST /instance/key/write      受限 K/V 浏览器：按 op 写入（set/hset/push/sadd/zadd/expire/hdel/srem/zrem/lset）；仅 admin
  POST /instance/key/delete     受限 K/V 浏览器：删除整个 key（DEL）；仅 admin

备份管理（组级；转发到目标服务器 Agent）
  POST /backup/exec             执行组备份（异步，返回 task_id；平台自动优先选健康从节点执行）
  GET  /backup/status           查询备份或恢复任务的持久化状态
  POST /backup/restore          从备份恢复整个组（异步，返回 task_id；恢复到组 master）
  GET  /backup/list             列出组的可用备份
  GET  /backup/schedule         读取组备份计划
  POST /backup/schedule         设置组备份计划（cron + retention）

运维任务（读取 pilot.db，不依赖原操作弹窗存活）
  GET  /tasks                   分页查询任务；支持 status/kind/operator/q 筛选
  GET  /tasks/:id               查询任务详情、目标、执行时间、结果与失败原因
  POST /tasks/:id/cancel        请求取消可安全停止的任务；当前仅支持大 Key 扫描

代理管理
  GET  /api/v1/proxy/snapshot   查看代理层结构化快照
  GET  /proxy/snapshot          查看代理层结构化快照
  POST /api/v1/proxy/heartbeat  Proxy 上报进程在线、已应用快照与监听/后端数量（internal_token）
  POST /proxy/heartbeat         心跳接口别名
  GET  /proxy/status            Dashboard 查询 Proxy 运行状态（JWT / API Token）
```

Proxy 运行状态采用单向上报，不建立 Server → Proxy 回调。`proxy_id` 优先取
`proxy.yaml` 显式配置，未配置时使用主机 hostname。Server 仅在内存中按 `proxy_id`
保留最新一条心跳：30 秒内为 online、30–60 秒为 stale、超过 60 秒为 offline；Server
重启后等待 Proxy 下一次上报。心跳只承载控制面低频状态，QPS、连接数、错误计数等时序
指标继续由 Prometheus 抓取 Proxy `/metrics`，不写入 `pilot.db`。

#### Dashboard 总览

总览定位为运维入口，不重复实例管理页的全部字段，也不把时序监控图表塞进首页。页面按处理
优先级组织为：待处理事项、运行保障、轻量实例列表、最近任务和最近事件。

- **待处理事项**：读取异常汇总，只展示最高优先级事项及可执行入口；未配置备份属于警告，
  不把所有持久化实例强制判定为异常。
- **运行保障**：合并 Agent 节点健康、Sentinel quorum、Proxy 心跳/快照接入和最近节点对账时间。
  Proxy 卡片只展示控制面状态；QPS、连接数和慢查询等时序指标仍留在监控视图。
- **实例列表**：保留实例组、实例类别（缓存/持久化）、拓扑、标签、内存使用和状态，最多展示六组；Redis/Kvrocks 使用
  对应引擎图标，标签直接平铺并默认选中“全部”，多选标签按 AND 语义筛选。详细操作继续放在
  实例管理页。
- **最近任务 / 最近事件**：任务读取 `/tasks` 的持久化状态，事件读取审计记录。两者并列是为了
  分别回答“后台工作执行到哪了”和“平台最近发生了什么”，不以虚构进度代替任务真实状态。

总览只组合现有 API 数据，不引入独立首页状态表；刷新时同时更新实例、节点、任务、审计、
Sentinel 与 Proxy 状态，并记录本次成功刷新时间。

#### 受限 K/V 浏览器

面向 admin 的受限 key 查看/读写能力，替代登机器跑 `redis-cli`。定位是**受限浏览器而非命令控制台**：只暴露白名单命令，禁止 `KEYS`/`FLUSHALL`/`CONFIG`/`EVAL` 等。

- **接口**：读一个 `GET /instance/key/get`（读 key 是单一操作，类型由响应里的 `type` 驱动前端渲染，不需要 op）；写一个 `POST /instance/key/write`（`op` = set/hset/push/sadd/zadd/expire/hdel/srem/zrem/lset，参数按 op 取用）；删一个 `POST /instance/key/delete`（整 key，前端二次确认）。
- **行内删除/编辑**：集合类值表格每行支持行内操作——删除单个元素 hash→`HDEL`/set→`SREM`/zset→`ZREM`（list 无按元素删的干净单命令，不提供）；行内编辑复用写 op：hash 改值走 `HSET`、zset 改分数走 `ZADD`、list 按下标改值走 `LSET`（set 成员即值，只能删不能改）。
- **权限**：仅 admin。`/instance` 组的 `adminOnly()` 会放行 viewer 的 GET，故这三个 handler 内再**硬校验 admin**（含读 GET）；前端按 JWT 角色隐藏入口。
- **路由**：读写都打实例组的当前 master（`CurrentMaster`），绕开 replica 的 READONLY；不做读写分离读。
- **大 value 保护**：先 `TYPE` 分流，再用 O(1) 命令探大小（STRLEN/HLEN/LLEN/SCARD/ZCARD）；string 用 `GETRANGE` 64KB 分片、list 用 `LRANGE` 偏移分页、hash/set/zset 用 `HSCAN`/`SSCAN`/`ZSCAN` 游标分页；响应值累计 256KB 兜底截断，写入 value 上限 512KB。绝不无脑 `GET`/`HGETALL`/`LRANGE 0 -1`。
- **单一能力源**：Server 侧核心逻辑落在 service 层（`keyGet`/`keyMeta`/`keyWrite`/`keyDelete`），HTTP handler 仅薄封装。读/写也已暴露给 AI 助手（`describe_key`/`get_key` 只读、仅 admin 可见；`write_key`/`delete_key` 经 confirm 二次确认）——复用同一份 service，不重写逻辑（见 ai-assistant.md）。
- **写审计**：`key.write`（important）/`key.delete`（critical）落审计，记录 operator/实例/key/op。
- **输出解析**：Agent 用宿主机 `redis-cli --raw` 执行并按行解析，不引入 RESP 客户端；value 含换行的极端情况会被拆行，属受限浏览器可接受的已知限制。



#### 认证

用户体系（人 -> Server）：

```yaml
# server.yaml
port: 8080
jwt_secret: ""        # JWT 签名密钥，为空则使用内置默认值（仅限本地开发）
internal_token: ""    # 内部组件（Proxy）通信 token，与 proxy.yaml 的 server_token 一致
admin:                # 初始管理员（仅首次启动、users 表为空时生效）
  username: admin
  password: admin
data_dir: /opt/redis-server/state
```

- Dashboard 登录签发 JWT（HS256，72h 过期），仅用于浏览器会话；用户与 `rpt_` 前缀 API Token 存 `pilot.db` 的 `users` / `user_tokens` 表，页面「用户管理」可自助创建/吊销
- CLI 长期凭证使用页面创建的 API Token（Bearer）
- 实例查询 API 使用安全投影返回实例组信息，不暴露仅供 Server 生成 Proxy 内部快照使用的前端密码。

内部组件（机器凭证走配置文件，与用户体系分离）：

- Proxy -> Server：`server.yaml` 的 `internal_token` 与 `proxy.yaml` 的 `server_token` 两侧一致，为空则跳过校验（快照含实例地址与密码，生产环境务必配置）
- Server -> Agent：per-node `agent_token`（每节点独立，泄露只影响单台）

#### Proxy 对外域名的统一管理与展示

Proxy 对客户端暴露的域名/IP（伪 Sentinel 应答的 announce host）与后端 Redis/Kvrocks 实际运行的
节点（`servers` 表的 `endpoint`）是两套独立的网络地址，物理上通常是不同的机器：

- **节点 endpoint**：Redis/Kvrocks 容器所在的物理机器，仅供运维/诊断定位实例位置，业务不应直连
- **Proxy 对外域名**：业务真正的连接入口，客户端应始终通过 `<Proxy 域名>:<Proxy 端口>` 访问，不经过后端节点

早期 Dashboard 曾直接把"节点名 + Proxy 端口"拼接展示，形似一个可连接地址（如 `redis02:16500`），
但 `redis02` 只是后端节点名，16500 是 Proxy 监听的端口，两者拼接产生的地址在多数部署下并不可达，
容易误导用户直连后端节点。

**Proxy 对外域名完全由 Server 统一管理，通过既有的 `/api/v1/proxy/snapshot` 快照下发**，是唯一权威
来源，不允许 Proxy 本地配置覆盖或兜底——这与 Server 作为"全局状态唯一写入点"的原则一致：

```yaml
# server.yaml
proxy_public_endpoint: "redis-proxy-dev.dev.ihuman.com"
```

Server 生成快照（`buildProxySnapshot`）时把该值填入 `ProxySnapshot.AnnounceIP` 字段，两个消费方：

1. **Proxy**：`discovery.ParseSnapshot` 解析出 `Topology.AnnounceIP`，伪 Sentinel 的
   `resolve()` 用它作为 `SENTINEL get-master-addr-by-name` 应答的 host。Proxy 侧
   `proxy.yaml` **没有** `sentinel_announce_ip` 配置项（已移除），完全依赖快照下发；
   若 `proxy_public_endpoint` 为空，退化为连接命中的本地 IP（跨 NAT 环境下不可用，生产环境务必配置）
2. **Dashboard**：通过既有的 `GET /server/info` 接口读取同一个值，拼出
   `<proxy_public_endpoint>:<port>` 作为可复制的完整连接地址，与"节点"列（后端物理位置）严格分列展示

单一配置源、双消费方，避免了"Server 和 Proxy 各自配置、需要手动保持一致"的漂移风险。

```yaml
# 以下字段现存储于 pilot.db 的 servers 表（每行对应一台服务器，含 Agent 连接信息）
# servers 表：
#   name TEXT PRIMARY KEY, endpoint, agent_port, agent_token,
#   labels(JSON), capacity(JSON), instances(JSON), status, last_heartbeat
servers:
  server-a:
    endpoint: 10.0.1.10
    agent_port: 8400
    agent_token: ""  # Server 调用 Agent 的鉴权 Token，为空则不鉴权
    ...
```

CLI 配置：

```yaml
# ~/.redis-pilot-cli/config.yaml
server: 127.0.0.1:8080   # 默认连接本机 Server
token: ""                # 填页面创建的 rpt_ API Token
```

CLI Token 优先级（从高到低）：`--token` 参数 > `REDIS_SERVER_TOKEN` 环境变量 > 配置文件。

---

### 3.1 管理 Agent

每台服务器部署一个 Agent，负责本机所有 Redis 实例的生命周期管理。

#### 3.1.1 职责

| 职责 | 说明 |
|------|------|
| Podman 生命周期 | 创建 / 启动 / 停止 / 删除容器 |
| 资源采集 | CPU / 内存 / 磁盘使用率，定时上报 |
| 配置管理 | 生成 redis.conf，挂载到容器 |
| 持久化目录管理 | 创建 / 清理 conf 和 data 目录 |
| 备份执行 | 本地执行 BGSAVE，校验产物非空且确为本次生成后再打包；恢复采用暂存 + 原子替换，原数据保留为 `.before-restore-<ts>` |
| 健康检查 | 定期 ping 实例，把结果**分类上报**（healthy / loading / auth_failed / repl_link_error / unreachable / unexpected），自身不采取任何恢复动作 |
| 指标采集 | INFO 命令解析，内存 / 连接 / 命令统计 |

> **Agent 不做故障自愈。** 这是一条明确的架构约束，理由见 [flows.md §4.3.5](flows.md)。
> 容器级自动重启由 `podman --restart on-failure:5` 承担（响应的是进程退出这个确定事实）；
> 主从组内实例不健康由 Sentinel failover 把流量切走；超过重启上限后容器停住，由 Server
> 的容器 reconcile 检测并告警。

#### 3.1.2 技术选型

**推荐方案：轻量 Python/Go 守护进程**

- 暴露 HTTP API，供 Server 层远程调用
- 内置健康检查循环，探测结果通过 `/instance/status` 的 `health` 字段暴露，**不自主重启实例**
- 使用 Podman API（libpod）管理容器，避免 shell 拼接
- 可选：结合 Podman Quadlet（systemd 单元文件）管理容器生命周期

#### 3.1.3 API 定义

```
实例管理
  POST /instance/create        创建实例
  POST /instance/start         启动实例
  POST /instance/stop          停止实例
  POST /instance/delete        删除实例（需确认数据清理）
  POST /instance/config        更新配置。三种互斥模式：
                               rewrite_config_only 只重写磁盘配置文件（角色落盘，不碰容器）、
                               restart 重写并重启、都不传则 CONFIG SET 热更新
  POST /instance/promote       从库提升为主库（REPLICAOF NO ONE）——底层原语，Server 仅在
                                Sentinel 未启用的组上调用；Sentinel 托管的组由 Sentinel 自己
                                直接对节点下发 REPLICAOF，不经过这个接口
  POST /instance/replicate     设置复制目标（REPLICAOF host port）——同上，仅无 Sentinel 托管
                                的组或 Sentinel 自身触发的降级会用到

  GET  /instance/list          列出本机所有实例，并附带两份角色观测供 Server 检测漂移：
                               configured_replica_of（磁盘配置文件里的 replicaof）
                               runtime_replica_of（进程此刻实际在复制谁，取健康探测缓存，
                               不现场探测；过期或探测失败的实例不出现在其中）
  GET  /instance/status        实例详细状态（含 INFO 解析）

诊断任务
  GET  /instance/bigkeys         兼容同步扫描
  POST /instance/bigkeys         提交异步扫描；同一实例仅允许一个
  GET  /instance/bigkeys/status  查询扫描数量、部分结果和取消状态
  POST /instance/bigkeys/cancel  协作式取消扫描

受限 K/V 浏览器（仅经 Server 的 admin 端点转发调用）
  GET  /instance/key/get         查看单个 key：TYPE 分流 + 探大小 + 分页值（meta=true 只取元信息）
  POST /instance/key/write       按 op 写入（set/hset/push/sadd/zadd/expire/hdel/srem/zrem/lset），白名单校验
  POST /instance/key/delete      删除整个 key（DEL）

备份管理
  POST /instance/backup        执行备份（异步，返回 task_id）
  GET  /instance/backup/status  查询备份任务状态
  POST /instance/restore       从备份恢复（异步，返回 task_id）
  GET  /instance/restore/status 查询恢复任务状态
  GET  /instance/backups       列出可用备份

主机管理
  GET  /host/resources         服务器资源使用情况
  GET  /host/health            健康检查
```

#### 3.1.4 请求/响应示例

**创建实例**

```
POST /instance/create

master/standalone 必须显式传 group；replica 不需要传 group，会从 replica_of 指向的 master 继承。

请求体：
{
  "name": "order-1",
  "group": "order",
  "port": 6379,
  "memory": "4Gi",
  "cpus": 2,
  "password": "******",
  "persistence": {
    "rdb": true,
    "rdb_frequency": "3600 1 300 100",   // save 配置
    "aof": true,
    "aof_policy": "everysec"
  },
  "replica_of": null,                      // 主库不设置
  "config_overrides": {                     // 额外配置覆盖
    "maxmemory-policy": "allkeys-lru",
    "timeout": 300
  }
}

> `config_overrides` 渲染在配置模板最末尾，而 Redis 配置是后者覆盖前者，
> 因此它是用户输入唯一能改变实例语义的入口。平台管理的配置项不允许覆盖，
> 值中不得含换行（否则可注入任意指令）。规则见 §4.7 与 `pkg/redisconf/validate.go`。

响应体：
{
  "success": true,
  "container_id": "a1b2c3d4",
  "container_name": "redis-order-1",
  "port": 6379,
  "conf_dir": "/data/redis/order-1/conf",
  "data_dir": "/data/redis/order-1/data",
  "backup_dir": "/data/redis/order-1/backup"
}
```

**查询主机资源**

```
GET /host/resources

响应体：
{
  "hostname": "server-a",
  "cpu": { "total_cores": 16, "usage_percent": 45.2 },
  "memory": { "total": "64Gi", "used": "28Gi", "available": "36Gi" },
  "disk": {
    "total": "500Gi",
    "used": "120Gi",
    "available": "380Gi",
    "redis_data": "80Gi"
  },
  "instances": ["order-1", "cache-1"],
  "podman": { "containers": 2, "running": 2 }
}
```

---

### 3.2 服务器节点

#### 3.2.1 服务器节点存储

全局维护在 `pilot.db` 的 `servers` 表中，记录每台服务器的容量和分配情况。字段以下用 YAML 风格展示概念性结构，实际存储为 SQLite 表的一行（`labels` / `capacity` / `instances` 等复合字段以 JSON 编码存入对应列）：

```yaml
# 概念性字段说明；实际存储为 pilot.db 的 servers 表
servers:
  server-a:
    endpoint: 10.0.1.10
    agent_port: 8400
    agent_token: ""            # Server 调用 Agent 的鉴权 Token，为空则不鉴权
    labels:
      zone: az-1
      role: production
    capacity:
      cpu_cores: 16
      memory: 64Gi
      disk: 500Gi
    allocated:
      cpu_cores: 4
      memory: 16Gi
      disk: 80Gi
    instances:
      - order-1
      - cache-1
    status: healthy
    last_heartbeat: "2026-04-23T18:00:00Z"

  server-b:
    endpoint: 10.0.1.11
    agent_port: 8400
    labels:
      zone: az-2
      role: production
    capacity:
      cpu_cores: 16
      memory: 64Gi
      disk: 500Gi
    allocated:
      cpu_cores: 8
      memory: 32Gi
      disk: 120Gi
    instances:
      - order-2
      - user-master
      - user-replica
    status: healthy
    last_heartbeat: "2026-04-23T18:00:00Z"

  server-c:
    endpoint: 10.0.1.12
    agent_port: 8400
    labels:
      zone: az-1
      role: standby
    capacity:
      cpu_cores: 8
      memory: 32Gi
      disk: 200Gi
    allocated:
      cpu_cores: 0
      memory: 0Gi
      disk: 0Gi
    instances: []
    status: healthy
    last_heartbeat: "2026-04-23T18:00:00Z"
```

> 注：`labels` / `capacity` / `instances` 在 `servers` 表中以 JSON 文本列存储；`allocated` 不落库，由 Server 在查询时读取 `instances` 表实时汇总计算得出（见 `computeAllocated`），因此不是 `servers` 表的持久化字段，上面的 `allocated` 段仅用于说明查询返回值的语义。

#### 3.2.2 调度策略

创建实例时的服务器选择逻辑：

```
1. 过滤：排除 status != healthy 的服务器
2. 过滤：排除剩余资源 < 申请量的服务器
3. 排序优先级：
   a. 主从必须分布在不同服务器（容灾）
   b. 主从优先分布在不同可用区（zone 标签）
   c. 剩余资源最多的服务器优先（均衡分配）
4. 锁定：选定后写入 `instances` 表（`server` 字段指向选中的服务器），服务器的已分配资源由 Server 查询时从 `instances` 表实时汇总派生，无需单独更新 `servers` 表
```

---

### 3.3 实例状态

#### 3.3.1 实例状态存储

全局维护在 `pilot.db` 的 `instance_groups` 表 + `instances` 表中，其中 `instance_groups` 是拓扑与入口的权威状态，`instances` 只记录单容器运行状态。以下用 YAML 风格展示概念性结构，实际存储为两张 SQLite 表（复合字段如 `proxy` / `persistence` / `config_overrides` / `backup` / `lock` 以 JSON 编码存入对应列）：

```yaml
# 概念性字段说明；实际存储为 pilot.db 的 instance_groups 表 + instances 表
groups:
  order:
    type: replication                 # standalone | replication
    engine: kvrocks                   # redis | kvrocks
    category: persistent              # cache | persistent
    labels: [生产, 交易, 核心]       # 实例组级分类元数据；主从成员共享，不参与权限或调度
    current_master: order-1      # 当前主库实例名
    topology_status: healthy          # standalone: 主库运行即 healthy；replication: 主库和至少 1 个从库运行才 healthy
    failover_conflict: false
    proxy:
      master_port: 16500              # Proxy 强制主库端口，所有命令走主库
      auto_port: 16379                # Proxy 读写分离端口，写走主库，读走主库+从库
    created_at: "2026-04-23T10:00:00Z"
    updated_at: "2026-04-23T10:05:00Z"

instances:
  order-1:
    group: order                      # 稳定实例组名；Sentinel/Proxy/审计均使用该名字
    role: master                      # master | replica
    server: server-a
    container: redis-order-1
    port: 6379
    memory: 4Gi
    cpus: 2
    password: "******"               # 脱敏存储
    config_path: /data/redis/order-1/conf
    data_path: /data/redis/order-1/data
    backup_path: /data/redis/order-1/backup
    persistence: null                 # Kvrocks 实例无需 RDB/AOF，RocksDB 原生持久化
    config_overrides:
      rocksdb.compression: lz4
      rocksdb.write_buffer_size: "256"
      rocksdb.max_write_buffer_number: "4"
      maxmemory-policy: noeviction    # 持久化实例不禁用数据
      timeout: "300"
    replica_of: null
    backup:
      schedule: "0 */6 * * *"        # 每 6 小时
      retention: 7                   # 保留 7 份
    status: running
    created_at: "2026-04-23T10:00:00Z"

  order-2:
    group: order                      # 从库继承主库 group；failover 后不变化
    role: replica
    server: server-b
    container: redis-order-2
    port: 6379
    memory: 4Gi
    cpus: 2
    password: "******"
    config_path: /data/redis/order-2/conf
    data_path: /data/redis/order-2/data
    backup_path: /data/redis/order-2/backup
    persistence: null                 # Kvrocks 实例无需 RDB/AOF，RocksDB 原生持久化
    config_overrides:
      rocksdb.compression: lz4
      rocksdb.write_buffer_size: "256"
      rocksdb.max_write_buffer_number: "4"
      maxmemory-policy: noeviction    # 持久化实例不禁用数据
      timeout: "300"
    replica_of: order-1         # 引用主库实例名
    backup:
      schedule: "0 */6 * * *"
      retention: 7
    status: running
    created_at: "2026-04-23T10:05:00Z"
```

#### 3.3.2 状态一致性规则

- **创建前**：先写入 `instances` 表（`desired_state=running`，`observed_status` 留空，
  `Phase()` 推导为 `creating`），再调用 Agent
- **创建成功**：更新 status → running；服务器已分配资源由查询时从 `instances` 表实时汇总派生，无需单独更新 `servers` 表
- **创建失败**：回滚 — 调用 Agent 清理已创建的资源，更新 status → failed
- **每次操作**：先读状态，操作完成后更新状态
- **定期校验**：Skills 可触发全量状态校验（实际容器 vs `pilot.db` 中的状态）

#### 3.3.3 操作锁机制

多 GAL 会话可能同时操作同一实例（如一个在 scale up，另一个在 migrate），并发读写需要锁保护。Server 通过 `WithInstances`（内部由 `DB.mu` 串行化"读快照 → 修改 → 全量重写"）保证同一时刻只有一个操作在改写 `instances` 表，业务层的实例级操作锁则在此基础上防止跨请求的逻辑冲突（如两个操作都读到了旧快照后各自认为自己拿到了锁）。

**实例级操作锁**：

```yaml
# lock 字段现存储于 instances 表的 lock 列（JSON 编码）
instances:
  order-1:
    name: order-1
    # ... 其他字段 ...
    lock:
      held_by: "gal-session-abc123"    # 持有锁的 GAL 会话 ID
      operation: "migrate"             # 正在执行的操作类型
      acquired_at: "2026-04-23T14:30:00Z"  # 锁获取时间
      timeout: 300                     # 锁超时（秒），默认 300
```

**锁规则**：

1. **获取锁**：任何修改操作前，必须先在 `instances` 表对应行的 `lock` 列（JSON）中写入锁信息
   - 如果 lock 已存在且未超时，拒绝操作并返回冲突提示
   - 如果 lock 已超时（`now - acquired_at > timeout`），视为锁失效，可被新会话抢占
2. **释放锁**：操作完成后（无论成功或失败），必须清除 lock 字段
3. **锁超时**：默认 300 秒，防止会话异常退出后锁永远不释放
4. **锁粒度**：实例级（同一实例组的主从共享一把锁，因为操作主库可能影响从库拓扑）
5. **只读操作**：查询类操作（info / list / status）不需要获取锁

**冲突解决**：

| 场景 | 处理策略 |
|------|----------|
| 锁未超时 + 同一会话 | 允许（同一会话可重入） |
| 锁未超时 + 不同会话 | 拒绝，返回当前锁持有者和操作类型 |
| 锁已超时 | 新会话可抢占，写入新 lock 并执行 |
| 锁字段缺失 | 无锁，可直接操作 |

**定期校验（Reconciliation）**：

- **触发条件**：Skill 手动触发 / 定时任务（每 30 秒）
- **权威源**：以 Agent 实时上报为事实源（actual），YAML 为期望状态（desired）
- **差异处理**：
  - actual=running + desired=running → 无需操作
  - actual=running + desired=creating → 状态漂移，更新 YAML 为 running
  - 观测 stopped + 意图 running → 异常，只写 `observed_status=stopped`，触发告警
    （`Phase()` 推导为 `unexpected_stopped`，不入库）
  - actual=running + desired=failed → 状态漂移，更新 YAML 为 running
- **校验范围**：实例状态 + 端口占用 + 资源分配

---

### 3.4 Proxy 统一代理

#### 3.4.1 端口分配策略

Redis 协议无 Host Header，**只能通过端口区分不同实例**：

| 端口范围 | 用途 | 说明 |
|----------|------|------|
| 16379-16499 | AUTO 端口（读写分离） | 每个实例组分配一个，写命令走主库，读命令在主库+从库间按 weight 分摊 |
| 16500-16619 | MASTER 端口（强制主库） | 主从实例组分配，所有命令都走主库 |

#### 3.4.2 代理模式

**双端口模型（主从实例组）**

```
客户端 -> Proxy:16379 (AUTO)    -> 写走主库，读走主库+从库（读写分离）
客户端 -> Proxy:16500 (MASTER) -> 所有命令走主库 order-1:6379
```

- AUTO 端口：写命令路由到主库，读命令在主库 + 所有健康从库间按 weight 分摊
- MASTER 端口：所有命令（读+写）都走主库，适合需要强一致读的场景
- 从库不可用时 AUTO 端口读回落到主库，同时打告警指标
- 不保证 read-your-writes：AUTO 端口读可能命中未同步的从库，需要强一致读用 MASTER 端口

**单端口模型（单点实例）**

```
客户端 -> Proxy:16379 (AUTO)  -> 主库 cache-1:6379
```

- 单点实例只分配 AUTO 端口，无 MASTER 端口

> **设计说明**：Proxy 是自研 Go 代理（`redis-pilot`），通过 HTTP 长轮询从 Server 获取拓扑快照（`/api/v1/proxy/snapshot`），在单端口上支持全部 Redis 命令（含阻塞/订阅/Stream/事务），读写分离按命令分类自动路由，无需业务侧显式选择端口。MASTER 端口提供强制主库访问入口，用于需要强一致读的场景。

#### 3.4.3 Proxy 配置

Proxy 是 Go 二进制（`redis-pilot`），通过 `proxy.yaml` 配置启动（`redis-pilot -config proxy.yaml`）：

```yaml
# proxy.yaml 配置示例
listen_bind: 0.0.0.0
server:                      # 拉快照的 Server
  endpoint: http://127.0.0.1:8080
  token: ""
poll:
  wait: 20s                  # 长轮询挂起上限（Server 侧上限 30s）
  timeout: 5s
rw_split:
  enabled_groups: []         # 按 group 显式开读写分离
  master_read_weight: 1      # master 在读池的权重
limits:
  deny_commands: [MONITOR, KEYS, FLUSHALL, FLUSHDB, CONFIG, SHUTDOWN, ACL, PSYNC]
  max_connections_per_client: 100
  max_request_bytes: 67108864
```

> **说明**：
> - Proxy 是 Go 二进制（`redis-pilot -config proxy.yaml`），不是外部容器
> - Server 暴露 proxy snapshot（`/api/v1/proxy/snapshot`），Proxy 通过 HTTP 长轮询拉取
> - 拓扑变化通过快照热更新，无需重启 Proxy
> - 代理级策略（deny_commands / master_read_weight）在 `proxy.yaml` 中，变更需重启 Proxy

#### 3.4.4 管理命令

管理命令（`INFO`、`CONFIG GET/SET`、`SLOWLOG GET`、`CLIENT LIST` 等）应通过 MASTER 端口执行，该端口直连主库。AUTO 端口会按命令分类路由，管理类命令自动走主库。

---

### 3.5 持久化与备份

#### 3.5.1 持久化策略

每个实例的持久化配置：

| 参数 | 主库默认值 | 从库默认值 | 说明 |
|------|-----------|-----------|------|
| save | 3600 1 / 300 100 / 60 10000 | 同主库 | RDB 快照频率 |
| appendonly | yes | yes | 开启 AOF |
| appendfsync | everysec | everysec | AOF 刷盘策略 |
| stop-writes-on-bgsave-error | yes | yes | BGSAVE 失败时拒绝写入 |

**Kvrocks 持久化实例（engine: kvrocks）：**

Kvrocks 基于 RocksDB 存储，数据天然持久化到磁盘，无需 RDB/AOF：
- `persistence` 字段设为 `null`（不生成 RDB/AOF 配置）
- 数据写入即落盘（WAL + MemTable → SST），崩溃恢复由 RocksDB 自动完成
- 无需 `save` / `appendonly` / `appendfsync` 等配置
- 持久化相关指标来自 RocksDB（见 §9.1）

#### 3.5.2 备份策略

**备份来源：优先从从库备份**

- **Redis 实例（engine: redis）：**
  - 避免对主库性能影响
  - 从库执行 `BGSAVE`，等待完成并校验产物后，上传 RDB 到 S3
  - 无从库的单点实例，在主库执行（安排在低峰期）
  - **如果开启了 AOF（appendonly: yes），备份时必须同时保存 AOF 文件**：
    1. 先执行 `BGREWRITEAOF` 生成精简 AOF
    2. 等待 `BGREWRITEAOF` 完成（`INFO Persistence: aof_rewrite_in_progress=0`）
    3. 备份 RDB 文件 + 重写后的 AOF 文件，打包为 tar.gz 上传 S3
    4. 两者作为同一备份集，使用相同时间戳

- **Kvrocks 实例（engine: kvrocks）：**
  - 使用 `BGSAVE` 生成 RocksDB 快照（不使用 `ROCKSDB.CHECKPOINT`，kvrocks 2.15.0 不存在该命令）
  - 从库执行 `BGSAVE`，等待完成后打包为 tar.gz 上传 S3
  - 无从库的单点实例，在主库执行（安排在低峰期）

备份调度由 Server 内置 cron 驱动，配置读取自 `instance_groups` 表每个组的 `backup` 列（JSON）。Server 遍历组，对配置了 schedule 且组内存在可服务实例的组注册定时任务；触发时自动优先选健康从节点执行，回退 master。Agent 不独立维护调度状态。

```yaml
# 组级备份策略（存在 instance_groups.backup 列）
backup:
  schedule: "0 2 * * *"   # cron 表达式，空字符串表示不启用自动备份
  retention: 7             # S3 保留份数
```

通过 CLI 配置：

```bash
# 设置定时备份
redis-pilot-cli backup set-schedule <group> --cron "0 2 * * *" --retention 7

# 查看当前配置
redis-pilot-cli backup get-schedule <group>

# 禁用自动备份
redis-pilot-cli backup set-schedule <group> --cron ""
```

Server 启动时读取所有组的 `backup.schedule`，注册内部 cron，并每 60 秒与 `instance_groups` 表同步一次。组备份配置变更后，Server 调度器在下一轮同步中增删对应任务。

**S3 对象存储：**

备份产物上传至 S3 兼容对象存储（如 MinIO），S3 是备份的唯一持久化位置，本地磁盘仅作暂存。S3 连接配置在 `agent.yaml`：

```yaml
backup:
  s3:
    endpoint: http://oss.internal:9000
    bucket: redis-pilot-backup
    access_key: xxx
    secret_key: xxx
    region: ""
    use_ssl: false
```

Agent 初始化时创建 S3 客户端，若 endpoint 为空则备份/恢复/列表接口返回 503。

**S3 Key 命名约定：**

```
{instance-name}/{backup-ts}.{suffix}

# Redis RDB-only
order-1/2026-08-11T02:00:00.rdb

# Redis RDB+AOF（打包为单个 tar.gz）
order-1/2026-08-11T02:00:00.rdbaof.tar.gz

# Kvrocks
order-2/2026-08-11T02:00:00.checkpoint.tar.gz
```

Key 完全可预测，不需要索引。`backup list` 通过 `ListObjects(prefix="{instance}/")` 获取列表；恢复时由 `{instance}/{backup-ts}.{suffix}` 拼出 key 下载。

**备份轮转：**

备份上传成功后，Agent 立即执行 S3 轮转清理（`ListObjects` + `DeleteObjects`），保留最近 N 份（`retention`）。实例删除时（`clean_data=true`），Agent 清理该实例在 S3 中的所有对象。

**延迟从库（可选高级功能）：**

- 设置 `REPLICA_DELAY`（如 3600 秒），从库延迟 1 小时应用主库写入
- 用途：误删除数据的快速恢复（延迟窗口内提升延迟从库为主库）
- 需要额外从库资源，按需开启

#### 3.5.3 备份恢复流程

备份与恢复都采用异步任务模型。Server 生成统一 `task_id` 并写入 `pilot.db.tasks`，Agent
立即接受任务并在后台执行；Server 自身每 2 秒轮询 Agent，不依赖浏览器或 CLI 保持连接。
任务终态为 `success`、`failed` 或 `interrupted`，持久化后即使 Server 重启仍可查询。
恢复任务执行期间 Server 持续续租整个实例组的操作锁，结束后释放，并写入最终审计。
Agent 重启导致内存任务丢失时，Server 将任务标记为 `interrupted`，避免无限显示运行中。
Dashboard 提供独立的「运维任务」入口，并定时读取 Server 持久化状态。备份与大 Key 弹窗
提交任务后继续轮询同一任务状态：用户愿意等待时可在原弹窗查看进度和结果，也可随时关闭；
关闭不会中止任务，后续状态、耗时、结果、失败原因及影响的实例/节点仍可在任务详情中查看。
任务状态与审计职责分离，二者使用任务 ID / `correlation_id` 关联。

大 Key 扫描复用同一任务模型：Agent 使用限速 `SCAN`，最多运行 60 秒、只保留按大小排序的
前 100 条命中结果，并持续上报已扫描 Key 数和部分结果。同一实例同时只允许一个扫描。
取消采用协作式停止，Server 先向 Agent 请求取消，Agent 在当前 Redis 命令结束后退出扫描；
最终任务标记为 `canceled`，保留已产生结果并标记 `incomplete=true`。备份与恢复不开放取消。

**Redis 实例恢复（优先 AOF → 回退 RDB）：**

```
1. 在目标实例仍运行时下载备份到实例目录下的 .restore-staging/data
2. 解压并校验：RDB 必须存在且非空；联合备份还必须包含非空 AOF 文件/目录
3. 校验通过后停止目标实例（停机窗口从这里开始）
4. 以一次目录 rename 原子替换整个 data 目录：
   - RDB+AOF 作为同一个数据集切换，不允许出现“新 RDB + 旧 AOF”
   - 原 data 目录保留为 data.before-restore-<ts>
   - 仅 RDB 备份的 staged data 不包含 AOF，因此不会残留旧 AOF
5. 启动实例；若启动失败，撤销目录替换、恢复原 data 并重新启动原实例
6. 验证步骤：
   - 检查 INFO Server: uptime > 0
   - 检查 INFO Keyspace: keyspace 与预期一致
   - 检查 INFO Persistence: rdb_last_load_status=ok / aof_enabled=1
   - 可选：对关键 key 执行 GET 验证值正确性
```

**Kvrocks 实例恢复：**

```
1. 实例运行时下载并校验 checkpoint.tar.gz，解压到 .restore-staging/data/db
2. 停止目标实例
3. 原子替换整个 data 目录并保留原目录
4. 启动实例；启动失败则回滚原目录并重启
5. 验证数据完整性（DBSIZE / 指定 key 抽查）
```

> **为什么优先使用 AOF 恢复？**
> - AOF 记录了所有写操作，数据完整性高于 RDB（RDB 是定时快照，可能丢失最后一次快照后的写入）
> - 联合备份时已通过 `BGREWRITEAOF` 生成精简 AOF，恢复效率与 RDB 相当
> - 仅在 AOF 文件损坏或备份中无 AOF 时，才回退到 RDB 恢复

---

### 3.7 Sentinel 高可用设计

Sentinel 用于 Redis/Kvrocks 主从实例组的自动故障转移。Sentinel 只负责判定主库故障、选举从库并执行主从切换；平台仍负责状态文件同步、刷新 Proxy 和记录审计。

#### 3.7.1 部署策略

Sentinel 是 Redis 高可用控制面，不要求每台 Agent 都部署，但必须由运维提前规划并在 Server 配置中显式声明。Server 不根据当前 healthy Agent 自动选择 Sentinel 节点，避免控制面随节点健康状态漂移。

| 声明的 Sentinel 节点 | Sentinel 数 | quorum | 处理策略 |
|----------------------|-------------|--------|----------|
| `sentinel.nodes` 未配置 | 0 | - | 不启用自动故障转移，创建主从时返回 warning |
| 3 个显式节点 | 3 | 2 | 标准生产部署 |
| 5 个显式节点 | 5 | 3 | 高等级业务使用，容忍更多 Sentinel 节点故障 |
| 其他数量的显式节点 | - | - | 配置非法，不启用自动故障转移 |

Sentinel 节点规划规则：

1. 节点名必须来自 `servers` 表的 `name` 主键
2. Sentinel 数固定为奇数（3 或 5），避免偶数投票拓扑
3. 节点应跨 `zone` / 机架 / 故障域部署
4. 避免选择临时节点、维护节点、低规格节点或 `role=standby` 节点
5. 节点列表写入 `server.yaml` 的 `sentinel.nodes`，作为 Server 下发和 reconcile 的唯一依据

示例：

```yaml
sentinel:
  enabled: true
  nodes:
    - redis-a
    - redis-b
    - redis-c
  port: 26379
  quorum: 2
```

#### 3.7.2 Server 与 Sentinel 交互

Server 直接通过 Redis/Sentinel 协议连接 `sentinel.nodes` 的 `port` 下发监控配置，不依赖 Agent 操作 Sentinel 容器。Sentinel 由运维独立部署，并负责把动态配置 rewrite 到自身可写、持久化的 `sentinel.conf`。

```
SENTINEL MONITOR <group> <master-ip> <master-port> <quorum>
  添加实例组主库监控

SENTINEL SET <group> down-after-milliseconds <ms>
SENTINEL SET <group> failover-timeout <ms>
SENTINEL SET <group> parallel-syncs <n>
  设置故障检测和切换参数

SENTINEL REMOVE <group>
  移除指定实例组监控

SENTINEL MASTERS
SENTINEL get-master-addr-by-name <group>
  查询 Sentinel 当前监控状态和主库地址
```

Server API：

```
GET /sentinel/status
  Server 直连各 Sentinel 节点，返回已监控 master 列表和连通状态

POST /sentinel/sync
  Server 从 `instances` 表派生完整 master 列表，并直接同步到各 Sentinel 节点

POST /sentinel/event
  外部 Sentinel watcher 上报 +switch-master 事件；Server 会再次查询 Sentinel 确认当前 master 后再同步拓扑
```

Server 下发的逻辑配置示例：

```json
{
  "port": 26379,
  "quorum": 2,
  "masters": [
    {
      "group": "order",
      "host": "10.0.1.10",
      "port": 6379,
      "password": "******",
      "down_after_milliseconds": 5000,
      "failover_timeout": 30000,
      "parallel_syncs": 1
    }
  ]
}
```

#### 3.7.3 sentinel.conf 模板

```conf
port {{ sentinel_port }}
bind 0.0.0.0
dir /data

sentinel resolve-hostnames yes

{% if announce_ip %}
sentinel announce-ip {{ announce_ip }}
sentinel announce-port {{ sentinel_port }}
{% endif %}

{% for master in masters %}
sentinel monitor {{ master.group }} {{ master.host }} {{ master.port }} {{ quorum }}
{% if master.password %}
sentinel auth-pass {{ master.group }} {{ master.password }}
{% endif %}
sentinel down-after-milliseconds {{ master.group }} {{ master.down_after_milliseconds | default(5000) }}
sentinel failover-timeout {{ master.group }} {{ master.failover_timeout | default(30000) }}
sentinel parallel-syncs {{ master.group }} {{ master.parallel_syncs | default(1) }}
{% endfor %}
```

当前采用"仅 Proxy 前端鉴权"模型：后端实例无 `requirepass`，Server 下发的 `master.password` 恒为空，故 `sentinel auth-pass` 分支不会渲染，Sentinel 以无密码方式直连实例执行 INFO、REPLICAOF、故障转移。上面的 `{% if master.password %}` 条件保留是为了兼容"实例带密码"的历史/静态部署——一旦实例启用了 `requirepass`，则必须配置 `sentinel auth-pass`，否则 Sentinel 能检测端口存活但无法执行故障转移命令。

#### 3.7.4 Podman 容器规范

Sentinel 容器推荐使用 host network，避免容器 NAT/端口映射导致 Sentinel 宣告错误地址、无法发现其他 Sentinel 或无法连接从库。

```bash
podman run -d \
  --name redis-sentinel \
  --network host \
  --restart on-failure:5 \
  -v /data/redis-sentinel/conf:/etc/redis:Z \
  -v /data/redis-sentinel/data:/data:Z \
  docker.io/redis:7 \
  redis-sentinel /etc/redis/sentinel.conf
```

**注意：必须挂载目录而非单文件。** Sentinel 保存配置时先写 `sentinel.conf.tmp` 再 `rename` 替换原文件。若将 `sentinel.conf` 以单文件 bind mount 挂入容器，`rename` 会因 inode 被宿主机锁定而报 `Device or resource busy`，导致新增 master 监控失败。挂载目录可让 rename 在同一文件系统内完成。

如果不能使用 host network，必须显式配置 `sentinel announce-ip` / `sentinel announce-port`，并确保 Redis 实例的 replica announce 信息与实际可达地址一致。

#### 3.7.5 创建与删除时机

创建主从实例组成功后，Server 执行：

```
1. 从 server.yaml 的 sentinel.nodes 读取已规划 Sentinel 节点
2. 从 `instances` 表派生所有需要监控的 master 列表
3. 直接连接各 Sentinel 节点 26379，执行 SENTINEL REMOVE / MONITOR / SET
4. 如果 sentinel.nodes 未配置或数量不是 3/5，跳过 Sentinel 并在创建结果中返回 warning
```

删除实例组时，Server 执行：

```
1. 直连所有 Sentinel 节点执行 SENTINEL REMOVE <group>
2. 从 `instance_groups` 表删除实例组
3. 状态变更后 Proxy 自动通过长轮询获取新拓扑
4. 写入审计日志
```

Sentinel 配置应由 Server 统一派生并下发；Agent 不应独立决定监控哪些 master。
Sentinel 的 `group` 必须来自 `instance_groups` 表中显式保存的实例组名，而不是当前 master 实例名；master 发生 failover 后，monitor name 仍保持不变。

#### 3.7.6 故障转移同步机制

实现采用 Server 内置 watcher + 定期 reconcile 的双路径：

```
Server 启动时：
  1. 从 server.yaml 的 sentinel.nodes 读取 3 或 5 个 Sentinel 节点
  2. 对每个 Sentinel 节点启动 1 个 watcher
  3. watcher 执行 SUBSCRIBE +switch-master
  4. watcher 断线只记录日志并退避重连，不修改实例状态
```

`+switch-master` 是 Sentinel 的全局事件频道，一个 watcher 会收到该 Sentinel 监控的所有实例组切换事件；因此 watcher 数量等于 `sentinel.nodes` 数量，不按实例组放大。

事件路径作为加速机制：

```
收到 +switch-master:
  1. 解析事件 payload 中的 group
  2. Server 再次查询 Sentinel：
     SENTINEL get-master-addr-by-name <group>
  3. 使用查询确认后的 master 地址调用 handleFailover(group, confirmedMasterAddr)
```

定期 reconcile 作为权威兜底：

```
每 5-10 秒：
  1. Server 查询任一健康 Sentinel：
     SENTINEL get-master-addr-by-name <group>
  2. 对比 Sentinel 返回的 master 地址和 `instances` 表当前 master
  3. 如果不一致，调用 handleFailover(group, newMasterAddr)
```

`handleFailover` 必须是幂等的：

1. 如果 Sentinel 确认的新 master 已经是 `groups[group].current_master`，直接 no-op
2. 获取实例组编排锁
3. 校验新 master 地址属于当前实例组
4. 更新 `groups[group].current_master/topology_status`，并更新实例 `role` / `replica_of` / `status`
5. 状态变更后 Proxy 自动通过长轮询获取新拓扑
6. 写 `topology.failover` 审计日志
7. 释放编排锁

#### 3.7.7 失败处理

| 场景 | 处理策略 |
|------|----------|
| healthy Sentinel 数 < quorum | 标记自动故障转移能力降级，保留现有实例运行，触发告警 |
| 单个 Sentinel watcher 断线 | 记录日志并后台退避重连，不修改 Redis 实例状态；其他 watcher 和 reconcile 继续工作 |
| 所有 watcher 断线 | 丢失实时事件，但定期 reconcile 仍会查询 Sentinel 当前 master 并最终同步 |
| 直连 Sentinel 同步部分节点失败 | 成功节点数 >= quorum 则继续并告警；否则返回 warning 并等待 reconcile 重试 |
| Sentinel 返回的新 master 不在 `instances` 表 | 标记 `failover_conflict`，不自动改拓扑，触发人工介入 |
| 新主库再次故障 | 不主动抢占 Sentinel，等待下一轮 Sentinel failover；编排锁超时后允许重新同步 |
| Proxy 快照拉取失败 | 不影响状态更新，Proxy 下次长轮询时自动获取新拓扑 |

#### 3.7.8 验证矩阵

| 场景 | 期望 |
|------|------|
| Redis 主库容器停止 | Sentinel 提升从库，Proxy 写入口切到新 master，Server 状态最终同步 |
| Kvrocks 主库容器停止 | 行为同 Redis，需验证 `ROLE` / Sentinel failover / replication 状态兼容 |
| 实例无密码（默认）| Sentinel 无鉴权直连实例，failover 成功；鉴权只在 Proxy 前端 |
| Sentinel 节点少于 quorum | 不发生自动故障转移，系统发出降级告警 |
| Proxy 重启 | 启动时从 Server 拉取快照恢复拓扑，业务端口不变化 |

---
