# Redis 多实例管理系统 — 关键流程与 Skills 设计

> 上一篇：[组件详细设计](components.md) · 下一篇：[部署与运维](deployment-ops.md)

---

## 4. 关键流程

### 4.1 创建单点实例

```
用户: "创建一个缓存 Redis，2G 内存，服务器 server-c"

Skill: redis-create
  │
  ├─1. node_list()
  │   → 确认 server-c 资源充足（2Gi 内存可用）
  │
  ├─2. port_allocate()
  │   → 分配实例端口 6379（本机无冲突）
  │   → 分配 Proxy 端口 16381
  │
  ├─3. state_update(instances, "cache-1", status=creating)
  │
  ├─4. agent_exec(server-c, "instance/create", {
  │       name: "cache-1",
  │       category: "cache",
  │       engine: "redis",
  │       port: 6379,
  │       memory: "2Gi",
  │       persistence: { rdb: false, aof: false },
  │       maxmemory-policy: "allkeys-lru"
  │   })
  │   → Agent: 创建目录 → 生成 redis.conf → podman run
  │
  ├─5. health_check(server-c, 6379)
  │   → 验证 PONG + INFO 验证持久化配置生效
  │
  ├─6. state_update(instances, "cache-1", status=running, proxy.auto_port=16381)
  │   → Proxy 自动通过长轮询发现拓扑变更
  │
  ├─7. state_update(instances, "cache-1", status=running)
  │   state_update(servers, server-c, allocated += 2Gi)
  │
  └─8. 返回结果: "缓存 Redis 已创建，连接地址 env-proxy:16381"
```

### 4.2 创建主从实例

```
用户: "创建一个订单 Redis，4G 内存，一主一从"

Skill: redis-create
  │
  ├─1. node_list()
  │   → 选择 server-a（剩余 36Gi，zone: az-1）→ 主库
  │   → 选择 server-b（剩余 28Gi，zone: az-2）→ 从库（不同 zone）
  │
  ├─2. port_allocate()
  │   → 主库端口 6379，Proxy 端口 16379 (读写) + 16380 (只读)
  │   → 从库端口 6379
  │
  ├─3. state_update(instances, "order-1", status=creating)
  │   state_update(instances, "order-2", status=creating)
  │
  ├─4. agent_exec(server-a, "instance/create", {
  │       name: "order-1",
  │       category: "persistent",
  │       engine: "kvrocks",
  │       port: 6379,
  │       memory: "4Gi",
  │       persistence: null,
  │       config_overrides: {
  │         "rocksdb.compression": "lz4",
  │         "rocksdb.write_buffer_size": "256",
  │         "rocksdb.max_write_buffer_number": "4",
  │         "maxmemory-policy": "noeviction"
  │       }
  │   })
  │
  ├─5. health_check(server-a, 6379)
  │
  ├─6. agent_exec(server-b, "instance/create", {
  │       name: "order-2",
  │       category: "persistent",
  │       engine: "kvrocks",
  │       port: 6379,
  │       memory: "4Gi",
  │       replica_of: "10.0.1.10:6379",
  │       persistence: null,
  │       config_overrides: {
  │         "rocksdb.compression": "lz4",
  │         "rocksdb.write_buffer_size": "256",
  │         "rocksdb.max_write_buffer_number": "4",
  │         "maxmemory-policy": "noeviction"
  │       }
  │   })
  │
  ├─7. health_check(server-b, 6379)
  │   → 验证 INFO replication: role=slave, master_link_status=up
  │
  ├─8. state_update(instances, proxy.auto_port=16379, proxy.master_port=16380)
  │   → Proxy 自动通过长轮询发现拓扑变更
  │
  ├─9. state_update(instances, "order-1", status=running)
  │   state_update(instances, "order-2", status=running)
  │   state_update(servers, server-a, allocated += 4Gi)
  │   state_update(servers, server-b, allocated += 4Gi)
  │
  └─10. 返回: "订单 Redis 主从已创建
              读写地址: env-proxy:16379
              只读地址: env-proxy:16380"
```

### 4.3 故障转移

**权威模型**：故障转移的唯一决策者是 Redis Sentinel（角色变更由 `SENTINEL FAILOVER` 触发或 Sentinel 自动探测触发），Server 只负责被动同步 Sentinel 已经做出的决策，不直接向任何节点下发 `REPLICAOF`。这样避免 Server 和 Sentinel 两个独立的决策者同时操作同一份拓扑产生冲突或脑裂（详见 §4.3.4）。

只有当 `sentinel.enabled=false`（未部署 Sentinel）时，Server 才会退回直接操作 Agent 的方式，作为唯一手段。

#### 4.3.1 用户主动触发（`instance failover`）

```
用户: "把 order-2 提升为主库" / "手动切换 order 组的主库"

CLI: redis-pilot-cli instance failover order-2

Server: instanceFailover(name="order-2")
  │
  ├─1. 解析 name 找到所属实例组 order 及其类型
  │
  ├─2a. 若该组由 Sentinel 托管（sentinel.enabled=true && group.type=replication）：
  │    │
  │    ├─ 向已配置的 Sentinel 节点发送 SENTINEL FAILOVER order
  │    │  注意：SENTINEL FAILOVER 是异步的，且不能指定提升哪个从库——
  │    │  最终提升哪个由 Sentinel 自己的选举算法决定（复制进度、
  │    │  replica-priority、Run ID）。name 只用于定位"操作哪个组"，
  │    │  不保证 name 对应的实例一定成为新主库。
  │    │
  │    ├─ 轮询 SENTINEL GET-MASTER-ADDR-BY-NAME order 等待地址变化
  │    │  （超时 = sentinel.failover_timeout，默认 30s）
  │    │
  │    ├─ 检测到地址变化后，等待 3 秒安全边际，再确认地址仍然是同一个新值
  │    │  （GET-MASTER-ADDR-BY-NAME 在 failover 状态机走到 reconf-slaves
  │    │  阶段就已经会返回新地址，但 Sentinel 内部收尾——switch-master 事件、
  │    │  把旧主库"虚拟加入"新主库的从库列表——还没完成。如果这时立刻同步
  │    │  Server 自己的 Sentinel 配置会打断 Sentinel 的收尾，见 §4.3.4）
  │    │
  │    ├─ 调用 handleSentinelFailover 把结果同步到状态库（§4.3.2 的编排步骤）
  │    │
  │    └─ 返回 { group, new_master, requested, matched }
  │       matched=false 表示 Sentinel 最终选择的不是 name 对应的实例
  │
  └─2b. 若该组未被 Sentinel 托管：直接向新主库所在 Agent 下发
       REPLICAOF NO ONE，并尝试让旧主库追随新主库（fallback，保证
       name 对应的实例一定成为新主库）
```

`instance replicate` 是 Server 内部用于 drift 修复的底层接口，不对 CLI 暴露。Sentinel 托管组的拓扑变更统一通过 `instance failover` 走 Sentinel 选举完成。

创建从库时，目标组是数据语义的权威来源：Server 强制继承组分类、组类型和引擎版本，再把规范化请求发给 Agent。注意：当前采用"仅 Proxy 前端鉴权"模型，后端实例无密码——前端密码存于 `InstanceGroupState.Password`（仅用于生成 Proxy 快照的 `ProxyListener.Password`），**不下发给 Agent**，实例配置不写 `requirepass`/`masterauth`，主从复制以无密码方式直连。

#### 4.3.2 Sentinel 自动检测（推荐）

对于核心业务实例组，建议部署 Redis Sentinel 实现秒级故障检测：

```
架构:
  按 §3.7 提前规划并声明 3 或 5 台 Sentinel 节点（quorum=2/3）
  一个 Sentinel 实例可监控多个主库，每个主库用不同名字区分
  Sentinel 监控所有实例组的主库

检测流程:
  Sentinel(3) → quorum(2) → 判定主库客观下线(sdown → odown)

  0. redis-create 创建主从实例时同步 Sentinel 监控配置：
     - 检查 server.yaml 中是否声明 sentinel.nodes
     - 已声明 3 或 5 台 Sentinel 节点：Server 直连 26379 下发 SENTINEL MONITOR/SET
     - 未声明或数量不是 3/5：跳过 Sentinel，返回警告"未正确配置 Sentinel 节点，未启用自动故障转移"
     - redis-delete 删除实例组时执行 sentinel remove {instance-group}

  1. Sentinel 检测主库不可用
     → 投票判定 odown，自动执行 failover
     → 选择优先级最高的从库提升为主库

  2. Server Sentinel watcher 监听 Sentinel 事件（+switch-master）
     → Server 启动时为 sentinel.nodes 中每个 Sentinel 节点启动 1 个 watcher
     → 收到事件后只把它作为唤醒信号，不直接信任事件里的 new-master
     → Server 通过 SENTINEL get-master-addr-by-name <group> 再次确认当前 master
     → 确认后执行后续编排：
       a. 获取同组实例编排锁
       b. 使用确认后的 master 地址修正 `instances`/`instance_groups` 表拓扑
       c. Proxy 自动通过长轮询发现拓扑变更
       d. 写入 topology.failover 审计日志
       e. 释放编排锁

     `instances` 表更新要求：
       - 实例组：
         current_master: <新主库实例名>
         topology_status: healthy 或 degraded
         proxy: 保持原实例组 master_port / auto_port 不变
       - 旧主库：
         status: failed 或 unexpected_stopped
         role: replica
         replica_of: <新主库实例名>
       - 被 Sentinel 提升的新主库：
         role: master
         replica_of: null
       - 其他存活从库：
         role: replica
         replica_of: <新主库实例名>

     Proxy 更新要求：
       - 业务端口保持不变，避免业务侧修改连接地址
       - 快照中后端 endpoints 必须包含新主库和所有健康从库
       - Proxy 通过长轮询自动发现拓扑变更，无需手动刷新配置


     审计要求：
       - action: topology.failover
       - level: critical
       - target 记录实例组、旧主库、新主库、故障服务器、新主库服务器
       - params 记录 Sentinel 事件和 Proxy 端口
       - result 记录 success / failed / conflict

  3. 编排锁与幂等保护机制：
     - 收到重复 +switch-master 或 reconcile 同时触发时，若当前 master 已同步则 no-op
     - 编排锁期间，同一实例组的后续 +switch-master 事件由锁保护或下一轮 reconcile 兜底
       （防止 Sentinel failover-timeout 内重试触发重复编排）
     - 编排锁超时：60 秒（超过后自动释放，允许重新编排）
     - 编排完成后必须释放锁

  4. 编排期间二次故障处理：
     - 如果编排过程中新主库再次宕机，Server 不主动抢占 Sentinel
     - 等待 Sentinel 发起新一轮 failover（编排锁超时后）
     - 将实例状态标记为 failover_conflict，触发告警通知人工介入

  5. 通知: "Sentinel 故障转移完成，主库已切换"
```

**Sentinel 配置要点**：

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| sentinel down-after-milliseconds | 5000 | 5 秒无响应判定主观下线 |
| sentinel failover-timeout | 30000 | 故障转移超时 |
| sentinel parallel-syncs | 1 | 同时可从新主库同步的从库数 |
| sentinel resolve-hostnames | yes | 支持主机名解析 |
| min-replicas-to-write | 1 | 主库至少有 1 个从库才接受写入，防止数据丢失 |

> **为什么用 Sentinel 而非纯 Agent 检测？**
> - Agent 上报依赖 Agent 自身存活 + 网络可达，存在单点盲区
> - Sentinel 采用分布式投票（quorum），对网络分区有更强容错
> - Sentinel 内置从库选举逻辑（优先级 → 复制偏移量 → Run ID），无需 Skill 重复实现

> **编排锁为什么重要？**
> - Sentinel 的 failover-timeout（30s）内可能重试 failover，导致 Server watcher 收到多个 +switch-master 事件
> - 没有锁保护时，多个编排流程并发执行会造成 Proxy 路由混乱、状态文件冲突和重复告警
> - 编排锁确保同一实例组同一时刻只有一个故障转移编排流程在执行

#### 4.3.4 Sentinel 唯一权威模型下的四个关键实现约束

以下四点是实机验证（反复触发 failover + 重启旧主库容器）与代码审计踩到的真实竞态/缺口，均已修复，记录约束原因以避免回归：

**a. 同步 Sentinel 配置时禁止无条件 `REMOVE`+`MONITOR`**

Redis Sentinel 官方机制：failover 完成后，旧主库会被 Sentinel"虚拟加入"新主库的从库列表（即使它当前不可达），这样它重新上线时会被自动探测并降级为从库——这是 Sentinel 自愈脑裂的核心依据。

`SENTINEL REMOVE <group>` 会清空 Sentinel 对该组的**全部**历史状态，包括这条刚建立的追踪记录。因此 Server 同步 Sentinel 配置（`syncSentinelNode`）时，只有当 Sentinel 当前认知的主库地址与期望地址**不一致**时才执行 `REMOVE`+`MONITOR`；地址已经一致时只允许用 `SENTINEL SET` 调整不影响拓扑追踪状态的参数（`down-after-milliseconds`、`failover-timeout` 等）。

**b. 后端实例无密码，配置文件不写 `requirepass`/`masterauth`**

当前采用"仅 Proxy 前端鉴权"模型：鉴权只在客户端 → Proxy 这一段完成，后端实例一律无密码。因此实例配置模板不写 `requirepass` 与 `masterauth`，Sentinel 与主从复制均以无密码方式直连实例，安全边界靠网络隔离保证（实例端口只对 Proxy 机器与数据节点互访网段开放）。

> 历史说明：早期方案为"实例带 `requirepass`"，当时要求 `masterauth` 与 `requirepass` 同级总是写入，以避免 master 被 Sentinel 降级为 replica 后因 `NOAUTH` 无法建立复制。改为无密码后此问题自然消失——没有密码就没有降级时的认证缺口。

**c. 检测到主库地址变化后必须等待收尾，不能立即同步**

`SENTINEL GET-MASTER-ADDR-BY-NAME` 在 failover 状态机走到 `reconf-slaves` 阶段就已经会返回新地址，但此时 Sentinel 还没有触发 `failover-end`/`switch-master` 事件、也还没有把旧主库"虚拟加入"新主库的从库列表（即 a 所述的追踪记录）。如果 Server 检测到地址变化后立刻同步 Sentinel 配置，会打断 Sentinel 还没走完的收尾流程，抹掉刚要建立的追踪记录，导致旧主库永远不会被自动纠正。

Kvrocks 场景复现明显（收尾阶段耗时更长，窗口期更长）；Redis 理论上存在同样的窗口但更短，实测未必稳定复现。缓解方式：检测到地址变化后，等待一个安全边际（≥3 秒）并二次确认地址未变，才认为 failover 真正收尾完成，再触碰 Sentinel 配置。

**d. "是否监控某个组"只能由拓扑意图决定，不能由主库健康状态决定**

`syncSentinelNode` 需要两个独立的输入，不能混为一谈：

| 输入 | 含义 | 用途 | 是否看健康状态 |
|------|------|------|----------------|
| `known`（`knownSentinelGroups`） | Server 认识的全部 replication 组 | 判断 Sentinel 上哪些组已不该存在，决定 `REMOVE` | **否** |
| `desired`（`buildSentinelMasters`） | 需要下发 `MONITOR`/`SET` 配置的组及其地址 | 决定配置下发 | 否，但需要地址可解析 |

早期实现只有 `desired` 一个集合，且 `buildSentinelMasters` 要求主库 `Role == "master" && Status == "running"`，`REMOVE` 又是"凡不在 `desired` 中的组一律删除"。两者叠加会静默摘除故障转移能力：

```
主库容器崩溃（Agent 仍存活，只是容器不在了）
  → 30s 容器 reconcile 把主库标为 failed（§4.2，无需任何人为操作）
  → 之后任意一次与该组无关的管理操作（instance create / delete）调用 syncSentinel
  → 该组被剔出 desired → SENTINEL REMOVE → Sentinel 彻底忘记这个组
```

结果是恰好在最需要 failover 的时刻，Sentinel 被告知不再监控该组，且全程无告警、无审计。此后即使从库健康、即使旧主库恢复，也不会有任何自动切换。

约束：**主库不健康恰恰是 Sentinel 最需要继续监控的时刻。** 健康状态只应影响"要不要重新 `MONITOR` 到新地址"，绝不能影响"要不要监控这个组"。`REMOVE` 只允许发生在组真正被删除或从 `replication` 改成 `standalone` 时——即只由 `known` 驱动。

这条约束与状态字段设计有关。早期实现里 `instances.status` 单字段同时承担"观测结果"（由 reconcile 写入 `failed`/`unexpected_stopped`）和"意图声明"（被其他代码读取用于判断该不该做某事）两种语义，读取方无法知道拿到的是哪一种。该字段已按 §4.6 拆分为 `desired_state` 与 `observed_status`。

#### 4.3.5 Agent 不得自主重启实例

"Sentinel 唯一权威"只约束了 Server，但系统里实际存在四个能影响拓扑的决策者。前两个是设计中承认的，后两个曾经不在视野里：

| 决策者 | 能改什么 | 决策依据 | 是否知道其他决策者 |
|--------|----------|----------|-------------------|
| Server | 容器生命周期、配置、状态记账、Proxy 路由 | `pilot.db` + Agent 上报 | 知道 Sentinel |
| Sentinel | 节点角色（`REPLICAOF`） | 自己探测 + 多数派选举 | 不知道 |
| Agent `runHealthCheck` | ~~`podman restart`~~（已删除） | 本机单次 `PING` | 不知道 |
| podman `--restart on-failure:5` | 容器自动重启 | 进程退出码 | 不知道 |

后两个不直接写角色，但**通过重启容器让角色回退到配置文件里固化的旧值**，从而间接推翻 Sentinel 的决策。这条路径已由 §4.3.6 的角色落盘机制堵住。

**Agent 自主重启已删除**，理由：

**a. 一个 bit 的信息量撑不起破坏性决策**

`PING` 失败可能意味着：实例真的死了（重启有用）、正在 load 大数据集（重启导致永远起不来）、正在 fork/AOF rewrite 短暂阻塞（重启可能丢数据）、密码不一致（重启无用，问题在配置）、负载高只是慢（重启把"慢"变成"挂"）、正在被 Sentinel failover（重启打断编排）。Agent 无法区分，却对全部六种情况采取同一个动作。

而重启一个 Redis/Kvrocks 实例的代价是：断开所有客户端连接、丢失未落盘数据、按配置文件旧角色启动、可能触发一次本不必要的 failover、replica 可能需要全量重同步打满网络。

**b. 判定门槛比 Sentinel 更低，总会抢在正确机制之前动手**

`tcpPing` 的超时是 3 秒，而 Sentinel 的 `down-after-milliseconds` 默认 5000ms。一个响应慢到 3~5 秒之间的主库，Sentinel 认为它还活着，Agent 已经把它重启了——**Agent 自己制造了本来不存在的故障**，然后 Sentinel 才被迫 failover。

**c. 主从架构里自愈的正确层次是切流量，不是修实例**

切流量无损、可逆、秒级（Sentinel failover + Proxy 路由更新）；重启有损、不可逆、慢。Agent 用错了工具。

**d. 静默重启会掩盖问题**

`podmanRestart` 连返回值都不检查，日志只有一行。一个每 30 秒被重启一次的实例，在监控上会看起来"大部分时间是 running"。而 `--restart on-failure:5` 的上限语义是"重启 5 次后放弃"——放弃是**期望行为**：容器停住，reconcile 检测到并告警，人来看。Agent 无限重启把这个上限绕过了。

**各层职责划分（修复后）**：

| 情况 | 负责人 |
|------|--------|
| 进程真正退出 | podman `--restart on-failure:5`（响应确定事实，无推断） |
| 超过重启上限后容器停住 | Server 容器 reconcile 检测 → 告警 → 人工介入 |
| 主从组内实例不健康 | Sentinel failover 切走流量 |
| 容器在跑但实例无响应 | Agent 分类上报 → `/instance/status` 的 `health` 字段 → Server/运维决策 |

最后一行补上了一个此前的观测盲区：容器 reconcile 只问 podman"容器在不在跑"，看不到"容器活着但实例 hang 住 / 正在 loading / 认证不一致"。`tcpPing` 现在返回分类结果而非无区分的 error，这个信息被记录并暴露出来，但 Agent 自己不据此采取任何动作。

#### 4.3.6 角色落盘：配置文件的角色必须跟随 Sentinel 的决定

删掉 Agent 自主重启只解决了角色回退的一半。剩下的一半是：

Sentinel 用 `REPLICAOF` 改变角色，这个变更**只存在于 Redis 进程内存中**。磁盘配置文件里还写着旧的 `replicaof`，而 podman 的 `--restart on-failure:5`（`podman.go:48,63`）会在进程异常退出时自动重启容器。重启后实例按旧配置起来，角色就回退了 —— Sentinel 认为 A 是 master，A 自己却又变回了 B 的 replica。

**为什么不用 `CONFIG REWRITE`**

看起来最直接的做法是让 Redis 自己把运行时角色写回配置文件。但配置文件在本项目里由 Go 模板生成（`config_template.go` 的 `writeConfig` → `os.WriteFile`，整体覆盖），而 `instance config --restart` 会用 Server 记录的 `ReplicaOf` 重新渲染整个文件。所以 `CONFIG REWRITE` 会让 Redis 成为配置文件的第二个写入者，且注定输给模板 —— 下一次任何配置变更都会把它写的内容抹掉。附带问题是 `CONFIG REWRITE` 重排格式、丢掉模板注释，文件会在两种形态之间来回翻。

这等于在配置文件内容上再加一个决策者，与本节要解决的问题同类。因此保持**模板是配置文件的唯一写入者**，由 Server 把角色推进去。

**机制**

| 环节 | 实现 | 作用 |
|------|------|------|
| Agent 新增 `rewrite_config_only` 模式 | `agent.go` `instanceConfig` | 只重写磁盘配置文件，**不重启容器、不做 `CONFIG SET`** |
| 角色变化后立即推送 | `sentinel.go` `handleSentinelFailover` → `persistGroupRoles(group)` | 故障转移同步完成后把组内所有实例的新角色写进各自配置文件 |
| Agent 上报磁盘角色 | `agent.go` `instanceList` 响应新增 `configured_replica_of` | 让 Server 能看到"磁盘上写的角色" |
| reconcile 检测并补推 | `reconcile_handler.go` → `detectRoleDrift` / `reconcileRoleDrift` | 磁盘配置与 Server 记录不一致时补推一次重写 |

`rewrite_config_only` 刻意不重启容器：重启本身就是要避免的动作，而且此时运行时角色已经是对的，需要修正的只有磁盘上那份。

**为什么 reconcile 那一步是关键**

只做"角色变化后立即推送"是不够的。Sentinel 事件一旦丢失（Server 重启、Server 与 Sentinel 之间网络分区），配置文件就停在旧角色上，回退窗口依然存在。周期性比对让这套机制**收敛到期望状态**，而不是依赖事件可靠投递 —— 与 §4.3.4 中"是否监控只由拓扑意图决定"用的是同一个思路：按期望状态收敛，不按事件驱动。

漂移修正会记审计 `reconcile.role_drift`，带上 `configured_replica_of` 与 `expected_replica_of`，便于事后判断丢了多少事件。

**判定上的保守取舍**

`detectRoleDrift` 在三种情况下**不判定漂移**，避免把一个正常实例改坏：

- 实例所属组不是 `replication` 类型 —— standalone 没有角色概念
- Agent 没有上报该实例的 `configured_replica_of` —— 配置文件缺失说明实例刚创建或已删除；旧版本 Agent 不上报该字段时也不会被误判为全组漂移
- 期望地址解析失败 —— 否则会拿一个空地址去覆盖配置文件，把 replica 错误地改写成 master

**顺带修复的一个既有 bug**

`inst.ReplicaOf` 在 `pilot.db` 里存的是**实例名**（`instance_handler.go:343`、`:1002`），而配置模板期待 `ip:port`（`config_template.go:157` 把冒号替换成空格拼进 `replicaof` 指令）。`instanceConfig` 此前直接把 `inst.ReplicaOf` 传给 Agent，导致对 replica 执行 `instance config --restart` 时配置文件被写成 `replicaof order-1` —— 缺少端口，Redis 启动时报 `wrong number of arguments`，而容器已经被 Stop，**实例起不来**。

现在统一经 `resolveReplicaOfAddr` 解析。它与用于校验用户输入的 `resolveReplicaOf` 的区别是**不断言目标必须是 master**：角色落盘发生在故障转移前后，组内角色可能正处于中间状态，此时需要如实写出 Server 的记录，而不是校验这份记录是否合理。

**保留的已知窗口**

配置文件仍然是"重启后读到的角色"的来源，所以从"Sentinel 改变角色"到"Server 把新角色写进配置文件"之间存在一个窗口。窗口内如果容器恰好重启，角色仍会回退一次，随后由下一轮 reconcile（30 秒）纠正。彻底消除需要让配置文件完全不表达角色，但那会引入"重启瞬间出现一个独立 master"的新问题，且 kvrocks 模板没有 `min-replicas-to-write` 这类写保护（`config_template.go:79` 只在 Redis 模板内），因此没有采纳。

**漂移修正必须基于锁内重新读取的状态**

`reconcileRoleDrift` 只接收漂移清单，`node` / `instances` 在持 `topologyMu` 之后重新读，刻意不沿用检测时的快照。

原因是检测和修正之间的窗口比看起来长得多：`runReconcile` 在开头读快照，随后**顺序遍历每台节点的 Agent** 调 `/instance/list`（`agentHTTPClient` 每个最多 10 秒），漂移是在这个遍历过程中检测出来的。窗口里完成一次故障转移，沿用旧快照就会把刚被 `persistGroupRolesLocked` 改对的 `replicaof` 按旧值写回磁盘，容器下次重启即按错角色起来——正是本节要消除的那个回退，只是换了个成因。**持锁但用锁外读的快照，锁是白加的。**

重新解析出的期望地址与检测时不一致，说明拓扑在这中间变过了。此时对"磁盘上现在写着什么"的认知已经过期（它来自变更之前的那次 Agent 上报），据此判定的漂移可能根本不存在，所以跳过并交给下一轮 reconcile 重新检测；而且造成变更的那次故障转移自己已经把正确值推给了全组，本来就不需要这里补。审计记 `skipped`。

审计结果也必须反映实际下发结果（`corrected` / `failed` / `skipped`）。此前是在 `runReconcile` 里对每个 drift 无条件写 `corrected`，而 `pushConfigRewrite` 失败只是 `continue`——排查故障时审计日志会声称修好了没修好的东西，把人引开。

#### 4.3.7 运行时复制关系的自愈

§4.3.6 处理的是"配置文件里的角色"和"Server 记录"之间的偏差。还有第三份记录：**实例进程内存里此刻实际在复制谁**。三份可以互相偏离：

| 记录 | 含义 | 生效时机 |
|------|------|----------|
| `Instance.ReplicaOf`（`pilot.db`） | 意图，跟随 Sentinel 的决定 | —— |
| 磁盘配置文件的 `replicaof` | 容器下次重启会读到的角色 | 下次重启 |
| `INFO replication` 的 `master_host` | 此刻正在发生的事实 | 立即 |

制造 1↔3 偏差的入口有好几个：Sentinel 的 `+switch-master` 事件在 Server 重启或网络分区期间丢失、实例恰好在故障转移期间重启后按旧配置文件起来、有人直接对容器执行了 `redis-cli REPLICAOF`。而在补上本机制之前，Server 侧三条自愈路径**一条都不看运行时复制关系**：`runReconcile` 只问容器在不在跑，`reconcileSentinelOnce` 只比对每组的 master 地址，`detectRoleDrift` 只比对配置文件。出口缺失让任何一次瞬时偏差都变成永久偏差。

典型形态：三节点组 A/B/C，Sentinel 把 master 从 A 切到 B，C 因为某个入口没收到，仍在复制 A。数据本身不会错（B→A→C 的链式复制在 Redis 里合法），但 C 在 `proxy_group` 眼里 `IsServing()` 为真，照样进 replica cluster 接读流量；一旦 A 停掉或 A→B 的复制链断开，**C 就长期提供陈旧数据，而所有监控指标都显示它健康**。这类"看起来正常的错"比直接报错危险得多。

| 环节 | 实现 | 说明 |
|------|------|------|
| Agent 观测运行时角色 | `monitor.go` `probeInstance` | 健康探测（30s）在同一条 TCP 连接上顺带发一条 `INFO replication` |
| Agent 上报 | `agent.go` `instanceList` 响应新增 `runtime_replica_of` | 读的是探测缓存，**不现场探测** |
| Server 检测 | `replication_drift.go` `detectReplicaDrift` | 与 `detectRoleDrift` 同样的保守约定 |
| Server 修正 | `replication_drift.go` `reconcileReplicaDrift` | 持 `topologyMu`，锁内重读状态 |

**为什么上报的是缓存而不是现场探测**：现场探测会让 `/instance/list` 的耗时随实例数线性增长，而任何一个 hang 住的实例都会把整个响应拖过 Server 侧 `agentHTTPClient` 的 10 秒超时，让 reconcile 把这台机器上**所有**实例误判为 `unreachable`。用一个纯观测功能换掉整台机器的可观测性是净损失。缓存超过 `replicationStaleAfter`（90s，探测周期的 3 倍）的实例不出现在上报里，探测失败时显式 `clearRepl` 丢弃旧值——Server 会据此下发 `REPLICAOF`，判据必须是"刚刚看到的事实"。

**纠正动作的边界刻意收得很窄**，理由是不能让 Server 变成第二个拓扑决策者：

- 只对 replica 下发 `REPLICAOF`，目标地址取自 Sentinel 确认过的当前 master。
- **绝不提升任何实例**（不发 `REPLICAOF NO ONE`）。期望是 master 而实际在复制别人时只记审计 `alert`：要么 Server 记录本身落后于 Sentinel（`reconcileSentinelOnce` 会纠正），要么 Sentinel 确实认为它该是 master 而它自己不知道（该由 Sentinel 提升）。
- Sentinel 启用时，下发前先向 Sentinel 确认该组 master 与 Server 记录一致，不一致就什么都不做——那属于 `reconcileSentinelOnce` 的职责，两条路径同时动手会互相打架。查询失败一律视为不一致（拿不到权威意见时宁可这一轮不动）。

这样本机制只做一件事：**把 Sentinel 已经做出、但在某个实例上丢失了的决定重新贴上去。** 审计 `reconcile.replica_drift` 记为 Critical 级别——重新下发 `REPLICAOF` 会让目标实例丢弃自己与新 master 分叉的那部分数据（复制的固有语义），属于影响数据的动作。

`detectReplicaDrift` 在这些情况下不判定漂移：组不是 `replication` 类型；`runtime_replica_of` 里没有该实例（Agent 这一轮没探到、实例正在 loading、或观测已过期）；期望地址解析失败。**缺失必须跳过而不是当成"它以为自己是 master"**——猜错的方向是对一个正常 replica 动手。旧版本 Agent 完全不上报该字段时同理，不会被误判为全组漂移。

### 4.4 实例迁移

```
用户: "把 order-1 从 server-a 迁移到 server-c"

Skill: redis-migrate
  │
  ├─1. 在目标服务器创建从库
  │   agent_exec(server-c, "instance/create", {
  │       name: "order-3",
  │       replica_of: "10.0.1.10:6379",
  │       ...
  │   })
  │
  ├─2. 等待全量同步完成
  │   health_check → INFO replication: master_sync_in_progress=0
  │
  ├─3. 提升新从库为主库
  │   redis-pilot-cli instance failover order-3
  │   → 若该组由 Sentinel 托管：Server 发送 SENTINEL FAILOVER，等待并同步结果
  │   → 若未托管：Server 直接向 Agent 下发 REPLICAOF NO ONE（fallback）
  │
  ├─4. 更新旧从库的复制目标
  │   若该组由 Sentinel 托管：Sentinel 会在下一轮探测中自动将旧从库/旧主库
  │   重新指向新主库，无需手动调用；仅在未托管的组才需要手动调用：
  │   agent_exec(server-b, "instance/replicate", {
  │       name: "order-2",
  │       replica_of: "10.0.1.12:6379"   // 新主库
  │   })
  │
  ├─5. 停止并删除旧主库
  │   agent_exec(server-a, "instance/stop", { name: "order-1" })
  │   agent_exec(server-a, "instance/delete", { name: "order-1" })
  │
  ├─6. 更新状态（Proxy 自动通过长轮询发现拓扑变更）
  │   state_update(instances, order,
  │       master=server-c:6379,
  │       replica=server-b:6379)
  │
  ├─7. 更新状态
  
  │   state_update(...)
  │
  └─8. 通知: "迁移完成，主库已从 server-a 切换到 server-c"
```

### 4.5 备份管理

```
用户: "备份订单 Redis"

Skill: redis-backup
  │
  ├─1. 平台确定执行节点（备份是组级概念）
  │   -> 优先健康从库: order-2 (server-b)
  │   -> 无可用从库时回退主库（state.PickBackupExecInstance）
  │
  ├─2. agent_exec(server-b, "instance/backup", {
  │       name:      "order-2",   // 平台选出的执行实例
  │       s3_prefix: "order",     // 组名，S3 归档前缀
  │   })
  │   -> 立即返回 {task_id: "order-16989xxxxx"}
  │
  ├─3. Server 后台轮询 agent "instance/backup/status?task_id=order-16989xxxxx"
  │   -> Agent 后台: BGSAVE -> 等待完成（失败/超时即报错）-> 校验产物
  │                   -> 上传 S3（key: order/{ts}.rdb，按组名归档）-> 删本地临时文件
  │                   -> S3 轮转清理（保留最近 N 份）
  │   -> 轮询直到 status=success 或 failed
  │
  └─4. 通知: "备份完成" / "备份失败: {error}"
```

> 备份策略挂在组上（`group.Backup`）：`/backup/*` 接口以 `group` 为参数，执行节点每次
> 触发重新选定，failover 后自然落到新拓扑下的合适节点；备份对象按组名归档，无论由哪个
> 实例执行都归属同一组，可统一列出与恢复。

恢复使用同一模型：`POST /backup/restore` 立即返回 `task_id`，Agent 使用独立于 HTTP
请求的后台 context 完成 S3 下载、校验、停机、原子替换和启动。Server 后台轮询
`/instance/restore/status`，把终态写入 `pilot.db.tasks`，写最终审计并释放实例组锁。
CLI 可通过 `backup status` 查询，或使用 `backup restore --wait` 等待终态。

---

### 4.6 状态字段的写入者约束

前面几节的多个问题（Sentinel 监控被静默摘除、故障转移时 down 掉的 replica 拓扑记录永久错误）有同一个根因：`instances.status` 单字段同时承担"意图"和"观测"两种语义，写入方表达一种意思，读取方理解成另一种。

字段已拆成两个，各自只有一类写入者：

| 字段 | 含义 | 允许的写入者 | 取值 |
|------|------|-------------|------|
| `desired_state` | 人的意图：这实例该不该在跑 | 仅用户发起的操作（create/start/stop） | `running` / `stopped` |
| `observed_status` | 最近一次观测到的实际状态 | 仅观测通道（`runReconcile`；以及 Sentinel 提升新 master 时那一次） | `""`(未观测) / `running` / `stopped` / `missing` / `unreachable` |

对外展示的 `status` 由两者推导（`Instance.Phase()`），**不入库**：

| 意图 | 观测 | 派生状态 | 含义 |
|------|------|---------|------|
| running | `""` | `creating` | 要求它跑，但一次都还没观测到 |
| running | running | `running` | 一致 |
| running | stopped | `unexpected_stopped` | 没人要求它停，它自己停了 |
| running | missing | `failed` | 容器不见了 |
| running | unreachable | `unreachable` | 观测能力本身失效 |
| stopped | stopped / missing | `stopped` | 一致（容器被清掉也算停） |
| stopped | running / `""` | `stopping` | 已要求停止但还没观测到结果 |

存储派生值会产生"派生值与来源不一致"的第三种坏状态，所以派生值只读不存。`unexpected_stopped` 本质上是两个字段的差值，此前把它塞进同一个字段正是问题形态本身。

**三条硬约束**

**a. 拓扑意图不得受观测状态门禁**

`Role` / `ReplicaOf` / `CurrentMaster` 表达"这个实例在组里应该扮演什么角色"，与"它现在活不活"无关。

`handleSentinelFailover` 此前有一道 `if inst.Status == "running"` 门禁，导致故障转移发生时恰好不 running 的 replica 的拓扑记录不被更新，永久停留在旧 master 上：三节点组 A(master)/B/C，C 先崩，Sentinel 把 B 提为 master，C 的 `ReplicaOf` 仍指向 A —— 而 A 此刻已是 B 的 replica，Server 于是相信一条 Sentinel 从未创建过的级联复制 C→A→B。

危害在于**没有自愈路径**：`reconcileSentinelOnce` 只比对 master 地址，`runReconcile` 只管观测状态，两者都不会发现这处错误。而 §4.3.6 的角色落盘会拿着这份错误记录，持续把 `replicaof A` 写进 C 的配置文件。回归测试：`TestFailover_UpdatesTopologyOfDownReplica`。

**b. 故障转移不猜测旧 master 的观测状态**

`handleSentinelFailover` 同时服务 Sentinel 自动故障转移（旧 master 通常真的挂了）和用户手动 `instance failover`（旧 master 可能完全健康）。把它一律标成异常是猜测而非观测，交给 reconcile 在 30 秒内如实观测。

新 master 的 `observed_status` 则会被写成 `running`：Sentinel 能把它选为新 master，说明 Sentinel 探测到它健康，这是有依据的观测。不写的话，Proxy 要等下一轮 reconcile 才会把写流量指向新 master（`proxy_group` 只挂 `IsServing()` 的实例），白丢最多 30 秒写入。

**c. "Agent 不可达"不得写进实例的观测状态**

Agent 不可达是对 **Agent** 的观测，不是对实例的观测。Agent 重启或网络抖动时，它名下的 Redis 实例通常完好无损。若把 `unreachable` 写进实例的 `observed_status`，`IsServing()` 会变成 false，`proxy_group` 就会把这些健康实例从 Proxy 端点里摘掉——一次 Agent 重启造成一次业务中断。

所以 `runReconcile` 的三处 Agent 不可达分支只告警、不写观测字段。回归测试：`TestReconcile_AgentUnreachableDoesNotOverwriteObservedStatus`。

**各读取点该读哪个字段**

| 读取点 | 读哪个 | 为什么 |
|--------|--------|--------|
| `proxy_group` Proxy 端点 | 观测（`IsServing()`） | 不能把流量发给一个"应该在跑但已经挂了"的实例 |
| `backup_scheduler` 排备份 | 观测（`IsServing()`） | 对挂掉的实例发 BGSAVE 只会失败 |
| `scheduler` 容量计算 | 观测（`missing`） | 容器已消失的实例不该继续占配额 |
| `RecalculateGroupTopology` | 观测 | 组健康度是事实结论，用意图判断只能得出"配置上应该是健康的" |
| `runReconcile` 动作判定 | 两者的差值 | 告警的定义就是意图与观测不符 |
| `instance list --status` 过滤 | 派生（`Phase()`） | 保持 CLI 词汇不变 |

### 4.7 并发与输入边界

**编排锁（`Server.topologyMu`）**

`state.WithInstances` 自身持数据库互斥锁，所以状态变更是原子的。但故障转移在事务提交之后还有两步在锁外：`persistGroupRoles`（把新角色写进各实例的磁盘配置文件）和 `syncSentinel`（把新 master 地址下发给 Sentinel）。

四条路径会并发触发故障转移：Sentinel 事件回调（`sentinel.go` 的 `/sentinel/event`）、watcher 协程、周期 reconcile、用户手动 `instance failover`。

缺锁的具体后果：连续两次转移 A→B、B→C 产生两个同步协程，各自读到不同的状态快照。若后者先把 Sentinel 的 MONITOR 指向 C、前者随后改回 B，Sentinel 就被指向了一个已经降级的实例。接着 `reconcileSentinelOnce` 发现 Sentinel 与 Server 认知不一致，而 Sentinel 是设计上的权威，于是把 Server 的 `CurrentMaster` 也改回 B —— 一次来回震荡，两边都错。磁盘配置文件同理：两个 `persistGroupRoles` 交错可能把过期的 `replicaof` 写进去，容器下次重启就按错的角色启动（即 §4.3.6 要消除的角色回退，换了个成因）。

锁必须覆盖"读快照 → 下发"整段而不只是下发动作，否则两个协程仍可能各自读到不同快照再错序下发。因此拆出 `*Locked` 变体：

| 函数 | 加锁 | 说明 |
|------|------|------|
| `handleSentinelFailover` | 取锁 | 全程持有，覆盖改状态 + 落盘 + 下发 |
| `syncSentinelMasters` | 取锁 | 给未持锁的调用方（建组/删组/`/sentinel/sync`） |
| `syncSentinelMastersLocked` | 要求已持锁 | 供 `handleSentinelFailover` 调用 |
| `persistGroupRoles` / `persistGroupRolesLocked` | 同上 | |
| `removeSentinelMaster` | 取锁 | 否则删组的 REMOVE 可能夹在另一次同步的 REMOVE+MONITOR 之间 |
| `reconcileRoleDrift` | 取锁 | 同样往磁盘写 `replicaof` |

Go 的 `Mutex` 不可重入，持锁路径调错变体会当场卡死整个 Server，`TestSyncSentinel_NoDeadlockFromFailoverPath` 守这一点。

**`syncSentinelNode` 的单组失败隔离**

早期实现在组循环里直接 `return err`：任意一组出问题，排在它后面的组这一轮完全不会被配置。最坏的一种是 REMOVE 已执行、MONITOR 失败时返回——该组在这个 Sentinel 节点上处于"已删除且未重建"的无监控状态，失去故障转移能力，同时后面的组也没被处理，一次局部故障扩散成整个节点的配置停滞。现在逐组独立处理，错误汇总后一并返回。

**节点增删改的原子性与引用约束**

`node add` / `node remove` / `node update` 此前都是 `ReadNode` → 检查 → `WriteNode`，检查与写入之间无互斥：两个同名的并发 `node add` 会双双通过存在性检查，后写的静默覆盖先写的；`node remove` 与 `node update` 并发时，update 可能用过期快照把已删除的节点写回来。三者已改用 `WithNode`（内部走 `store.WithServers`，在数据库互斥锁内完成读-改-写）。

`node remove` 还必须在同一个数据库事务里检查 `instances.server` 引用。只要仍有任何受管实例引用该节点（包括 stopped 实例）就返回 409；“先读实例、再删节点”仍会被并发 create 插入孤儿引用，因此不能拆成两个事务。

**`config_overrides` 的输入边界**

渲染出的配置覆盖项位于配置模板的**最末尾**（`config_template.go` 的 `{{ .ConfigOverrides }}`），而 Redis 配置文件是后出现的指令覆盖先出现的。这意味着这里是用户输入唯一能改变实例语义的入口，且改动在配置文件开头完全看不出来。

两类拦截，规则在 `pkg/redisconf` 由 Server 与 Agent 共用一份（分成两份清单迟早会漂移）：

- **值不得含换行**。`value = "100\nreplicaof 203.0.113.1 6379"` 会让实例去同步外部地址，而同步的第一步是清空本地数据集——一条指令抹掉整个实例。同时拒绝 `#`（注释掉行尾）、NUL 和空值。空格是合法的（`save 3600 1 300 100`），不能一并禁掉。
- **保留字不得覆盖**。`replicaof`/`slaveof`、`requirepass`/`masterauth`、`dir`/`dbfilename`/`appendfilename`、`port`/`bind`、`replica-announce-*`、`min-replicas-to-write`/`min-replicas-max-lag`、`rename-command`、`daemonize` 等由平台管理，覆盖它们会破坏复制拓扑、认证、备份路径或防脑裂保护。完整清单与逐项理由见 `pkg/redisconf/validate.go`。

校验放在 `configOverridesString` 内部（该函数因此返回 `error`），调用方想拿到渲染结果就必须先通过校验，没有"忘记校验"的可能。热更新路径（`CONFIG SET`）不经过配置文件渲染、也没有换行注入问题（`redisCmd` 用 argv 数组调用 podman），但同样做保留字校验——运行时把 `replicaof` 改掉的破坏力与写进配置文件一样。

渲染输出按 key 排序：`map` 迭代顺序随机会让同样的输入每次渲染出不同的文件，既让 diff 失去意义，也让"配置文件是否需要更新"无法靠比较内容判断。

**RESP 解析的资源上限**

`readRESP` 的长度字段来自网络，此前直接用于分配内存和递归。三种崩法都会带走整个 Server 进程（`watchSentinelNode` 跑在裸 goroutine 里没有 recover，栈溢出更是不可恢复的 fatal error）：

| 输入 | 后果 |
|------|------|
| `$9223372036854775807` | `make([]byte, n+2)` 的 `n+2` 溢出成负数，`make` panic |
| `*1000000000` | 一次要求预分配十亿个元素 |
| `*1\r\n` 无限嵌套 | 递归耗尽 goroutine 栈 |

现在限制 bulk 64MiB、数组 1M 元素、嵌套深度 8（`SENTINEL MASTERS` 的回复深度为 2）。数组刻意不按声称的 `count` 预分配容量——声称一百万却只发两个元素时预分配就白占内存。

**Token 比较**

Server / Agent / Proxy 三处 Bearer Token 校验改用 `subtle.ConstantTimeCompare`。字符串比较在首个不同字节处短路，耗时随匹配前缀长度变化。这条路径的可利用性很低（网络抖动远大于纳秒级差异），改动成本也极低，按纵深防御处理。

**`reconcileSentinelOnce` 的二次确认耗时**

检测到 Sentinel 与 Server 认知不一致时，不能立刻同步——`GET-MASTER-ADDR-BY-NAME` 在 Sentinel 的 failover 状态机走到 `reconf-slaves` 阶段就会返回新地址，但此时 Sentinel 还没走完收尾（详见 §4.3 的说明），立刻 REMOVE+MONITOR 会抹掉它刚建立的旧 master 追踪记录。所以要隔一段时间再确认地址仍是同一个新值。

早期实现是在组循环里就地 `time.Sleep(3s)`，耗时是 3 秒 × 不一致的组数。sentinel reconcile 的间隔是 10 秒，4 个组同时不一致就会让单轮超过间隔，后续轮次持续被挤压；组数再多就变成整个 reconcile 通道被拖垮，连带那些本来一致、只是排在后面的组也迟迟得不到处理。

二次确认的语义只要求两次观测之间有间隔，并不要求每组各睡一次。现在改为两趟：先收集全部不一致的组，再统一 sleep 一次，然后逐个确认。总耗时恒定 3 秒，与组数无关。

**Proxy 快照下发的延迟（长轮询）**

`/proxy/snapshot` 支持两个可选查询参数，不带时行为与原来完全一致：

| 参数 | 含义 |
|------|------|
| `known_version` | 调用方已持有的快照版本 |
| `wait` | 最长挂起时长，服务端上限 30s |

版本相同时请求挂起直到版本变化或超时，超时返回 `304 Not Modified`。收益有两条：故障转移传播到 Proxy 的延迟从"最多一个轮询周期"降到接近即时——在这段窗口里 Proxy 还把写流量发给已经挂掉的旧 master，写请求直接失败，缩短窗口就是缩短写不可用时间；另外快照里含 Redis 明文密码（Proxy 需要它连后端），原来每 2 秒完整传输一次，现在只在真正变化时传输。

成功鉴权、解析且版本变化的空快照同样是权威全量状态，表示最后一个组已删除或当前没有可服务 master。Proxy 必须应用它并关闭旧监听/后端；只有请求失败、超时或响应非法时才保持最后快照。若无条件忽略空快照，删除最后一个组后 Proxy 会永久向旧后端路由。

**关键设计：等的是代际计数器，不是重复重算快照。**

`store.DB` 维护一个 `generation` 原子计数器，每次拓扑写入成功后自增，`Generation()` 对外暴露。挂起期间每 100ms 检查这个内存计数器，只有它变了才重算快照并比对版本。

若改成每 100ms 重算快照来检测变化，要读数据库 + 算 SHA256，反而比原来 2 秒轮询一次更耗资源——那样"优化"就成了负优化。

计数器放在 store 层而不是各个 handler 里发通知，是因为 store 是所有状态写入的唯一收口点，9 个写方法都在同一个包内，改动时一眼能看全。非事务写入统一走 `execWrite`（写成功后自增），事务写入在 `Commit` 成功后自增。漏掉任何一处的后果是 Proxy 配置永久停在旧值——一个不报错、只静默错下去的故障，比多几次无用重算糟糕得多。`TestGeneration_EveryMutatingMethodBumps` 逐个方法验证，不抽样。

计数器的语义刻意做弱：变了只说明"有写入发生"，不保证代理配置真的变了（例如只更新备份时间戳）。所以变了之后仍要重算并比对内容哈希——弱信号 + 强比对，既不会漏也不会误报。审计日志写入刻意不自增：它不影响代理配置，而写入很频繁，自增会让每条日志都唤醒一次无谓的重算。

**取代际基线必须早于读状态。** 顺序反了会漏掉一次变更：落在"`buildProxySnapshot` 读完实例状态"和"取代际"之间的那次写入，它的代际已经被计入基线，而它的效果又没进这份快照，等待循环因此永远不会为它触发重算。这次变更要等满整个 `wait`（20s）才被下一次请求发现，比改造前 2 秒定期轮询还慢一个数量级——故障转移后 Proxy 会在这 20 秒里持续把写流量发给已经降级的旧 master。所以基线在 `buildProxySnapshot` 之前取：代价只是可能多醒一次、重算后发现版本没变，与"弱信号 + 强比对"的取向一致。

这个顺序是正确性的一部分，但错了既不报错也不崩，只在几毫秒窗口内成立，靠并发去撞是撞不到的。因此 `buildProxySnapshot` 里留了一个仅测试赋值的注入点 `proxySnapshotAfterStateReadHook`（生产环境恒为 nil），让 `TestProxySnapshot_DoesNotMissWriteRacingTheGenerationBaseline` 能把一次写入精确放进那个窗口——顺序反过来时该测试会等满 `wait` 并收到 304 而失败。

两道保护：

- **服务端超时上限（30s）**。意义不是省资源，而是保证这条路径永远会结束。任何"等待唤醒"的机制都有漏唤醒的可能，没有超时就会让 Proxy 配置永久停在旧值且不报错；有了上限，最坏情况退化成原来的定期轮询。
- **客户端最小间隔（`Poll.Interval`）**。长轮询模式下拿到"版本未变"后会立刻发起下一次请求，因为正常情况服务端已挂起了 `Wait`。但服务端若不认识这两个参数（版本不匹配的旧 Server，或中间代理吃掉了查询串），会立即返回相同版本的完整快照——没有下限就变成不停打 Server 的忙循环。客户端按实际耗时补足到 `Poll.Interval`，`TestPoll_DoesNotBusyLoopWhenServerIgnoresLongPollParams` 守这一点。

Proxy 侧 `http.Client` 刻意不设 `Timeout`（它会连正常挂起一起掐断），每次请求的超时由 `refresh` 用 context 按 `Wait + Timeout` 单独设定。`Poll.Timeout` 的含义因此变成"网络往返余量"，默认从 1s 调到 5s。首次拉取还没有版本可比，不进长轮询，避免启动时白等一个 `Wait`。

**Proxy 运行状态心跳**

Discovery 模式启动后，Proxy 每 10 秒向 `POST /api/v1/proxy/heartbeat` 上报一次。上报内容
只包含稳定 `proxy_id`（未配置时回退 hostname）、进程启动时间、最近应用的快照版本和时间、
当前 listener 数与快照内 backend 数。该请求与快照拉取复用 `internal_token`，通信方向始终为
Proxy → Server，避免 Server 主动访问 Proxy 带来的双向防火墙和循环依赖。

Server 按 `proxy_id` 覆盖内存中的最新观测，不把每 10 秒一次的心跳写入 SQLite。Dashboard
查询 `GET /proxy/status` 时依据 `last_seen` 动态计算 online / stale / offline；Server 重启后的
短暂空白明确显示为“等待 Proxy 上报”。业务流量数据不进入心跳，仍由 Prometheus 从
Proxy `/metrics` 采集。

## 5. Skills 设计



### 5.1 Skill 清单

| Skill 名称 | 触发场景 | 核心功能 |
|-------------|----------|----------|
| `redis-create` | "创建 Redis" | 资源分配 → 创建实例 → 健康检查 → 注册代理 → 更新状态 |
| `redis-delete` | "删除 Redis" | 停止实例 → 数据清理确认 → 删除容器 → 注销代理 → 释放资源 |
| `redis-config` | "修改 Redis 配置" | 配置校验 → 热更新 or 重启 → 验证生效 |
| `redis-scale` | "加从库 / 扩容" | 加从库：创建从库 → 同步 → 注册代理；扩容：调整内存/CPU |
| `redis-migrate` | "迁移 Redis" | 新从库 → 同步 → 提升 → 更新拓扑 → 清理旧实例 |
| `redis-failover` | "故障转移" / 自动触发 | 检测故障 → 提升从库 → 创建新从库 → 更新路由 |
| `redis-backup` | "备份 / 恢复 Redis" | 备份：BGSAVE -> 校验产物 -> 上传 S3 -> S3 轮转；恢复：从 S3 下载 -> 停止 -> 暂存并校验 -> 原子替换（原数据保留）-> 启动 |
| `redis-diagnose` | "Redis 有问题" | 采集 INFO/SLOWLOG/MEMORY → AI 分析 → 建议 |
| `redis-status` | "查看 Redis 状态" | 池状态 / 实例状态 / 主从同步状态 |
| `redis-proxy` | "代理管理" | 查看路由 / 更新配置 / 重载 Proxy |
| `redis-audit` | "操作记录" / "谁做了什么" | 查询审计日志，支持按时间/实例/操作级别过滤 |
| `redis-node` | "添加服务器" / "移除服务器" | 维护 `servers` 表，注册/移除/更新服务器信息，纯本地操作 |

### 5.2 Skill 与 Tool 映射

```
redis-create     → node_list, port_allocate, agent_exec(create), health_check,
                   state_update (Proxy 自动发现拓扑变更)

redis-delete     → agent_exec(stop), agent_exec(delete), state_update (Proxy 自动发现拓扑变更)

redis-config     → agent_exec(config), health_check, state_update

redis-scale      → node_list, agent_exec(create/replicate), health_check,
                   state_update (Proxy 自动发现拓扑变更)

redis-migrate    → node_list, agent_exec(create), agent_exec(promote),
                   agent_exec(replicate), agent_exec(stop/delete),
                   state_update (Proxy 自动发现拓扑变更)

redis-failover   → health_check, agent_exec(promote), agent_exec(create/replicate),
                   state_update (Proxy 自动发现拓扑变更)

redis-backup     -> agent_exec(backup), agent_exec(restore),
                   state_update

redis-diagnose   → agent_exec(status), metrics_collect, AI 分析

redis-status     → state_read, agent_exec(status), node_list

redis-audit      → audit_log_read

redis-node       → node_add, node_remove, node_update, node_list  (纯本地，不调用 Agent API)
```

---
