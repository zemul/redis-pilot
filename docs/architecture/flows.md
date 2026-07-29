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
  │   → 分配 Envoy 端口 16381
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
  ├─6. state_update(instances, "cache-1", status=running, envoy.auto_port=16381)
  │   → xDS 自动检测到 snapshot 变化并推送新配置到 Envoy
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
  │   → 主库端口 6379，Envoy 端口 16379 (读写) + 16380 (只读)
  │   → 从库端口 6379
  │
  ├─3. state_update(instances, "order-master", status=creating)
  │   state_update(instances, "order-replica", status=creating)
  │
  ├─4. agent_exec(server-a, "instance/create", {
  │       name: "order-master",
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
  │       name: "order-replica",
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
  ├─8. state_update(instances, envoy.auto_port=16379, envoy.master_port=16380)
  │   → xDS 自动检测到 snapshot 变化并推送新配置到 Envoy
  │
  ├─9. state_update(instances, "order-master", status=running)
  │   state_update(instances, "order-replica", status=running)
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
用户: "把 order-replica 提升为主库" / "手动切换 order 组的主库"

CLI: redis-pilot-cli instance failover order-replica

Server: instanceFailover(name="order-replica")
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

`instance replicate`（手动指定复制目标）在 Sentinel 托管的组上会被拒绝（409），因为它与 Sentinel 的自动拓扑管理直接冲突；未托管的组仍可使用。

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
       c. 更新 Envoy 路由并重新生成/落盘 Envoy 配置
       d. 写入 topology.failover 审计日志
       e. 释放编排锁

     `instances` 表更新要求：
       - 实例组：
         current_master: <新主库实例名>
         topology_status: healthy 或 degraded
         envoy: 保持原实例组 master_port / auto_port 不变
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

     Envoy 更新要求：
       - 业务读写端口保持不变，避免业务侧修改连接地址
       - 后端 endpoints 必须包含新主库和所有健康从库
       - RO 只读集群后端更新为当前所有健康从库
       - 生成的 Envoy 配置必须落盘，保证 Envoy/Server 重启后拓扑不回退

     审计要求：
       - action: topology.failover
       - level: critical
       - target 记录实例组、旧主库、新主库、故障服务器、新主库服务器
       - params 记录 Sentinel 事件和 Envoy 端口
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
> - 没有锁保护时，多个编排流程并发执行会造成 Envoy 路由混乱、状态文件冲突和重复告警
> - 编排锁确保同一实例组同一时刻只有一个故障转移编排流程在执行

#### 4.3.4 Sentinel 唯一权威模型下的四个关键实现约束

以下四点是实机验证（反复触发 failover + 重启旧主库容器）与代码审计踩到的真实竞态/缺口，均已修复，记录约束原因以避免回归：

**a. 同步 Sentinel 配置时禁止无条件 `REMOVE`+`MONITOR`**

Redis Sentinel 官方机制：failover 完成后，旧主库会被 Sentinel"虚拟加入"新主库的从库列表（即使它当前不可达），这样它重新上线时会被自动探测并降级为从库——这是 Sentinel 自愈脑裂的核心依据。

`SENTINEL REMOVE <group>` 会清空 Sentinel 对该组的**全部**历史状态，包括这条刚建立的追踪记录。因此 Server 同步 Sentinel 配置（`syncSentinelNode`）时，只有当 Sentinel 当前认知的主库地址与期望地址**不一致**时才执行 `REMOVE`+`MONITOR`；地址已经一致时只允许用 `SENTINEL SET` 调整不影响拓扑追踪状态的参数（`down-after-milliseconds`、`failover-timeout` 等）。

**b. 主库创建时配置文件也必须写 `masterauth`，不能只在从库分支写**

在 Sentinel 唯一权威模型下，任何当前是主库的实例都可能在未来被 Sentinel 自动降级为从库，且 Sentinel 只会对它发 `REPLICAOF`，不会代为设置认证信息。如果 `masterauth` 只写在"创建时指定了 `--replica-of`"的分支里，一个从创建起就是主库的实例，配置文件永远没有 `masterauth`，一旦被降级会因为 `NOAUTH` 无法与新主库建立复制连接（`master_link_status` 长期 `down`）。

因此配置模板中 `masterauth` 与 `requirepass` 同级：只要设置了密码就总是写入，不依赖当前角色。

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
| Server | 容器生命周期、配置、状态记账、Envoy 路由 | `pilot.db` + Agent 上报 | 知道 Sentinel |
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

切流量无损、可逆、秒级（Sentinel failover + Envoy 路由更新）；重启有损、不可逆、慢。Agent 用错了工具。

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

`inst.ReplicaOf` 在 `pilot.db` 里存的是**实例名**（`instance_handler.go:343`、`:1002`），而配置模板期待 `ip:port`（`config_template.go:157` 把冒号替换成空格拼进 `replicaof` 指令）。`instanceConfig` 此前直接把 `inst.ReplicaOf` 传给 Agent，导致对 replica 执行 `instance config --restart` 时配置文件被写成 `replicaof order-master` —— 缺少端口，Redis 启动时报 `wrong number of arguments`，而容器已经被 Stop，**实例起不来**。

现在统一经 `resolveReplicaOfAddr` 解析。它与用于校验用户输入的 `resolveReplicaOf` 的区别是**不断言目标必须是 master**：角色落盘发生在故障转移前后，组内角色可能正处于中间状态，此时需要如实写出 Server 的记录，而不是校验这份记录是否合理。

**保留的已知窗口**

配置文件仍然是"重启后读到的角色"的来源，所以从"Sentinel 改变角色"到"Server 把新角色写进配置文件"之间存在一个窗口。窗口内如果容器恰好重启，角色仍会回退一次，随后由下一轮 reconcile（30 秒）纠正。彻底消除需要让配置文件完全不表达角色，但那会引入"重启瞬间出现一个独立 master"的新问题，且 kvrocks 模板没有 `min-replicas-to-write` 这类写保护（`config_template.go:79` 只在 Redis 模板内），因此没有采纳。

### 4.4 实例迁移

```
用户: "把 order-master 从 server-a 迁移到 server-c"

Skill: redis-migrate
  │
  ├─1. 在目标服务器创建从库
  │   agent_exec(server-c, "instance/create", {
  │       name: "order-master-new",
  │       replica_of: "10.0.1.10:6379",
  │       ...
  │   })
  │
  ├─2. 等待全量同步完成
  │   health_check → INFO replication: master_sync_in_progress=0
  │
  ├─3. 提升新从库为主库
  │   redis-pilot-cli instance failover order-master-new
  │   → 若该组由 Sentinel 托管：Server 发送 SENTINEL FAILOVER，等待并同步结果
  │   → 若未托管：Server 直接向 Agent 下发 REPLICAOF NO ONE（fallback）
  │
  ├─4. 更新旧从库的复制目标
  │   若该组由 Sentinel 托管：Sentinel 会在下一轮探测中自动将旧从库/旧主库
  │   重新指向新主库，无需手动调用；仅在未托管的组才需要手动调用：
  │   agent_exec(server-b, "instance/replicate", {
  │       name: "order-replica",
  │       replica_of: "10.0.1.12:6379"   // 新主库
  │   })
  │
  ├─5. 停止并删除旧主库
  │   agent_exec(server-a, "instance/stop", { name: "order-master" })
  │   agent_exec(server-a, "instance/delete", { name: "order-master" })
  │
  ├─6. 更新状态（xDS 自动推送新路由到 Envoy）
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
  ├─1. 确定备份源
  │   → 优先从库: order-replica (server-b)
  │   → 无从库时从主库备份
  │
  ├─2. agent_exec(server-b, "instance/backup", {
  │       name: "order-replica",
  │   })
  │   → Agent: BGSAVE → 等待完成（失败/超时即报错）→ 校验产物 → 复制 dump.rdb 到 backup/ 目录
  │
  ├─3. 轮转清理
  │   agent_exec(server-b, "instance/backups/cleanup", {
  │       name: "order-replica",
  │       retention: 7
  │   })
  │
  └─4. state_update(instances, order-replica, last_backup=now)
```

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

新 master 的 `observed_status` 则会被写成 `running`：Sentinel 能把它选为新 master，说明 Sentinel 探测到它健康，这是有依据的观测。不写的话，Envoy 要等下一轮 reconcile 才会把写流量指向新 master（`proxy_group` 只挂 `IsServing()` 的实例），白丢最多 30 秒写入。

**c. "Agent 不可达"不得写进实例的观测状态**

Agent 不可达是对 **Agent** 的观测，不是对实例的观测。Agent 重启或网络抖动时，它名下的 Redis 实例通常完好无损。若把 `unreachable` 写进实例的 `observed_status`，`IsServing()` 会变成 false，`proxy_group` 就会把这些健康实例从 Envoy 端点里摘掉——一次 Agent 重启造成一次业务中断。

所以 `runReconcile` 的三处 Agent 不可达分支只告警、不写观测字段。回归测试：`TestReconcile_AgentUnreachableDoesNotOverwriteObservedStatus`。

**各读取点该读哪个字段**

| 读取点 | 读哪个 | 为什么 |
|--------|--------|--------|
| `proxy_group` Envoy 端点 | 观测（`IsServing()`） | 不能把流量发给一个"应该在跑但已经挂了"的实例 |
| `backup_scheduler` 排备份 | 观测（`IsServing()`） | 对挂掉的实例发 BGSAVE 只会失败 |
| `scheduler` 容量计算 | 观测（`missing`） | 容器已消失的实例不该继续占配额 |
| `RecalculateGroupTopology` | 观测 | 组健康度是事实结论，用意图判断只能得出"配置上应该是健康的" |
| `runReconcile` 动作判定 | 两者的差值 | 告警的定义就是意图与观测不符 |
| `instance list --status` 过滤 | 派生（`Phase()`） | 保持 CLI 词汇不变 |

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
| `redis-backup` | "备份 / 恢复 Redis" | 备份：BGSAVE → 校验产物 → 复制 → 轮转；恢复：停止 → 暂存并校验 → 原子替换（原数据保留）→ 启动 |
| `redis-diagnose` | "Redis 有问题" | 采集 INFO/SLOWLOG/MEMORY → AI 分析 → 建议 |
| `redis-status` | "查看 Redis 状态" | 池状态 / 实例状态 / 主从同步状态 |
| `redis-envoy` | "代理管理" | 查看路由 / 更新配置 / 重载 Envoy |
| `redis-inventory` | "资源清单" / "端口对应什么" | 从 `instances`/`instance_groups` 表 + `servers` 表派生端口/集群/用途映射表，支持按端口/服务器/引擎查询 |
| `redis-audit` | "操作记录" / "谁做了什么" | 查询审计日志，支持按时间/实例/操作级别过滤 |
| `redis-node` | "添加服务器" / "移除服务器" | 维护 `servers` 表，注册/移除/更新服务器信息，纯本地操作 |

### 5.2 Skill 与 Tool 映射

```
redis-create     → node_list, port_allocate, agent_exec(create), health_check,
                   state_update (xDS 自动推送)

redis-delete     → agent_exec(stop), agent_exec(delete), state_update (xDS 自动推送)

redis-config     → agent_exec(config), health_check, state_update

redis-scale      → node_list, agent_exec(create/replicate), health_check,
                   state_update (xDS 自动推送)

redis-migrate    → node_list, agent_exec(create), agent_exec(promote),
                   agent_exec(replicate), agent_exec(stop/delete),
                   state_update (xDS 自动推送)

redis-failover   → health_check, agent_exec(promote), agent_exec(create/replicate),
                   state_update (xDS 自动推送)

redis-backup     → agent_exec(backup), agent_exec(restore), agent_exec(cleanup),
                   state_update

redis-diagnose   → agent_exec(status), metrics_collect, AI 分析

redis-status     → state_read, agent_exec(status), node_list

redis-inventory  → state_read, node_list, audit_log_read(只读)

redis-audit      → audit_log_read

redis-node       → node_add, node_remove, node_update, node_list  (纯本地，不调用 Agent API)
```

---

