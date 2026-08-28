---
name: redis-scale
description: 扩容 Redis 实例（加从库或提升配置）
---

# redis-scale

加从库或提升实例配置。提升配置有两种方式：**原地变配（resize）**和**替换式升级**。

## 加从库流程

1. 创建从库（Server 自动选择有资源的服务器）
   ```
   redis-pilot-cli instance create <replica-name> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <master-name>
   ```

2. 等待同步完成
   ```
   redis-pilot-cli instance metrics <replica-name>
   ```
   确认 INFO replication 中 master_link_status=up，master_sync_in_progress=0

3. 确认控制面快照已包含新从库。Proxy 不通过 CLI 手动改路由；Proxy 通过长轮询 Server snapshot 自动发现拓扑变更。
   ```
   redis-pilot-cli instance status <replica-name>
   ```

## 提升配置：优先用原地变配（resize，不停机）

内存和 CPU 支持在同一节点上原地变配，不停机、不重建容器：

```
redis-pilot-cli instance resize <name> --memory <new-memory>   # 内存
redis-pilot-cli instance resize <name> --cpus <n>              # CPU
redis-pilot-cli instance resize <name> --disk <new-disk>       # 磁盘规划配额
redis-pilot-cli instance resize <name> --memory 8Gi --cpus 4   # 一起改
```

三个维度都可选，只改传入的那个；用 flag 是否出现（Changed）判定，不指定即保持不变。

各维度语义与生效方式：

| 维度 | 底层动作 | 生效 | 持久性 |
|------|---------|------|--------|
| memory | `podman update --memory/--memory-swap` + 引擎 `maxmemory`/`max-db-size` | 不停机立即 | maxmemory 写入配置文件持久化；cgroup 变更在容器重建前有效 |
| cpus | `podman update --cpus` | 不停机立即 | 重建前有效 |
| disk | 仅更新控制面记账 | 立即 | 持久（纯规划元数据，不下发容器） |

**Server 侧护栏（自动执行，无需手动检查）**：
- **升配容量校验**：目标节点剩余资源不足以容纳新规格时返回 409（校验时排除实例自身当前占用，只看增量）。
- **降配 OOM 护栏**：内存降到低于实例实时 `used_memory` 的 110% 时返回 400，避免 cgroup 收缩触发 OOM Kill。
- **仅改磁盘**：不触碰容器，只更新控制面记录。

**已知限制**：`podman update` 调整的是运行中容器的 cgroup，若容器在下次由本系统重建前被**非受控事件**重启（崩溃自动重启策略 `--restart on-failure`、宿主机重启），cgroup 会回退到创建时的值。内存的 `maxmemory` 因写入了配置文件不受影响；容器内存/CPU 上限的回退会在下次受控重建时用控制面保存的新规格纠正。对可靠性要求极高的变配，可在 resize 后安排一次受控重建（如借助下方替换式升级流程换到新容器）。

## 提升配置：替换式升级（跨节点 / 需要全新容器时）

当需要**换到其它节点**、**规格族跨越 podman 在线变配能力**、或要求变配后立即落到全新容器时，用替换式升级：通过创建高配从库，确保其为唯一候选后 promote 为主库来实现扩容。

**核心原则**：
- failover 前确保只有一个 replica，使 Sentinel 选举结果确定
- 全程保持至少一个 replica 存活，避免 min-replicas-to-write 阻断写入

1. 创建高配 replica
   ```
   redis-pilot-cli instance create <new-name> \
     --engine <engine> \
     --memory <new-memory> \
     --replica-of <master-name>
   ```

2. 等待复制追平
   ```
   redis-pilot-cli instance metrics <new-name>
   ```
   确认 INFO replication 中 master_link_status=up，master_sync_in_progress=0，master_repl_offset 与主库接近

3. 删除旧 replica（此时新 replica 已就位，master 始终有从库不会拒绝写入）
   ```
   redis-pilot-cli instance delete <old-replica> --clean-data
   ```

4. 触发故障转移（此时只有一个 replica，Sentinel 必然选中它）
   ```
   redis-pilot-cli instance failover <group-内任意实例名>
   ```

5. 删除旧主库（已降为 replica）
   ```
   redis-pilot-cli instance delete <old-master> --clean-data
   ```

6. 补齐从库（恢复一主 N 从架构）
   ```
   redis-pilot-cli instance create <new-replica> \
     --engine <engine> \
     --memory <new-memory> \
     --replica-of <new-master-name>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| master | 是 | 主库名称 |
| action | 是 | add-replica 或 upgrade |
| memory | upgrade 时必填 | 新内存规格 |

## 注意事项

- **提升配置优先用 `instance resize`（原地、不停机）**，内存/CPU 直接在同节点变配；容量与 OOM 护栏由 Server 自动执行。
- 仅在需要换节点、跨规格族或要求全新容器时才用替换式升级（建高配从库 → 同步 → 删旧从库 → promote → 清理旧主 → 补从库）。
- 加从库时优先选择与主库不同的服务器（跨机容灾）
- 缩容内存/CPU 用 `instance resize`（降配）；缩容副本数（删从库）用 redis-delete
- 磁盘只能通过 `instance resize --disk` 调整规划配额（纯元数据，不强制、可增可减）
- Redis/Proxy 端口由 Server 自动分配和发布，不从 CLI 传 `--port`
- 替换式升级全程保持至少一个 replica 存活，确保 min-replicas-to-write 不阻断写入
- 多从场景：先建一个高配 replica 并同步追平，再逐个删除旧 replica，最后 failover
