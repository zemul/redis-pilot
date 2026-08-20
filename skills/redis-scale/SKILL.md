---
name: redis-scale
description: 扩容 Redis 实例（加从库或提升配置）
---

# redis-scale

加从库或通过替换式升级提升实例配置。

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

## 提升配置流程（替换式扩容）

通过创建高配从库，确保其为唯一候选后 promote 为主库来实现扩容。

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

- 加从库时优先选择与主库不同的服务器（跨机容灾）
- 缩容（删从库）使用 redis-delete
- Redis/Proxy 端口由 Server 自动分配和发布，不从 CLI 传 `--port`
- 提升配置本质是替换式升级：建高配从库 → 同步 → 删旧从库 → promote → 清理旧主 → 补从库
- 全程保持至少一个 replica 存活，确保 min-replicas-to-write 不阻断写入
- 多从场景：先建一个高配 replica 并同步追平，再逐个删除旧 replica，最后 failover
