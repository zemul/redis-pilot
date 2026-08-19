---
name: redis-migrate
description: 跨服务器迁移 Redis/Kvrocks 实例
---

# redis-migrate

将实例从一台服务器迁移到另一台。

## 流程

1. 在目标服务器创建从库
   ```
   redis-pilot-cli instance create <name>-new \
     --node <target-server> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <source-instance>
   ```

2. 等待全量同步完成
   ```
   redis-pilot-cli instance metrics <name>-new
   ```
   确认 INFO replication 中 master_link_status=up，master_sync_in_progress=0，master_repl_offset 与主库接近

3. 触发故障转移（参数为组内任意实例名，仅用于定位 group；Sentinel 自行选举新主库）
   ```
   redis-pilot-cli instance failover <group-内任意实例名>
   ```

4. 停止并删除旧主库
   ```
   redis-pilot-cli instance delete <old-name>
   ```

5. 确认控制面快照已反映新拓扑。Proxy 不通过 CLI 手动改路由；Proxy 通过长轮询 Server snapshot 自动发现拓扑变更。
   ```
   redis-pilot-cli instance list --group <group>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 要迁移的实例名 |
| target | 是 | 目标服务器 |

## 注意事项

- 迁移期间实例持有锁，其他操作会被阻塞
- 同步完成前不要执行 promote
- Redis/Proxy 端口由 Server 管理；迁移流程不要传 `--port`，也不要生成或 reload Proxy 配置
- 审计级别为 critical
