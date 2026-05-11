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
     --port <port> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <source-instance>
   ```

2. 等待全量同步完成
   ```
   redis-pilot-cli instance status <name>-new
   ```
   确认 replication 状态为 connected，master_sync_in_progress=0

3. 提升新从库为主库
   ```
   redis-pilot-cli instance promote <name>-new
   ```

4. 更新其他从库的复制目标
   ```
   redis-pilot-cli instance replicate <replica-name> --replica-of <name>-new
   ```

5. 停止并删除旧主库
   ```
   redis-pilot-cli instance stop <old-name>
   redis-pilot-cli instance delete <old-name>
   ```

6. 更新 Envoy 路由
   ```
   redis-pilot-cli envoy route-update --group <group> --master <new-addr>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 要迁移的实例名 |
| target | 是 | 目标服务器 |

## 注意事项

- 迁移期间实例持有锁，其他操作会被阻塞
- 同步完成前不要执行 promote
- 审计级别为 critical
