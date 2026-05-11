---
name: redis-delete
description: 删除 Redis/Kvrocks 实例
---

# redis-delete

停止并删除实例。

## 流程

1. 确认实例存在
   ```
   redis-pilot-cli instance status <name>
   ```

2. 如果是主库且有从库，提示用户确认（数据丢失风险）

3. 停止实例
   ```
   redis-pilot-cli instance stop <name>
   ```

4. 删除实例
   ```
   redis-pilot-cli instance delete <name>
   ```

5. 如果启用 Envoy，更新路由移除该实例

6. 如果是主从组的最后一个实例，从 Sentinel 移除监控

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |

## 注意事项

- 删除主库前确认是否需要先迁移或提升从库
- 删除操作不可逆，审计级别为 critical
